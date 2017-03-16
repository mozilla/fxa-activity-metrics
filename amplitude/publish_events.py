#
# Script to prepare metrics data to send to third-party vendors,
# for our proof-of-concept vendor evaluation.
#
# This script merges, samples and transforms the FxA flow and
# activity events into a single event stream that can be sent
# to each vendor.  The important things it does are:
#
#  * downsampling of events based on flowid and uid
#  * renaming events to map then to the new taxonomy,
#    and in some cases inferring appropriate names
#    based on previous events.
#  * anonymizing device ids, which are not anonmyized
#    in our raw metrics stream (see Bug 1345008)
#

import os
import sys
import json
import time
import hmac
import Queue
import hashlib
import datetime
import tempfile
import threading
import contextlib

import requests

import boto.s3
import boto.provider

# Load config from disk,
# and pull in credentials from the environment.

with open("config.json") as f:
    CONFIG = json.loads(f.read())

if "aws_access_key_id" not in CONFIG:
    p = boto.provider.Provider("aws")
    CONFIG["aws_access_key_id"] = p.get_access_key()
    CONFIG["aws_secret_access_key"] = p.get_secret_key()


EVENTS_BUCKET = "net-mozaws-prod-us-west-2-pipeline-analysis"
EVENTS_PREFIX = "fxa-vendor-export/data/"


def process_events(api_key, from_date, to_date):
    s3conn = boto.s3.connect_to_region('us-east-1').get_bucket(EVENTS_BUCKET)
    with fetch_events_in_background(s3conn, from_date, to_date) as event_files:
        with publish_event_batches_in_background(api_key) as publisher:
            for (day, filename, fp) in event_files:
                printit("  ... pushing events for", filename)
                for i, event in enumerate(read_events_from_file(fp)):
                    publisher.push(event)
                    if i % 10000 == 9999:
                        printit("  ... pushed", i + 1, "events for", filename)
            printit("  ... finalizing uploads")


class fetch_events_in_background(object):

    def __init__(self, s3conn, from_date, to_date):
        self.s3conn = s3conn
        self.from_date = from_date
        self.to_date = to_date
        self._queue = Queue.Queue(maxsize=2)
        self._exiting = False
        self._worker_thread = threading.Thread(target=self._worker_method)
        self._worker_error = None
        self._worker_thread.start()

    def _worker_method(self):
        try:
            files_by_date = find_available_event_files_by_date(self.s3conn)
            for day in dates_in_range(self.from_date, self.to_date):
                if self._exiting:
                    break
                # Stream the events into temporary files.
                for filename in files_by_date[day]:
                    printit("  ... fetching", filename)
                    tf = tempfile.TemporaryFile()
                    k = self.s3conn.get_key(EVENTS_PREFIX + filename)
                    k.get_contents_to_file(tf)
                    tf.seek(0)
                    if self._exiting:
                        break
                    self._queue.put((day, filename, tf))
        except Exception, e:
            printit("FETCH WORKER ERROR:", e)
            self._worker_error = e
        finally:
            self._queue.put((None, None, None))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            printit("Exiting with error", exc_type, exc_val)
        self._exiting = True
        if not self._queue.empty():
            (_, _, tf) = self._queue.get()
            while tf is not None:
                tf.close()
                (_, _, tf) = self._queue.get()
        self._worker_thread.join()

    def __iter__(self):
        while True:
            if self._worker_error is not None:
                raise self._worker_error
            (day, filename, fp) = self._queue.get()
            if fp is None:
                break
            yield (day, filename, fp)


class publish_event_batches_in_background(object):

    def __init__(self, api_key):
        self.api_key = api_key
        self._next_batch = []
        self._queue = Queue.Queue(maxsize=30)
        self._force_exit = False
        # We run 5 worker threads for the uploads, each capped to 200 events/second.
        # That will keep us under the total limit of 1000 events/second set by amplitude.
        # My initial tests show each thread can upload around 150 to 200 events per second.
        self._worker_error = None
        self._num_threads = 5
        self._max_per_second = 1000 / self._num_threads
        self._worker_threads = []
        for _ in xrange(5):
            self._worker_threads.append(threading.Thread(target=self._worker_method))
        for t in self._worker_threads:
            t.start()

    def push(self, event):
        if self._worker_error is not None:
            for t in self._worker_threads:
                self._queue.put(None)
            raise self._worker_error
        self._next_batch.append(event)
        # Amplitude docs recommend no more than 10 events per batch upload.
        if len(self._next_batch) >= 10:
            self._queue.put(self._next_batch)
            self._next_batch = []

    def _worker_method(self):
        try:
            session = requests.Session()
            this_second = int(time.time())
            num_published = 0
            while not self._force_exit and self._worker_error is None:
                events = self._queue.get()
                if events is None:
                    break
                # Avoid sending too many events per second.
                now = time.time()
                delta = now - this_second
                if num_published + len(events) > self._max_per_second:
                    while delta < 1:
                        time.sleep(1 - delta)
                        now = time.time()
                        delta = now - this_second
                if delta >= 1:
                    this_second = int(now)
                    num_published = 0
                # Try to recover from transient errors,
                # using exponential backoff and hoping for the best...
                for retry_count in xrange(10):
                    try:
                        r = session.post("https://api.amplitude.com/httpapi", data={
                            "api_key": self.api_key,
                            "event": json.dumps(events),
                        })
                        r.raise_for_status()
                        break
                    except Exception as e:
                        retry_after = 2 ** retry_count
                        printit(" ... got '", e, "', retrying after", retry_after, "seconds...")
                        time.sleep(retry_after)
                else:
                    printit(" ... REPEATED REQUEST FAILURES, BAILING OUT !")
                    raise RuntimeError("repeated request failures, bailing out")
                num_published += len(events)
        except Exception, e:
            printit("PUBLISH WORKER ERROR:", e)
            self._worker_error = e
        finally:
            # We must empty the queue on exit, to unblock main thread.
            while events is not None:
                events = self._queue.get()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            printit("Exiting with error", exc_type, exc_val)
            self._force_exit = True
        elif len(self._next_batch) > 0:
            self._queue.put(self._next_batch)
            self._next_batch = []
        for t in self._worker_threads:
            self._queue.put(None)
        for t in self._worker_threads:
            t.join()


def dates_in_range(start_date, end_date):
    one_day = datetime.timedelta(days=1)
    end = parse_date(end_date)
    date = parse_date(start_date)
    while date <= end:
        yield "{:0>4}-{:0>2}-{:0>2}".format(date.year, date.month, date.day)
        date += one_day


def parse_date(when):
    year, month, day = map(int, when.split("-"))
    return datetime.date(year, month, day)


def read_events_from_file(fp):
    for ln in fp:
        ln = ln.strip()
        try:
            columns = ln.split(",")
            event = {}
            event["event_properties"] = event_properties = {}
            event["user_properties"] = user_properties = {}
            event["time"] = int(columns[0]) * 1000
            event["event_type"] = columns[1]
            if columns[2]:
                event["user_id"] = columns[2]
                user_properties["fxa_uid"] = columns[2]
            if columns[3]:
                event["device_id"] = columns[3]
            if columns[4]:
                event["session_id"] = int(columns[4]) * 1000
            if columns[5]:
                event["event_id"] = int(columns[5])
            if columns[6]:
                event_properties["service"] = columns[6]
            if columns[7]:
                event["platform"] = columns[7]
                event_properties["ua_browser"] = columns[7]
                user_properties["ua_browser"] = columns[7]
            if columns[8]:
                event_properties["ua_version"] = columns[8]
                user_properties["ua_version"] = columns[8]
            if columns[9]:
                event["os_name"] = columns[9]
                event_properties["ua_os"] = columns[9]
                user_properties["ua_os"] = columns[9]
            if columns[10]:
                event_properties["entrypoint"] = columns[10]
            if columns[11]:
                event_properties["utm_campaign"] = columns[11]
                user_properties["utm_campaign"] = columns[11]
            if columns[12]:
                event_properties["utm_content"] = columns[12]
                user_properties["utm_content"] = columns[12]
            if columns[13]:
                event_properties["utm_medium"] = columns[13]
                user_properties["utm_medium"] = columns[13]
            if columns[14]:
                event_properties["utm_source"] = columns[14]
                user_properties["utm_source"] = columns[14]
            if columns[14]:
                event_properties["utm_term"] = columns[14]
                user_properties["utm_term"] = columns[14]
            # For deduplication, since we're likely to send events
            # multiple times while testing.
            event["insert_id"] = hashlib.sha1(ln).hexdigest()
            yield event
        except (ValueError, IndexError) as e:
            printit("MALFORED LINE?", e, repr(ln))


def find_available_event_files_by_date(b):
    files_by_date = {}
    for key in b.list(prefix=EVENTS_PREFIX):
        filename = os.path.basename(key.name)
        day = "-".join(filename.split("-")[1:4])
        files_by_date.setdefault(day, []).append(filename)
    return files_by_date



def printit(*args):
    sys.stdout.write(" ".join(map(str, args)))
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    process_events(*sys.argv[1:])
