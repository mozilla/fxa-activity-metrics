#
# Script to import "activity event" metrics from S3 into redshift.
#

import os
import json
import time
import datetime
import tempfile
import urlparse

import postgres

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

DB = "postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}".format(**CONFIG)

# Event data files are named like "events-2016-02-15.csv"
# and contain events for the specified date.
# Unfortunately some of the data files have missing fields :-(
# We work around this by downloading the file, fixing it up,
# and uploading the fixed copy to a new location.

EVENTS_BUCKET = "net-mozaws-prod-us-west-2-pipeline-analysis"
EVENTS_PREFIX = "fxa-retention/data/"
EVENTS_PREFIX_FIXED = "fxa-retention/data-rfkelly/"
EVENTS_FILE_URL = "s3://" + EVENTS_BUCKET + "/" + EVENTS_PREFIX + "events-{day}.csv"
EVENTS_FILE_FIXED_URL = "s3://" + EVENTS_BUCKET + "/" + EVENTS_PREFIX_FIXED + "events-{day}.fixed.csv"

# We import each into a temporary table and then
# INSERT them into the activity_events table

Q_DROP_CSV_TABLE = "DROP TABLE IF EXISTS temporary_raw_activity_data;"

Q_CREATE_CSV_TABLE = """
    CREATE TABLE IF NOT EXISTS temporary_raw_activity_data (
      timestamp BIGINT NOT NULL SORTKEY,
      ua_browser VARCHAR(40),
      ua_version VARCHAR(40),
      ua_os VARCHAR(40),
      uid VARCHAR(64) NOT NULL DISTKEY,
      type VARCHAR(30) NOT NULL,
      service VARCHAR(40),
      device_id VARCHAR(32)
    );
"""

Q_CREATE_EVENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS activity_events (
      timestamp TIMESTAMP NOT NULL SORTKEY,
      uid VARCHAR(64) NOT NULL DISTKEY,
      type VARCHAR(30) NOT NULL,
      device_id VARCHAR(32),
      service VARCHAR(30),
      ua_browser VARCHAR(30),
      ua_version VARCHAR(30),
      ua_os VARCHAR(30)
    );
"""

Q_CHECK_FOR_DAY = """
    SELECT timestamp FROM activity_events
    WHERE timestamp::DATE = '{day}'::DATE
    LIMIT 1;
"""

Q_CLEAR_DAY = """
    DELETE FROM activity_events
    WHERE timestamp::DATE = '{day}'::DATE;
"""

Q_COPY_CSV = """
    COPY temporary_raw_activity_data (
      timestamp,
      ua_browser,
      ua_version,
      ua_os,
      uid,
      type,
      service,
      device_id
    )
    FROM '{s3path}'
    CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
    FORMAT AS CSV;
"""

Q_INSERT_EVENTS = """
    INSERT INTO activity_events (
      timestamp,
      uid,
      type,
      device_id,
      service,
      ua_browser,
      ua_version,
      ua_os
    )
    SELECT
      'epoch'::TIMESTAMP + timestamp * '1 second'::INTERVAL,
      uid,
      type,
      device_id,
      service,
      ua_browser,
      ua_version,
      ua_os
    FROM temporary_raw_activity_data;
"""

def import_events(force_reload=False):
    b = boto.s3.connect_to_region('us-east-1').get_bucket(EVENTS_BUCKET)
    db = postgres.Postgres(DB)
    db.run(Q_DROP_CSV_TABLE)
    db.run(Q_CREATE_EVENTS_TABLE)
    days = []
    days_to_load = []
    # Find all the days available for loading.
    for key in b.list(prefix=EVENTS_PREFIX):
        filename = os.path.basename(key.name)
        day = "-".join(filename[:-4].split("-")[1:])
        days.append(day)
        if force_reload:
            days_to_load.append(day)
        else:
            if not db.one(Q_CHECK_FOR_DAY.format(day=day)):
                days_to_load.append(day)
    days_to_load.sort(reverse=True)
    print "LOADING {} DAYS OF DATA".format(len(days_to_load))
    db.run("BEGIN TRANSACTION")
    try:
        # Load data for each day direct from s3,
        # fixing it up if there's an error with the load.
        for day in days_to_load:
            print "LOADING", day
            # Create the temporary table
            db.run(Q_CREATE_CSV_TABLE)
            # Clear any existing data for the day, to avoid duplicates
            db.run(Q_CLEAR_DAY.format(day=day))
            s3path = EVENTS_FILE_URL.format(day=day)
            # Copy data from s3 into redshift
            try:
                # Attempt #1: original data
                db.run(Q_COPY_CSV.format(s3path=s3path, **CONFIG))
            except Exception:
                print "FAILED", day, "TRYING FIXED DATA"
                s3path = EVENTS_FILE_FIXED_URL.format(day=day)
                try:
                    # Attempt #2: previously-fixed data
                    db.run(Q_COPY_CSV.format(s3path=s3path, **CONFIG))
                except Exception:
                    print "FAILED, FIXING DATA", day
                    # Attempt #3: fix the data first
                    fixup_event_data(b, day)
                    db.run(Q_COPY_CSV.format(s3path=s3path, **CONFIG))
            # Populate the activity_events table
            db.run(Q_INSERT_EVENTS)
            # Print the timestamps for sanity-checking
            print "  MIN TIMESTAMP", db.one("SELECT MIN(timestamp) FROM temporary_raw_activity_data")
            print "  MAX TIMESTAMP", db.one("SELECT MAX(timestamp) FROM temporary_raw_activity_data")
            # Drop the temporary table
            db.run(Q_DROP_CSV_TABLE)
    except:
        db.run("ROLLBACK TRANSACTION")
        raise
    else:
        db.run("COMMIT TRANSACTION")

def fixup_event_data(b, day):
    """Download a data file, remove invalid lines, and re-upload"""
    orig_key = b.get_key(urlparse.urlparse(EVENTS_FILE_URL.format(day=day)).path[1:])
    assert orig_key is not None
    fixed_key = b.new_key(urlparse.urlparse(EVENTS_FILE_FIXED_URL.format(day=day)).path[1:])
    with tempfile.TemporaryFile() as tmp_orig:
        orig_key.get_contents_to_file(tmp_orig)
        tmp_orig.seek(0)
        with tempfile.TemporaryFile() as tmp_fixed:
            for ln in tmp_orig.xreadlines():
                if len(ln.split(",")) == 7:
                    tmp_fixed.write(ln)
            tmp_fixed.seek(0)
            fixed_key.set_contents_from_file(tmp_fixed)

if __name__ == "__main__":
    import_events()
