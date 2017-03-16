
import os
import hmac
import json
import math
import time
import hashlib
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

EVENTS_BUCKET = "net-mozaws-prod-us-west-2-pipeline-analysis"
EVENTS_PREFIX = "fxa-vendor-export/data/"

Q_DROP_CSV_TABLE = "DROP TABLE IF EXISTS temporary_raw_amplitude_data;"

Q_CREATE_CSV_TABLE = """
    CREATE TABLE IF NOT EXISTS temporary_raw_amplitude_data (
      timestamp BIGINT NOT NULL SORTKEY,
      type VARCHAR(64) NOT NULL,
      uid VARCHAR(64) DISTKEY,
      device_id VARCHAR(64),
      session_id BIGINT,
      event_id BIGINT,
      service VARCHAR(40),
      ua_browser VARCHAR(40),
      ua_version VARCHAR(40),
      ua_os VARCHAR(40),
      entrypoint VARCHAR(40),
      utm_campaign VARCHAR(40),
      utm_content VARCHAR(40),
      utm_medium VARCHAR(40),
      utm_source VARCHAR(40),
      utm_term VARCHAR(40)
    );
"""


Q_CREATE_EVENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS amplitude_events (
      timestamp TIMESTAMP NOT NULL SORTKEY ENCODE lzo,
      type VARCHAR(64) NOT NULL ENCODE LZO,
      uid VARCHAR(64) DISTKEY ENCODE LZO,
      device_id VARCHAR(64) ENCODE LZO,
      session_id BIGINT ENCODE LZO,
      event_id BIGINT ENCODE LZO,
      service VARCHAR(40) ENCODE LZO,
      ua_browser VARCHAR(40) ENCODE LZO,
      ua_version VARCHAR(40) ENCODE LZO,
      ua_os VARCHAR(40) ENCODE LZO,
      entrypoint VARCHAR(40) ENCODE LZO,
      utm_campaign VARCHAR(40) ENCODE LZO,
      utm_content VARCHAR(40) ENCODE LZO,
      utm_medium VARCHAR(40) ENCODE LZO,
      utm_source VARCHAR(40) ENCODE LZO,
      utm_term VARCHAR(40) ENCODE LZO
    );
"""


Q_CLEAR_DAY_EVENTS = """
    DELETE FROM amplitude_events
    WHERE timestamp::DATE = '{day}'::DATE
"""


Q_COPY_CSV = """
    COPY temporary_raw_amplitude_data (
      timestamp,
      type,
      uid,
      device_id,
      session_id,
      event_id,
      service,
      ua_browser,
      ua_version,
      ua_os,
      entrypoint,
      utm_campaign,
      utm_content,
      utm_medium,
      utm_source,
      utm_term
    )
    FROM '{s3path}'
    CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
    FORMAT AS CSV
    TRUNCATECOLUMNS;
"""

Q_INSERT_EVENTS = """
    INSERT INTO amplitude_events (
      timestamp,
      type,
      uid,
      device_id,
      session_id,
      event_id,
      service,
      ua_browser,
      ua_version,
      ua_os,
      entrypoint,
      utm_campaign,
      utm_content,
      utm_medium,
      utm_source,
      utm_term
    )
    SELECT
      'epoch'::TIMESTAMP + timestamp * '1 second'::INTERVAL,
      type,
      uid,
      device_id,
      session_id,
      event_id,
      service,
      ua_browser,
      ua_version,
      ua_os,
      entrypoint,
      utm_campaign,
      utm_content,
      utm_medium,
      utm_source,
      utm_term
    FROM temporary_raw_amplitude_data
    WHERE ('epoch'::TIMESTAMP + timestamp * '1 second'::INTERVAL)::DATE = '{day}'::DATE;
"""


def import_events(from_date, to_date):
    b = boto.s3.connect_to_region("us-east-1").get_bucket(EVENTS_BUCKET)
    db = postgres.Postgres(DB)
    files_by_date = find_available_event_files_by_date(b)
    db.run(Q_CREATE_EVENTS_TABLE)
    for day in dates_in_range(from_date, to_date):
        print "IMPORT", day
        db.run("BEGIN TRANSACTION")
        try:
            db.run(Q_DROP_CSV_TABLE)
            db.run(Q_CREATE_CSV_TABLE)
            db.run(Q_CLEAR_DAY_EVENTS.format(day=day))
            for filename in files_by_date[day]:
                s3path = "s3://" + EVENTS_BUCKET + "/" + EVENTS_PREFIX + filename
                db.run(Q_COPY_CSV.format(
                    s3path=s3path,
                    **CONFIG
                ))
            db.run(Q_INSERT_EVENTS.format(day=day))
        except:
            db.run("ROLLBACK TRANSACTION")
            raise
        else:
            db.run("COMMIT TRANSACTION")


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


def find_available_event_files_by_date(b):
    files_by_date = {}
    for key in b.list(prefix=EVENTS_PREFIX):
        filename = os.path.basename(key.name)
        day = "-".join(filename.split("-")[1:4])
        files_by_date.setdefault(day, []).append(filename)
    return files_by_date


if __name__ == "__main__":
    import sys
    import_events(*sys.argv[1:])
