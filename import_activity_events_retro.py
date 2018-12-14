#
# Script to import "activity event" metrics from S3 into redshift.
#

import os
import json
import time
from datetime import datetime
import tempfile
import urlparse

import postgres

import boto.s3
import boto.provider

# Load config from disk,
# and pull in credentials from the environment.

REDSHIFT_USER = os.environ["REDSHIFT_USER"]
REDSHIFT_PASSWORD = os.environ["REDSHIFT_PASSWORD"]
REDSHIFT_HOST = os.environ["REDSHIFT_HOST"]
REDSHIFT_PORT = os.environ["REDSHIFT_PORT"]
REDSHIFT_DBNAME = os.environ["REDSHIFT_DBNAME"]

DB = "postgresql://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DBNAME}".format(
    REDSHIFT_USER=REDSHIFT_USER, REDSHIFT_PASSWORD=REDSHIFT_PASSWORD, REDSHIFT_HOST=REDSHIFT_HOST,
    REDSHIFT_PORT=REDSHIFT_PORT, REDSHIFT_DBNAME=REDSHIFT_DBNAME
)

def env_or_default(variable_name, default_value):
    if variable_name in os.environ:
        return os.environ[variable_name]

    return default_value

p = boto.provider.Provider("aws")
AWS_ACCESS_KEY = env_or_default("AWS_ACCESS_KEY", p.get_access_key())
AWS_SECRET_KEY = env_or_default("AWS_SECRET_KEY", p.get_secret_key())

# Event data files are named like "events-2016-02-15.csv"
# and contain events for the specified date.
# Unfortunately some of the data files have missing fields :-(
# We work around this by downloading the file, fixing it up,
# and uploading the fixed copy to a new location.

EVENTS_BUCKET = "net-mozaws-prod-us-west-2-pipeline-analysis"
EVENTS_PREFIX = "whd/fxa-retention/"
EVENTS_FILE_URL = "s3://" + EVENTS_BUCKET + "/" + EVENTS_PREFIX + "events-{day}.csv"

# We have sampled data sets that include a longer history.
# This config controls the tables names, sample rates and
# history length for each data set.
SAMPLE_RATES = (
    {"percent":10, "months":24, "suffix":"_sampled_10"},
    {"percent":50, "months":6, "suffix":"_sampled_50"},
    {"percent":100, "months":3, "suffix":""}
)

# We import each into a temporary table and then
# INSERT them into the activity_events table

Q_DROP_CSV_TABLE = "DROP TABLE IF EXISTS temporary_raw_activity_data_retro;"

Q_CREATE_CSV_TABLE = """
    CREATE TABLE IF NOT EXISTS temporary_raw_activity_data_retro (
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

Q_GET_LAST_DAY = """
    SELECT MAX(timestamp)::DATE
    FROM activity_events_sampled_10;
"""

Q_CHECK_FOR_DAY = """
    SELECT timestamp FROM activity_events_sampled_10
    WHERE timestamp::DATE = '{day}'::DATE
    LIMIT 1;
"""

Q_CLEAR_DAY = """
    DELETE FROM activity_events{suffix}
    WHERE timestamp::DATE = '{day}'::DATE;
"""

Q_COPY_CSV = """
    COPY temporary_raw_activity_data_retro (
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
    CREDENTIALS 'aws_access_key_id={AWS_ACCESS_KEY};aws_secret_access_key={AWS_SECRET_KEY}'
    FORMAT AS CSV
    TRUNCATECOLUMNS;
"""

Q_INSERT_EVENTS = """
    INSERT INTO activity_events{suffix} (
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
      ts,
      uid,
      type,
      device_id,
      service,
      ua_browser,
      ua_version,
      ua_os
    FROM (
      SELECT
        *,
        'epoch'::TIMESTAMP + timestamp * '1 second'::INTERVAL AS ts,
        STRTOL(SUBSTRING(uid FROM 0 FOR 8), 16) % 100 AS cohort
      FROM temporary_raw_activity_data_retro
    )
    WHERE cohort <= {percent}
      AND ts::DATE >= '{last_day}'::DATE - '{months} months'::INTERVAL;
"""

Q_DELETE_EVENTS = """
    DELETE FROM activity_events{suffix}
    WHERE timestamp::DATE < '{last_day}'::DATE - '{months} months'::INTERVAL;
"""

Q_VACUUM_TABLES = """
    END;
    VACUUM FULL activity_events{suffix};
"""

def import_events(force_reload=False):
    b = boto.s3.connect_to_region("us-west-2").get_bucket(EVENTS_BUCKET)
    db = postgres.Postgres(DB)
    db.run(Q_DROP_CSV_TABLE)
    # Deliberately don't create the activity_events tables here,
    # to avoid duplicating the schema in 2 places. This script
    # will only run against a pre-created activity_events table.
    last_day = db.one(Q_GET_LAST_DAY)
    days_to_load = []
    # Find all the days available for loading.
    for key in b.list(prefix=EVENTS_PREFIX):
        filename = os.path.basename(key.name)
        # There are log files in S3, we don't want to process those
        if not filename.endswith(".csv"):
            continue
        day = "-".join(filename[:-4].split("-")[1:])
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
            for rate in SAMPLE_RATES:
                db.run(Q_CLEAR_DAY.format(suffix=rate["suffix"], day=day))
            s3path = EVENTS_FILE_URL.format(day=day)
            # Copy data from s3 into redshift
            db.run(Q_COPY_CSV.format(s3path=s3path, AWS_ACCESS_KEY=AWS_ACCESS_KEY, AWS_SECRET_KEY=AWS_SECRET_KEY))
            # Populate the activity_events table
            for rate in SAMPLE_RATES:
                db.run(Q_INSERT_EVENTS.format(suffix=rate["suffix"], percent=rate["percent"], last_day=last_day, months=rate["months"]))
            # Print the timestamps for sanity-checking
            print "  MIN TIMESTAMP", db.one("SELECT MIN(timestamp) FROM temporary_raw_activity_data_retro")
            print "  MAX TIMESTAMP", db.one("SELECT MAX(timestamp) FROM temporary_raw_activity_data_retro")
            # Drop the temporary table
            db.run(Q_DROP_CSV_TABLE)
        for rate in SAMPLE_RATES:
            # Expire old data
            print "EXPIRING", last_day, "+", rate["months"], "MONTHS"
            db.run(Q_DELETE_EVENTS.format(suffix=rate["suffix"], last_day=last_day, months=rate["months"]))
    except:
        db.run("ROLLBACK TRANSACTION")
        raise
    else:
        db.run("COMMIT TRANSACTION")

    for rate in SAMPLE_RATES:
        print "VACUUMING activity_events{suffix}".format(suffix=rate["suffix"])
        db.run(Q_VACUUM_TABLES.format(suffix=rate["suffix"]))

if __name__ == "__main__":
    import_events()
