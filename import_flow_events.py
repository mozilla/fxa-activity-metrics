#
# Script to import "flow event" metrics from S3 into redshift.
#

import os
import json
import time
import datetime

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

# Event data files are named like "flow-2016-02-15.csv"
# and contain events for that day.

EVENTS_BUCKET = "net-mozaws-prod-us-west-2-pipeline-analysis"
EVENTS_PREFIX = "fxa-flow/data/"
EVENTS_FILE_URL = "s3://" + EVENTS_BUCKET + "/" + EVENTS_PREFIX + "flow-{day}.csv"

# There are three tables:
#   * temporary_raw_flow_data - raw data from the CSV file
#   * flow_metadata           - metadata for each flow
#   * flow_events             - individual flow events

Q_DROP_CSV_TABLE = "DROP TABLE IF EXISTS temporary_raw_flow_data;"

Q_CREATE_CSV_TABLE = """
    CREATE TABLE IF NOT EXISTS temporary_raw_flow_data (
      timestamp BIGINT NOT NULL SORTKEY,
      type VARCHAR(30) NOT NULL,
      flow_id VARCHAR(64) NOT NULL DISTKEY,
      flow_time BIGINT NOT NULL,
      ua_browser VARCHAR(40),
      ua_version VARCHAR(40),
      ua_os VARCHAR(40),
      context VARCHAR(40),
      entrypoint VARCHAR(40),
      migration VARCHAR(40),
      service VARCHAR(40),
      utm_campaign VARCHAR(40),
      utm_content VARCHAR(40),
      utm_medium VARCHAR(40),
      utm_source VARCHAR(40),
      utm_term VARCHAR(40)
    );
"""
Q_CREATE_METADATA_TABLE = """
    CREATE TABLE IF NOT EXISTS flow_metadata (
      flow_id VARCHAR(64) NOT NULL UNIQUE,
      begin_time TIMESTAMP NOT NULL SORTKEY,
      -- Ideally duration would be type INTERVAL
      -- but redshift doesn't support that.
      duration INTEGER NOT NULL DEFAULT 0,
      completed BOOLEAN NOT NULL DEFAULT FALSE,
      new_account BOOLEAN NOT NULL DEFAULT FALSE,
      ua_browser VARCHAR(40),
      ua_version VARCHAR(40),
      ua_os VARCHAR(40),
      context VARCHAR(40),
      entrypoint VARCHAR(40),
      migration VARCHAR(40),
      service VARCHAR(40),
      utm_campaign VARCHAR(40),
      utm_content VARCHAR(40),
      utm_medium VARCHAR(40),
      utm_source VARCHAR(40),
      utm_term VARCHAR(40)
    );
"""
Q_CREATE_EVENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS flow_events (
      timestamp TIMESTAMP NOT NULL SORTKEY,
      -- Ideally flow_time would be type INTERVAL
      -- but redshift doesn't support that.
      flow_time INTEGER NOT NULL,
      flow_id VARCHAR(64) NOT NULL DISTKEY,
      type VARCHAR(30) NOT NULL
    );
"""

Q_CHECK_FOR_DAY = """
    SELECT timestamp FROM flow_events
    WHERE timestamp::DATE = '{day}'::DATE
    LIMIT 1;
"""

Q_CLEAR_DAY_METADATA = """
    DELETE FROM flow_metadata
    WHERE begin_time::DATE = '{day}'::DATE;
"""
Q_CLEAR_DAY_EVENTS = """
    DELETE FROM flow_events
    WHERE timestamp::DATE = '{day}'::DATE;
"""

Q_COPY_CSV = """
    COPY temporary_raw_flow_data (
      timestamp,
      type,
      flow_id,
      flow_time,
      ua_browser,
      ua_version,
      ua_os,
      context,
      entrypoint,
      migration,
      service,
      utm_campaign,
      utm_content,
      utm_medium,
      utm_source,
      utm_term
    )
    FROM '{s3path}'
    CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
    FORMAT AS CSV;
"""

Q_INSERT_METADATA = """
    INSERT INTO flow_metadata (
      flow_id,
      begin_time,
      ua_browser,
      ua_version,
      ua_os,
      context,
      entrypoint,
      migration,
      service,
      utm_campaign,
      utm_content,
      utm_medium,
      utm_source,
      utm_term
    )
    SELECT
      flow_id,
      'epoch'::TIMESTAMP + timestamp * '1 second'::INTERVAL,
      ua_browser,
      ua_version,
      ua_os,
      context,
      entrypoint,
      migration,
      service,
      utm_campaign,
      utm_content,
      utm_medium,
      utm_source,
      utm_term
    FROM temporary_raw_flow_data
    WHERE type = 'flow.begin';
"""
Q_UPDATE_DURATION = """
    UPDATE flow_metadata
    SET duration = durations.flow_time
    FROM (
      SELECT flow_id, MAX(flow_time) AS flow_time
      FROM temporary_raw_flow_data
      GROUP BY flow_id
    ) AS durations
    WHERE flow_metadata.flow_id = durations.flow_id;
"""
Q_UPDATE_COMPLETED = """
    UPDATE flow_metadata
    SET completed = TRUE
    FROM (
      SELECT flow_id
      FROM temporary_raw_flow_data
      WHERE type = 'account.signed'
    ) AS signed
    WHERE flow_metadata.flow_id = signed.flow_id;
"""
Q_UPDATE_NEW_ACCOUNT = """
    UPDATE flow_metadata
    SET new_account = TRUE
    FROM (
      SELECT flow_id
      FROM temporary_raw_flow_data
      WHERE type = 'account.created'
    ) AS created
    WHERE flow_metadata.flow_id = created.flow_id;
"""

Q_INSERT_EVENTS = """
    INSERT INTO flow_events (
      timestamp,
      flow_time,
      flow_id,
      type
    )
    SELECT
      'epoch'::TIMESTAMP + timestamp * '1 second'::INTERVAL,
      flow_time,
      flow_id,
      type
    FROM temporary_raw_flow_data;
"""

def import_events(force_reload=False):
    b = boto.s3.connect_to_region('us-east-1').get_bucket(EVENTS_BUCKET)
    db = postgres.Postgres(DB)
    db.run(Q_DROP_CSV_TABLE)
    db.run(Q_CREATE_METADATA_TABLE)
    db.run(Q_CREATE_EVENTS_TABLE)
    days = []
    days_to_load = []
    print EVENTS_BUCKET, EVENTS_PREFIX
    # Find all the days available for loading.
    for key in b.list(prefix=EVENTS_PREFIX):
        filename = os.path.basename(key.name)
        day = "-".join(filename[:-4].split("-")[1:])
        print day
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
        for day in days_to_load:
            print "LOADING", day
            # Create the temporary table
            db.run(Q_CREATE_CSV_TABLE)
            # Clear any existing data for the day, to avoid duplicates.
            db.run(Q_CLEAR_DAY_METADATA.format(day=day))
            db.run(Q_CLEAR_DAY_EVENTS.format(day=day))
            s3path = EVENTS_FILE_URL.format(day=day)
            # Copy data from s3 into redshift
            db.run(Q_COPY_CSV.format(
                s3path=s3path,
                **CONFIG
            ))
            # Populate the flow_metadata table
            db.run(Q_INSERT_METADATA)
            db.run(Q_UPDATE_DURATION)
            db.run(Q_UPDATE_COMPLETED)
            db.run(Q_UPDATE_NEW_ACCOUNT)
            # Populate the flow_events table
            db.run(Q_INSERT_EVENTS)
            # Print the timestamps for sanity-checking.
            print "  MIN TIMESTAMP", db.one("SELECT MIN(timestamp) FROM temporary_raw_flow_data")
            print "  MAX TIMESTAMP", db.one("SELECT MAX(timestamp) FROM temporary_raw_flow_data")
            # Drop the temporary table
            db.run(Q_DROP_CSV_TABLE)
    except:
        db.run("ROLLBACK TRANSACTION")
        raise
    else:
        db.run("COMMIT TRANSACTION")

if __name__ == "__main__":
    import_events(True)
