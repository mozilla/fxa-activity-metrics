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
#   * flow_csv      - raw data from the CSV file
#   * flow_metadata - metadata for each flow
#   * flow_events   - individual flow events

Q_DROP_CSV_TABLE = "DROP TABLE IF EXISTS flow_csv;"

Q_CREATE_CSV_TABLE = """
    CREATE TABLE IF NOT EXISTS flow_csv (
      timestamp BIGINT NOT NULL SORTKEY,
      type VARCHAR(30) NOT NULL,
      flowId VARCHAR(64) NOT NULL DISTKEY,
      flowTime BIGINT NOT NULL,
      uaBrowser VARCHAR(40),
      uaVersion VARCHAR(40),
      uaOS VARCHAR(40),
      context VARCHAR(40),
      entrypoint VARCHAR(40),
      migration VARCHAR(40),
      service VARCHAR(40),
      utmCampaign VARCHAR(40),
      utmContent VARCHAR(40),
      utmMedium VARCHAR(40),
      utmSource VARCHAR(40),
      utmTerm VARCHAR(40)
    );
"""
Q_CREATE_METADATA_TABLE = """
    CREATE TABLE IF NOT EXISTS flow_metadata (
      flowId VARCHAR(64) NOT NULL UNIQUE SORTKEY,
      beginTime TIMESTAMP NOT NULL,
      duration INTERVAL NOT NULL,
      completed BOOLEAN NOT NULL,
      newAccount BOOLEAN NOT NULL,
      uaBrowser VARCHAR(40),
      uaVersion VARCHAR(40),
      uaOS VARCHAR(40),
      context VARCHAR(40),
      entrypoint VARCHAR(40),
      migration VARCHAR(40),
      service VARCHAR(40),
      utmCampaign VARCHAR(40),
      utmContents VARCHAR(40),
      utmMedium VARCHAR(40),
      utmSource VARCHAR(40),
      utmTerm VARCHAR(40)
    );
"""
Q_CREATE_EVENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS flow_events (
      timestamp TIMESTAMP NOT NULL SORTKEY,
      flowTime INTERVAL NOT NULL,
      flowId VARCHAR(64) NOT NULL DISTKEY,
      type VARCHAR(30) NOT NULL
    );
"""

Q_CHECK_FOR_DAY = """
    SELECT timestamp FROM flow_events
    WHERE timestamp::date >= '{day}'::date
    AND timestamp::date < '{day}'::date + 1
    LIMIT 1;
"""

Q_CLEAR_DAY_METADATA = """
    DELETE FROM flow_metadata
    WHERE beginTime::date >= '{day}'::date
    AND beginTime::date < '{day}'::date + 1;
"""
Q_CLEAR_DAY_EVENTS = """
    DELETE FROM flow_events
    WHERE timestamp::date >= '{day}'::date
    AND timestamp::date < '{day}'::date + 1;
"""

Q_COPY_CSV = """
    COPY flow_csv (
      timestamp,
      type,
      flowId,
      flowTime,
      uaBrowser,
      uaVersion,
      uaOS,
      context,
      entrypoint,
      migration,
      service,
      utmCampaign,
      utmContent,
      utmMedium,
      utmSource,
      utmTerm
    )
    FROM '{s3path}'
    CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
    FORMAT AS CSV;
"""
Q_INSERT_METADATA = """
    WITH durations AS (
      SELECT flowId, MAX(flowTime)
      FROM flow_csv
      GROUP BY flowId
    )
    INSERT INTO flow_metadata (
      flowId,
      beginTime,
      duration,
      completed,
      newAccount,
      uaBrowser,
      uaVersion,
      uaOS,
      context,
      entrypoint,
      migration,
      service,
      utmCampaign,
      utmContents,
      utmMedium,
      utmSource,
      utmTerm
    )
    SELECT (
      begin.flowId,
      (begin.timestamp * 1000.0),
      (durations.flowTime * 1000.0),
      (CASE WHEN signed.flowId IS NULL THEN FALSE ELSE TRUE END),
      (CASE WHEN created.flowId IS NULL THEN FALSE ELSE TRUE END),
      begin.uaBrowser,
      begin.uaVersion,
      begin.uaOS,
      begin.context,
      begin.entrypoint,
      begin.migration,
      begin.service,
      begin.utmCampaign,
      begin.utmContents,
      begin.utmMedium,
      begin.utmSource,
      begin.utmTerm
    )
    FROM flow_csv AS begin
    INNER JOIN durations
      ON begin.flowId = durations.flowId AND begin.type = 'flow.begin'
    LEFT JOIN flow_csv AS created
      ON begin.flowId = created.flowId AND created.type = 'account.created'
    LEFT JOIN flow_csv AS signed
      ON begin.flowId = signed.flowId AND signed.type = 'account.signed';
"""
Q_INSERT_EVENTS = """
    INSERT INTO flow_metadata (
      timestamp,
      flowTime,
      flowId,
      type
    )
    SELECT (
      (timestamp * 1000.0),
      (flowTime * 1000.0),
      flowId,
      type
    )
    FROM flow_csv;
"""

def import_events(force_reload=False):
    b = boto.s3.connect_to_region('us-east-1').get_bucket(EVENTS_BUCKET)
    db = postgres.Postgres(DB)
    db.run(Q_DROP_TABLE_CSV)
    db.run(Q_CREATE_TABLE_CSV)
    db.run(Q_CREATE_TABLE_METADATA)
    db.run(Q_CREATE_TABLE_EVENTS)
    days = []
    days_to_load = []
    # Find all the days available for loading.
    print EVENTS_BUCKET, EVENTS_PREFIX
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
        # Load data for each day direct from s3,
        for day in days_to_load:
            print "LOADING", day
            # Clear any existing data for that day, to avoid duplicates.
            db.run(Q_CLEAR_DAY_METADATA.format(day=day))
            db.run(Q_CLEAR_DAY_EVENTS.format(day=day))
            s3path = EVENTS_FILE_URL.format(day=day)
            db.run(Q_COPY_CSV.format(
                s3path=s3path,
                **CONFIG
            ))
            db.run(Q_INSERT_METADATA)
            db.run(Q_INSERT_EVENTS)

        # Print the timestamps for sanity-checking.
        print "MIN TIMESTAMP", db.one("SELECT MIN(timestamp) FROM flow_csv")
        print "MAX TIMESTAMP", db.one("SELECT MAX(timestamp) FROM flow_csv")

        db.run(Q_DROP_TABLE_CSV)
    except:
        db.run("ROLLBACK TRANSACTION")
        raise
    else:
        db.run("COMMIT TRANSACTION")

if __name__ == "__main__":
    import_events(True)