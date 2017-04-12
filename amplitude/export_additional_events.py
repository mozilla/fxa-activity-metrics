
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
EVENTS_PREFIX = "fxa-vendor-export/extra-data/"
EVENTS_FILE_URL = "s3://" + EVENTS_BUCKET + "/" + EVENTS_PREFIX + "export-extra-{day}-"

Q_CREATE_MERGED_EVENT_STREAM = """
    WITH
      transformed_flow_events AS (
        SELECT
          EXTRACT(EPOCH FROM e.timestamp) AS timestamp,
          (CASE
            WHEN e.type = 'account.created' THEN 'fxa_reg-created'
            WHEN e.type = 'account.signed' AND f.new_account THEN 'fxa_reg-signed'
            WHEN e.type = 'account.login' THEN 'fxa_login-success'
            WHEN e.type = 'account.reset' THEN 'fxa_login-forgot_complete'
            WHEN e.type = 'account.signed' AND NOT f.new_account THEN 'fxa_login-signed'
            ELSE 'fxa_unknown-unknown'
          END) AS type,
          -- Infer the uid by joining onto the activity_events table.
          -- Note that in the unlikely event of multiple matching activity events,
          -- this will produce multiple rows for that flow event.
          (CASE
            WHEN f.uid IS NOT NULL AND f.uid != '' THEN f.uid
            ELSE a.uid
          END) AS uid,
          e.flow_id AS device_id,
          EXTRACT(EPOCH FROM f.begin_time) AS session_id,
          e.flow_time AS event_id,
          f.service AS service,
          f.ua_browser AS ua_browser,
          f.ua_version AS ua_version,
          f.ua_os AS ua_os,
          f.entrypoint AS entrypoint,
          f.utm_campaign AS utm_campaign,
          f.utm_content AS utm_content,
          f.utm_medium AS utm_medium,
          f.utm_source AS utm_source,
          f.utm_term AS utm_term
        FROM flow_metadata AS f
        INNER JOIN flow_events AS e
        ON f.flow_id = e.flow_id
        AND f.begin_time::DATE >= ('{day}'::DATE - '1 day'::INTERVAL)
        AND f.begin_time::DATE <= '{day}'::DATE
        AND e.timestamp::DATE = '{day}'::DATE
        LEFT OUTER JOIN activity_events AS a
        ON a.type = e.type
        AND (a.type = 'account.created' OR a.type = 'account.login' OR a.type = 'account.reset' OR a.type = 'account.signed')
        AND a.timestamp = e.timestamp
        AND a.timestamp::DATE = '{day}'::DATE
        AND (a.service = f.service OR a.service = '')
        AND a.ua_os = f.ua_os
        AND a.ua_browser = f.ua_browser
        AND a.ua_version = f.ua_version
        WHERE (
            e.type = 'account.created' OR
            e.type = 'account.login' OR
            e.type = 'account.reset' OR
            e.type = 'account.signed'
        )
        -- Sample by uid, not flow_id
        AND (
            (f.uid IS NOT NULL AND f.uid != '' AND STRTOL(SUBSTRING(f.uid FROM 0 FOR 8), 16) % 100 < 10)
            OR (a.uid IS NOT NULL AND STRTOL(SUBSTRING(a.uid FROM 0 FOR 8), 16) % 100 < 10)
        )
        ORDER BY 1, 6
      )

    SELECT * FROM transformed_flow_events
    ORDER BY "timestamp", event_id
"""


Q_EXPORT_MERGED_EVENT_STREAM = """
    UNLOAD ('{query}')
    TO '{s3path}'
    CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
    DELIMITER AS ','
    ALLOWOVERWRITE
"""


def export_events(from_date, to_date):
    b = boto.s3.connect_to_region("us-east-1").get_bucket(EVENTS_BUCKET)
    db = postgres.Postgres(DB)
    # OK, now we can get redshift to export data for each day into S3.
    for day in dates_in_range(from_date, to_date):
        s3path = EVENTS_FILE_URL.format(day=day)
        print "EXPORTING", day, "to", s3path
        query = Q_CREATE_MERGED_EVENT_STREAM.format(day=day).replace("'", "\\'")
        db.run(Q_EXPORT_MERGED_EVENT_STREAM.format(s3path=s3path, query=query, **CONFIG))


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


if __name__ == "__main__":
    import sys
    export_events(*sys.argv[1:])
