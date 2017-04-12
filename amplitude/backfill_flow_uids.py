
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


Q_MATERIALIZE_BACKFILL_TABLE = """
    CREATE TEMPORARY TABLE flow_uid_backfill AS
        SELECT
          f.flow_id AS flow_id,
          a.uid AS uid
        FROM flow_metadata AS f
        INNER JOIN flow_events AS e
        ON f.flow_id = e.flow_id
        AND f.begin_time::DATE >= ('{day}'::DATE - '1 day'::INTERVAL)
        AND f.begin_time::DATE <= '{day}'::DATE
        AND e.timestamp::DATE = '{day}'::DATE
        AND (e.type = 'account.created' OR e.type = 'account.login' OR e.type = 'account.reset')
        LEFT OUTER JOIN activity_events AS a
        ON a.type = e.type
        AND (a.type = 'account.created' OR a.type = 'account.login' OR a.type = 'account.reset')
        AND a.timestamp = e.timestamp
        AND a.timestamp::DATE = '{day}'::DATE
        AND (a.service = f.service OR a.service = '')
        AND a.ua_os = f.ua_os
        AND a.ua_browser = f.ua_browser
        AND a.ua_version = f.ua_version
        WHERE (f.uid IS NULL OR f.uid = '')
        AND a.uid IS NOT NULL
    ;
"""


Q_BACKFILL_FLOW_UIDS = """
    UPDATE flow_metadata
    SET uid = backfill.uid
    FROM flow_uid_backfill AS backfill
    WHERE flow_metadata.flow_id = backfill.flow_id
    AND (flow_metadata.uid IS NULL OR flow_metadata.uid = '')
    AND flow_metadata.begin_time::DATE >= ('{day}'::DATE - '1 day'::INTERVAL)
    AND flow_metadata.begin_time::DATE <= '{day}'::DATE
    ;
"""


Q_DROP_BACKFILL_TABLE = """
    DROP TABLE IF EXISTS flow_uid_backfill;
"""


def backfill_events(from_date, to_date):
    db = postgres.Postgres(DB)
    for day in dates_in_range(from_date, to_date):
        print "BACKFILLING uids FOR", day
        db.run(Q_MATERIALIZE_BACKFILL_TABLE.format(day=day))
        db.run(Q_BACKFILL_FLOW_UIDS.format(day=day))
        db.run(Q_DROP_BACKFILL_TABLE.format(day=day))


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
    backfill_events(*sys.argv[1:])
