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
# and contain events for the week starting that date.
# Unfortunately some of the data files have missing fields :-(
# We work around this by downloading the file, fixing it up,
# and uploading the fixed copy to a new location.

EVENTS_BUCKET = "net-mozaws-prod-us-west-2-pipeline-analysis"
EVENTS_PREFIX = "fxa-retention/data/"
EVENTS_PREFIX_FIXED = "fxa-retention/data-rfkelly/"
EVENTS_FILE_URL = "s3://" + EVENTS_BUCKET + "/" + EVENTS_PREFIX + "events-{week}.csv"
EVENTS_FILE_FIXED_URL = "s3://" + EVENTS_BUCKET + "/" + EVENTS_PREFIX_FIXED + "events-{week}.fixed.csv"

# We import each into is own table and maintain a UNION ALL view over them.

Q_CHECK_FOR_TABLE = "SELECT MIN(timestamp) FROM {table_name}"

Q_DROP_UNION = "DROP VIEW IF EXISTS events"

Q_DROP_TABLE = "DROP TABLE IF EXISTS {table_name}"

Q_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS {table_name} (
      timestamp BIGINT NOT NULL sortkey, 
      type VARCHAR(20) NOT NULL,
      uid VARCHAR(64) distkey,
      duration INTEGER,
      service VARCHAR(30),
      userAgentBrowser VARCHAR(30),
      userAgentVersion VARCHAR(30),
      userAgentOS VARCHAR(30)
    );
"""

Q_COPY_EVENTS = """
    COPY {table_name} (
      timestamp,
      userAgentBrowser,
      userAgentVersion,
      userAgentOS,
      uid, 
      type,
      service
    )
    FROM '{s3path}'
    CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
    FORMAT AS CSV;
"""

Q_SELECT_FOR_UNION = """
    SELECT
      timestamp AS timestamp,
      type,
      uid,
      duration,
      service,
      userAgentbrowser,
      userAgentVersion,
      userAgentOS
    FROM {table_name}
"""

Q_CREATE_UNION = """
    CREATE VIEW events AS ({select_query});
"""

def import_events(force_reload=False):
    b = boto.s3.connect_to_region('us-east-1').get_bucket(EVENTS_BUCKET)
    db = postgres.Postgres(DB)
    weeks = []
    weeks_to_load = []
    # Find all the weeks available for loading.
    for key in b.list(prefix=EVENTS_PREFIX):
        filename = os.path.basename(key.name)
        week = "-".join(filename[:-4].split("-")[1:])
        weeks.append(week)
        if force_reload:
            weeks_to_load.append(week)
        else:
            table_name = "events_" + week.replace("-", "_")
            try:
                if not db.one(Q_CHECK_FOR_TABLE.format(table_name=table_name)):
                    weeks_to_load.append(week)
            except Exception:
                weeks_to_load.append(week)
    weeks_to_load.sort(reverse=True)
    print "LOADING {} WEEKS OF DATA".format(len(weeks_to_load))
    db.run("BEGIN TRANSACTION")
    try:
        # Clear the union view while to change the tables available.
        print "CLEARING VIEW"
        db.run(Q_DROP_UNION)

        # Load data for each week direct from s3,
        # fixing it up if there's an error with the load.
        for week in weeks_to_load:
            table_name = "events_" + week.replace("-", "_")
            db.run(Q_DROP_TABLE.format(table_name=table_name))
            db.run(Q_CREATE_TABLE.format(table_name=table_name))
            print "LOADING", week
            s3path = EVENTS_FILE_URL.format(week=week)
            try:
                db.run(Q_COPY_EVENTS.format(
                    table_name=table_name,
                    s3path=s3path,
                    **CONFIG
                ))
            except Exception:
                print "FAILED", week, "TRYING FIXED DATA"
                s3path = EVENTS_FILE_FIXED_URL.format(week=week)
                try:
                    db.run(Q_COPY_EVENTS.format(
                        table_name=table_name,
                        s3path=s3path,
                        **CONFIG
                    ))
                except Exception:
                    print "FAILED, FIXING DATA", week
                    fixup_event_data(b, week)
                    db.run(Q_COPY_EVENTS.format(
                        table_name=table_name,
                        s3path=s3path,
                        **CONFIG
                    ))

        # Re-create the view to incorporate all loaded tables.
        print "RE-CREATING VIEW"
        select_union_all = []
        for week in weeks:
            table_name = "events_" + week.replace("-", "_")
            select_union_all.append(Q_SELECT_FOR_UNION.format(
                table_name=table_name
            ))
        db.run(Q_CREATE_UNION.format(
            select_query=" UNION ALL " .join(select_union_all)
        ))

        # Print the timestamps for sanity-checking.
        print "MIN TIMESTAMP", db.one("SELECT MIN(timestamp) FROM events")
        print "MAX TIMESTAMP", db.one("SELECT MAX(timestamp) FROM events")
    except:
        db.run("ROLLBACK TRANSACTION")
        raise
    else:
        db.run("COMMIT TRANSACTION")


def fixup_event_data(b, week):
    """Download a data file, remove invalid lines, and re-upload"""
    orig_key = b.get_key(urlparse.urlparse(EVENTS_FILE_URL.format(week=week)).path[1:])
    assert orig_key is not None
    fixed_key = b.new_key(urlparse.urlparse(EVENTS_FILE_FIXED_URL.format(week=week)).path[1:])
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
