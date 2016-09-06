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

# We import each into is own table and maintain a UNION ALL view over them.

Q_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS flow_events (
      timestamp BIGINT NOT NULL sortkey, 
      flowid VARCHAR(64) distkey,
      type VARCHAR(30) NOT NULL,
      flowTime INTEGER,
      userAgentBrowser VARCHAR(40),
      userAgentVersion VARCHAR(40),
      userAgentOS VARCHAR(40),
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

Q_CLEAR_EVENTS = """
    DELETE FROM flow_events
    WHERE timestamp >= {timestamp}
    AND timestamp < {timestamp} + 86400
"""

Q_COPY_EVENTS = """
    COPY flow_events (
      timestamp,
      type,
      flowid,
      flowTime,
      userAgentBrowser,
      userAgentVersion,
      userAgentOS,
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

Q_CHECK_FOR_DAY = """
    SELECT timestamp FROM flow_events
    WHERE timestamp >= {timestamp}
    AND timestamp < {timestamp} + 86400
    LIMIT 1;
"""

def import_events(force_reload=False):
    b = boto.s3.connect_to_region('us-east-1').get_bucket(EVENTS_BUCKET)
    db = postgres.Postgres(DB)
    db.run(Q_CREATE_TABLE)
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
            if not db.one(Q_CHECK_FOR_DAY.format(timestamp=day_to_timestamp(day))):
                days_to_load.append(day)
    days_to_load.sort(reverse=True)
    print "LOADING {} DAYS OF DATA".format(len(days_to_load))
    db.run("BEGIN TRANSACTION")
    try:
        # Load data for each day direct from s3,
        for day in days_to_load:
            print "LOADING", day
            # Clear any existing data for that day, to avoid duplicates.
            db.run(Q_CLEAR_EVENTS.format(timestamp=day_to_timestamp(day)))
            s3path = EVENTS_FILE_URL.format(day=day)
            db.run(Q_COPY_EVENTS.format(
                s3path=s3path,
                **CONFIG
            ))

        # Print the timestamps for sanity-checking.
        print "MIN TIMESTAMP", db.one("SELECT MIN(timestamp) FROM flow_events")
        print "MAX TIMESTAMP", db.one("SELECT MAX(timestamp) FROM flow_events")
    except:
        db.run("ROLLBACK TRANSACTION")
        raise
    else:
        db.run("COMMIT TRANSACTION")


def day_to_timestamp(day):
    """This nonsense turns a"YYYY-MM-DD" into a unix timestamp."""
    return int(time.mktime(datetime.datetime(*map(int, day.split("-"))).utctimetuple()))

if __name__ == "__main__":
    import_events(True)
