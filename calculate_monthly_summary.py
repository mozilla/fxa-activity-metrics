#
# Script to calculate monthly summary tables from raw activity-event data.
# It mintain a monthly rollup summarizing users who met the criteria for the
# `daily_activity_per_device` and `daily_multi_device_users` tables on any
# day in the preceeding 28 days.
#

import json
import time
import datetime

import postgres

# Load config from disk,
# and pull in credentials from the environment.

with open("config.json") as f:
    CONFIG = json.loads(f.read())

DB = "postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}".format(**CONFIG)

# For the monthly device activity summary,
# we maintain a table giving unique (uid, device_id, service)
# values seen in the last 28 days

Q_MONTHLY_ACTIVITY_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS unique_activity_in_previous_month (
      day DATE NOT NULL SORTKEY ENCODE lzo,
      uid VARCHAR(64) NOT NULL DISTKEY ENCODE lzo,
      device_id VARCHAR(32) NOT NULL ENCODE lzo,
      service VARCHAR(40) ENCODE lzo,
      ua_browser VARCHAR(40) ENCODE lzo,
      ua_version VARCHAR(40) ENCODE lzo,
      ua_os VARCHAR(40) ENCODE lzo
    );
"""

Q_MONTHLY_ACTIVITY_CLEAR = """
    DELETE FROM unique_activity_in_previous_month
    WHERE day >= '{day_from}'::DATE
    AND day <= '{day_until}'::DATE;
"""

Q_MONTHLY_ACTIVITY_SUMMARIZE = """
    INSERT INTO unique_activity_in_previous_month
      (day, uid, device_id, service, ua_browser, ua_version, ua_os)
    SELECT DISTINCT
      days.day, past.uid, past.device_id, past.service,
      past.ua_browser, past.ua_version, past.ua_os
    FROM (
      SELECT DISTINCT day FROM daily_activity_per_device
      WHERE day >= '{day_from}'::DATE AND day <= '{day_until}'::DATE
      ORDER BY 1
    ) AS days
    INNER JOIN daily_activity_per_device AS past
      ON past.day <= days.day
      AND past.day > (days.day - '28 days'::INTERVAL)
      -- This is not strictly necessary, but it speeds up the query.
      AND past.day > ('{day_from}'::DATE - '28 days'::INTERVAL)
      AND past.day > '{day_from}'::DATE - '28 days'::INTERVAL
      AND past.day <= '{day_until}'::DATE
    ORDER BY 1;
"""

# For the monthly multi-device-users summary, we maintain
# a table of (day, uid, device_now, device_prev) tuples
# based on whether that user as in the daily_multi_device_users
# table on any day in the previous 28 days.

Q_MONTHLY_MD_USERS_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS multi_device_users_in_previous_month (
      day DATE NOT NULL SORTKEY ENCODE lzo,
      uid VARCHAR(64) NOT NULL DISTKEY ENCODE lzo,
      device_now VARCHAR(32) NOT NULL ENCODE lzo,
      device_prev VARCHAR(32) NOT NULL ENCODE lzo
    );
"""

Q_MONTHLY_MD_USERS_CLEAR = """
    DELETE FROM multi_device_users_in_previous_month
    WHERE day >= '{day_from}'::DATE
    AND day <= '{day_until}'::DATE;
"""

Q_MONTHLY_MD_USERS_SUMMARIZE = """
    INSERT INTO multi_device_users_in_previous_month
      (day, uid, device_now, device_prev)
    SELECT DISTINCT
      days.day, past.uid, past.device_now, past.device_prev
    FROM (
      SELECT DISTINCT day FROM daily_multi_device_users
      WHERE day >= '{day_from}'::DATE AND day <= '{day_until}'::DATE
      ORDER BY 1
    ) AS days
    INNER JOIN daily_multi_device_users AS past
      ON past.day <= days.day
      AND past.day > (days.day - '28 days'::INTERVAL)
      -- This is not strictly necessary, but it speeds up the query.
      AND past.day > '{day_from}'::DATE - '28 days'::INTERVAL
      AND past.day <= '{day_until}'::DATE
    ORDER BY 1;
"""

# Some additional management queries.

Q_GET_FIRST_UNPROCESSED_DAY = """
    SELECT (MAX(day) + '1 day'::INTERVAL)::DATE AS day
    FROM unique_activity_in_previous_month;
"""

Q_GET_FIRST_AVAILABLE_DAY = """
    SELECT MIN(day) AS day
    FROM daily_activity_per_device;
"""

Q_GET_LAST_AVAILABLE_DAY = """
    SELECT MAX(day)
    FROM daily_activity_per_device;
"""

Q_VACUUM_TABLES = """
    END;
    VACUUM FULL unique_activity_in_previous_month;
    VACUUM FULL multi_device_users_in_previous_month;
"""

def summarize_events(day_from=None, day_until=None):
    db = postgres.Postgres(DB)
    db.run("BEGIN TRANSACTION")
    print "CREATING TABLES"
    db.run(Q_MONTHLY_ACTIVITY_CREATE_TABLE)
    db.run(Q_MONTHLY_MD_USERS_CREATE_TABLE)
    print "FINDING DATES"
    # By default, summarize the latest days that are not yet summarized.
    if day_from is None:
        day_from = db.one(Q_GET_FIRST_UNPROCESSED_DAY)
        if day_from is None:
            day_from = db.one(Q_GET_FIRST_AVAILABLE_DAY)
            if day_from is None:
                raise RuntimeError('no events in db')
    print "FROM:", day_from
    if day_until is None:
        day_until = db.one(Q_GET_LAST_AVAILABLE_DAY)
    print "UNTIL:", day_until
    days = {
        "day_from": day_from,
        "day_until": day_until,
    }
    # This uses a lot of temporary disk space so we
    # process them a few days at a time.
    for days_chunk in chunk_day_range(days):
        try:
            print "CHUNK: {day_from}...{day_until}".format(**days_chunk)
            print "UPDATING MONTHLY UNIQUE ACTIVITY SUMMARY"
            db.run(Q_MONTHLY_ACTIVITY_CLEAR.format(**days_chunk))
            db.run(Q_MONTHLY_ACTIVITY_SUMMARIZE.format(**days_chunk))
            print "UPDATING MONTHLY MULTI-DEVICE USERS SUMMARY"
            db.run(Q_MONTHLY_MD_USERS_CLEAR.format(**days_chunk))
            db.run(Q_MONTHLY_MD_USERS_SUMMARIZE.format(**days_chunk))
        except:
            db.run("ROLLBACK TRANSACTION")
            raise
        else:
            db.run("COMMIT TRANSACTION")

    db.run(Q_VACUUM_TABLES)


def chunk_day_range(days, step=5):
    """Split up a day_from...day_until range into smaller chunks."""
    day_from = days["day_from"]
    day_until = days["day_until"]
    delta = datetime.timedelta(days=step)
    while day_from <= day_until:
        day_next = min(day_from + delta, day_until)
        yield {
            "day_from": day_from,
            "day_until": day_next,
        }
        day_from = day_next + datetime.timedelta(days=1)


if __name__ == "__main__":
    summarize_events()
