#
# Script to calculate daily summary tables from raw activity-event data.
#
# We calculate the following summary tables:
#
#  Devices active on each day: (day, uid, userAgentOS)
#  Users who were multi-device on each day: (day, uid, userAgentOS)
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

# For the daily device activity summary,
# we maintain a table giving (day, uid, device_id).

Q_DAILY_DEVICES_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS daily_activity_per_device (
      day DATE NOT NULL SORTKEY ENCODE lzo,
      uid VARCHAR(64) NOT NULL DISTKEY ENCODE lzo,
      device_id VARCHAR(32) NOT NULL ENCODE lzo,
      service VARCHAR(40) ENCODE lzo,
      ua_browser VARCHAR(40) ENCODE lzo,
      ua_version VARCHAR(40) ENCODE lzo,
      ua_os VARCHAR(40) ENCODE lzo
    );
"""

Q_DAILY_DEVICES_CLEAR = """
    DELETE FROM daily_activity_per_device
    WHERE day >= '{day_from}'::DATE
    AND day <= '{day_until}'::DATE;
"""

Q_DAILY_DEVICES_SUMMARIZE = """
    INSERT INTO daily_activity_per_device
      (day, uid, device_id, service, ua_browser, ua_version, ua_os)
    SELECT DISTINCT
      timestamp::DATE as day,
      uid, device_id, service, ua_browser, ua_version, ua_os
    FROM activity_events
    WHERE device_id != ''
    AND timestamp::DATE >= '{day_from}'::DATE
    AND timestamp::DATE <= '{day_until}'::DATE
    ORDER BY 1;
"""

# For the daily multi-device-users summary, we maintain
# a table of (day, uid, device_now, device_prev) tuples
# based on whether that user had multiple devices
# active in the last seven days.

Q_MD_USERS_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS daily_multi_device_users (
      day DATE NOT NULL SORTKEY ENCODE lzo,
      uid VARCHAR(64) NOT NULL DISTKEY ENCODE lzo,
      device_now VARCHAR(32) NOT NULL ENCODE lzo,
      device_prev VARCHAR(32) NOT NULL ENCODE lzo
    );
"""

Q_MD_USERS_CLEAR = """
    DELETE FROM daily_multi_device_users
    WHERE day >= '{day_from}'::DATE
    AND day <= '{day_until}'::DATE;
"""

Q_MD_USERS_SUMMARIZE = """
    INSERT INTO daily_multi_device_users (day, uid, device_now, device_prev)
    SELECT DISTINCT present.day, present.uid, present.device_id, past.device_id
    FROM daily_activity_per_device as present
    INNER JOIN daily_activity_per_device as past
    ON
      present.uid = past.uid
      AND present.device_id != past.device_id
      AND past.day <= present.day
      AND past.day >= (present.day - '7 days'::INTERVAL)
    WHERE present.day >= '{day_from}'::DATE
    AND present.day <= '{day_until}'::DATE
    ORDER BY 1;
"""

Q_GET_FIRST_UNPROCESSED_DAY = """
    SELECT (MAX(day) + '1 day'::INTERVAL) AS timestamp
    FROM daily_multi_device_users;
"""

Q_GET_FIRST_AVAILABLE_DAY = """
    SELECT MIN(timestamp)::DATE
    FROM activity_events;
"""

Q_GET_LAST_AVAILABLE_DAY = """
    SELECT MAX(timestamp)::DATE
    FROM activity_events;
"""

def summarize_events(day_from=None, day_until=None):
    db = postgres.Postgres(DB)
    db.run("BEGIN TRANSACTION")
    try:
        db.run(Q_DAILY_DEVICES_CREATE_TABLE)
        db.run(Q_MD_USERS_CREATE_TABLE)
        # By default, summarize the latest days that are not yet summarized.
        if day_from is None:
            day_from = db.one(Q_GET_FIRST_UNPROCESSED_DAY)
            if day_from is None:
                day_from = db.one(Q_GET_FIRST_AVAILABLE_DAY)
                if day_from is None:
                    raise RuntimeError('no events in db')
        if day_until is None:
            day_until = db.one(Q_GET_LAST_AVAILABLE_DAY)
        days = {
            "day_from": day_from,
            "day_until": day_until,
        }
        print "SUMMARIZING FROM", day_from, "UNTIL", day_until
        # Update daily device activity.
        print "UPDATING DAILY ACTIVE DEVICES SUMMARY"
        db.run(Q_DAILY_DEVICES_CLEAR.format(**days))
        db.run(Q_DAILY_DEVICES_SUMMARIZE.format(**days))
        # Update multi-device-user assessments.
        print "UPDATING MULTI-DEVICE USERS SUMMARY"
        db.run(Q_MD_USERS_CLEAR.format(**days))
        db.run(Q_MD_USERS_SUMMARIZE.format(**days))
    except:
        db.run("ROLLBACK TRANSACTION")
        raise
    else:
        db.run("COMMIT TRANSACTION")

if __name__ == "__main__":
    summarize_events()
