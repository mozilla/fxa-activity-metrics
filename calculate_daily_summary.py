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

TABLE_SUFFIXES = (
    "_sampled_10",
    "_sampled_50",
    ""
)

# For the daily device activity summary,
# we maintain a table giving (day, uid, device_id).

Q_DAILY_DEVICES_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS daily_activity_per_device{suffix} (
      day DATE NOT NULL SORTKEY ENCODE RAW,
      uid VARCHAR(64) NOT NULL DISTKEY ENCODE zstd,
      device_id VARCHAR(32) NOT NULL ENCODE zstd,
      service VARCHAR(40) ENCODE zstd,
      ua_browser VARCHAR(40) ENCODE zstd,
      ua_version VARCHAR(40) ENCODE zstd,
      ua_os VARCHAR(40) ENCODE zstd
    );
"""

Q_DAILY_DEVICES_CLEAR = """
    DELETE FROM daily_activity_per_device{suffix}
    WHERE day >= '{day_from}'::DATE
    AND day <= '{day_until}'::DATE;
"""

Q_DAILY_DEVICES_SUMMARIZE = """
    INSERT INTO daily_activity_per_device{suffix}
      (day, uid, device_id, service, ua_browser, ua_version, ua_os)
    SELECT DISTINCT
      timestamp::DATE as day,
      uid, device_id, service, ua_browser, ua_version, ua_os
    FROM activity_events{suffix}
    WHERE device_id != ''
    AND timestamp::DATE >= '{day_from}'::DATE
    AND timestamp::DATE <= '{day_until}'::DATE
    ORDER BY 1;
"""

Q_DAILY_DEVICES_EXPIRE = """
    DELETE FROM daily_activity_per_device{suffix}
    WHERE day < '{day_first}'::DATE;
"""

# For the daily multi-device-users summary, we maintain
# a table of (day, uid, device_now, device_prev) tuples
# based on whether that user had multiple devices
# active in the last seven days.

Q_MD_USERS_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS daily_multi_device_users{suffix} (
      day DATE NOT NULL SORTKEY ENCODE RAW,
      uid VARCHAR(64) NOT NULL DISTKEY ENCODE zstd,
      device_now VARCHAR(32) NOT NULL ENCODE zstd,
      device_prev VARCHAR(32) NOT NULL ENCODE zstd
    );
"""

Q_MD_USERS_CLEAR = """
    DELETE FROM daily_multi_device_users{suffix}
    WHERE day >= '{day_from}'::DATE
    AND day <= '{day_until}'::DATE;
"""

Q_MD_USERS_SUMMARIZE = """
    INSERT INTO daily_multi_device_users{suffix} (day, uid, device_now, device_prev)
    SELECT DISTINCT present.day, present.uid, present.device_id, past.device_id
    FROM daily_activity_per_device{suffix} as present
    INNER JOIN daily_activity_per_device{suffix} as past
    ON
      present.uid = past.uid
      AND present.device_id != past.device_id
      AND past.day <= present.day
      AND past.day >= (present.day - '7 days'::INTERVAL)
    WHERE present.day >= '{day_from}'::DATE
    AND present.day <= '{day_until}'::DATE
    ORDER BY 1;
"""

Q_MD_USERS_EXPIRE = """
    DELETE FROM daily_multi_device_users{suffix}
    WHERE day < '{day_first}'::DATE;
"""

Q_GET_FIRST_AVAILABLE_DAY = """
    SELECT MIN(timestamp)::DATE
    FROM activity_events{suffix};
"""

Q_GET_FIRST_UNPROCESSED_DAY = """
    SELECT (MAX(day) + '1 day'::INTERVAL) AS timestamp
    FROM daily_multi_device_users{suffix};
"""

Q_GET_LAST_AVAILABLE_DAY = """
    SELECT MAX(timestamp)::DATE
    FROM activity_events{suffix};
"""

Q_VACUUM_TABLES = """
    END;
    VACUUM FULL daily_activity_per_device{suffix};
    ANALYZE daily_activity_per_device{suffix};
    VACUUM FULL daily_multi_device_users{suffix};
    ANALYZE daily_multi_device_users{suffix};
"""

def summarize_events():
    db = postgres.Postgres(DB)
    db.run("BEGIN TRANSACTION")
    try:
        for suffix in TABLE_SUFFIXES:
            db.run(Q_DAILY_DEVICES_CREATE_TABLE.format(suffix=suffix))
            db.run(Q_MD_USERS_CREATE_TABLE.format(suffix=suffix))
            day_first = db.one(Q_GET_FIRST_AVAILABLE_DAY.format(suffix=suffix))
            # Summarize the latest days that are not yet summarized.
            day_from = db.one(Q_GET_FIRST_UNPROCESSED_DAY.format(suffix=suffix))
            if day_from is None:
                day_from = day_first
                if day_from is None:
                    raise RuntimeError('no events in db')
            day_until = db.one(Q_GET_LAST_AVAILABLE_DAY.format(suffix=suffix))
            days = {
                "day_from": day_from,
                "day_until": day_until,
                "suffix": suffix
            }
            print "SUMMARIZING FROM", day_from, "UNTIL", day_until, "FOR SUFFIX", suffix
            # Update daily device activity.
            print "  UPDATING DAILY ACTIVE DEVICES SUMMARY"
            db.run(Q_DAILY_DEVICES_CLEAR.format(**days))
            db.run(Q_DAILY_DEVICES_SUMMARIZE.format(**days))
            # Update multi-device-user assessments.
            print "  UPDATING MULTI-DEVICE USERS SUMMARY"
            db.run(Q_MD_USERS_CLEAR.format(**days))
            db.run(Q_MD_USERS_SUMMARIZE.format(**days))
            # Expire old data
            print "EXPIRING", day_first, "FOR SUFFIX", suffix
            db.run(Q_DAILY_DEVICES_EXPIRE.format(suffix=suffix, day_first=day_first))
            db.run(Q_MD_USERS_EXPIRE.format(suffix=suffix, day_first=day_first))
    except:
        db.run("ROLLBACK TRANSACTION")
        raise
    else:
        db.run("COMMIT TRANSACTION")

    for suffix in TABLE_SUFFIXES:
        print "VACUUMING daily_activity_per_device{suffix} AND daily_multi_device_users{suffix}".format(suffix=suffix)
        db.run(Q_VACUUM_TABLES.format(suffix=suffix))

if __name__ == "__main__":
    summarize_events()
