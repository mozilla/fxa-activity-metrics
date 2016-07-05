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
# we maintain a table giving (day, uid, userAgentOS).
# In the future this would use device-id rather than userAgentOS.

Q_DAILY_DEVICES_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS daily_activity_per_device (
      day BIGINT NOT NULL sortkey, 
      uid VARCHAR(64) distkey,
      userAgentOS VARCHAR(30)
    );
"""

Q_DAILY_DEVICES_CLEAR = """
  DELETE FROM daily_activity_per_device
  WHERE day >= {timestamp_min}
  AND day < {timestamp_max}
"""

Q_DAILY_DEVICES_SUMMARIZE = """
  INSERT INTO daily_activity_per_device (day, uid, userAgentOS)
  SELECT DISTINCT ((timestamp / 86400) * 86400) as day, uid, userAgentOS
  FROM events
  WHERE userAgentOS != ''
  AND timestamp >= {timestamp_min}
  AND timestamp < {timestamp_max}
  ORDER BY 1
"""

# For the daily multi-device-users summary,
# we maintain a table of (day, uid) pairs
# based on whether that user had multiple devices
# active in the last five days.


Q_MD_USERS_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS daily_multi_device_users (
      day BIGINT NOT NULL sortkey, 
      uid VARCHAR(64) distkey
    );
"""

Q_MD_USERS_CLEAR = """
  DELETE FROM daily_multi_device_users
  WHERE day >= {timestamp_min}
  AND day < {timestamp_max}
"""

Q_MD_USERS_SUMMARIZE = """
  INSERT INTO daily_multi_device_users (day, uid)
  SELECT DISTINCT aPresent.day, aPresent.uid
  FROM daily_activity_per_device as aPresent
  INNER JOIN daily_activity_per_device as aPast
  ON
    aPresent.uid = aPast.uid
    AND aPresent.userAgentOS != aPast.userAgentOS
    AND aPast.day <= aPresent.day
    AND aPast.day >= (aPresent.day - (86400 * 5))
  WHERE aPresent.day >= {timestamp_min}
  AND aPresent.day < {timestamp_max}
  AND aPast.day < {timestamp_max}
  AND aPast.day >= ({timestamp_min} - (86400 * 5))
  ORDER BY 1
"""


def summarize_events(timestamp_max=None, timestamp_min=None):
    db = postgres.Postgres(DB)
    db.run("BEGIN TRANSACTION")
    try:
        db.run(Q_DAILY_DEVICES_CREATE_TABLE)
        db.run(Q_MD_USERS_CREATE_TABLE)
        # By default, summarize the latest days that are not yet summarized.
        if timestamp_max is None:
            timestamp_max = db.one("SELECT MAX(timestamp) FROM events")
            if timestamp_max is None:
                raise RuntimeError('no events in db')
        if timestamp_min is None:
            timestamp_min = db.one("SELECT MAX(day) + 86400 AS timestamp FROM daily_multi_device_users")

            if timestamp_min is None:
                timestamp_min = db.one("SELECT MIN(timestamp) FROM events")
        # Round up/down to nearest whole day to ensure consistency.
        timestamp_max = ((timestamp_max // 86400) + 1) * 86400
        timestamp_min = (timestamp_min // 86400) * 86400
        params = {
            "timestamp_max": timestamp_max,
            "timestamp_min": timestamp_min,
        }
        print "SUMMARIZING", (timestamp_max - timestamp_min) / 86400, "DAYS"
        # Update daily device activity.
        print "UPDATING DAILY ACTIVE DEVICES SUMMARY"
        db.run(Q_DAILY_DEVICES_CLEAR.format(**params))
        db.run(Q_DAILY_DEVICES_SUMMARIZE.format(**params))
        # Update multi-device-user assessments.
        print "UPDATING MULTI-DEVICE USERS SUMMARY"
        db.run(Q_MD_USERS_CLEAR.format(**params))
        db.run(Q_MD_USERS_SUMMARIZE.format(**params))
    except:
        db.run("ROLLBACK TRANSACTION")
        raise
    else:
        db.run("COMMIT TRANSACTION")


if __name__ == "__main__":
    summarize_events()
