#
# Script to import "activity event" metrics from S3 into redshift.
#

import import_events

SCHEMA = """
    uid VARCHAR(64) NOT NULL DISTKEY ENCODE lzo,
    type VARCHAR(30) NOT NULL ENCODE lzo,
    device_id VARCHAR(32) ENCODE lzo,
    service VARCHAR(40) ENCODE lzo,
    ua_browser VARCHAR(40) ENCODE lzo,
    ua_version VARCHAR(40) ENCODE lzo,
    ua_os VARCHAR(40) ENCODE lzo
"""

COLUMNS = "ua_browser, ua_version, ua_os, uid, type, service, device_id"

import_events.run(s3_prefix="fxa-retention/data/events",
                  event_type="activity",
                  temp_schema=SCHEMA,
                  temp_columns=COLUMNS,
                  perm_schema=SCHEMA,
                  perm_columns=COLUMNS)

