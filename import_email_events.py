#
# Script to import "email event" metrics from S3 into redshift.
#

import import_events
SCHEMA = """
    flow_id VARCHAR(64) DISTKEY ENCODE zstd,
    domain VARCHAR(40) ENCODE zstd,
    template VARCHAR(64) ENCODE zstd,
    type VARCHAR(64) NOT NULL ENCODE zstd,
    bounced VARCHAR(64) ENCODE zstd,
    complaint VARCHAR(64) ENCODE zstd,
    locale VARCHAR(64) ENCODE zstd
"""

COLUMNS = "flow_id, domain, template, type, bounced, complaint, locale"

import_events.run(s3_prefix="fxa-email/data/email-events",
                  event_type="email",
                  temp_schema=SCHEMA,
                  temp_columns=COLUMNS,
                  perm_schema=SCHEMA,
                  perm_columns=COLUMNS,
                  id_column="flow_id")
