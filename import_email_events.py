#
# Script to import "email event" metrics from S3 into redshift.
#

import import_events
SCHEMA = """
    timestamp BIGINT NOT NULL SORTKEY,
    type VARCHAR(30) NOT NULL,
    locale VARCHAR(40),
    domain VARCHAR(40),
    template VARCHAR(64)
    flow_id VARCHAR(64)
"""

COLUMNS = "timestamp, type, locale, domain, template, flow_id"

import_events.run("fxa-email", "email", SCHEMA, COLUMNS, SCHEMA, COLUMNS)

