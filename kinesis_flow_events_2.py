#
# Script to import flow metrics from a temporary table.
#

import json
import postgres

with open("config.json") as f:
  CONFIG = json.loads(f.read())

DB_URI = "postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}".format(**CONFIG)

# The default data set automatically expires data at
# three months. We also have sampled data sets that
# cover a longer history.
SAMPLE_RATES = (
  {"percent":10, "months":24, "suffix":"_sampled_10"},
  {"percent":50, "months":6, "suffix":"_sampled_50"},
  {"percent":100, "months":3, "suffix":""}
)

TABLE_NAMES = {
  "temp":"kinesis_temporary_raw_flow_data",
  "events":"kinesis_flow_events{suffix}",
  "metadata":"kinesis_flow_metadata{suffix}",
  "experiments":"kinesis_flow_experiments{suffix}"
}

Q_CREATE_METADATA_TABLE = """
  CREATE TABLE IF NOT EXISTS {table_name} (
    flow_id VARCHAR(64) NOT NULL UNIQUE DISTKEY ENCODE zstd,
    begin_time TIMESTAMP NOT NULL SORTKEY ENCODE RAW,
    -- Ideally duration would be type INTERVAL
    -- but redshift doesn't support that.
    duration BIGINT NOT NULL DEFAULT 0 ENCODE zstd,
    completed BOOLEAN NOT NULL DEFAULT FALSE ENCODE zstd,
    new_account BOOLEAN NOT NULL DEFAULT FALSE ENCODE zstd,
    ua_browser VARCHAR(40) ENCODE zstd,
    ua_version VARCHAR(40) ENCODE zstd,
    ua_os VARCHAR(40) ENCODE zstd,
    context VARCHAR(40) ENCODE zstd,
    entrypoint VARCHAR(40) ENCODE zstd,
    migration VARCHAR(40) ENCODE zstd,
    service VARCHAR(40) ENCODE zstd,
    utm_campaign VARCHAR(40) ENCODE zstd,
    utm_content VARCHAR(40) ENCODE zstd,
    utm_medium VARCHAR(40) ENCODE zstd,
    utm_source VARCHAR(40) ENCODE zstd,
    utm_term VARCHAR(40) ENCODE zstd,
    locale VARCHAR(40) ENCODE zstd,
    uid VARCHAR(64) ENCODE zstd,
    continued_from VARCHAR(64) ENCODE zstd
  );
"""

# type is VARCHAR(79) so it can contain `flow.continued.${flow_id}`
Q_CREATE_EVENTS_TABLE = """
  CREATE TABLE IF NOT EXISTS {table_name} (
    timestamp TIMESTAMP NOT NULL SORTKEY ENCODE RAW,
    type VARCHAR(79) NOT NULL ENCODE zstd,
    flow_id VARCHAR(64) NOT NULL DISTKEY ENCODE zstd,
    flow_time BIGINT NOT NULL ENCODE zstd,
    locale VARCHAR(40) ENCODE zstd,
    uid VARCHAR(64) ENCODE zstd
  );
"""

Q_CREATE_EXPERIMENTS_TABLE = """
  CREATE TABLE IF NOT EXISTS {table_name} (
    experiment VARCHAR(40) NOT NULL DISTKEY ENCODE zstd,
    cohort VARCHAR(40) NOT NULL ENCODE zstd,
    timestamp TIMESTAMP NOT NULL SORTKEY ENCODE RAW,
    flow_id VARCHAR(64) NOT NULL ENCODE zstd,
    uid VARCHAR(64) ENCODE zstd
  );
"""

Q_INSERT_METADATA = """
  INSERT INTO {table_name} (
    flow_id,
    begin_time,
    ua_browser,
    ua_version,
    ua_os,
    context,
    entrypoint,
    migration,
    service,
    utm_campaign,
    utm_content,
    utm_medium,
    utm_source,
    utm_term
  )
  SELECT
    flow_id,
    'epoch'::TIMESTAMP + timestamp * '1 second'::INTERVAL,
    ua_browser,
    ua_version,
    ua_os,
    context,
    entrypoint,
    migration,
    service,
    utm_campaign,
    utm_content,
    utm_medium,
    utm_source,
    utm_term
  FROM (
    SELECT *, STRTOL(SUBSTRING(flow_id FROM 0 FOR 8), 16) % 100 AS sample
    FROM {temp_table}
  )
  WHERE sample <= {percent}
  AND type LIKE 'flow%begin';
""".format(table_name="{table_name}", temp_table=TABLE_NAMES["temp"], percent="{percent}")

Q_UPDATE_METADATA = """
  UPDATE {table_name}
  SET
    duration = events.flow_time,
    locale = events.locale,
    uid = events.uid
  FROM (
    SELECT flow_id, MAX(flow_time) AS flow_time, MAX(locale) AS locale, MAX(uid) AS uid
    FROM {temp_table}
    GROUP BY flow_id
  ) AS events
  WHERE {table_name}.flow_id = events.flow_id;
""".format(table_name="{table_name}", temp_table=TABLE_NAMES["temp"])

Q_UPDATE_COMPLETED = """
  UPDATE {table_name}
  SET completed = TRUE
  FROM (
    SELECT flow_id
    FROM {temp_table}
    WHERE type = 'flow.complete'
  ) AS complete
  WHERE {table_name}.flow_id = complete.flow_id;
""".format(table_name="{table_name}", temp_table=TABLE_NAMES["temp"])

Q_UPDATE_NEW_ACCOUNT = """
  UPDATE {table_name}
  SET new_account = TRUE
  FROM (
    SELECT flow_id
    FROM {temp_table}
    WHERE type = 'account.created'
  ) AS created
  WHERE {table_name}.flow_id = created.flow_id;
""".format(table_name="{table_name}", temp_table=TABLE_NAMES["temp"])

Q_UPDATE_CONTINUED_FROM = """
  UPDATE {table_name}
  SET continued_from = SUBSTRING(continued.type, 16, 64)
  FROM (
    SELECT flow_id, type
    FROM {temp_table}
    WHERE type LIKE 'flow.continued.%'
  ) AS continued
  WHERE {table_name}.flow_id = continued.flow_id;
""".format(table_name="{table_name}", temp_table=TABLE_NAMES["temp"])

Q_INSERT_EVENTS = """
  INSERT INTO {table_name} (timestamp, type, flow_id, flow_time, locale, uid)
  SELECT ts, type, flow_id, flow_time, locale, uid
  FROM (
    SELECT
      *,
      'epoch'::TIMESTAMP + timestamp * '1 second'::INTERVAL AS ts,
      STRTOL(SUBSTRING(flow_id FROM 0 FOR 8), 16) % 100 AS cohort
    FROM {temp_table}
  )
  WHERE cohort <= {percent}
  AND type NOT LIKE 'flow.continued.%';
""".format(table_name="{table_name}", temp_table=TABLE_NAMES["temp"], percent="{percent}")

Q_INSERT_EXPERIMENTS = """
  INSERT INTO {table_name} (
    experiment,
    cohort,
    timestamp,
    flow_id,
    uid
  )
  SELECT
    SPLIT_PART(type, '.', 3) AS experiment,
    SPLIT_PART(type, '.', 4) AS cohort,
    'epoch'::TIMESTAMP + timestamp * '1 second'::INTERVAL,
    flow_id,
    uid
  FROM (
    SELECT *, STRTOL(SUBSTRING(flow_id FROM 0 FOR 8), 16) % 100 AS sample
    FROM {temp_table}
  )
  WHERE sample <= {percent}
  AND type LIKE 'flow.experiment.%';
""".format(table_name="{table_name}", temp_table=TABLE_NAMES["temp"], percent="{percent}")

Q_UPDATE_EXPERIMENTS = """
  UPDATE {table_name}
  SET uid = events.uid
  FROM (
    SELECT flow_id, MAX(uid) AS uid
    FROM {temp_table}
    GROUP BY flow_id
  ) AS events
  WHERE {table_name}.flow_id = events.flow_id;
""".format(table_name="{table_name}", temp_table=TABLE_NAMES["temp"])

Q_GET_MAX_TIME = """
  SELECT MAX(begin_time) FROM {table_name}
"""

Q_EXPIRE = """
  DELETE FROM {table_name}
  WHERE {column} < '{max_time}'::TIMESTAMP - '{months} months'::INTERVAL;
"""

Q_VACUUM = """
  END;
  VACUUM FULL {table_name};
  ANALYZE {table_name};
"""

def expire(table_name, max_time, months, column="timestamp"):
  print "  EXPIRING FOR", max_time, "+", months, "MONTHS"
  db.run(Q_EXPIRE.format(table_name=table_name, column=column, max_time=max_time, months=months))

def vacuum(table_name):
  print "  VACUUMING AND ANALYZING"
  db.run(Q_VACUUM.format(table_name=table_name))

db = postgres.Postgres(DB_URI)

for rate in SAMPLE_RATES:
  suffix = rate["suffix"]
  months = rate["months"]
  percent = rate["percent"]

  table_name = TABLE_NAMES["metadata"].format(suffix=suffix)
  print table_name
  db.run(Q_CREATE_METADATA_TABLE.format(table_name=table_name))
  print "  INSERTING"
  db.run(Q_INSERT_METADATA.format(table_name=table_name, percent=percent))
  print "  UPDATING"
  db.run(Q_UPDATE_METADATA.format(table_name=table_name))
  db.run(Q_UPDATE_COMPLETED.format(table_name=table_name))
  db.run(Q_UPDATE_NEW_ACCOUNT.format(table_name=table_name))
  db.run(Q_UPDATE_CONTINUED_FROM.format(table_name=table_name))
  max_time = db.one(Q_GET_MAX_TIME.format(table_name=table_name))
  expire(table_name, max_time, months, column="begin_time")
  vacuum(table_name)

  table_name = TABLE_NAMES["events"].format(suffix=suffix)
  print table_name
  db.run(Q_CREATE_EVENTS_TABLE.format(table_name=table_name))
  print "  INSERTING"
  db.run(Q_INSERT_EVENTS.format(table_name=table_name, percent=percent))
  expire(table_name, max_time, months)
  vacuum(table_name)

  table_name = TABLE_NAMES["experiments"].format(suffix=suffix)
  print table_name
  db.run(Q_CREATE_EXPERIMENTS_TABLE.format(table_name=table_name))
  print "  INSERTING"
  db.run(Q_INSERT_EXPERIMENTS.format(table_name=table_name, percent=percent))
  print "  UPDATING"
  db.run(Q_UPDATE_EXPERIMENTS.format(table_name=table_name))
  expire(table_name, max_time, months)
  vacuum(table_name)

