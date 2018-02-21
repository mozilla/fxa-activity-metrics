#
# Script to copy flow metrics from CSV to a temporary table
#

import json
import boto.s3
import boto.provider
import postgres

with open("config.json") as f:
  CONFIG = json.loads(f.read())

if "aws_access_key_id" not in CONFIG:
  aws = boto.provider.Provider("aws")
  CONFIG["aws_access_key_id"] = aws.get_access_key()
  CONFIG["aws_secret_access_key"] = aws.get_secret_key()

S3_BUCKET = "net-mozaws-prod-us-west-2-pipeline-analysis"
S3_URI = "s3://" + S3_BUCKET + "/fxa-flow/data/flow-{day}.csv"
DB_URI = "postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}".format(**CONFIG)

TABLE_NAME = "kinesis_temporary_raw_flow_data"

DAYS = ("2018-02-12", "2018-02-13", "2018-02-14", "2018-02-15", "2018-02-16", "2018-02-17", "2018-02-18")

# flow_id is VARCHAR(64) because it's 32 bytes hex-encoded
# type is VARCHAR(79) so it can contain `flow.continued.${flow_id}`
Q_CREATE_TABLE = """
  CREATE TABLE IF NOT EXISTS {table} (
    timestamp BIGINT NOT NULL SORTKEY,
    type VARCHAR(79) NOT NULL ENCODE zstd,
    flow_id VARCHAR(64) NOT NULL DISTKEY ENCODE zstd,
    flow_time BIGINT NOT NULL ENCODE zstd,
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
    uid VARCHAR(64) ENCODE zstd
  );
""".format(table=TABLE_NAME)

Q_CLEAR_TABLE = """
  DELETE FROM {table};
""".format(table=TABLE_NAME)

Q_COPY_CSV = """
  COPY {table} (
    timestamp,
    type,
    flow_id,
    flow_time,
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
    utm_term,
    locale,
    uid
  )
  FROM '{s3_path}'
  CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
  FORMAT AS CSV
  TRUNCATECOLUMNS;
""".format(table=TABLE_NAME,
           s3_path="{s3_path}",
           aws_access_key_id="{aws_access_key_id}",
           aws_secret_access_key="{aws_secret_access_key}")

Q_GET_TIMESTAMP = """
  SELECT {which}(timestamp) FROM {table};
""".format(which="{which}", table=TABLE_NAME)

def import_day(day):
  print day
  db.run(Q_COPY_CSV.format(s3_path=S3_URI.format(day=day), **CONFIG))

def get_timestamp(which):
  return db.one(Q_GET_TIMESTAMP.format(which=which))

def print_timestamp(which):
  print "{which} timestamp".format(which=which), get_timestamp(which)

s3 = boto.s3.connect_to_region("us-east-1").get_bucket(S3_BUCKET)
db = postgres.Postgres(DB_URI)

db.run(Q_CREATE_TABLE)
db.run(Q_CLEAR_TABLE)

for day in DAYS:
  import_day(day)

print_timestamp("MIN")
print_timestamp("MAX")

