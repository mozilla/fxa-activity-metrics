from os import path
from datetime import datetime
import json
import boto.s3
import boto.provider
import postgres
import os

REDSHIFT_USER = os.environ["REDSHIFT_USER"]
REDSHIFT_PASSWORD = os.environ["REDSHIFT_PASSWORD"]
REDSHIFT_HOST = os.environ["REDSHIFT_HOST"]
REDSHIFT_PORT = os.environ["REDSHIFT_PORT"]
REDSHIFT_DBNAME = os.environ["REDSHIFT_DBNAME"]

DB_URI = "postgresql://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DBNAME}".format(
    REDSHIFT_USER=REDSHIFT_USER, REDSHIFT_PASSWORD=REDSHIFT_PASSWORD, REDSHIFT_HOST=REDSHIFT_HOST,
    REDSHIFT_PORT=REDSHIFT_PORT, REDSHIFT_DBNAME=REDSHIFT_DBNAME
)

def env_or_default(variable_name, default_value):
    if variable_name in os.environ:
        return os.environ[variable_name]

    return default_value

AWS_IAM_ROLE = env_or_default('AWS_IAM_ROLE', None)
if AWS_IAM_ROLE is not None:
    CREDENTIALS="aws_iam_role={AWS_IAM_ROLE}".format(AWS_IAM_ROLE=AWS_IAM_ROLE)
else:
    aws = boto.provider.Provider("aws")
    AWS_ACCESS_KEY = env_or_default("AWS_ACCESS_KEY", aws.get_access_key())
    AWS_SECRET_KEY = env_or_default("AWS_SECRET_KEY", aws.get_secret_key())
    CREDENTIALS="aws_access_key_id={AWS_ACCESS_KEY};aws_secret_access_key={AWS_SECRET_KEY}".format(
        AWS_ACCESS_KEY=AWS_ACCESS_KEY,
        AWS_SECRET_KEY=AWS_SECRET_KEY
    )

S3_REGION = "us-west-2"
S3_BUCKET = "net-mozaws-prod-us-west-2-pipeline-analysis"
S3_PREFIX = "fxa-basic-metrics/"
S3_URI = "s3://" + S3_BUCKET + "/" + S3_PREFIX + "fxa-basic-metrics-{day}.txt"

COUNTS_BEGIN = datetime.strptime("2017-05-30", "%Y-%m-%d")

Q_DROP_CSV_TABLE = "DROP TABLE IF EXISTS temporary_raw_counts;"

Q_CREATE_COUNTS_TABLE = """
    CREATE TABLE IF NOT EXISTS counts (
      day DATE NOT NULL UNIQUE SORTKEY ENCODE RAW,
      accounts BIGINT NOT NULL ENCODE ZSTD,
      verified_accounts BIGINT NOT NULL ENCODE ZSTD
    );
"""

Q_CHECK_FOR_DAY = """
    SELECT day FROM counts
    WHERE day = '{day}';
"""

Q_CREATE_CSV_TABLE = """
    CREATE TABLE IF NOT EXISTS temporary_raw_counts (
      day CHAR(10) NOT NULL UNIQUE SORTKEY,
      accounts BIGINT NOT NULL,
      verified_accounts BIGINT NOT NULL
    );
"""

Q_CLEAR_DAY = """
    DELETE FROM counts
    WHERE day = '{day}';
"""

Q_COPY_CSV = """
    COPY temporary_raw_counts (day, accounts, verified_accounts)
    FROM '{s3_uri}'
    CREDENTIALS '{CREDENTIALS}'
    FORMAT AS CSV
    MAXERROR AS 10
    TRUNCATECOLUMNS;
""".format(s3_uri=S3_URI, CREDENTIALS=CREDENTIALS)

Q_INSERT_COUNTS = """
    INSERT INTO counts (day, accounts, verified_accounts)
    SELECT day::DATE, accounts, verified_accounts
    FROM temporary_raw_counts;
"""

Q_VACUUM_COUNTS = """
    END;
    VACUUM FULL counts;
    ANALYZE counts;
"""

def import_events(force_reload=False):
    s3 = boto.s3.connect_to_region(S3_REGION).get_bucket(S3_BUCKET)
    db = postgres.Postgres(DB_URI)
    db.run(Q_DROP_CSV_TABLE)
    db.run(Q_CREATE_COUNTS_TABLE)
    days = []
    for key in s3.list(prefix=S3_PREFIX):
        filename = path.basename(key.name)
        day = "-".join(filename[:-4].split("-")[-3:])
        date = datetime.strptime(day, "%Y-%m-%d")
        if date >= COUNTS_BEGIN:
            if force_reload:
                days.append(day)
            else:
                if not db.one(Q_CHECK_FOR_DAY.format(day=day)):
                    days.append(day)
    days.sort(reverse=True)
    print "FOUND", len(days),  "DAYS"
    for day in days:
        print day
        print "  COPYING CSV"
        db.run(Q_CREATE_CSV_TABLE)
        db.run(Q_COPY_CSV.format(day=day))
        print "  CLEARING"
        db.run(Q_CLEAR_DAY.format(day=day))
        print "  INSERTING"
        db.run(Q_INSERT_COUNTS)
        db.run(Q_DROP_CSV_TABLE)
    print "VACUUMING"
    db.run(Q_VACUUM_COUNTS)

if __name__ == "__main__":
    import_events()
