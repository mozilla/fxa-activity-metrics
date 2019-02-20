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

S3_BUCKET = "net-mozaws-prod-us-west-2-pipeline-analysis"

# The default data set automatically expires data at
# three months. We also have sampled data sets that
# cover a longer history.
SAMPLE_RATES = (
    {"percent":10, "months":24, "suffix":"_sampled_10"},
    {"percent":50, "months":6, "suffix":"_sampled_50"},
    {"percent":100, "months":3, "suffix":""}
)

# The temporary table receives raw data from S3.
# The permenant table then receives data appropriately typed.
TABLE_NAMES = {
    "temp":"temporary_raw_{event_type}_data",
    "perm":"{event_type}_events{suffix}"
}

Q_DROP_TEMPORARY_TABLE = """
    DROP TABLE IF EXISTS {table};
""".format(table=TABLE_NAMES["temp"])

Q_CREATE_EVENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS {table} (
        timestamp TIMESTAMP NOT NULL SORTKEY ENCODE RAW,
        {schema}
    );
""".format(table=TABLE_NAMES["perm"], schema="{schema}")

Q_GET_MAX_DAY = """
    SELECT MAX(timestamp)::DATE FROM {table};
""".format(table=TABLE_NAMES["perm"].format(event_type="{event_type}",
                                            suffix=""))

Q_CHECK_FOR_DAY = """
    SELECT timestamp FROM {table}
    WHERE timestamp::DATE = '{day}'::DATE
    LIMIT 1;
""".format(table=TABLE_NAMES["perm"].format(event_type="{event_type}",
                                            suffix="_sampled_10"),
           day="{day}")

Q_CREATE_CSV_TABLE = """
    CREATE TEMPORARY TABLE IF NOT EXISTS {table} (
        timestamp BIGINT NOT NULL SORTKEY,
        {schema}
    );
""".format(table=TABLE_NAMES["temp"], schema="{schema}")

Q_COPY_CSV = """
    COPY {table} (
        timestamp,
        {columns}
    )
    FROM '{s3_path}'
    CREDENTIALS '{CREDENTIALS}'
    FORMAT AS CSV
    MAXERROR AS 100
    TRUNCATECOLUMNS;
""".format(table=TABLE_NAMES["temp"],
           columns="{columns}",
           s3_path="{s3_path}",
           CREDENTIALS="{CREDENTIALS}")

Q_CLEAR_DAY = """
    DELETE FROM {table}
    WHERE timestamp::DATE = '{day}'::DATE;
""".format(table=TABLE_NAMES["perm"], day="{day}")

Q_INSERT_EVENTS = """
    INSERT INTO {perm_table} (timestamp, {columns})
    SELECT ts, {columns}
    FROM (
        SELECT
            *,
            'epoch'::TIMESTAMP + timestamp * '1 second'::INTERVAL AS ts,
            STRTOL(SUBSTRING({id_column} FROM 0 FOR 8), 16) % 100 AS cohort
        FROM {temp_table}
    )
    WHERE cohort < {percent}
    AND ts::DATE = '{day}'::DATE
    AND ts::DATE >= '{max_day}'::DATE - '{months} months'::INTERVAL;
""".format(perm_table=TABLE_NAMES["perm"],
           temp_table=TABLE_NAMES["temp"],
           columns="{columns}",
           id_column="{id_column}",
           percent="{percent}",
           day="{day}",
           max_day="{max_day}",
           months="{months}")

Q_GET_TIMESTAMP = """
    SELECT {which}(timestamp) FROM {table};
""".format(which="{which}", table=TABLE_NAMES["temp"])

Q_DELETE_EVENTS = """
    DELETE FROM {table}
    WHERE timestamp::DATE < '{day}'::DATE - '{months} months'::INTERVAL;
""".format(table=TABLE_NAMES["perm"], day="{day}", months="{months}")

Q_VACUUM_TABLES = """
    END;
    VACUUM FULL {table};
    ANALYZE {table};
""".format(table=TABLE_NAMES["perm"])

def nop(*args):
    pass

def run(s3_prefix, event_type, temp_schema, temp_columns, perm_schema, perm_columns, id_column="uid",
        before_import=nop, after_day=nop, after_import=nop, day_from=None, day_until=None):

    def drop_temporary_table():
        db.run(Q_DROP_TEMPORARY_TABLE.format(event_type=event_type))

    def create_events_tables():
        for rate in SAMPLE_RATES:
            db.run(Q_CREATE_EVENTS_TABLE.format(event_type=event_type,
                                                suffix=rate["suffix"],
                                                schema=perm_schema))

    def get_max_day():
        result = db.one(Q_GET_MAX_DAY.format(event_type=event_type))
        if result:
            return datetime.strftime(result, "%Y-%m-%d")
        return result

    def is_candidate_day(day):
        return (not day_from or day_from <= day) and (not day_until or day_until >= day)

    def is_day_populated(day):
        return bool(db.one(Q_CHECK_FOR_DAY.format(event_type=event_type, day=day)))

    def get_unpopulated_days():
        days = []
        message = "FINDING UNPOPULATED DAYS"
        if day_from:
            message += " FROM {day_from}".format(day_from=day_from)
        if day_until:
            message += " UNTIL {day_until}".format(day_until=day_until)
        print message
        for key in s3.list(prefix=s3_prefix):
            filename = path.basename(key.name)
            # Ignore the last four characters (".csv") then join the last
            # three parts ("YYYY-MM-DD") of the hyphen-split string.
            day = "-".join(filename[:-4].split("-")[-3:])
            if is_candidate_day(day) and not is_day_populated(day):
                days.append(day)
        return days

    def get_timestamp(which):
        return db.one(Q_GET_TIMESTAMP.format(which=which, event_type=event_type))

    def print_timestamp(which):
        print "  {which} timestamp".format(which=which), get_timestamp(which)

    def import_day(day):
        print day
        print "  COPYING CSV"
        db.run(Q_CREATE_CSV_TABLE.format(event_type=event_type, schema=temp_schema))
        s3_path = s3_uri.format(day=day)
        db.run(Q_COPY_CSV.format(event_type=event_type,
                                 columns=temp_columns,
                                 s3_path=s3_path,
                                 CREDENTIALS=CREDENTIALS))
        print_timestamp("MIN")
        print_timestamp("MAX")
        for rate in SAMPLE_RATES:
            print " ", TABLE_NAMES["perm"].format(event_type=event_type, suffix=rate["suffix"])
            print "    CLEARING"
            db.run(Q_CLEAR_DAY.format(event_type=event_type,
                                      suffix=rate["suffix"],
                                      day=day))
            print "    INSERTING"
            db.run(Q_INSERT_EVENTS.format(event_type=event_type,
                                          columns=perm_columns,
                                          id_column=id_column,
                                          suffix=rate["suffix"],
                                          percent=rate["percent"],
                                          day=day,
                                          max_day=max_day,
                                          months=rate["months"]))
        after_day(db, day,
                  TABLE_NAMES["temp"].format(event_type=event_type),
                  TABLE_NAMES["perm"].format(event_type=event_type, suffix="{suffix}"),
                  SAMPLE_RATES)
        drop_temporary_table()

    def expire_events():
        for rate in SAMPLE_RATES:
            table_name = TABLE_NAMES["perm"].format(event_type=event_type, suffix=rate["suffix"])
            print "EXPIRING", table_name, "FOR", max_day, "+", rate["months"], "MONTHS"
            db.run(Q_DELETE_EVENTS.format(event_type=event_type,
                                          suffix=rate["suffix"],
                                          day=max_day,
                                          months=rate["months"]))
            print "VACUUMING AND ANALYZING", table_name
            db.run(Q_VACUUM_TABLES.format(event_type=event_type,
                                          suffix=rate["suffix"]))

    s3 = boto.s3.connect_to_region("us-west-2").get_bucket(S3_BUCKET)
    db = postgres.Postgres(DB_URI)
    s3_uri = "s3://" + S3_BUCKET + "/" + s3_prefix + "-{day}.csv"

    before_import(db, SAMPLE_RATES)
    drop_temporary_table()
    create_events_tables()
    max_extant_day = get_max_day()
    if not day_from:
        day_from = max_extant_day
    unpopulated_days = get_unpopulated_days()
    unpopulated_days.sort(reverse=True)
    if max_extant_day > unpopulated_days[0]:
        max_day = max_extant_day
    else:
        max_day = unpopulated_days[0]
    print "FOUND", len(unpopulated_days), "DAYS"
    for day in unpopulated_days:
        import_day(day)
    expire_events()
    after_import(db, SAMPLE_RATES, max_day)

