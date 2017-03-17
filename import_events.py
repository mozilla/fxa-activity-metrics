from os import path
from datetime import datetime
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
DB_URI = "postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}".format(**CONFIG)

# The default data set automatically expires data at
# three months. We also have sampled data sets that
# cover a longer history.
SAMPLE_RATES = (
    {"percent":10, "months":24, "suffix":"_sampled_10"},
    {"percent":50, "months":6, "suffix":"_sampled_50"},
    {"percent":100, "months":3, "suffix":""}
)

Q_DROP_TEMPORARY_TABLE = """
    DROP TABLE IF EXISTS temporary_raw_{event_type}_data;
"""

Q_CREATE_EVENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS {event_type}_events{suffix} (
        timestamp TIMESTAMP NOT NULL SORTKEY ENCODE lzo,
        export_date DATE NOT NULL ENCODE lzo,
        {schema}
    );
"""

# Tables are maintained in unison, so we only check
# for days in the table with the longest history.
Q_CHECK_FOR_DAY = """
    SELECT timestamp FROM {event_type}_events_sampled_10
    WHERE timestamp::DATE = '{day}'::DATE
    LIMIT 1;
"""

Q_CREATE_CSV_TABLE = """
    CREATE TABLE IF NOT EXISTS temporary_raw_{event_type}_data (
        timestamp BIGINT NOT NULL SORTKEY,
        {schema}
    );
"""

Q_CLEAR_DAY = """
    DELETE FROM {event_type}_events{suffix}
    WHERE export_date::DATE = '{day}'::DATE;
"""

Q_COPY_CSV = """
    COPY temporary_raw_{event_type}_data (
        timestamp,
        {columns}
    )
    FROM '{s3_path}'
    CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
    FORMAT AS CSV
    TRUNCATECOLUMNS;
"""

Q_INSERT_EVENTS = """
    INSERT INTO {event_type}_events{suffix} (timestamp, {columns})
    SELECT ts, {columns}
    FROM (
        SELECT
            *,
            'epoch'::TIMESTAMP + timestamp * '1 second'::INTERVAL AS ts,
            STRTOL(SUBSTRING(uid FROM 0 FOR 8), 16) % 100 AS cohort
        FROM temporary_raw_{event_type}_data
    )
    WHERE cohort <= {percent}
    AND ts::DATE >= '{max_day}'::DATE - '{months} months'::INTERVAL;
"""

Q_GET_TIMESTAMP = """
    SELECT {which}(timestamp) FROM temporary_raw_{event_type}_data;
"""

Q_GET_MAX_DAY = """
    SELECT MAX(timestamp)::DATE FROM {event_type}_events;
"""

Q_DELETE_EVENTS = """
    DELETE FROM {event_type}_events{suffix}
    WHERE timestamp::DATE < '{day}'::DATE - '{months} months'::INTERVAL;
"""

Q_VACUUM_TABLES = """
    END;
    VACUUM FULL {event_type}_events{suffix};
"""

def run(s3_prefix, event_type, schema, columns, day_from=None, day_until=None):
    def drop_temporary_table():
        db.run(Q_DROP_TEMPORARY_TABLE.format(event_type=event_type))

    def create_events_tables():
        for rate in SAMPLE_RATES:
            db.run(Q_CREATE_EVENTS_TABLE.format(event_type=event_type,
                                                suffix=rate["suffix"],
                                                schema=schema))

    def get_max_day():
        return db.one(Q_GET_MAX_DAY.format(event_type=event_type))

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
            day = "-".join(filename[:-4].split("-")[1:])
            if is_candidate_day(day) and not is_day_populated(day):
                days.append(day)
        return days

    def begin_transaction():
        db.run("BEGIN TRANSACTION")

    def get_timestamp(which):
        return db.one(Q_GET_TIMESTAMP.format(which=which, event_type=event_type))

    def print_timestamp(which):
        print "  {which} TIMESTAMP".format(which=which), get_timestamp(which)

    def import_day(day):
        print "IMPORTING", day
        db.run(Q_CREATE_CSV_TABLE.format(event_type=event_type, schema=schema))
        for rate in SAMPLE_RATES:
            db.run(Q_CLEAR_DAY.format(event_type=event_type,
                                      suffix=rate["suffix"],
                                      day=day))
        s3_path = s3_uri.format(day=day)
        db.run(Q_COPY_CSV.format(event_type=event_type,
                                 columns=columns,
                                 s3_path=s3_path,
                                 **CONFIG))
        for rate in SAMPLE_RATES:
            db.run(Q_INSERT_EVENTS.format(event_type=event_type,
                                          columns=columns,
                                          suffix=rate["suffix"],
                                          percent=rate["percent"],
                                          max_day=max_day,
                                          months=rate["months"]))
        print_timestamp("MIN")
        print_timestamp("MAX")
        drop_temporary_table()

    def expire_events():
        max_day_after_import = get_max_day()
        for rate in SAMPLE_RATES:
            print "EXPIRING", max_day_after_import, "+", rate["months"], "MONTHS"
            db.run(Q_DELETE_EVENTS.format(event_type=event_type,
                                          suffix=rate["suffix"],
                                          day=max_day_after_import,
                                          months=rate["months"]))

    def rollback_transaction():
        db.run("ROLLBACK TRANSACTION")

    def commit_transaction():
        db.run("COMMIT TRANSACTION")

    def optimize_tables():
        for rate in SAMPLE_RATES:
            print "VACUUMING {event_type}_events{suffix}".format(event_type=event_type,
                                                                 suffix=rate["suffix"])
            db.run(Q_VACUUM_TABLES.format(event_type=event_type,
                                          suffix=rate["suffix"]))

    s3 = boto.s3.connect_to_region("us-east-1").get_bucket(S3_BUCKET)
    db = postgres.Postgres(DB_URI)
    s3_uri = "s3://" + S3_BUCKET + "/" + s3_prefix + "-{day}.csv"

    drop_temporary_table()
    create_events_tables()
    if not day_from:
        max_extant_day = get_max_day()
        if max_extant_day:
            day_from = datetime.strftime(max_extant_day, "%Y-%m-%d")
    unpopulated_days = get_unpopulated_days()
    unpopulated_days.sort()
    max_day = unpopulated_days[-1]
    print "IMPORTING {} DAYS OF DATA".format(len(unpopulated_days))
    begin_transaction()
    try:
        for day in unpopulated_days:
            import_day(day)
        expire_events()
    except:
        rollback_transaction()
        raise
    else:
        commit_transaction()
    optimize_tables()

