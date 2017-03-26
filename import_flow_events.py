#
# Script to import "flow event" metrics from S3 into redshift.
#

import import_events

TEMPORARY_SCHEMA = """
    type VARCHAR(64) NOT NULL ENCODE lzo,
    flow_id VARCHAR(64) NOT NULL DISTKEY ENCODE lzo,
    flow_time BIGINT NOT NULL ENCODE lzo,
    ua_browser VARCHAR(40) ENCODE lzo,
    ua_version VARCHAR(40) ENCODE lzo,
    ua_os VARCHAR(40) ENCODE lzo,
    context VARCHAR(40) ENCODE lzo,
    entrypoint VARCHAR(40) ENCODE lzo,
    migration VARCHAR(40) ENCODE lzo,
    service VARCHAR(40) ENCODE lzo,
    utm_campaign VARCHAR(40) ENCODE lzo,
    utm_content VARCHAR(40) ENCODE lzo,
    utm_medium VARCHAR(40) ENCODE lzo,
    utm_source VARCHAR(40) ENCODE lzo,
    utm_term VARCHAR(40) ENCODE lzo,
    locale VARCHAR(40) ENCODE lzo,
    uid VARCHAR(64) ENCODE lzo
"""

TEMPORARY_COLUMNS = """
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
"""

EVENT_SCHEMA = """
    type VARCHAR(64) NOT NULL ENCODE lzo,
    flow_id VARCHAR(64) NOT NULL DISTKEY ENCODE lzo,
    flow_time BIGINT NOT NULL ENCODE lzo,
    locale VARCHAR(40) ENCODE lzo,
    uid VARCHAR(64) ENCODE lzo
"""

EVENT_COLUMNS = """
    type,
    flow_id,
    flow_time,
    locale,
    uid
"""

Q_CREATE_METADATA_TABLE = """
    CREATE TABLE IF NOT EXISTS flow_metadata{suffix} (
      flow_id VARCHAR(64) NOT NULL UNIQUE DISTKEY ENCODE lzo,
      begin_time TIMESTAMP NOT NULL SORTKEY ENCODE lzo,
      -- Ideally duration would be type INTERVAL
      -- but redshift doesn't support that.
      duration BIGINT NOT NULL DEFAULT 0 ENCODE lzo,
      completed BOOLEAN NOT NULL DEFAULT FALSE ENCODE raw,
      new_account BOOLEAN NOT NULL DEFAULT FALSE ENCODE raw,
      ua_browser VARCHAR(40) ENCODE lzo,
      ua_version VARCHAR(40) ENCODE lzo,
      ua_os VARCHAR(40) ENCODE lzo,
      context VARCHAR(40) ENCODE lzo,
      entrypoint VARCHAR(40) ENCODE lzo,
      migration VARCHAR(40) ENCODE lzo,
      service VARCHAR(40) ENCODE lzo,
      utm_campaign VARCHAR(40) ENCODE lzo,
      utm_content VARCHAR(40) ENCODE lzo,
      utm_medium VARCHAR(40) ENCODE lzo,
      utm_source VARCHAR(40) ENCODE lzo,
      utm_term VARCHAR(40) ENCODE lzo,
      export_date DATE NOT NULL ENCODE lzo,
      locale VARCHAR(40) ENCODE lzo,
      uid VARCHAR(64) ENCODE lzo
    );
"""

Q_CREATE_EXPERIMENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS flow_experiments{suffix} (
      experiment VARCHAR(40) NOT NULL DISTKEY ENCODE lzo,
      cohort VARCHAR(40) NOT NULL ENCODE lzo,
      timestamp TIMESTAMP NOT NULL SORTKEY ENCODE lzo,
      flow_id VARCHAR(64) NOT NULL ENCODE lzo,
      uid VARCHAR(64) ENCODE lzo,
      export_date DATE NOT NULL ENCODE lzo
    );
"""

Q_CLEAR_DAY = """
    DELETE FROM flow_{table}{suffix}
    WHERE export_date = '{day}';
"""

Q_INSERT_METADATA = """
    INSERT INTO flow_metadata{suffix} (
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
      utm_term,
      export_date
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
      utm_term,
      '{day}'::DATE
    FROM (
      SELECT *, STRTOL(SUBSTRING(flow_id FROM 0 FOR 8), 16) % 100 AS sample
      FROM {table_name}
    )
    WHERE sample <= {percent}
    AND type LIKE 'flow%begin';
"""

Q_UPDATE_METADATA = """
    UPDATE flow_metadata{suffix}
    SET
      duration = events.flow_time,
      locale = events.locale,
      uid = events.uid
    FROM (
      SELECT
        flow_id,
        MAX(flow_time) AS flow_time,
        MAX(locale) AS locale,
        MAX(uid) AS uid
      FROM {table_name}
      WHERE {table_name}.timestamp::DATE = '{day}'
        OR {table_name}.timestamp::DATE = '{day}'::DATE + '1 day'::INTERVAL
      GROUP BY flow_id
    ) AS events
    WHERE flow_metadata{suffix}.flow_id = events.flow_id;
"""

Q_UPDATE_COMPLETED = """
    UPDATE flow_metadata{suffix}
    SET completed = TRUE
    FROM (
      SELECT flow_id
      FROM {table_name}
      WHERE type = 'flow.complete'
        AND (
          {table_name}.timestamp::DATE = '{day}'
          OR {table_name}.timestamp::DATE = '{day}'::DATE + '1 day'::INTERVAL
        )
    ) AS complete
    WHERE flow_metadata{suffix}.flow_id = complete.flow_id;
"""

Q_UPDATE_NEW_ACCOUNT = """
    UPDATE flow_metadata{suffix}
    SET new_account = TRUE
    FROM (
      SELECT flow_id
      FROM {table_name}
      WHERE type = 'account.created'
        AND (
          {table_name}.timestamp::DATE = '{day}'
          OR {table_name}.timestamp::DATE = '{day}'::DATE + '1 day'::INTERVAL
        )
    ) AS created
    WHERE flow_metadata{suffix}.flow_id = created.flow_id;
"""

Q_UPDATE_METRICS_CONTEXT = """
    UPDATE flow_metadata{suffix}
    SET
      -- See https://github.com/mozilla/fxa-content-server/issues/4135
      context = (CASE WHEN flow_metadata{suffix}.context = '' THEN metrics_context.context ELSE flow_metadata{suffix}.context END),
      entrypoint = (CASE WHEN flow_metadata{suffix}.entrypoint = '' THEN metrics_context.entrypoint ELSE flow_metadata{suffix}.entrypoint END),
      migration = (CASE WHEN flow_metadata{suffix}.migration = '' THEN metrics_context.migration ELSE flow_metadata{suffix}.migration END),
      service = (CASE WHEN flow_metadata{suffix}.service = '' THEN metrics_context.service ELSE flow_metadata{suffix}.service END),
      utm_campaign = (CASE WHEN flow_metadata{suffix}.utm_campaign = '' THEN metrics_context.utm_campaign ELSE flow_metadata{suffix}.utm_campaign END),
      utm_content = (CASE WHEN flow_metadata{suffix}.utm_content = '' THEN metrics_context.utm_content ELSE flow_metadata{suffix}.utm_content END),
      utm_medium = (CASE WHEN flow_metadata{suffix}.utm_medium = '' THEN metrics_context.utm_medium ELSE flow_metadata{suffix}.utm_medium END),
      utm_source = (CASE WHEN flow_metadata{suffix}.utm_source = '' THEN metrics_context.utm_source ELSE flow_metadata{suffix}.utm_source END),
      utm_term = (CASE WHEN flow_metadata{suffix}.utm_term = '' THEN metrics_context.utm_term ELSE flow_metadata{suffix}.utm_term END)
    FROM (
      SELECT
        flow_id,
        MAX(context) AS context,
        MAX(entrypoint) AS entrypoint,
        MAX(migration) AS migration,
        MAX(service) AS service,
        MAX(utm_campaign) AS utm_campaign,
        MAX(utm_content) AS utm_content,
        MAX(utm_medium) AS utm_medium,
        MAX(utm_source) AS utm_source,
        MAX(utm_term) AS utm_term
      FROM (
        SELECT *, STRTOL(SUBSTRING(flow_id FROM 0 FOR 8), 16) % 100 AS sample
        FROM {table_name}
      )
      WHERE sample <= {percent}
      GROUP BY flow_id
    ) AS metrics_context
    WHERE flow_metadata{suffix}.flow_id = metrics_context.flow_id;
"""

Q_INSERT_EXPERIMENTS = """
    INSERT INTO flow_experiments{suffix} (
      experiment,
      cohort,
      timestamp,
      flow_id,
      uid,
      export_date
    )
    SELECT
      SPLIT_PART(type, '.', 3) AS experiment,
      SPLIT_PART(type, '.', 4) AS cohort,
      'epoch'::TIMESTAMP + timestamp * '1 second'::INTERVAL,
      flow_id,
      uid,
      '{day}'::DATE
    FROM (
      SELECT *, STRTOL(SUBSTRING(flow_id FROM 0 FOR 8), 16) % 100 AS sample
      FROM {table_name}
    )
    WHERE sample <= {percent}
    AND type LIKE 'flow.experiment%';
"""

Q_UPDATE_EXPERIMENTS = """
    UPDATE flow_experiments{suffix}
    SET uid = events.uid
    FROM (
      SELECT flow_id, MAX(uid) AS uid
      FROM {table_name}
      WHERE {table_name}.timestamp::DATE = '{day}'
        OR {table_name}.timestamp::DATE = '{day}'::DATE + '1 day'::INTERVAL
      GROUP BY flow_id
    ) AS events
    WHERE flow_experiments{suffix}.flow_id = events.flow_id;
"""

Q_EXPIRE = """
    DELETE FROM {table_name}
    WHERE export_date < '{max_day}'::DATE - '{months} months'::INTERVAL;
"""

Q_VACUUM = """
    END;
    VACUUM FULL {table_name};
    ANALYZE {table_name};
"""

def before_import(db, sample_rates):
    for rate in sample_rates:
        db.run(Q_CREATE_METADATA_TABLE.format(suffix=rate["suffix"]))
        db.run(Q_CREATE_EXPERIMENTS_TABLE.format(suffix=rate["suffix"]))

def after_day(db, day, temporary_table_name, permanent_table_name, sample_rates):
    for rate in sample_rates:
        print "  flow_metadata{suffix}".format(suffix=rate["suffix"])
        print "    CLEARING"
        db.run(Q_CLEAR_DAY.format(table="metadata", suffix=rate["suffix"], day=day))
        table_name = permanent_table_name.format(suffix=rate["suffix"])
        print "    INSERTING"
        db.run(Q_INSERT_METADATA.format(suffix=rate["suffix"],
                                        day=day,
                                        table_name=temporary_table_name,
                                        percent=rate["percent"]))
        print "    UPDATING"
        db.run(Q_UPDATE_METADATA.format(suffix=rate["suffix"],
                                        table_name=table_name,
                                        day=day))
        db.run(Q_UPDATE_COMPLETED.format(suffix=rate["suffix"],
                                         table_name=table_name,
                                         day=day))
        db.run(Q_UPDATE_NEW_ACCOUNT.format(suffix=rate["suffix"],
                                           table_name=table_name,
                                           day=day))
        if day < '2016-10-25':
            # This query only exists because, once upon a time, metrics context
            # data was not emitted reliably with the flow.begin event. There's no
            # need to execute it on data emitted since train 71 shipped. I think
            # that was around the 14th October 2016 but I'm not certain, hence
            # the conservative estimate used here.
            db.run(Q_UPDATE_METRICS_CONTEXT.format(suffix=rate["suffix"],
                                                   table_name=temporary_table_name,
                                                   percent=rate["percent"]))
        print "  flow_experiments{suffix}".format(suffix=rate["suffix"])
        print "    CLEARING"
        db.run(Q_CLEAR_DAY.format(table="experiments", suffix=rate["suffix"], day=day))
        print "    INSERTING"
        db.run(Q_INSERT_EXPERIMENTS.format(suffix=rate["suffix"],
                                           day=day,
                                           table_name=temporary_table_name,
                                           percent=rate["percent"]))
        print "    UPDATING"
        db.run(Q_UPDATE_EXPERIMENTS.format(suffix=rate["suffix"],
                                           table_name=table_name,
                                           day=day))

def expire(db, table_name, max_day, months):
    print "EXPIRING", table_name, "FOR", max_day, "+", months, "MONTHS"
    db.run(Q_EXPIRE.format(table_name=table_name, max_day=max_day, months=months))

def vacuum(db, table_name):
    print "VACUUMING AND ANALYZING", table_name
    db.run(Q_VACUUM.format(table_name=table_name))

def after_import(db, sample_rates, max_day):
    for rate in sample_rates:
        table_name = "flow_metadata{suffix}".format(suffix=rate["suffix"])
        expire(db, table_name, max_day, rate["months"])
        vacuum(db, table_name)
        table_name = "flow_experiments{suffix}".format(suffix=rate["suffix"])
        expire(db, table_name, max_day, rate["months"])
        vacuum(db, table_name)

import_events.run("fxa-flow/data/flow",
                  "flow",
                  TEMPORARY_SCHEMA,
                  TEMPORARY_COLUMNS,
                  EVENT_SCHEMA,
                  EVENT_COLUMNS,
                  "flow_id",
                  before_import,
                  after_day,
                  after_import)

