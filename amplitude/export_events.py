
import os
import hmac
import json
import math
import time
import hashlib
import datetime
import tempfile
import urlparse

import postgres

import boto.s3
import boto.provider

# Load config from disk,
# and pull in credentials from the environment.

with open("config.json") as f:
    CONFIG = json.loads(f.read())

if "aws_access_key_id" not in CONFIG:
    p = boto.provider.Provider("aws")
    CONFIG["aws_access_key_id"] = p.get_access_key()
    CONFIG["aws_secret_access_key"] = p.get_secret_key()

DB = "postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}".format(**CONFIG)

EVENTS_BUCKET = "net-mozaws-prod-us-west-2-pipeline-analysis"
EVENTS_PREFIX = "fxa-vendor-export/data/"
EVENTS_FILE_URL = "s3://" + EVENTS_BUCKET + "/" + EVENTS_PREFIX + "export-{day}-"

Q_CREATE_MERGED_EVENT_STREAM = """
    WITH
      transformed_flow_events AS (
        SELECT
          EXTRACT(EPOCH FROM e.timestamp) AS timestamp,
          (CASE
            -- Registration events
            WHEN e.type = 'flow.signup.view' THEN 'fxa_reg-view'
            WHEN e.type = 'flow.signup.engage' THEN 'fxa_reg-engage'
            WHEN e.type = 'flow.signup.submit' THEN 'fxa_reg-submit'
            WHEN e.type = 'flow.signup.have-account' THEN 'fxa_reg-have_account'
            WHEN e.type = 'account.created' THEN 'fxa_reg-created'
            WHEN e.type = 'account.verified' AND f.new_account THEN 'fxa_reg-email_confirmed'
            WHEN e.type = 'account.signed' AND f.new_account THEN 'fxa_reg-signed'
            WHEN e.type = 'flow.complete' AND f.new_account THEN 'fxa_reg-complete'
            -- Login events
            WHEN e.type = 'flow.signin.view' THEN 'fxa_login-view'
            WHEN e.type = 'flow.signin.engage' THEN 'fxa_login-engage'
            WHEN e.type = 'flow.signin.submit' THEN 'fxa_login-submit'
            WHEN e.type = 'email.confirmation.sent' THEN 'fxa_login-email_sent'
            WHEN e.type = 'email.verification.sent' THEN 'fxa_login-email_sent'
            WHEN e.type = 'account.confirmed' THEN 'fxa_login-email_confirmed'
            WHEN e.type = 'account.verified' AND NOT f.new_account THEN 'fxa_login-email_confirmed'
            WHEN e.type = 'account.login' THEN 'fxa_login-success'
            WHEN e.type = 'flow.signin.forgot-password' THEN 'fxa_login-forgot_pwd'
            WHEN e.type = 'flow.reset-password.submit' THEN 'fxa_login-forgot_submit'
            WHEN e.type = 'password.forgot.send_code.start' THEN 'fxa_login-forgot_sent'
            WHEN e.type = 'account.reset' THEN 'fxa_login-forgot_complete'
            WHEN e.type = 'account.login.blocked' THEN 'fxa_login-blocked'
            WHEN e.type = 'account.login.sentUnblockCode' THEN 'fxa_login-unblock_sent'
            WHEN e.type = 'account.login.confirmedUnblockCode' THEN 'fxa_login-unblock_success'
            WHEN e.type = 'account.signed' AND NOT f.new_account THEN 'fxa_login-signed'
            WHEN e.type = 'flow.complete' AND NOT f.new_account THEN 'fxa_login-complete'
            -- This should never happen, but you never know...
            ELSE 'fxa_unknown-unknown'
          END) AS type,
          -- Infer the uid by joining onto the activity_events table.
          -- Note that in the unlikely event of multiple matching activity events,
          -- this will produce multiple rows for that flow event.
          (CASE
            WHEN f.uid IS NOT NULL AND f.uid != '' THEN f.uid
            ELSE a.uid
          END) AS uid,
          e.flow_id AS device_id,
          EXTRACT(EPOCH FROM f.begin_time) AS session_id,
          e.flow_time AS event_id,
          f.service AS service,
          f.ua_browser AS ua_browser,
          f.ua_version AS ua_version,
          f.ua_os AS ua_os,
          f.entrypoint AS entrypoint,
          f.utm_campaign AS utm_campaign,
          f.utm_content AS utm_content,
          f.utm_medium AS utm_medium,
          f.utm_source AS utm_source,
          f.utm_term AS utm_term
        FROM flow_metadata AS f
        INNER JOIN flow_events AS e
        ON f.flow_id = e.flow_id
        AND f.begin_time::DATE >= ('{day}'::DATE - '1 day'::INTERVAL)
        AND f.begin_time::DATE <= '{day}'::DATE
        AND e.timestamp::DATE = '{day}'::DATE
        LEFT OUTER JOIN activity_events AS a
        ON a.type = e.type
        AND (a.type = 'account.created' OR a.type = 'account.login' OR a.type = 'account.reset')
        AND a.timestamp = e.timestamp
        AND a.timestamp::DATE = '{day}'::DATE
        AND (a.service = f.service OR a.service = '')
        AND a.ua_os = f.ua_os
        AND a.ua_browser = f.ua_browser
        AND a.ua_version = f.ua_version
        WHERE (
            e.type = 'flow.signup.view' OR
            e.type = 'flow.signup.engage' OR
            e.type = 'flow.signup.submit' OR
            e.type = 'flow.signup.have-account' OR
            e.type = 'account.created' OR
            e.type = 'account.verified' OR
            e.type = 'flow.signin.view' OR
            e.type = 'flow.signin.engage' OR
            e.type = 'flow.signin.submit' OR
            e.type = 'email.confirmation.sent' OR
            e.type = 'email.verification.sent' OR
            e.type = 'account.confirmed' OR
            e.type = 'account.verified' OR
            e.type = 'account.login' OR
            e.type = 'flow.signin.forgot-password' OR
            e.type = 'flow.reset-password.submit' OR
            e.type = 'password.forgot.send_code.start' OR
            e.type = 'account.reset' OR
            e.type = 'account.login.blocked' OR
            e.type = 'account.login.sentUnblockCode' OR
            e.type = 'account.login.confirmedUnblockCode' OR
            e.type = 'account.signed' OR
            e.type = 'flow.complete'
        )
        AND STRTOL(SUBSTRING(f.flow_id FROM 0 FOR 8), 16) % 100 < 10
        ORDER BY 1, 6
      ),

      transformed_activity_events AS (
        SELECT
          EXTRACT(EPOCH FROM a.timestamp) AS timestamp,
          (CASE
            WHEN a.type = 'account.signed' THEN 'fxa_activity-signed'
          END) AS type,
          a.uid AS uid,
          -- This is HMAC-SHA1, using two random keys instead one key with two different XOR masks.
          -- I checked the original "Keying Hash Functions for Message Authentication" paper to confirm
          -- that this is fine from a security standpoint as long as they're independent keys.
          -- Redshift SQL doesn't have SHA-256, but SHA-1 will suffice for our purposes.
          func_sha1('{outer_hmac_key}' || func_sha1('{inner_hmac_key}' || device_id)) as device_id,
          NULL AS session_id,
          -- This ensures activity-events sort before flow events of the same timestamp.
          -1 AS event_id,
          a.service AS service,
          a.ua_browser AS ua_browser,
          a.ua_version AS ua_version,
          a.ua_os AS ua_os,
          NULL entrypoint,
          NULL AS utm_campaign,
          NULL AS utm_content,
          NULL AS utm_medium,
          NULL AS utm_source,
          NULL AS utm_term
        FROM activity_events AS a
        WHERE a.timestamp::DATE = '{day}'::DATE
        AND a.type = 'account.signed'
        AND STRTOL(SUBSTRING(a.uid FROM 0 FOR 8), 16) % 100 < 10
        ORDER BY 1
      )

    (SELECT * FROM transformed_flow_events)
    UNION ALL
    (SELECT * FROM transformed_activity_events)
    ORDER BY "timestamp", event_id
"""


Q_EXPORT_MERGED_EVENT_STREAM = """
    UNLOAD ('{query}')
    TO '{s3path}'
    CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
    DELIMITER AS ','
    ALLOWOVERWRITE
"""


def export_events(hmac_key, from_date, to_date):
    b = boto.s3.connect_to_region("us-east-1").get_bucket(EVENTS_BUCKET)
    db = postgres.Postgres(DB)
    # To get redshift to do the HMAC, we need two independent 64-byte keys.
    # We'll use hex values because they're easy to pass in via string interpolation.
    key_material = HKDF(hmac_key, "", "accounts.firefox.com/v1/metrics/vendor-export", 64)
    outer_hmac_key = key_material[:32].encode('hex')
    inner_hmac_key = key_material[32:].encode('hex')
    # OK, now we can get redshift to export data for each day into S3.
    for day in dates_in_range(from_date, to_date):
        print "EXPORTING", day
        s3path = EVENTS_FILE_URL.format(day=day)
        query = Q_CREATE_MERGED_EVENT_STREAM.format(
            day=day,
            outer_hmac_key=outer_hmac_key,
            inner_hmac_key=inner_hmac_key,
        ).replace("'", "\\'")
        db.run(Q_EXPORT_MERGED_EVENT_STREAM.format(s3path=s3path, query=query, **CONFIG))


def dates_in_range(start_date, end_date):
      one_day = datetime.timedelta(days=1)
      end = parse_date(end_date)
      date = parse_date(start_date)
      while date <= end:
          yield "{:0>4}-{:0>2}-{:0>2}".format(date.year, date.month, date.day)
          date += one_day


def parse_date(when):
      year, month, day = map(int, when.split("-"))
      return datetime.date(year, month, day)


def HKDF_extract(salt, IKM, hashmod=hashlib.sha256):
    """HKDF-Extract; see RFC-5869 for the details."""
    if salt is None:
        salt = b"\x00" * hashmod().digest_size
    return hmac.new(salt, IKM, hashmod).digest()


def HKDF_expand(PRK, info, L, hashmod=hashlib.sha256):
    """HKDF-Expand; see RFC-5869 for the details."""
    digest_size = hashmod().digest_size
    N = int(math.ceil(L * 1.0 / digest_size))
    assert N <= 255
    T = b""
    output = []
    for i in xrange(1, N + 1):
        data = T + info + chr(i)
        T = hmac.new(PRK, data, hashmod).digest()
        output.append(T)
    return b"".join(output)[:L]


def HKDF(secret, salt, info, size, hashmod=hashlib.sha256):
    """HKDF-extract-and-expand as a single function."""
    PRK = HKDF_extract(salt, secret, hashmod)
    return HKDF_expand(PRK, info, size, hashmod)


if __name__ == "__main__":
    import sys
    export_events(*sys.argv[1:])
