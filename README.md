# Activity Metrics for Firefox Accounts

This repo hosts some supporting scripts
for the "activity metrics" pipeline in Firefox Accounts.
It's all a little ad-hoc while we figure out
exactly what that pipeline will look like.

We're experimenting with [AWS Redshift](https://aws.amazon.com/redshift/)
and a hosted [redash](http://redash.io/) instance for visualization.

Right now we have the following moving parts:

* A [heka filter](https://github.com/mozilla-services/puppet-config/blob/master/shared/modules/fxa/files/hekad/lua_outputs/fxa_retention_csv.lua)
  that dumps an anonymized 10% sample of user activity events
  as a weekly export in S3.

* An EC2 instance accessible at
  `ec2-user@fxa-redshift-helper.dev.lcip.org`,
  which is configured to talk to a Redshift instance
  at `rfkelly-fxa-test.cfvijmj2d97c.us-west-2.redshift.amazonaws.com`,
  and which we run the following via cron:

  * The `import_activity_events.py` script,
    which pulls the activity event data
    from S3 into Redshift
    to populate an "events" table.

  * The `calculate_daily_summary.py` script,
    which summarizes the raw activity event data
    to populate of users who were active on each day,
    and a table of users who qualified as "multi-device" users
    on each day.

* Some redash dashboards that visualize the data from these
  tables in various ways.  They include:

  * An overview of [daily-active and monthly-active user](https://sql.telemetry.mozilla.org/dashboard/fxa-test)
    activity over time.

  * A basic [retention dashboard](https://sql.telemetry.mozilla.org/dashboard/fxa-retention).

  * An overview of [login behaviour](https://sql.telemetry.mozilla.org/dashboard/fxa-logins).

Feel free to create your own visualizations of this data
by logging in at [the hosted redash instance](https://sql.telemetry.mozilla.org).
The data source is now "rfkelly-fxa-test".
