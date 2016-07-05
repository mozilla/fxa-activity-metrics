# Activity Metrics for Firefox Accounts

This repo hosts some supporting scripts
for the "activity metrics" pipeline in Firefox Accounts.
It's all very ad-hoc while we figure out
exactly what that pipeline will look like.

Right now we have:

* A small redshift cluster at `rfkelly-fxa-test.cfvijmj2d97c.us-west-2.redshift.amazonaws.com`
  into which we can import metrics data.
* An EC2 instance at `fxa-redshift-helper.dev.lcip.org` configured for accessing
  the redshift instance, on which we can run scripting tasks.
* A script to import the weekly activity-event dump
  from S3 into redshift, `import_events.py` in this repository.
* A redash dashboard at `https://sql.telemetry.mozilla.org/dashboard/fxa-test`
  where we can try out visalizations based on all these metrics.
  
