# Activity Metrics for Firefox Accounts

This repo hosts supporting scripts
for the "activity" and "flow" metrics pipelines
in Firefox Accounts.
Currently,
the data proceeds via Heka to an S3 bucket,
from where these scripts read the data
and send it to Redshift.
You can view the data [via Redash](https://sql.telemetry.mozilla.org/),
using the `FxA Activity Metrics` data source.

We're also in the process of
migrating away from Heka
to Stackdriver logging,
which may mean we'll move the data
from Redshift to BigQuery at some point.
