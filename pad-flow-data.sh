#!/bin/sh

if [ "$#" != 1 ]; then
  echo "Usage: $0 <DAY>"
  exit 1
fi

FILE="flow-$1.csv"
REMOTE_DIR="s3://net-mozaws-prod-us-west-2-pipeline-analysis/fxa-flow/data/"
REMOTE_FILE="$REMOTE_DIR$FILE"
FIXED_FILE="$FILE.fixed"

# Copy data for the specified day from S3
if [ ! -e "$FILE" ]; then
  aws s3 cp "$REMOTE_FILE" .
fi

# Ensure every line has the correct number of fields
awk 'BEGIN {FS = OFS = ","} {$18 = $18; print}' "$FILE" > "$FIXED_FILE"

echo "Now sanity check the contents of $FIXED_FILE. If it looks ok, upload it with:"
echo "  aws s3 cp $FIXED_FILE $REMOTE_FILE"

