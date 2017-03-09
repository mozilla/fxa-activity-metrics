#!/bin/sh

if [ "$#" != 1 ]; then
  echo "Usage: $0 <DAY>"
  exit 1
fi

FILE="flow-$1.csv"
REMOTE_DIR="s3://net-mozaws-prod-us-west-2-pipeline-analysis/fxa-flow/data/"
REMOTE_FILE="$REMOTE_DIR$FILE"
TMP_FILE="$FILE.tmp"
FIXED_FILE="$FILE.fixed"

# Copy data for the specified day from S3
if [ ! -e "$FILE" ]; then
  aws s3 cp "$REMOTE_FILE" .
fi

# Eliminate lines that look like injection attempts
grep -v '"' "$FILE" > "$TMP_FILE"
mv "$TMP_FILE" "$FIXED_FILE"
grep -v "'" "$FIXED_FILE" > "$TMP_FILE"
mv "$TMP_FILE" "$FIXED_FILE"
grep -v '`' "$FIXED_FILE" > "$TMP_FILE"
mv "$TMP_FILE" "$FIXED_FILE"
grep -v ";" "$FIXED_FILE" > "$TMP_FILE"
mv "$TMP_FILE" "$FIXED_FILE"
grep -v "\\\\" "$FIXED_FILE" > "$TMP_FILE"
mv "$TMP_FILE" "$FIXED_FILE"
grep -v "\\.\\./" "$FIXED_FILE" > "$TMP_FILE"
mv "$TMP_FILE" "$FIXED_FILE"
grep -iv "select " "$FIXED_FILE" > "$TMP_FILE"
mv "$TMP_FILE" "$FIXED_FILE"
grep -iv "declare " "$FIXED_FILE" > "$TMP_FILE"
mv "$TMP_FILE" "$FIXED_FILE"
grep -v "burpcollab" "$FIXED_FILE" > "$TMP_FILE"
mv "$TMP_FILE" "$FIXED_FILE"
grep -v "nslookup" "$FIXED_FILE" > "$TMP_FILE"
mv "$TMP_FILE" "$FIXED_FILE"

# Eliminate lines that have the wrong number of fields
grep -E '^([^,]*,){17}[^,]*$' "$FIXED_FILE" > "$TMP_FILE"
mv "$TMP_FILE" "$FIXED_FILE"

echo "Now sanity check the contents of $FIXED_FILE. If it looks ok, upload it with:"
echo "  aws s3 cp $FIXED_FILE $REMOTE_FILE"

