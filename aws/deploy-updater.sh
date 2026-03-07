#!/usr/bin/env bash
set -euo pipefail

FUNCTION_NAME="openjdk-mail-es-updater"
REGION="us-west-1"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$SCRIPT_DIR/../src"

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

cp "$SRC_DIR/sync.py" "$SRC_DIR/mbox.py" "$WORK_DIR/"

(cd "$WORK_DIR" && zip -j function.zip sync.py mbox.py > /dev/null)

aws lambda update-function-code \
    --region "$REGION" \
    --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://$WORK_DIR/function.zip" \
    --query 'CodeSha256' \
    --output text

echo "Deployed $FUNCTION_NAME"
