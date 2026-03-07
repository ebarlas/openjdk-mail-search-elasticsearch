#!/usr/bin/env bash
set -euo pipefail

FUNCTION_NAME="openjdk-mail-es-api-server"
REGION="us-east-1"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC="$SCRIPT_DIR/../src/server.py"

if [ -z "${ES_URL:-}" ]; then
    echo "ES_URL environment variable is required" >&2
    exit 1
fi

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

cp "$SRC" "$WORK_DIR/server.py"

sed -i '' "s|_init_es_auth(os.environ\['ES_URL'\])|_init_es_auth('${ES_URL}')|" "$WORK_DIR/server.py"

(cd "$WORK_DIR" && zip -j server.zip server.py > /dev/null)

aws lambda update-function-code \
    --region "$REGION" \
    --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://$WORK_DIR/server.zip" \
    --query 'CodeSha256' \
    --output text

echo "Deployed $FUNCTION_NAME"
