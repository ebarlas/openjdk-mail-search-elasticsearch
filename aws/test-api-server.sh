#!/usr/bin/env bash
set -euo pipefail

FUNCTION_NAME="openjdk-mail-es-api-server"
REGION="us-east-1"
OUTFILE=$(mktemp)
trap 'rm -f "$OUTFILE"' EXIT

invoke() {
    local desc="$1" uri="$2" qs="$3"

    aws lambda invoke --region "$REGION" ${AWS_PROFILE:+--profile "$AWS_PROFILE"} \
        --function-name "$FUNCTION_NAME" \
        --payload "{\"Records\":[{\"cf\":{\"request\":{\"method\":\"GET\",\"uri\":\"$uri\",\"querystring\":\"$qs\"}}}]}" \
        --cli-binary-format raw-in-base64-out \
        "$OUTFILE" > /dev/null 2>&1

    local http_status
    http_status=$(python3 -c "import json; d=json.load(open('$OUTFILE')); print(d.get('status','ERR'))")

    local detail
    detail=$(python3 -c "
import json
d = json.load(open('$OUTFILE'))
body = json.loads(d.get('body', '{}'))
items = body.get('items', [])
if items:
    i = items[0]
    print(f\"[{len(items)} items] {i.get('list','')} | {i.get('author','')} | {i.get('subject','')[:50]}\")
else:
    print(json.dumps(body)[:80])
" 2>/dev/null || echo "parse error")

    printf "%-20s %s  %s\n" "$desc" "$http_status" "$detail"
}

echo "Testing $FUNCTION_NAME in $REGION"
echo ""

invoke "global-search"   "/mail/search"                          "q=panama&limit=3"
invoke "list-search"     "/lists/amber-dev/mail/search"          "q=pattern+matching&limit=3"
invoke "global-latest"   "/mail"                                 "limit=3"
invoke "list-latest"     "/lists/loom-dev/mail"                  "limit=3"
invoke "global-byauthor" "/mail/byauthor"                        "author=Alan+Bateman&limit=3"
invoke "list-byauthor"   "/lists/loom-dev/mail/byauthor"         "author=Alan+Bateman&limit=3"
invoke "global-byemail"  "/mail/byemail"                         "email=alanb@openjdk.org&limit=3"
invoke "list-byemail"    "/lists/loom-dev/mail/byemail"          "email=alanb@openjdk.org&limit=3"
invoke "status"          "/mail/status"                          ""

echo ""
echo "Done"
