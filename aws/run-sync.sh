#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$SCRIPT_DIR/../src"
KEY="$SCRIPT_DIR/openjdk-mail-key.pem"
SSH_OPTS=(-o StrictHostKeyChecking=no -o ConnectTimeout=10 -i "$KEY")
REMOTE_DIR="/tmp/openjdk-mail-sync"
LOG_FILE="$REMOTE_DIR/sync.log"

: "${SYNC_HOST:?Set SYNC_HOST to the EC2 instance IP}"
: "${ES_URL:?Set ES_URL (e.g. https://elastic:pass@localhost:9200)}"

if [[ ! -f "$KEY" ]]; then
    echo "SSH key not found: $KEY" >&2
    exit 1
fi

echo "Copying source files to $SYNC_HOST:$REMOTE_DIR ..."
ssh "${SSH_OPTS[@]}" "ec2-user@$SYNC_HOST" "mkdir -p $REMOTE_DIR"
scp "${SSH_OPTS[@]}" "$SRC_DIR/sync.py" "$SRC_DIR/mbox.py" "ec2-user@$SYNC_HOST:$REMOTE_DIR/"

SYNC_ARGS="--es-url $ES_URL $*"

echo "--- Reminder: disable the scheduled Lambda to avoid concurrent syncs ---"
echo "Starting sync: python3 sync.py $SYNC_ARGS"
echo "Output: $SYNC_HOST:$LOG_FILE"
echo ""

ssh "${SSH_OPTS[@]}" "ec2-user@$SYNC_HOST" bash -c "'
    cd $REMOTE_DIR
    nohup python3 sync.py $SYNC_ARGS > $LOG_FILE 2>&1 &
    PID=\$!
    echo \"Remote PID: \$PID\"
    echo \"Tailing log (Ctrl-C to detach — sync continues on remote) ...\"
    echo \"\"
    tail -f $LOG_FILE --pid=\$PID
'"
