#!/usr/bin/env bash
# Tail ds2-bridge logs from Akamai Functions.
#
# There is no --follow on `spin aka logs`, so this polls and de-duplicates.
#
#   ./tail_logs.sh              # errors and warnings only (default)
#   ./tail_logs.sh all          # every line
#   ./tail_logs.sh all 2        # every line, poll every 2s
#   ./tail_logs.sh errors 30    # errors only, poll every 30s
#
# Ctrl-C to stop.
#
# Sizing note: `-n` is capped at 5000 by the platform, and at ~17 req/s the app
# emits ~50 lines/sec, so 5000 lines is only ~100s of history. The poll interval
# must stay well under that or you will miss lines between polls.

set -uo pipefail

# Spin CLI: $SPIN wins, then PATH, then the default install location.
SPIN="${SPIN:-$(command -v spin || echo "$HOME/.spin/spin")}"
# The app directory is the one holding spin.toml - this script's own directory.
APP_DIR="${APP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
if [[ ! -f "${APP_DIR:-}/spin.toml" ]]; then
  echo "ERROR: no spin.toml found in $HERE or $HERE/ds2-bridge-rust." >&2
  echo "Set APP_DIR=/path/to/app and re-run." >&2
  exit 1
fi

MODE="${1:-errors}"
INTERVAL="${2:-}"

if [[ "$MODE" == "all" ]]; then
  LINES=500          # ~10s of traffic
  SINCE=20s
  INTERVAL="${INTERVAL:-5}"
else
  LINES=5000         # ~100s of traffic
  SINCE=90s
  INTERVAL="${INTERVAL:-30}"
fi

if (( INTERVAL > 90 )); then
  echo "WARNING: interval ${INTERVAL}s exceeds the ~100s visible window; you will miss lines." >&2
fi

echo "tailing ds2-bridge  mode=$MODE  poll=${INTERVAL}s  -n $LINES  (Ctrl-C to stop)" >&2
if [[ "$MODE" != "all" ]]; then
  echo "(quiet output is good news - nothing prints unless something is wrong)" >&2
fi

while :; do
  "$SPIN" aka logs -n "$LINES" --since "$SINCE" --from "$APP_DIR/spin.toml" 2>/dev/null \
    | sed -r 's/\x1b\[[0-9;]*m//g' \
    | if [[ "$MODE" == "all" ]]; then cat
      else grep -aiE 'ERROR|WARN|allocation|Throughput|DispatchFailure|missingDisplayName|No records to upload'
      fi
  sleep "$INTERVAL"
done | awk '!seen[$0]++'
