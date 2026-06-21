#!/usr/bin/env bash
set -euo pipefail

addr="${1:-${OCTOPOS_ADDR:-shedwards-octo1:50051}}"
ctl="${OCTOPOSCTL:-./bin/octoposctl}"
session="remote-child-validate-$(date +%s)-$$"
tmp="/cluster/tmp/octopos-remote-child-validate-$$"

run() {
  printf '==> %s\n' "$*"
  timeout 30s "$@"
}

cleanup() {
  run "$ctl" --addr "$addr" exec --session "$session" -- /bin/bash -lc "rm -rf '$tmp'" >/dev/null 2>&1 || true
}
trap cleanup EXIT

run "$ctl" --addr "$addr" exec --session "$session" -- /bin/bash -lc "mkdir -p '$tmp'"

run "$ctl" --addr "$addr" exec --session "$session" --remote-children=safe -- /bin/bash -lc 'hostname >/dev/null'

run "$ctl" --addr "$addr" exec --session "$session" --remote-children=safe -- /bin/bash -lc \
  'test "$(printf abcdef | wc -c | tr -d " ")" = 6'

run "$ctl" --addr "$addr" exec --session "$session" --remote-children=safe -- /bin/bash -lc \
  "mkfifo '$tmp/fifo'; { printf fifo >'$tmp/fifo'; } & test \"\$(cat '$tmp/fifo')\" = fifo"

run "$ctl" --addr "$addr" exec --session "$session" -- /bin/bash -lc \
  "octopos-lockcheck --role self-test --path '$tmp/lockcheck'"

run "$ctl" --addr "$addr" pipe stats

printf 'remote-child validation completed for %s\n' "$addr"
