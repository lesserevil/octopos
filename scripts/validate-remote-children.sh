#!/usr/bin/env bash
set -euo pipefail

addr="${1:-${OCTOPOS_ADDR:-shedwards-octo1:50051}}"
ctl="${OCTOPOSCTL:-./bin/octoposctl}"
timeout_duration="${OCTOPOS_VALIDATE_TIMEOUT:-45s}"
large_bytes="${OCTOPOS_VALIDATE_LARGE_BYTES:-8388608}"
session="remote-child-validate-$(date +%s)-$$"
tmp="/cluster/tmp/octopos-remote-child-validate-$$"

fail() {
  printf 'validation failed: %s\n' "$*" >&2
  exit 1
}

case "$large_bytes" in
  ''|*[!0-9]*) fail "OCTOPOS_VALIDATE_LARGE_BYTES must be an integer byte count" ;;
esac

run() {
  printf '==> %s\n' "$*"
  timeout "$timeout_duration" "$@"
}

run_capture() {
  printf '==> %s\n' "$*" >&2
  timeout "$timeout_duration" "$@"
}

shell_quote() {
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\"'\"'/g")"
}

stat_from_output() {
  printf '%s\n' "$1" | awk -v key="$2:" '$1 == key {print $2; found=1} END {if (!found) exit 1}'
}

pipe_stats() {
  run_capture "$ctl" --addr "$addr" pipe stats
}

pick_target_node() {
  if [ -n "${OCTOPOS_VALIDATE_TARGET_NODE:-}" ]; then
    printf '%s\n' "$OCTOPOS_VALIDATE_TARGET_NODE"
    return
  fi
  local control_host="${addr%%:*}"
  run_capture "$ctl" --addr "$addr" node list |
    awk -v avoid="$control_host" '
      NR > 1 && $3 ~ /ACTIVE/ {
        if (first == "") first = $1
        if (choice == "" && $1 != avoid && $2 != avoid) choice = $1
      }
      END {
        if (choice != "") print choice
        else if (first != "") print first
      }'
}

assert_no_active_children() {
  local active active_count
  active="$(run_capture "$ctl" --addr "$addr" job children --session "$session" --active)"
  active_count="$(printf '%s\n' "$active" | awk 'NR > 1 && NF {count++} END {print count + 0}')"
  if [ "$active_count" -ne 0 ]; then
    printf '%s\n' "$active" >&2
    fail "remote children still active after validation"
  fi
}

cleanup() {
  run "$ctl" --addr "$addr" exec --session "$session" -- /bin/bash -lc "rm -rf '$tmp'" >/dev/null 2>&1 || true
}
trap cleanup EXIT

target_node="$(pick_target_node)"
[ -n "$target_node" ] || fail "no active node found for explicit remote-child validation"

tmp_q="$(shell_quote "$tmp")"
target_node_q="$(shell_quote "$target_node")"

initial_stats="$(pipe_stats)"
initial_total="$(stat_from_output "$initial_stats" total_streams)"
initial_broken="$(stat_from_output "$initial_stats" broken_pipes)"

printf 'remote-child validation target node: %s\n' "$target_node"
run "$ctl" --addr "$addr" exec --session "$session" -- /bin/bash -lc "mkdir -p $tmp_q"

run "$ctl" --addr "$addr" exec --session "$session" -- /bin/bash -lc \
  "test \"\$(octopos-remote-child --node $target_node_q -- /bin/sh -c 'printf explicit-child-ok')\" = explicit-child-ok"

run "$ctl" --addr "$addr" exec --session "$session" -- /bin/bash -lc \
  "test \"\$(printf stdin-ok | octopos-remote-child --node $target_node_q -- /usr/bin/wc -c | tr -d ' ')\" = 8"

run "$ctl" --addr "$addr" exec --session "$session" -- /bin/bash -lc \
  "test \"\$(head -c $large_bytes /dev/zero | octopos-remote-child --node $target_node_q -- /usr/bin/wc -c | tr -d ' ')\" = $large_bytes"

run "$ctl" --addr "$addr" exec --session "$session" -- /bin/bash -lc \
  "rm -f $tmp_q/stderr; octopos-remote-child --node $target_node_q -- /bin/sh -c 'printf stderr-ok >&2' 2>$tmp_q/stderr; test \"\$(cat $tmp_q/stderr)\" = stderr-ok"

run "$ctl" --addr "$addr" exec --session "$session" --remote-children=safe -- /bin/bash -lc 'hostname >/dev/null'

run "$ctl" --addr "$addr" exec --session "$session" --remote-children=safe -- /bin/bash -lc \
  'test "$(printf abcdef | wc -c | tr -d " ")" = 6'

run "$ctl" --addr "$addr" exec --session "$session" --remote-children=safe -- /bin/bash -lc \
  'test "$(seq 1 4096 | awk "{print \$1}" | sed "s/.*/x/" | wc -l | tr -d " ")" = 4096'

run "$ctl" --addr "$addr" exec --session "$session" --remote-children=safe -- /bin/bash -lc \
  "test \"\$(head -c $large_bytes /dev/zero | wc -c | tr -d ' ')\" = $large_bytes"

run "$ctl" --addr "$addr" exec --session "$session" --remote-children=safe -- /bin/bash -lc \
  "mkfifo $tmp_q/fifo; { printf fifo >$tmp_q/fifo; } & test \"\$(cat $tmp_q/fifo)\" = fifo"

run "$ctl" --addr "$addr" exec --session "$session" -- /bin/bash -lc \
  "octopos-lockcheck --role self-test --path $tmp_q/lockcheck"

assert_no_active_children

children="$(run_capture "$ctl" --addr "$addr" job children --session "$session" --node "$target_node")"
printf '%s\n' "$children"
printf '%s\n' "$children" |
  awk -v node="$target_node" 'NR > 1 && $3 == node && $6 == "completed" {found=1} END {exit !found}' ||
  fail "no completed remote-child record found for $target_node"

final_stats="$(pipe_stats)"
printf '%s\n' "$final_stats"
final_active="$(stat_from_output "$final_stats" active_streams)"
final_total="$(stat_from_output "$final_stats" total_streams)"
final_broken="$(stat_from_output "$final_stats" broken_pipes)"

[ "$final_active" -eq 0 ] || fail "pipe graph still has $final_active active streams"
[ "$final_total" -gt "$initial_total" ] || fail "pipe stream total did not increase"
[ "$final_broken" -eq "$initial_broken" ] || fail "broken pipe count changed from $initial_broken to $final_broken"

printf 'remote-child validation completed for %s\n' "$addr"
