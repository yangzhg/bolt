#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${ROOT_DIR}/_build/Release/bolt/tool/boltfs/boltfs"

export BOLTFS_CLIENT_MODE=auto
export BOLTFS_BINARY="${BINARY}"

expect << 'EOF'
set timeout -1

proc type_text {text {delay 45}} {
  foreach ch [split $text ""] {
    send_user -- $ch
    send -- $ch
    after $delay
  }
}

proc run_cmd {cmd {delay 600}} {
  expect "boltfs:/> "
  send -- $cmd
  send_user -- "\r\n"
  send -- "\r"
  after $delay
}

proc run_cmd_slow {cmd {delay 600}} {
  expect "boltfs:/> "
  type_text $cmd 45
  send_user -- "\r\n"
  send -- "\r"
  after $delay
}

proc run_cmd_mixed {prefix suffix {delay 900} {slow_delay 45}} {
  expect "boltfs:/> "
  type_text $prefix $slow_delay
  after 200
  send_user -- $suffix
  send_user -- "\r\n"
  send -- $suffix
  send -- "\r"
  after $delay
}

log_user 0
spawn sh -c "stty -echo; exec env BOLTFS_CLIENT_MODE=auto \"$env(BOLTFS_BINARY)\""
log_user 1

run_cmd_slow "help" 1200
run_cmd_slow "ls boltfs://warehouse" 1000
run_cmd_slow "ls boltfs://warehouse/demo" 1000
run_cmd_slow "schema boltfs://warehouse/demo/error_events | to json" 1000
run_cmd_slow "sample boltfs://warehouse/demo/error_events?limit=3 | to json" 1100
run_cmd_slow "cd boltfs://warehouse/demo" 500
run_cmd_slow "pwd" 500
run_cmd_mixed "sample error_events" "|head -n 3|to json" 1000
run_cmd_mixed "cat error_events|group-by region" "|agg \"count(*)\"|to json" 1200
run_cmd "explain last" 1100
run_cmd_slow "cd boltfs://warehouse/tpch" 700
run_cmd_slow "pwd" 500
run_cmd_slow "ls boltfs://warehouse/tpch" 900
run_cmd_slow "schema orders | to json" 900
run_cmd_mixed "cat orders|group-by o_orderstatus" "|agg \"count(*)\"|to json" 1800
run_cmd "explain last" 1200
run_cmd_slow "exit" 300

expect eof
EOF
