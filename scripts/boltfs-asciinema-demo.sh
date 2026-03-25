#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${ROOT_DIR}/_build/Release/bolt/tool/boltfs/boltfs"
TASK="find the top error regions yesterday and summarize the main error code"

pause() {
  sleep "${1:-0.7}"
}

run() {
  local command="$1"
  printf '$ %s\n' "${command}"
  pause 0.4
  eval "${command}"
  printf '\n'
  pause 0.8
}

run \
  "BOLTFS_CLIENT_MODE=human \"${BINARY}\" ls boltfs://warehouse"
run \
  "BOLTFS_CLIENT_MODE=human \"${BINARY}\" ls boltfs://warehouse/demo"
run \
  "BOLTFS_CLIENT_MODE=human \"${BINARY}\" schema boltfs://warehouse/demo/error_events"
run \
  "BOLTFS_CLIENT_MODE=human \"${BINARY}\" sample 'boltfs://warehouse/demo/error_events?limit=3'"
run \
  "BOLTFS_CLIENT_MODE=human \"${BINARY}\" ask \"${TASK}\""
run \
  "BOLTFS_CLIENT_MODE=agent \"${BINARY}\" ask \"${TASK}\""
run \
  "printf 'cd boltfs://warehouse/demo\npwd\nschema error_events\nsample error_events | head -n 2 | to json\nexit\n' | BOLTFS_CLIENT_MODE=agent \"${BINARY}\""
