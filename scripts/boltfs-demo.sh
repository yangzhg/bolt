#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${ROOT_DIR}/_build/Release/bolt/tool/boltfs/boltfs"

echo "[1/7] build release boltfs"
ninja -C "${ROOT_DIR}/_build/Release" boltfs

echo
echo "[2/7] discover warehouses"
BOLTFS_CLIENT_MODE=human "${BINARY}" ls boltfs://warehouse

echo
echo "[3/7] discover business datasets"
BOLTFS_CLIENT_MODE=human "${BINARY}" ls boltfs://warehouse/demo

echo
echo "[4/7] inspect business schema and sample"
BOLTFS_CLIENT_MODE=human "${BINARY}" schema boltfs://warehouse/demo/error_events
echo
BOLTFS_CLIENT_MODE=human "${BINARY}" sample 'boltfs://warehouse/demo/error_events?limit=3'

echo
echo "[5/7] complete a task in human mode"
BOLTFS_CLIENT_MODE=human "${BINARY}" ask "find the top error regions yesterday and summarize the main error code"

echo
echo "[6/7] return the same task in agent mode"
BOLTFS_CLIENT_MODE=agent "${BINARY}" ask "find the top error regions yesterday and summarize the main error code"

echo
echo "[7/7] prove the engine path with a tpch aggregate"
BOLTFS_CLIENT_MODE=agent "${BINARY}" cat "boltfs://warehouse/tpch/orders?filter=o_orderstatus = 'F'&group_by=o_orderstatus&metrics=count(*),sum(o_totalprice)"
