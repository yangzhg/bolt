# BoltFS

BoltFS is a filesystem-style data access shell for Bolt.

The MVP is intentionally optimized for agent workflows instead of human SQL
authoring:

- `ls` discovers datasets.
- `schema` exposes typed structure.
- `sample` returns small structured previews.
- `cat` pushes filtering, projection, and aggregation down to Bolt.

The current hackathon implementation focuses on a local-first virtual catalog:

- `boltfs://warehouse/tpch`
- `boltfs://warehouse/demo`
- one-shot CLI and interactive REPL
- dual client modes:
  - `agent`: structured JSON / NDJSON
  - `human`: MySQL-style ASCII tables
- constrained task entry:
  - `ask "<task>"` maps a supported task template into safe BoltFS commands

## Why This Matters

BoltFS demonstrates a different interface for "Data for AI":

- Agents use a stable filesystem mental model instead of ad-hoc SQL.
- Heavy compute stays in Bolt instead of falling back to local Pandas.
- Guardrails are built into the interface: schema inspection, bounded sampling,
  constrained filters, bounded result sizes, and explicit output formats.

## Quick Start

Build the release binary:

```bash
ninja -C _build/Release boltfs
```

Run one-shot commands:

```bash
BOLTFS_CLIENT_MODE=human _build/Release/bolt/tool/boltfs/boltfs ls boltfs://warehouse
BOLTFS_CLIENT_MODE=human _build/Release/bolt/tool/boltfs/boltfs ls boltfs://warehouse/demo
BOLTFS_CLIENT_MODE=human _build/Release/bolt/tool/boltfs/boltfs schema boltfs://warehouse/demo/error_events
BOLTFS_CLIENT_MODE=human _build/Release/bolt/tool/boltfs/boltfs sample 'boltfs://warehouse/demo/error_events?limit=3'
BOLTFS_CLIENT_MODE=human _build/Release/bolt/tool/boltfs/boltfs ask "find the top error regions yesterday and summarize the main error code"
BOLTFS_CLIENT_MODE=agent _build/Release/bolt/tool/boltfs/boltfs ask "find the top error regions yesterday and summarize the main error code"
BOLTFS_CLIENT_MODE=human _build/Release/bolt/tool/boltfs/boltfs ls boltfs://warehouse/tpch
BOLTFS_CLIENT_MODE=human _build/Release/bolt/tool/boltfs/boltfs schema boltfs://warehouse/tpch/orders
BOLTFS_CLIENT_MODE=human _build/Release/bolt/tool/boltfs/boltfs sample 'boltfs://warehouse/tpch/orders?limit=2'
BOLTFS_CLIENT_MODE=agent _build/Release/bolt/tool/boltfs/boltfs cat "boltfs://warehouse/tpch/orders?columns=o_orderstatus,o_totalprice&limit=2&format=json"
BOLTFS_CLIENT_MODE=agent _build/Release/bolt/tool/boltfs/boltfs cat "boltfs://warehouse/tpch/orders?filter=o_orderstatus = 'F'&group_by=o_orderstatus&metrics=count(*),sum(o_totalprice)"
```

Open the REPL:

```bash
_build/Release/bolt/tool/boltfs/boltfs
```

## Client Modes

BoltFS supports one configuration knob:

```bash
export BOLTFS_CLIENT_MODE=auto|agent|human
```

Behavior:

- `auto`: default behavior; TTY output uses `human`, non-TTY uses `agent`
- `agent`: always return structured machine-friendly JSON / NDJSON
- `human`: always render ASCII tables for terminal inspection

Examples:

```bash
BOLTFS_CLIENT_MODE=human _build/Release/bolt/tool/boltfs/boltfs sample 'boltfs://warehouse/tpch/orders?limit=2'
BOLTFS_CLIENT_MODE=agent _build/Release/bolt/tool/boltfs/boltfs sample 'boltfs://warehouse/tpch/orders?limit=2'
```

## Demo Story

Suggested 8-minute flow:

1. Start from `ls boltfs://warehouse` and `ls boltfs://warehouse/demo` to show that an agent can discover business datasets without being handed SQL table names.
2. Use `schema` and `sample` on `demo/error_events` to show safe exploration before scanning.
3. Run `ask "find the top error regions yesterday..."` in `human` mode to show an operator-friendly answer.
4. Re-run the same `ask` in `agent` mode to show the task, chosen BoltFS command, and JSON result envelope for downstream agents.
5. Finish with a TPCH aggregate query to show the underlying engine path is real Bolt execution, not a toy local shell.

## Query Model

Supported URI shape:

```text
boltfs://warehouse/tpch/<table>?columns=...&filter=...&group_by=...&metrics=...&limit=...&format=json|ndjson
boltfs://warehouse/demo/<table>?columns=...&filter=...&group_by=...&metrics=...&limit=...&format=json|ndjson
```

Current MVP guardrails:

- `filter`: only `column op literal` clauses joined by `AND`
- `metrics`: `count(*)`, `sum(col)`, `avg(col)`, `min(col)`, `max(col)`
- `limit`: capped at `200`
- `sample`: deterministic first-N rows

## Current Scope

Included:

- TPCH-backed virtual warehouse
- demo warehouse with business-flavored operational datasets
- real Bolt execution for scan, filter, project, limit, and aggregation
- agent-friendly structured output
- human-friendly ASCII tables
- constrained `ask` task routing for demo use cases

Deferred:

- Hive / lakehouse backends
- FUSE mount
- free-form SQL grammar
- auth / policy integration
