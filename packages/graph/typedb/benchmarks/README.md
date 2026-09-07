# Write-path benchmarks (Phase 0)

Scripts: `bulk_insert.py` (end-to-end adapter throughput by batching strategy),
`stage_isolation.py` (which pipeline stage costs what; transaction-size sweep).
Both need a TypeDB 3.12+ server and create/drop their own databases.

Measured 2026-09-04 against a local TypeDB 3.12 on an Apple M2 laptop
(single-node server, driver 3.12.3). Absolute numbers are machine-specific;
the ratios are what matter.

## Headline results (1,000 nodes / 1,500 edges, ~600-char payloads)

| Write path | rows/s |
|---|---|
| nodes: plain `insert`, one tx | 18,400 |
| nodes: `put` identity + `update` mirrors, one tx | 18,000 |
| nodes: + set-once `created-at` (`match … not {…}; insert`) | **826** |
| edges: `match` both endpoints by key + `insert`, one tx | 275–299 |
| edges: just matching both endpoints (read-only!) | 284 |
| edges: attribute-first endpoint lookup + `insert`, one tx | 594 |
| edges: attribute-first, 100-row txs, 4 concurrent (+retry) | **1,767** |
| read: `get_graph_data` | 27,000 |
| read: `get_neighborhood` (10 seeds, depth 2) | 2 s total |

## What determines throughput

1. **Rows per transaction, not rows per query.** Splitting a batch into 250-row
   `given` queries inside one transaction changed nothing; committing every
   250 rows did (+30 % nodes, +120 % edges). One query per row was only ~20 %
   slower than one query per batch. `given` batching is worth keeping, but it
   is not the lever.
2. **Node cost is the set-once `created-at` statement.** `put`+`update` runs at
   18k rows/s; the `match … not { $n has created-at $c }; insert` statement
   alone drops the pipeline to 826. The negation is the cost (an
   attribute-first lookup did not help: 664 rows/s).
3. **Edge cost is the endpoint key lookup.** Matching two nodes by
   `has node-id == $var` costs ~1.7 ms per lookup and grows with graph size
   (0.4 ms at 100 nodes, 1.7 ms at 1,000) — a scan-like profile, while `put`
   on the identical pattern runs at 37k rows/s, so the key index exists and
   `put` uses it. Phrasing the lookup attribute-first
   (`$a isa node-id == $id; $n isa node, has $a;`) halves the cost.
4. **Concurrent transactions scale ~2–3×.** With 4 writers × 250 rows one
   shared driver matched one driver per writer (1,771 vs 1,713 rows/s); with
   4 writers × 1,250 rows (16 chunked transactions in flight through one
   driver + one 4-thread pool) the shared driver fell to 379 rows/s vs 1,632
   with four drivers — retry storms and/or the per-driver I/O thread. Phase 4
   should test a driver pool. Concurrent commits DO conflict
   (`[STC2] isolation conflict`) even on disjoint ids, so a commit retry is
   required.
5. **An `or` over which role the anchor plays is 12–18× slower than two
   directional queries.** Sweeping 200 anchors' incident edges on a
   1,000-node graph: `{ $s has $a; } or { $t has $a; }` 6.3 s, the same with
   inline key matches 4.1 s, two role-specific queries merged client-side
   0.34 s. This was the whole cost of `get_neighborhood` / `get_edges`.
6. **Everything lookup-bound degrades with graph size** (point 3): from
   1,000 to 5,000 nodes, edge writes fall 1,101 → 256 rows/s and node writes
   1,935 → 842. Batching cannot fix this; it is the server-side finding to
   raise.

## Decisions applied to the adapter

- Attribute-first key lookups in every template (node-id and edge-key).
- Incident-edge sweeps run as two directional queries (never an `or` over
  the anchor's role).
- `add_nodes` / `add_edges` chunk rows into transactions of
  `WRITE_CHUNK_ROWS` (200) and run up to `WRITE_CONCURRENCY` (4) transactions
  concurrently, retrying `STC2` commit conflicts with backoff. A batch no
  longer commits atomically (neither do the sibling adapters' batches).
- Node `created-at` mirrors the DataPoint payload's own `created_at` (the
  same "mirror the JSON" rule as node-type/name), written in the `update`
  stage; the set-once negation statement — which capped node writes at
  ~800–1,900 rows/s — is gone from the node path. Edges keep set-once
  semantics (their payload carries no timestamp).

## Shipped adapter, before → after

| | 1,000 nodes / 1,500 edges | 5,000 nodes / 7,500 edges |
|---|---|---|
| `add_nodes` | 672 → **~3,800** rows/s | ~1,700 rows/s |
| `add_edges` | 157 → **~900–1,100** rows/s | ~260 rows/s |
| re-upsert (update path) | 561 → ~2,200 rows/s | ~740 rows/s |
| `get_neighborhood` (10 seeds, depth 2) | 1.97 s → **0.17 s** | 24.6 s → **1.19 s** |
| `get_edges` (one node) | 40 ms → 4 ms | 10 ms |
| `get_graph_data` | 27k rows/s | 25k rows/s |

Ranges are across runs; the laptop server is noisy at ±20 %.

## Server observations worth raising with the TypeDB team

- `match $n isa node, has node-id == $v` (key attribute, bound value) does
  not appear to use the key index in `given` pipelines or with literal
  values; `put` on the same pattern does. Attribute-first phrasing is 2× faster
  but still ~0.8 ms/row.
- A negation-only `match not { … };` stage placed after `update` in a `given`
  pipeline did not filter rows that already had the attribute; the same
  negation works as a standalone statement.
- `match $x iid $var` rejects a `given`-bound variable (syntax error), and
  `iid($x)` inside `fetch` is a syntax error on 3.12.3.
- `[STC2]` commit conflicts between concurrent transactions writing disjoint
  node ids (likely the negation's absence-read locks).
- `{ $s has $a; } or { $t has $a; }` (disjunction over the role a bound
  node plays) is 12–18× slower than two role-specific queries.
- `typeql-check` accepts `from` as a role label; the server rejects it
  (`[SYR16]` reserved keyword).
