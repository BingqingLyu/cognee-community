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
   required. Isolated later (2026-09-08): the conflicts come from rows that
   share one *large* attribute value — every benchmark row carries the same
   600-char `properties-json` — not from the ids. With per-row payloads
   (which real cognee data always has) 5,000 nodes / 7,500 edges written by
   four concurrent writers needed zero retries; identical short values
   (`node-type`, `name`) never conflicted either.
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

Phase 0 (2026-09-04, sequential `UUID(int=i)` test ids, which we later
found hit a shared-prefix scan on the server, see below) and Phase 4
(2026-09-09, random ids, 100-row chunks, provenance folded into each chunk
with provenance chunks run serially):

| | 1,000 nodes / 1,500 edges | 5,000 nodes / 7,500 edges |
|---|---|---|
| `add_nodes` (+provenance) | 672 → 3,800 → **1,350** rows/s | 1,700 → **2,300** rows/s |
| `add_edges` (+provenance) | 157 → 1,000 → **1,170** rows/s | 260 → **1,540** rows/s |
| re-upsert (update path) | 561 → 2,200 → **4,300** rows/s | 740 → **4,300** rows/s |
| `get_neighborhood` (10 seeds, depth 2) | 1.97 s → 0.17 s → **0.04 s** | 24.6 s → 1.19 s → **0.07 s** |
| `get_graph_data` | 27k → **21k** rows/s | 25k → **18k** rows/s |

Ranges are across runs; the laptop server is noisy at ±20 %. The 1k
`add_nodes` figure is below Phase 0's 3,800 because provenance chunks now
run one at a time (the Phase 0 number was bare upserts with 4 in flight);
`bulk_insert.py`'s `chunk-N/ptx` scenarios still show the bare upsert at
5,200 (1k) and 7,900 (5k) rows/s. A two-phase variant (concurrent upserts,
then one serial attach pass) measured ~30 % faster (5k: 1.55 s / 3.36 s at
its then-default 200-row chunks vs 2.19 s / 4.87 s folded at 100-row
chunks, so the gap is if anything understated) and was rejected: it leaves
a window, and on a phase-2 failure a permanent state, where artifacts exist
without provenance, which cognee's rollback and dataset-delete planners
cannot see. The remaining lever in the fold is round trips: each chunk's
transaction issues one provenance read plus up to five diff writes.

## Phase 4: write-path tuning (2026-09-09)

`tuning.py` at 5,000 nodes / 7,500 edges, bare upserts (no provenance),
rows/s:

| chunk rows | conc 1 | conc 2 | conc 4 | conc 8 |
|---|---|---|---|---|
| 100 | 6,660 / 3,305 | 9,790 / 5,479 | **9,442 / 7,617** | 7,658 / 5,946 |
| 200 | 6,714 / 3,438 | 8,740 / 4,731 | 7,648 / 5,760 | 4,317 / 4,399 |
| 500 | 5,228 / 2,337 | 4,681 / 2,177 | 2,252 / 1,547 | 2,516 / 1,524 |
| 1000 | 2,419 / 980 | 1,268 / 606 | 1,007 / 471 | 8,251 / 1,440 |

(nodes / edges). Per-row cost grows superlinearly with the rows in a
transaction: at concurrency 1, 100- and 200-row chunks are equal; at 2 and
4 in flight, 100 beats 200 by 12–32 %, and 1,000-row chunks are 2.7–16×
slower than 100 (the conc-8 / 1,000-row node run at 8,251 rows/s is an
unexplained outlier that did not reproduce for edges). Concurrency helps up
to 4 and hurts at 8. The default moved from 200 to 100 rows per chunk; 4
in flight stays.

**Driver pool: not worth it.** The same rows written through 1, 2 or 4
adapter instances (one native driver each, 200-row chunks, 4 transactions
in flight per instance) into one database:

| drivers | nodes | edges |
|---|---|---|
| 1 | 8,335 rows/s | 4,971 rows/s |
| 2 | 5,875 | 4,951 |
| 4 | 3,846 | 4,919 |

More drivers only add contention on the server; the single driver's gRPC
thread is not the ceiling at this scale. The adapter keeps one driver.

**One big transaction is the worst option.** All chunks pipelined into a
single transaction (`bulk_insert.py`'s `chunk-N/tx`) runs at 300 rows/s
for nodes and **34 rows/s for edges** at 5k, against 7,900 / 2,950 with one
transaction per chunk: a transaction's per-query cost grows with the writes
already buffered in it.

## Phase 4: TypeDB vs Ladybug vs Neo4j (2026-09-09)

`compare_adapters.py` runs one workload through cognee's `GraphDBInterface`
on each backend: TypeDB 3.12.3 (this adapter as shipped: 100-row chunks,
4 in flight, provenance folded per chunk and serialized), Ladybug 0.17.1
(cognee's default, embedded in-process) and Neo4j 5.28 community (cognee's
built-in adapter, dockerized, no GDS plugin). Same laptop, same seeded data
(random uuid4 ids, 400–900-char payloads), wall time per step. The TypeDB
column was re-measured after the fold was restored, twenty minutes after
the Ladybug and Neo4j columns, on an otherwise idle machine.

| step (5,000 nodes / 7,500 edges) | TypeDB | Ladybug | Neo4j |
|---|---|---|---|
| `add_nodes` + provenance | 2.19 s | 0.27 s | 1.14 s |
| `add_edges` + provenance | 4.87 s | 0.72 s | 1.89 s |
| re-upsert 5,000 nodes (second run id) | 1.16 s | 0.35 s | 0.91 s |
| `get_graph_data` (12,500 rows) | 0.68 s | 0.05 s | 2.25 s |
| `get_neighborhood` (10 seeds, depth 2) | 67 ms | 14 ms | 138 ms |
| `get_edges` × 100 nodes | 174 ms | 141 ms | 252 ms |
| `get_id_filtered_graph_data` (200 ids) | 63 ms | 16 ms | 157 ms |
| `get_graph_metrics` | 0.30 s | 1.00 s | needs GDS |
| `find_nodes_by_source_ref` + `get_node_delete_data` (500) | 236 ms | 15 ms | 704 ms |
| `delete_nodes` (500) | 195 ms | 21 ms | 73 ms |
| 4 concurrent `add_nodes` (5,000 total, provenance) | 2.00 s | 0.50 s | 0.54 s |

| step (1,000 nodes / 1,500 edges) | TypeDB | Ladybug | Neo4j |
|---|---|---|---|
| `add_nodes` + provenance | 0.74 s | 0.11 s | 0.25 s |
| `add_edges` + provenance | 1.28 s | 0.11 s | 0.61 s |
| re-upsert 1,000 nodes | 0.23 s | 0.08 s | 0.20 s |
| `get_graph_data` (2,500 rows) | 0.12 s | 0.01 s | 0.43 s |
| `get_neighborhood` (10 seeds, depth 2) | 40 ms | 9 ms | 81 ms |
| `get_graph_metrics` | 64 ms | 210 ms | needs GDS |
| `delete_nodes` (500) | 159 ms | 16 ms | 60 ms |

Reading it (ratios from the 5k table):

- **Ladybug, embedded in-process, is faster at everything except
  `get_graph_metrics`** (TypeDB about 3.3× faster there, at both sizes): 1.2×
  on `get_edges`, 3–5× on re-upsert, neighborhoods and id-filtered
  projections, 7–8× on bulk writes, 9–16× on `get_graph_data`, deletes and
  the delete planner. That is the price of a server with commit isolation
  and per-dataset databases, not of this adapter.
- **Against Neo4j, the other server: TypeDB is 1.3–2.6× slower on bulk
  writes** (re-upsert 1.3×, `add_nodes` 1.9×, `add_edges` 2.6×; 3.7× on
  four concurrent provenance-carrying `add_nodes` calls, which serialize
  their chunks) **and 1.5–3.3× faster on the read paths cognee hits most**:
  `get_graph_data` 3.3× (projected on every GRAPH_COMPLETION search),
  neighborhoods 2.1×, id-filtered projections 2.5×, the delete planner's
  provenance lookups 3.0×, `get_edges` 1.5×. `delete_nodes` is 2.7× slower
  (the edge cascade is a separate query).
- Both comparisons are with random ids; the Phase 0 tables above were
  measured with sequential `UUID(int=i)` ids, which hit the shared-prefix
  scan described below, and understate TypeDB by 5–20×.

## Server observations worth raising with the TypeDB team

- **String-value lookups degrade to a scan when the values share a prefix
  of 8 or more characters** (shorter prefixes were not measured;
  `server_probes.py prefix-scan` reproduces it; found in Phase 4,
  2026-09-09, and it is what the Phase 0 note below was really seeing). On
  a one-attribute `@key` entity, 200 lookups of `has k == $v`:

  | value shape | 5,000 values | 20,000 values |
  |---|---|---|
  | random uuid4 (36 chars) | 53,000 lookups/s | 54,000 lookups/s |
  | 12 random chars | 53,000 | 55,000 |
  | 12 chars with an 8-char shared prefix | 607 | 147 |
  | uuid with 8-char shared prefix, rest random | 228 | 59 |
  | sequential `UUID(int=i)` | 224 | 59 |

  Random values are O(1); a shared 8-char prefix is O(N) in the number of
  values, regardless of total length. Cognee's ids are uuid5, so real
  graphs are on the fast path; the Phase 0 benchmarks used sequential
  `UUID(int=i)` ids and understated write and lookup throughput by 5–20×.
  Two cognee values do share a prefix: `source_ref:v1:<dataset uuid>:…`
  (all data of one dataset) and `source_run_ref:v1:<run uuid>:…`, so
  `find_*_by_source_ref` scans that dataset's distinct ref keys.
- (Phase 0, superseded by the above) `match $n isa node, has node-id == $v`
  looked like it was not using the key index; it was the sequential test ids.
- A negation-only `match not { … };` stage placed after `update` in a `given`
  pipeline did not filter rows that already had the attribute; the same
  negation works as a standalone statement.
- `match $x iid $var` rejects a `given`-bound variable (syntax error), and
  `iid($x)` inside `fetch` is a syntax error on 3.12.3.
- `[STC2]` commit conflicts between concurrent transactions whose rows own
  the same string value **above some length between 17 and 24 characters**
  (`server_probes.py shared-value`): with a shared 16-char `name` 0
  conflicts across 20 concurrent 50-row transactions; 24 chars and up, 9–15
  (all the way to a shared 600-char `properties-json`). Distinct values
  never conflict, and pre-creating the attribute instance in its own
  transaction changes nothing. This matters for cognee: every row of a
  provenance batch owns the same `source-ref-key` (86 chars) and run /
  dataset ids (36 chars), so the adapter runs provenance-carrying chunks
  one at a time per adapter, and writers in other adapter instances retry
  against a time budget.
- `{ $s has $a; } or { $t has $a; }` (disjunction over the role a bound
  node plays) is 12–18× slower than two role-specific queries.
- `typeql-check` accepts `from` as a role label; the server rejects it
  (`[SYR16]` reserved keyword).
