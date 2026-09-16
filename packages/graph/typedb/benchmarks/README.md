# Benchmarks

Scripts, all against a TypeDB 3.12+ server (they create and drop their own
`cognee_bench_*` databases):

| script | measures |
|---|---|
| `compare_adapters.py` | one `GraphDBInterface` workload on TypeDB, Ladybug and Neo4j |
| `tuning.py` | chunk size × concurrency sweep; driver-instance sweep |
| `bulk_insert.py` | `add_nodes` / `add_edges` by batching strategy, plus the read side |
| `stage_isolation.py` | which pipeline stage costs what; transaction-size sweep |
| `server_probes.py` | reproduces the two server observations at the bottom |

Measured 2026-09-09 against a local TypeDB 3.12.3 (driver 3.12.3) on an
Apple M2 laptop. Absolute numbers are machine-specific and noisy at ±20 %;
the ratios are what matter.

## What determines throughput

1. **Rows per transaction.** Per-row cost grows superlinearly with the rows
   in a transaction (tuning table below): 100-row chunks are the knee, and
   pipelining every chunk into one transaction is the worst option by far
   (34 edges/s at 5k). Rows per *query* barely matters: `given` batching is
   worth keeping for round trips, but it is not the lever.
2. **Concurrent transactions help up to 4 in flight** and hurt at 8, and
   more native drivers only add contention: one driver per adapter.
3. **Provenance must not put shared strings on artifacts.** TypeDB
   conflicts concurrent inserts of ownership of the same long string value,
   and every row of a provenance batch carries the same source-ref key,
   dataset id and run id. Stored as artifact attributes, the fold had to
   run one chunk at a time (2,300 nodes/s at 5k). Stored as one entity per
   ref with a link per artifact, chunks run concurrently again: 3,700
   nodes/s and 3,500 edges/s with provenance (the bare upsert was not
   re-measured in that run).
4. **The set-once `created-at` negation was the node-write cost** (826
   rows/s against 18,000 without it), so nodes mirror the payload's
   `created_at` instead; edges keep the set-once statement.
5. **An `or` over which role the anchor plays is 12–18× slower than two
   directional queries**; incident-edge sweeps always run as two queries.
6. **Key lookups are O(1) only for values without a shared prefix** (the
   first server observation below). With cognee's random ids the direct
   `has node-id == $id` form and the attribute-first form perform the same,
   so templates use the direct form.

## Decisions applied to the adapter

- `add_nodes` / `add_edges` chunk rows into transactions of
  `TYPEDB_WRITE_CHUNK_ROWS` (100) with up to `TYPEDB_WRITE_CONCURRENCY` (4)
  in flight; provenance-carrying chunks fold the attach into their
  transaction (links to ref entities put beforehand) and run concurrently;
  commit conflicts retry against a time budget. A batch does not commit
  atomically (nor do the sibling adapters').
- Provenance is relational (`source-ref` / `run-ref` entities, `sourced-from`
  / `run-attached` links with a `position`), never attributes on artifacts.
- Incident-edge sweeps run as two directional queries.
- Node `created-at` mirrors the DataPoint payload's own `created_at`,
  written in the `update` stage; edges keep set-once semantics.
- One native driver per adapter.

## Write-path tuning

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

## TypeDB vs Ladybug vs Neo4j

`compare_adapters.py` runs one workload through cognee's `GraphDBInterface`
on each backend: TypeDB 3.12.3 (this adapter as shipped: 100-row chunks,
4 in flight, provenance folded per chunk as links to ref entities), Ladybug 0.17.1
(cognee's default, embedded in-process) and Neo4j 5.28 community (cognee's
built-in adapter, dockerized, no GDS plugin). Same laptop, same seeded data
(random uuid4 ids, 400–900-char payloads), wall time per step. The TypeDB
column was re-measured with the relational provenance model a week after
the Ladybug and Neo4j columns, on the same otherwise idle machine.

| step (5,000 nodes / 7,500 edges) | TypeDB | Ladybug | Neo4j |
|---|---|---|---|
| `add_nodes` + provenance | 1.35 s | 0.27 s | 1.14 s |
| `add_edges` + provenance | 2.13 s | 0.72 s | 1.89 s |
| re-upsert 5,000 nodes (second run id) | 0.69 s | 0.35 s | 0.91 s |
| `get_graph_data` (12,500 rows) | 0.44 s | 0.05 s | 2.25 s |
| `get_neighborhood` (10 seeds, depth 2) | 52 ms | 14 ms | 138 ms |
| `get_edges` × 100 nodes | 150 ms | 141 ms | 252 ms |
| `get_id_filtered_graph_data` (200 ids) | 50 ms | 16 ms | 157 ms |
| `get_graph_metrics` | 0.30 s | 1.00 s | needs GDS |
| `find_nodes_by_source_ref` + `get_node_delete_data` (500) | 430 ms | 15 ms | 704 ms |
| `delete_nodes` (500) | 340 ms | 21 ms | 73 ms |
| 4 concurrent `add_nodes` (5,000 total, provenance) | 1.01 s | 0.50 s | 0.54 s |

| step (1,000 nodes / 1,500 edges) | TypeDB | Ladybug | Neo4j |
|---|---|---|---|
| `add_nodes` + provenance | 0.47 s | 0.11 s | 0.25 s |
| `add_edges` + provenance | 0.73 s | 0.11 s | 0.61 s |
| re-upsert 1,000 nodes | 0.16 s | 0.08 s | 0.20 s |
| `get_graph_data` (2,500 rows) | 0.08 s | 0.01 s | 0.43 s |
| `get_neighborhood` (10 seeds, depth 2) | 28 ms | 9 ms | 81 ms |
| `get_graph_metrics` | 62 ms | 210 ms | needs GDS |
| `delete_nodes` (500) | 311 ms | 16 ms | 60 ms |

Reading it (ratios from the 5k table):

- **Ladybug, embedded in-process, is faster at everything except
  `get_graph_metrics`** (TypeDB about 3.3× faster there, at both sizes):
  1.1× on `get_edges`, 2–5× on bulk writes, re-upsert, neighborhoods and
  id-filtered projections, 9× on `get_graph_data`, 16–29× on deletes and
  the delete planner. That is the price of a server with commit isolation
  and per-dataset databases, not of this adapter.
- **Against Neo4j, the other server: TypeDB is on par on bulk writes**
  (`add_nodes` 1.18×, `add_edges` 1.13×, re-upsert 0.76× of Neo4j's time;
  four concurrent provenance-carrying `add_nodes` calls 1.9× slower, since
  one adapter's four slots are shared by all callers) **and 1.7–5×
  faster on the read paths cognee hits most**: `get_graph_data` 5.1×
  (projected on every GRAPH_COMPLETION search), neighborhoods 2.7×,
  id-filtered projections 3.1×, the delete planner's provenance lookups
  1.6×, `get_edges` 1.7×. `delete_nodes` is 4.7× slower (the edge cascade
  and the provenance-link cascade are separate queries).
- Both comparisons use random ids; see the shared-prefix observation below
  for why that matters.

## Server observations worth raising with the TypeDB team

- **String-value lookups degrade to a scan when the values share a prefix
  of 8 or more characters** (shorter prefixes were not measured;
  `server_probes.py prefix-scan` reproduces it; found 2026-09-09). On
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
  graphs are on the fast path; the first round of these benchmarks used
  sequential `UUID(int=i)` ids and understated write and lookup throughput
  by 5–20× (see History).
  Two cognee values do share a prefix: `source_ref:v1:<dataset uuid>:…`
  (all data of one dataset) and `source_run_ref:v1:<run uuid>:…`, so
  `find_*_by_source_ref` scans that dataset's distinct ref keys.
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
  transaction changes nothing. Relations linking many artifacts to one
  entity that owns the value do not conflict (0 retries for 5,000 links to
  one hub from 4 concurrent writers), which is how the adapter stores
  provenance. Writes to the same owner from two transactions always
  conflict (an integer `update`, or inserting two distinct short
  attributes), which the adapter relies on to serialize changes to one
  artifact.
- **A `select` that reads an entity's relations by joining through the
  other role player's key, followed by inserts of more such relations in
  the same transaction, runs ~70× slower** than the same read written as a
  per-relation `fetch` (87 vs 5,974 rows/s for 100-row chunks linking to
  one hub; either read alone is fast). The adapter's link reads use the
  fetch form.
- `{ $s has $a; } or { $t has $a; }` (disjunction over the role a bound
  node plays) is 12–18× slower than two role-specific queries.
- `typeql-check` accepts `from` as a role label; the server rejects it
  (`[SYR16]` reserved keyword).

## History

The first round (2026-09-04) measured with sequential `UUID(int=i)` ids and
one shared 600-char payload, both of which hit server behaviours described
above (shared-prefix scans, shared-value commit conflicts). Its tables and
the before/after comparison were removed once re-measured; they remain in
git history (`git log -- benchmarks/README.md`, up to commit 550699f).
