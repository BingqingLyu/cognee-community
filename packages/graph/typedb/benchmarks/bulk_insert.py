"""Phase 0 throughput benchmark: bulk node/edge upserts at cognify-sized batches.

Measures the adapter's write path under different batching strategies so the
"rows per query / queries per transaction" decision rests on numbers:

  current     one `given` query carrying every row, one transaction (what
              add_nodes/add_edges do today)
  chunk-N/tx  rows split into `given` queries of N rows, all pipelined in ONE
              transaction
  chunk-N/ptx rows split into N-row queries, one transaction PER chunk
  per-row     one `given` query per row, one transaction (the pre-`given`
              shape, for reference)
  re-upsert   `current` again over identical data (the re-cognify path:
              put matches, update replaces)

plus the read side that every GRAPH_COMPLETION search pays (get_graph_data,
get_neighborhood) and a contention-free concurrent-writers run with one
driver per writer (the driver funnels gRPC through one I/O thread, so
parallelism scales with driver instances, not threads).

Usage:  uv run python benchmarks/bulk_insert.py [--sizes 100,1000,5000] [--address host:port]
Requires a TypeDB 3.12+ server; every scenario uses (and drops) its own database.
"""

import argparse
import asyncio
import json
import random
import string
import time
import uuid
from dataclasses import dataclass

from cognee.infrastructure.engine import DataPoint

from cognee_community_graph_adapter_typedb import TypeDBAdapter
from cognee_community_graph_adapter_typedb.typedb_adapter import (
    _SET_EDGE_CREATED_AT,
    _edge_key,
    _edge_upsert_template,
    _node_upsert_template,
    _now_ms,
)

RELATIONSHIPS = ["is_a", "contains", "mentions", "related_to", "part_of"]


class Entity(DataPoint):
    name: str
    description: str
    metadata: dict = {"index_fields": ["name"]}


def make_nodes(count: int, seed: int = 7) -> list[Entity]:
    rng = random.Random(seed)
    nodes = []
    for index in range(count):
        # ~400-900 chars of description: the ballpark of an LLM-extracted entity
        # or a summary; DocumentChunks are larger, EntityTypes much smaller.
        words = " ".join(
            "".join(rng.choices(string.ascii_lowercase, k=rng.randint(3, 9)))
            for _ in range(rng.randint(60, 140))
        )
        nodes.append(Entity(id=uuid.UUID(int=index + 1), name=f"entity {index}", description=words))
    return nodes


def make_edges(nodes: list[Entity], per_node: float = 1.5, seed: int = 11):
    rng = random.Random(seed)
    count = int(len(nodes) * per_node)
    edges, seen = [], set()
    while len(edges) < count:
        source, target = rng.sample(nodes, 2)
        rel = rng.choice(RELATIONSHIPS)
        key = (str(source.id), str(target.id), rel)
        if key in seen:
            continue
        seen.add(key)
        edges.append((str(source.id), str(target.id), rel, {"weight": rng.random()}))
    return edges


@dataclass
class Result:
    scenario: str
    rows: int
    seconds: float

    @property
    def rate(self) -> float:
        return self.rows / self.seconds if self.seconds else float("inf")


async def timed(scenario: str, rows: int, coro) -> Result:
    start = time.perf_counter()
    await coro
    result = Result(scenario, rows, time.perf_counter() - start)
    print(
        f"  {result.scenario:<34} {result.rows:>7,} rows  {result.seconds:7.2f}s  {result.rate:9,.0f} rows/s"
    )
    return result


def node_specs(adapter: TypeDBAdapter, nodes, chunk: int | None):
    now = _now_ms()
    rows = []
    for node in nodes:
        row = adapter._node_row(node)
        row.update({"now": now, "ref": "bench", "run": "run-1"})
        rows.append(row)
    template = _node_upsert_template(True, True)
    size = chunk or len(rows)
    return [[(template, rows[i : i + size])] for i in range(0, len(rows), size)]


def edge_specs(edges, chunk: int | None):
    now = _now_ms()
    rows = []
    for source, target, rel, props in edges:
        rows.append(
            {
                "key": _edge_key(source, target, rel),
                "sid": source,
                "tid": target,
                "rel": rel,
                "props": json.dumps({**props, "source_node_id": source, "target_node_id": target}),
                "now": now,
                "ref": "bench",
                "run": "run-1",
            }
        )
    template = _edge_upsert_template(True, True)
    size = chunk or len(rows)
    return [
        [
            (template, rows[i : i + size]),
            (_SET_EDGE_CREATED_AT, [{"key": r["key"], "now": now} for r in rows[i : i + size]]),
        ]
        for i in range(0, len(rows), size)
    ]


async def run_chunked(adapter, chunks, per_chunk_transaction: bool):
    if per_chunk_transaction:
        for specs in chunks:
            await adapter._write_batch(specs)
    else:
        await adapter._write_batch([spec for specs in chunks for spec in specs])


async def fresh_adapter(address: str) -> TypeDBAdapter:
    adapter = TypeDBAdapter(
        graph_database_url=address, database_name=f"cognee_bench_{uuid.uuid4().hex[:8]}"
    )
    await adapter._ensure_database()
    return adapter


async def drop(adapter: TypeDBAdapter):
    adapter._get_driver().databases.get(adapter.database_name).delete()
    await adapter.close()


async def bench_size(address: str, size: int, chunk_sizes: list[int]) -> list[Result]:
    print(f"\n=== {size:,} nodes / {int(size * 1.5):,} edges ===")
    nodes = make_nodes(size)
    edges = make_edges(nodes)
    results = []

    # --- current strategy (add_nodes / add_edges as shipped) ---
    adapter = await fresh_adapter(address)
    results.append(
        await timed(
            "nodes: current (1 query, 1 tx)",
            size,
            adapter.add_nodes(nodes, source_ref_key="bench", pipeline_run_id="run-1"),
        )
    )
    results.append(
        await timed(
            "edges: current (1 query, 1 tx)",
            len(edges),
            adapter.add_edges(edges, source_ref_key="bench", pipeline_run_id="run-1"),
        )
    )
    results.append(
        await timed(
            "nodes: re-upsert (update path)",
            size,
            adapter.add_nodes(nodes, source_ref_key="bench", pipeline_run_id="run-2"),
        )
    )
    # read side on the loaded graph
    results.append(await timed("read: get_graph_data", size + len(edges), adapter.get_graph_data()))
    seeds = [str(node.id) for node in nodes[:10]]
    results.append(
        await timed(
            "read: get_neighborhood(10 seeds, d=2)", 10, adapter.get_neighborhood(seeds, depth=2)
        )
    )
    await drop(adapter)

    # --- chunked variants ---
    for chunk in chunk_sizes:
        if chunk >= size:
            continue
        for per_tx in (False, True):
            adapter = await fresh_adapter(address)
            label = f"chunk-{chunk}/{'ptx' if per_tx else 'tx'}"
            results.append(
                await timed(
                    f"nodes: {label}",
                    size,
                    run_chunked(adapter, node_specs(adapter, nodes, chunk), per_tx),
                )
            )
            results.append(
                await timed(
                    f"edges: {label}",
                    len(edges),
                    run_chunked(adapter, edge_specs(edges, chunk), per_tx),
                )
            )
            await drop(adapter)

    # --- per-row reference (only at modest sizes; it is slow by construction) ---
    if size <= 1000:
        adapter = await fresh_adapter(address)
        results.append(
            await timed(
                "nodes: per-row (N queries, 1 tx)",
                size,
                run_chunked(adapter, node_specs(adapter, nodes, 1), False),
            )
        )
        await drop(adapter)
    return results


async def bench_concurrency(address: str, writers: int, per_writer: int) -> list[Result]:
    print(f"\n=== concurrency: {writers} writers x {per_writer:,} nodes (disjoint ids) ===")
    all_nodes = make_nodes(writers * per_writer)
    slices = [all_nodes[i * per_writer : (i + 1) * per_writer] for i in range(writers)]
    results = []

    adapter = await fresh_adapter(address)
    results.append(
        await timed("sequential, 1 driver", len(all_nodes), _sequential(adapter, slices))
    )
    await drop(adapter)

    # one adapter (= one driver) per writer, same database
    database = f"cognee_bench_{uuid.uuid4().hex[:8]}"
    adapters = [TypeDBAdapter(graph_database_url=address, database_name=database) for _ in slices]
    await adapters[0]._ensure_database()
    results.append(
        await timed(
            f"parallel, {writers} drivers",
            len(all_nodes),
            asyncio.gather(
                *(
                    a.add_nodes(s, source_ref_key="bench", pipeline_run_id="r")
                    for a, s in zip(adapters, slices, strict=True)
                )
            ),
        )
    )
    # one shared adapter, parallel calls (bounded by the adapter's 4-thread pool + 1 driver)
    shared = adapters[0]
    results.append(
        await timed(
            "parallel, 1 shared driver",
            len(all_nodes),
            asyncio.gather(
                *(shared.add_nodes(s, source_ref_key="bench", pipeline_run_id="r2") for s in slices)
            ),
        )
    )
    shared._get_driver().databases.get(database).delete()
    for a in adapters:
        await a.close()
    return results


async def _sequential(adapter, slices):
    for s in slices:
        await adapter.add_nodes(s, source_ref_key="bench", pipeline_run_id="r")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default="127.0.0.1:1729")
    parser.add_argument("--sizes", default="100,1000,5000")
    parser.add_argument("--chunks", default="250,1000")
    parser.add_argument("--writers", type=int, default=4)
    args = parser.parse_args()
    sizes = [int(s) for s in args.sizes.split(",")]
    chunks = [int(c) for c in args.chunks.split(",")]

    results = []
    for size in sizes:
        results += await bench_size(args.address, size, chunks)
    results += await bench_concurrency(args.address, args.writers, max(sizes) // args.writers)
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
