"""Phase 4: write-path tuning sweep for the TypeDB adapter.

Sweeps the adapter's two write knobs (WRITE_CHUNK_ROWS, WRITE_CONCURRENCY)
over add_nodes/add_edges at one graph size, and the "driver pool" question:
the same rows written through 1, 2 or 4 adapter instances (= native drivers,
each with its own gRPC I/O thread) into one database.

  python -u benchmarks/tuning.py --size 5000 --chunks 100,200,500,1000 \
      --concurrency 1,2,4,8 --drivers 1,2,4
"""

import argparse
import asyncio
import contextlib
import os
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from bulk_insert import make_edges, make_nodes, timed

import cognee_community_graph_adapter_typedb.typedb_adapter as adapter_module
from cognee_community_graph_adapter_typedb import TypeDBAdapter


@contextlib.asynccontextmanager
async def fresh(address: str, instances: int = 1):
    name = f"cognee_bench_{uuid.uuid4().hex[:8]}"
    adapters = [
        TypeDBAdapter(graph_database_url=address, database_name=name) for _ in range(instances)
    ]
    await adapters[0]._provision_database()
    try:
        yield adapters
    finally:
        with contextlib.suppress(Exception):
            adapters[0]._get_driver().databases.get(name).delete()
        for adapter in adapters:
            await adapter.close()


async def knob_sweep(address, nodes, edges, chunks, concurrencies):
    print(f"\n=== chunk rows x concurrency, {len(nodes):,} nodes / {len(edges):,} edges ===")
    for concurrency in concurrencies:
        adapter_module.WRITE_CONCURRENCY = concurrency
        for chunk in chunks:
            adapter_module.WRITE_CHUNK_ROWS = chunk
            async with fresh(address) as (adapter,):
                assert (adapter._chunk_rows, adapter._write_concurrency) == (chunk, concurrency)
                label = f"chunk={chunk:<5} conc={concurrency}"
                await timed(f"nodes {label}", len(nodes), adapter.add_nodes(nodes))
                await timed(f"edges {label}", len(edges), adapter.add_edges(edges))


async def driver_sweep(address, nodes, edges, drivers, chunk, concurrency):
    adapter_module.WRITE_CHUNK_ROWS, adapter_module.WRITE_CONCURRENCY = chunk, concurrency
    print(f"\n=== driver instances (chunk={chunk}, conc={concurrency} per instance) ===")
    for count in drivers:
        async with fresh(address, count) as adapters:
            share = len(nodes) // count
            slices = [nodes[i * share : (i + 1) * share] for i in range(count)]
            await timed(
                f"nodes via {count} driver(s)",
                share * count,
                asyncio.gather(*(a.add_nodes(s) for a, s in zip(adapters, slices, strict=True))),
            )
            share = len(edges) // count
            slices = [edges[i * share : (i + 1) * share] for i in range(count)]
            await timed(
                f"edges via {count} driver(s)",
                share * count,
                asyncio.gather(*(a.add_edges(s) for a, s in zip(adapters, slices, strict=True))),
            )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default="127.0.0.1:1729")
    parser.add_argument("--size", type=int, default=5000)
    parser.add_argument("--chunks", default="100,200,500,1000")
    parser.add_argument("--concurrency", default="1,2,4,8")
    parser.add_argument("--drivers", default="1,2,4")
    args = parser.parse_args()
    # The sweep sets the module defaults; env overrides would silently win.
    for name in ("TYPEDB_WRITE_CHUNK_ROWS", "TYPEDB_WRITE_CONCURRENCY"):
        os.environ.pop(name, None)
    nodes = make_nodes(args.size)
    edges = make_edges(nodes)
    default_chunk, default_conc = adapter_module.WRITE_CHUNK_ROWS, adapter_module.WRITE_CONCURRENCY
    await knob_sweep(
        address=args.address,
        nodes=nodes,
        edges=edges,
        chunks=[int(c) for c in args.chunks.split(",")],
        concurrencies=[int(c) for c in args.concurrency.split(",")],
    )
    await driver_sweep(
        args.address,
        nodes,
        edges,
        [int(d) for d in args.drivers.split(",")],
        default_chunk,
        default_conc,
    )
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
