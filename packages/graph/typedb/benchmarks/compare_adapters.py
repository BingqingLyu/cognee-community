"""Phase 4: the same workload against TypeDB, Ladybug (embedded) and Neo4j.

Every backend runs cognee's public GraphDBInterface only, so the numbers are
what a cognify pipeline would see from each adapter:

  write   add_nodes / add_edges with the provenance fold (source_ref_key +
          pipeline_run_id), then a re-upsert of the same nodes under a second
          run id (the re-cognify path)
  read    get_graph_data (every GRAPH_COMPLETION search), get_neighborhood,
          get_edges per node, get_id_filtered_graph_data, get_graph_metrics
  prov    get_node_delete_data + find_nodes_by_source_ref (the delete planner)
  delete  delete_nodes
  concur  4 concurrent add_nodes over disjoint slices

Usage (each backend is optional; skipped when not configured):

  python -u benchmarks/compare_adapters.py --sizes 1000,5000 \
      --typedb 127.0.0.1:1729 --ladybug --neo4j bolt://127.0.0.1:7688 \
      --neo4j-password benchpassword --json results.json

Neo4j needs the `bench` extra (`uv sync --all-extras`) and a disposable
server: the run WIPES the target Neo4j database.
"""

import argparse
import asyncio
import contextlib
import json
import shutil
import sys
import tempfile
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from bulk_insert import Result, make_edges, make_nodes, timed
from cognee.infrastructure.databases.provenance import make_source_ref_key

from cognee_community_graph_adapter_typedb import TypeDBAdapter

SOURCE_REF_KEY = make_source_ref_key(uuid.uuid4(), uuid.uuid4())
RUN_1, RUN_2 = str(uuid.uuid4()), str(uuid.uuid4())


@contextlib.asynccontextmanager
async def typedb_backend(address: str):
    adapter = TypeDBAdapter(
        graph_database_url=address, database_name=f"cognee_bench_{uuid.uuid4().hex[:8]}"
    )
    await adapter._provision_database()
    try:
        yield adapter
    finally:
        with contextlib.suppress(Exception):
            adapter._get_driver().databases.get(adapter.database_name).delete()
        await adapter.close()


@contextlib.asynccontextmanager
async def ladybug_backend(_: str):
    from cognee.infrastructure.databases.graph.ladybug.adapter import LadybugAdapter

    directory = tempfile.mkdtemp(prefix="cognee_bench_ladybug_")
    adapter = LadybugAdapter(str(Path(directory) / "graph"))
    try:
        yield adapter
    finally:
        await adapter.close()
        shutil.rmtree(directory, ignore_errors=True)


@contextlib.asynccontextmanager
async def neo4j_backend(url: str, username: str = "neo4j", password: str = ""):
    from cognee.infrastructure.databases.graph.neo4j_driver.adapter import Neo4jAdapter

    adapter = Neo4jAdapter(
        graph_database_url=url, graph_database_username=username, graph_database_password=password
    )
    await adapter.initialize()
    await adapter.query("MATCH (n) DETACH DELETE n")
    try:
        yield adapter
    finally:
        with contextlib.suppress(Exception):
            await adapter.query("MATCH (n) DETACH DELETE n")
        await adapter.close()


async def workload(label: str, adapter, size: int) -> list[Result]:
    print(f"\n=== {label}: {size:,} nodes / {int(size * 1.5):,} edges ===")
    nodes = make_nodes(size)
    edges = make_edges(nodes)
    ids = [str(node.id) for node in nodes]
    results = []

    async def run(name, rows, coro):
        results.append(await timed(name, rows, coro))

    await run(
        "write: add_nodes (+provenance)", size, adapter.add_nodes(nodes, SOURCE_REF_KEY, RUN_1)
    )
    await run(
        "write: add_edges (+provenance)",
        len(edges),
        adapter.add_edges(edges, SOURCE_REF_KEY, RUN_1),
    )
    await run(
        "write: re-upsert nodes (run 2)", size, adapter.add_nodes(nodes, SOURCE_REF_KEY, RUN_2)
    )

    await run("read: get_graph_data", size + len(edges), adapter.get_graph_data())
    await run("read: get_neighborhood(10, d=2)", 10, adapter.get_neighborhood(ids[:10], depth=2))

    async def edges_per_node():
        for node_id in ids[:100]:
            await adapter.get_edges(node_id)

    await run("read: get_edges x100 nodes", 100, edges_per_node())
    await run("read: get_id_filtered(200 ids)", 200, adapter.get_id_filtered_graph_data(ids[:200]))
    try:
        await run("read: get_graph_metrics", 1, adapter.get_graph_metrics())
    except Exception as error:  # Neo4j needs the GDS plugin for this one
        print(f"  read: get_graph_metrics                unsupported here ({type(error).__name__})")

    sample = min(500, size)

    async def delete_planner():
        found = await adapter.find_nodes_by_source_ref(SOURCE_REF_KEY)
        assert len(found) == size, (label, len(found))
        snaps = await adapter.get_node_delete_data(ids[:sample])
        assert len(snaps) == sample, (label, len(snaps))

    await run(f"prov: find_by_ref + delete_data({sample})", sample, delete_planner())
    await run(f"delete: delete_nodes({sample})", sample, adapter.delete_nodes(ids[:sample]))

    fresh = make_nodes(size, seed=99)
    for node in fresh:
        node.id = uuid.uuid4()
    quarter = len(fresh) // 4
    slices = [fresh[i * quarter : (i + 1) * quarter] for i in range(4)]
    await run(
        "concur: 4 x add_nodes (disjoint)",
        quarter * 4,
        asyncio.gather(*(adapter.add_nodes(s, SOURCE_REF_KEY, RUN_1) for s in slices)),
    )
    return results


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", default="1000,5000")
    parser.add_argument("--typedb", default="127.0.0.1:1729", help="'' to skip")
    parser.add_argument("--ladybug", action="store_true")
    parser.add_argument("--neo4j", default="", help="bolt URL; '' to skip")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="")
    parser.add_argument("--json", default="", help="write results here")
    args = parser.parse_args()

    backends = []
    if args.typedb:
        backends.append(("typedb", lambda: typedb_backend(args.typedb)))
    if args.ladybug:
        backends.append(("ladybug", lambda: ladybug_backend("")))
    if args.neo4j:
        backends.append(
            ("neo4j", lambda: neo4j_backend(args.neo4j, args.neo4j_user, args.neo4j_password))
        )

    report = {}
    for size in (int(s) for s in args.sizes.split(",")):
        for label, factory in backends:
            async with factory() as adapter:
                results = await workload(label, adapter, size)
            report[f"{label}@{size}"] = {r.scenario: round(r.seconds, 3) for r in results}
    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=2))
    print("\nDone.")


if __name__ == "__main__":
    started = time.perf_counter()
    asyncio.run(main())
    print(f"total {time.perf_counter() - started:.0f}s")
