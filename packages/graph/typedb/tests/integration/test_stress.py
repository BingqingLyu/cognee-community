"""Concurrent-cognify shaped stress: several datasets written at once.

Cognee runs data items concurrently and, under access control, each dataset
has its own database. This drives four dataset databases through one adapter
each, with two writers per dataset folding different source refs onto shared
entity nodes, then checks every count and every provenance record.
"""

import asyncio
from uuid import uuid4

import pytest
from cognee.infrastructure.databases.provenance import make_source_ref_key
from support import ADDRESS, Concept, drop_database, server_available

from cognee_community_graph_adapter_typedb import TypeDBAdapter

pytestmark = pytest.mark.skipif(not server_available(), reason=f"no TypeDB server at {ADDRESS}")

DATASETS = 4
NODES_PER_WRITER = 300
SHARED = 50  # entity nodes both writers of a dataset touch


async def _writer(adapter, nodes, edges, key, run):
    await adapter.add_nodes(nodes, source_ref_key=key, pipeline_run_id=run)
    await adapter.add_edges(edges, source_ref_key=key, pipeline_run_id=run)


async def test_concurrent_multi_dataset_writes_keep_counts_and_provenance():
    names = [f"cognee_test_{uuid4().hex[:12]}" for _ in range(DATASETS)]
    adapters = [TypeDBAdapter(graph_database_url=ADDRESS, database_name=name) for name in names]
    try:
        plans = []
        for adapter in adapters:
            shared = [Concept(name=f"shared {i}") for i in range(SHARED)]
            writers = []
            for w in range(2):
                own = [Concept(name=f"w{w} {i}") for i in range(NODES_PER_WRITER - SHARED)]
                nodes = shared + own
                edges = [
                    (str(nodes[i].id), str(nodes[(i * 7 + 1) % len(nodes)].id), "links", {})
                    for i in range(len(nodes))
                ]
                key, run = make_source_ref_key(uuid4(), uuid4()), str(uuid4())
                writers.append((nodes, edges, key, run))
            plans.append((adapter, shared, writers))

        await asyncio.gather(
            *(
                _writer(adapter, nodes, edges, key, run)
                for adapter, _shared, writers in plans
                for nodes, edges, key, run in writers
            )
        )

        for adapter, shared, writers in plans:
            expected_nodes = SHARED + 2 * (NODES_PER_WRITER - SHARED)
            nodes, edges = await adapter.get_graph_data()
            assert len(nodes) == expected_nodes
            assert len(edges) == len({(s, t, r) for w in writers for s, t, r, _ in w[1]})

            keys = {w[2] for w in writers}
            snaps = await adapter.get_node_delete_data([str(node.id) for node in shared])
            assert len(snaps) == SHARED
            for snap in snaps.values():
                assert set(snap.source_ref_keys) == keys  # both writers' refs survived the race
                assert len(snap.source_run_refs) == 2
            for _nodes, _edges, key, _run in writers:
                assert len(await adapter.find_nodes_by_source_ref(key)) == NODES_PER_WRITER
    finally:
        for adapter, name in zip(adapters, names, strict=True):
            await adapter.close()
            await drop_database(name)
