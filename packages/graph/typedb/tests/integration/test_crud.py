"""Integration tests: CRUD round-trips against a real TypeDB server.

Requires TypeDB 3.x on 127.0.0.1:1729 (or GRAPH_DB_URL); skipped otherwise.
No LLM or embedding secrets needed.
"""

import os
import socket
import uuid

import pytest
from cognee.infrastructure.engine import DataPoint

from cognee_community_graph_adapter_typedb import TypeDBAdapter

ADDRESS = os.environ.get("GRAPH_DB_URL", "127.0.0.1:1729")


def _server_available() -> bool:
    host, _, port = ADDRESS.rpartition(":")
    try:
        with socket.create_connection((host or "127.0.0.1", int(port)), timeout=2):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _server_available(), reason=f"no TypeDB server at {ADDRESS}")


class Concept(DataPoint):
    name: str
    description: str | None = None
    metadata: dict = {"index_fields": ["name"]}  # noqa: RUF012 (pydantic field, not class var)


@pytest.fixture
async def adapter():
    adapter = TypeDBAdapter(
        graph_database_url=ADDRESS,
        database_name=f"cognee_test_{uuid.uuid4().hex[:12]}",
    )
    yield adapter
    driver = adapter._get_driver()
    if driver.databases.contains(adapter.database_name):
        driver.databases.get(adapter.database_name).delete()
    await adapter.close()


async def test_full_crud_round_trip(adapter):
    ai = Concept(name="artificial intelligence", description='the "broad" field')
    ml = Concept(name="machine learning")
    dl = Concept(name="deep learning")

    assert await adapter.is_empty()

    # -- nodes ----------------------------------------------------------
    await adapter.add_nodes([ai, ml, dl], source_ref_key="ds:test", pipeline_run_id="run-1")

    assert not await adapter.is_empty()
    assert await adapter.has_node(str(ai.id))
    assert not await adapter.has_node("no-such-node")

    node = await adapter.get_node(str(ai.id))
    assert node is not None
    assert node["id"] == str(ai.id)
    assert node["name"] == "artificial intelligence"
    assert node["description"] == 'the "broad" field'  # quote round-trip

    nodes = await adapter.get_nodes([str(ai.id), str(ml.id)])
    assert {n["name"] for n in nodes} == {"artificial intelligence", "machine learning"}

    # upsert: re-adding replaces properties, does not duplicate
    ml_updated = Concept(id=ml.id, name="machine learning", description="updated")
    await adapter.add_nodes([ml_updated])
    nodes = await adapter.get_nodes([str(ml.id)])
    assert len(nodes) == 1
    assert nodes[0]["description"] == "updated"

    # -- edges ----------------------------------------------------------
    await adapter.add_edges(
        [
            (str(ml.id), str(ai.id), "is_subset_of", {"weight": 1}),
            (str(dl.id), str(ml.id), "is_subset_of", {}),
        ],
        source_ref_key="ds:test",
        pipeline_run_id="run-1",
    )
    await adapter.add_edge(str(dl.id), str(ai.id), "related_to")

    assert await adapter.has_edge(str(ml.id), str(ai.id), "is_subset_of")
    assert not await adapter.has_edge(str(ai.id), str(ml.id), "is_subset_of")  # direction
    assert not await adapter.has_edge(str(ml.id), str(ai.id), "unrelated_label")

    existing = await adapter.has_edges(
        [
            (str(ml.id), str(ai.id), "is_subset_of", {}),
            (str(ai.id), str(ml.id), "is_subset_of", {}),
        ]
    )
    assert existing == [(str(ml.id), str(ai.id), "is_subset_of")]

    # re-adding an edge must not duplicate it
    await adapter.add_edge(str(ml.id), str(ai.id), "is_subset_of", {"weight": 2})
    edges = await adapter.get_edges(str(ai.id))
    assert len(edges) == 2  # ml->ai (is_subset_of, upserted), dl->ai (related_to)

    # -- traversal ------------------------------------------------------
    predecessors = await adapter.get_predecessors(str(ai.id))
    assert {p["name"] for p in predecessors} == {"machine learning", "deep learning"}
    predecessors = await adapter.get_predecessors(str(ai.id), edge_label="is_subset_of")
    assert {p["name"] for p in predecessors} == {"machine learning"}

    successors = await adapter.get_successors(str(dl.id))
    assert {s["name"] for s in successors} == {"machine learning", "artificial intelligence"}

    neighbors = await adapter.get_neighbors(str(ml.id))
    assert {n["name"] for n in neighbors} == {"deep learning", "artificial intelligence"}

    connections = await adapter.get_connections(str(ai.id))
    assert len(connections) == 2  # both incoming: ml->ai, dl->ai
    assert all(rel["relationship_name"] for _, rel, _ in connections)

    # -- graph data -----------------------------------------------------
    nodes, edges = await adapter.get_graph_data()
    assert len(nodes) == 3
    assert len(edges) == 3
    node_ids = {node_id for node_id, _ in nodes}
    assert node_ids == {str(ai.id), str(ml.id), str(dl.id)}
    edge_triples = {(source, target, label) for source, target, label, _ in edges}
    assert (str(dl.id), str(ml.id), "is_subset_of") in edge_triples

    # -- raw TypeQL through query() ------------------------------------
    rows = await adapter.query("match $n isa node; reduce $count = count;")
    assert rows[0]["count"] == 3
    # reads mentioning updated_at must not be misclassified as writes
    rows = await adapter.query("match $n isa node, has updated_at $t; reduce $count = count($t);")
    assert rows[0]["count"] == 3
    with pytest.raises(ValueError):
        await adapter.query("match $n isa node;", {"param": 1})

    # -- removal --------------------------------------------------------
    await adapter.remove_connection_to_predecessors_of([str(ai.id)], "related_to")
    assert not await adapter.has_edge(str(dl.id), str(ai.id), "related_to")
    assert await adapter.has_edge(str(ml.id), str(ai.id), "is_subset_of")

    await adapter.delete_node(str(ml.id))
    assert not await adapter.has_node(str(ml.id))
    assert not await adapter.has_edge(str(dl.id), str(ml.id), "is_subset_of")
    assert await adapter.has_node(str(dl.id))  # neighbours survive

    await adapter.delete_graph()
    assert await adapter.is_empty()
