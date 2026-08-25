"""Whole-graph operations, raw TypeQL, and concurrency."""

import asyncio

import pytest
from conftest import Concept


async def test_get_graph_data(seeded):
    nodes, edges = await seeded.adapter.get_graph_data()
    assert {node_id for node_id, _ in nodes} == {seeded.ai, seeded.ml, seeded.dl}
    triples = {(source, target, label) for source, target, label, _ in edges}
    assert triples == {
        (seeded.ml, seeded.ai, "is_subset_of"),
        (seeded.dl, seeded.ml, "is_subset_of"),
        (seeded.dl, seeded.ai, "related_to"),
    }
    edge_props = {label: props for _, _, label, props in edges}
    assert edge_props["is_subset_of"].get("weight") in (1, None)  # dl->ml has no weight


async def test_remove_connections_by_label(seeded):
    adapter = seeded.adapter
    await adapter.remove_connection_to_predecessors_of([seeded.ai], "related_to")
    assert not await adapter.has_edge(seeded.dl, seeded.ai, "related_to")
    assert await adapter.has_edge(seeded.ml, seeded.ai, "is_subset_of")

    await adapter.remove_connection_to_successors_of([seeded.dl], "is_subset_of")
    assert not await adapter.has_edge(seeded.dl, seeded.ml, "is_subset_of")


async def test_delete_graph_keeps_schema_usable(seeded):
    adapter = seeded.adapter
    await adapter.delete_graph()
    assert await adapter.is_empty()
    # The database stays usable for new writes after the wipe.
    await adapter.add_nodes([Concept(name="fresh start")])
    assert not await adapter.is_empty()


async def test_raw_typeql_query(seeded):
    rows = await seeded.adapter.query("match $n isa node; reduce $count = count;")
    assert rows[0]["count"] == 3
    # Reads mentioning updated_at must not be misclassified as writes.
    rows = await seeded.adapter.query(
        "match $n isa node, has updated_at $t; reduce $count = count($t);"
    )
    assert rows[0]["count"] == 3


async def test_query_params_via_given(seeded):
    rows = await seeded.adapter.query(
        "given $name: string; match $n isa node, has node_name == $name; "
        'fetch { "node": { $n.* } };',
        {"name": "machine learning"},
    )
    assert len(rows) == 1
    assert rows[0]["node"]["node_id"] == seeded.ml


async def test_query_params_without_given_stage_is_rejected(seeded):
    with pytest.raises(Exception, match="given"):
        await seeded.adapter.query("match $n isa node;", {"param": 1})


async def test_query_explicit_transaction_type(seeded):
    rows = await seeded.adapter.query(
        "match $n isa node; reduce $count = count;", transaction_type="read"
    )
    assert rows[0]["count"] == 3


async def test_concurrent_readers_and_writers(adapter):
    """The shared driver must tolerate parallel calls from the thread pool."""
    batches = [[Concept(name=f"concept-{batch}-{i}") for i in range(5)] for batch in range(4)]
    await asyncio.gather(*(adapter.add_nodes(batch) for batch in batches))

    reads = await asyncio.gather(*(adapter.get_graph_data() for _ in range(4)))
    assert all(len(nodes) == 20 for nodes, _ in reads)

    node_id = str(batches[0][0].id)
    results = await asyncio.gather(
        adapter.has_node(node_id),
        adapter.add_nodes([Concept(name="one more")]),
        adapter.get_nodes([node_id]),
    )
    assert results[0] is True
