"""Node CRUD round-trips."""

from conftest import Concept


async def test_empty_database(adapter):
    assert await adapter.is_empty()
    assert not await adapter.has_node("no-such-node")
    assert await adapter.get_nodes([]) == []
    assert await adapter.get_node("no-such-node") is None


async def test_add_and_get_nodes(adapter):
    ai = Concept(name="artificial intelligence", description='the "broad" field')
    ml = Concept(name="machine learning")
    await adapter.add_nodes([ai, ml], source_ref_key="ds:test", pipeline_run_id="run-1")

    assert not await adapter.is_empty()
    assert await adapter.has_node(str(ai.id))

    node = await adapter.get_node(str(ai.id))
    assert node["id"] == str(ai.id)
    assert node["name"] == "artificial intelligence"
    assert node["description"] == 'the "broad" field'  # quote round-trip

    nodes = await adapter.get_nodes([str(ai.id), str(ml.id)])
    assert {n["name"] for n in nodes} == {"artificial intelligence", "machine learning"}


async def test_add_nodes_upserts_without_duplicating(adapter):
    ml = Concept(name="machine learning")
    await adapter.add_nodes([ml])
    await adapter.add_nodes([Concept(id=ml.id, name="machine learning", description="updated")])

    nodes = await adapter.get_nodes([str(ml.id)])
    assert len(nodes) == 1
    assert nodes[0]["description"] == "updated"


async def test_add_node_from_id_and_properties(adapter):
    await adapter.add_node("plain-id", {"name": "plain", "kind": "manual"})
    node = await adapter.get_node("plain-id")
    assert node["id"] == "plain-id"
    assert node["kind"] == "manual"


async def test_add_nodes_with_duplicate_ids_in_one_batch(adapter):
    first = Concept(name="first")
    second = Concept(id=first.id, name="second")
    await adapter.add_nodes([first, second])
    nodes = await adapter.get_nodes([str(first.id)])
    assert len(nodes) == 1
    assert nodes[0]["name"] == "second"  # last row wins


async def test_unstamped_upsert_preserves_provenance(seeded):
    # seeded stamped ds:test / run-1; a provenance-less re-add must not erase it.
    ml = seeded.concepts["ml"]
    await seeded.adapter.add_nodes([Concept(id=ml.id, name="machine learning")])
    rows = await seeded.adapter.query(
        "given $id: string; match $n isa node, has node_id == $id,"
        " has source_ref_key $r, has pipeline_run_id $p;"
        ' fetch { "ref": $r, "run": $p };',
        {"id": seeded.ml},
    )
    assert rows == [{"ref": "ds:test", "run": "run-1"}]


async def test_extract_node_aliases_get_node(seeded):
    node = await seeded.adapter.extract_node(seeded.ai)
    assert node["name"] == "artificial intelligence"
    nodes = await seeded.adapter.extract_nodes([seeded.ml, seeded.dl])
    assert len(nodes) == 2


async def test_delete_node_cascades_to_edges(seeded):
    adapter = seeded.adapter
    await adapter.delete_node(seeded.ml)
    assert not await adapter.has_node(seeded.ml)
    assert not await adapter.has_edge(seeded.dl, seeded.ml, "is_subset_of")
    assert await adapter.has_node(seeded.dl)  # neighbours survive
    assert await adapter.has_edge(seeded.dl, seeded.ai, "related_to")
