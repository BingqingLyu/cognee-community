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


async def test_provenance_stamps_accumulate_and_survive_unstamped_upserts(seeded):
    """source-ref-key / source-run-id are @card(0..): each stamped write adds
    its ids, and a provenance-less re-add leaves the existing stamps alone."""
    adapter = seeded.adapter
    ml = seeded.concepts["ml"]

    async def stamps():
        rows = await adapter.query(
            "given $id: string; match $n isa node, has node-id == $id,"
            " has source-ref-key $r, has source-run-id $p; select $r, $p;",
            {"id": seeded.ml},
        )
        return {(row["r"], row["p"]) for row in rows}

    assert await stamps() == {("ds:test", "run-1")}

    await adapter.add_nodes([Concept(id=ml.id, name="machine learning")])  # unstamped
    assert await stamps() == {("ds:test", "run-1")}

    await adapter.add_nodes(
        [Concept(id=ml.id, name="machine learning")],
        source_ref_key="ds:test",
        pipeline_run_id="run-2",
    )
    assert await stamps() == {("ds:test", "run-1"), ("ds:test", "run-2")}


async def test_created_at_attribute_is_set_once_and_updated_at_moves(adapter):
    """The TypeDB created-at/updated-at attributes (not the DataPoint payload's
    own created_at/updated_at fields, which live inside properties-json)."""
    import asyncio

    node = Concept(name="timestamps")

    async def stamps():
        rows = await adapter.query(
            "given $id: string; match $n isa node, has node-id == $id,"
            " has created-at $c, has updated-at $u; select $c, $u;",
            {"id": str(node.id)},
        )
        assert len(rows) == 1
        return rows[0]["c"], rows[0]["u"]

    await adapter.add_nodes([node])
    created, updated = await stamps()
    assert isinstance(created, int) and created == updated

    await asyncio.sleep(0.01)
    await adapter.add_nodes([Concept(id=node.id, name="timestamps", description="again")])
    created_after, updated_after = await stamps()
    assert created_after == created
    assert updated_after > updated


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
