"""Node CRUD round-trips."""

from uuid import UUID, uuid4

import pytest
from cognee.infrastructure.databases.provenance import make_source_ref_key, make_source_run_ref
from support import Concept


async def test_empty_database(adapter):
    assert await adapter.is_empty()
    assert not await adapter.has_node("no-such-node")
    assert await adapter.get_nodes([]) == []
    assert await adapter.get_node("no-such-node") is None


async def test_add_and_get_nodes(adapter):
    ai = Concept(name="artificial intelligence", description='the "broad" field')
    ml = Concept(name="machine learning")
    await adapter.add_nodes([ai, ml])

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
    """The indexed provenance attributes are @card(0..): each stamped write
    adds its run ref, and a provenance-less re-add leaves existing stamps alone."""
    adapter = seeded.adapter
    ml = seeded.concepts["ml"]

    async def stamps():
        rows = await adapter.query(
            "given $id: string; match $n isa node, has node-id == $id,"
            " has source-run-ref $r; select $r;",
            {"id": seeded.ml},
        )
        return {row["r"] for row in rows}

    first = make_source_run_ref(UUID(seeded.run), seeded.key)
    assert await stamps() == {first}

    await adapter.add_nodes([Concept(id=ml.id, name="machine learning")])  # unstamped
    assert await stamps() == {first}

    other_key, other_run = make_source_ref_key(uuid4(), uuid4()), uuid4()
    await adapter.add_nodes(
        [Concept(id=ml.id, name="machine learning")],
        source_ref_key=other_key,
        pipeline_run_id=str(other_run),
    )
    assert await stamps() == {first, make_source_run_ref(other_run, other_key)}


async def test_add_nodes_rejects_malformed_source_ref_key(adapter):
    with pytest.raises(ValueError):
        await adapter.add_nodes([Concept(name="x")], source_ref_key="ds:test")


async def test_created_at_mirrors_payload_and_updated_at_moves(adapter):
    """The TypeDB created-at attribute mirrors the DataPoint payload's own
    created_at (epoch ms); updated-at is the write time."""
    import asyncio

    node = Concept(name="timestamps")

    async def stamps():
        rows = await adapter.query(
            "given $id: string; match $a isa node-id == $id; $n isa node, has $a,"
            " has created-at $c, has updated-at $u; select $c, $u;",
            {"id": str(node.id)},
        )
        assert len(rows) == 1
        return rows[0]["c"], rows[0]["u"]

    await adapter.add_nodes([node])
    created, updated = await stamps()
    assert created == node.created_at
    assert updated >= created

    await asyncio.sleep(0.01)
    await adapter.add_nodes([node])  # same object: same created_at payload
    created_after, updated_after = await stamps()
    assert created_after == node.created_at
    assert updated_after > updated

    fresh = Concept(id=node.id, name="timestamps", description="rebuilt")
    await adapter.add_nodes([fresh])  # new object for the same id: follows its payload
    created_fresh, _ = await stamps()
    assert created_fresh == fresh.created_at


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
