"""Edge CRUD round-trips."""


async def test_has_edge_is_directional_and_label_aware(seeded):
    adapter = seeded.adapter
    assert await adapter.has_edge(seeded.ml, seeded.ai, "is_subset_of")
    assert not await adapter.has_edge(seeded.ai, seeded.ml, "is_subset_of")
    assert not await adapter.has_edge(seeded.ml, seeded.ai, "unrelated_label")


async def test_has_edges_returns_existing_tuples(seeded):
    existing = await seeded.adapter.has_edges(
        [
            (seeded.ml, seeded.ai, "is_subset_of", {}),
            (seeded.ai, seeded.ml, "is_subset_of", {}),  # wrong direction: absent
            (seeded.dl, seeded.ai, "related_to", {}),
        ]
    )
    assert existing == [
        (seeded.ml, seeded.ai, "is_subset_of"),
        (seeded.dl, seeded.ai, "related_to"),
    ]
    assert await seeded.adapter.has_edges([]) == []


async def test_edge_upsert_does_not_duplicate(seeded):
    adapter = seeded.adapter
    await adapter.add_edge(seeded.ml, seeded.ai, "is_subset_of", {"weight": 2})
    edges = await adapter.get_edges(seeded.ai)
    assert len(edges) == 2  # ml->ai (upserted), dl->ai


async def test_get_edges_covers_both_directions(seeded):
    edges = await seeded.adapter.get_edges(seeded.ml)
    triples = {(source, target, rel["relationship_name"]) for source, target, rel in edges}
    assert triples == {
        (seeded.ml, seeded.ai, "is_subset_of"),
        (seeded.dl, seeded.ml, "is_subset_of"),
    }


async def test_add_edges_skips_missing_endpoints(seeded):
    adapter = seeded.adapter
    await adapter.add_edges([(seeded.ml, "missing-node", "dangling", {})])
    assert await adapter.has_edges([(seeded.ml, "missing-node", "dangling", {})]) == []
