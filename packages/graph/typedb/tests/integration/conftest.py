"""Shared fixtures for integration tests against a real TypeDB server.

Tests are skipped when no server listens on 127.0.0.1:1729 (or GRAPH_DATABASE_URL).
No LLM or embedding secrets are needed.
"""

import uuid
from types import SimpleNamespace

import pytest
from support import ADDRESS, Concept, server_available

from cognee_community_graph_adapter_typedb import TypeDBAdapter

__all__ = ["ADDRESS", "Concept", "server_available"]


@pytest.fixture
async def adapter():
    """A TypeDBAdapter against a fresh, uniquely named database."""
    if not server_available():
        pytest.skip(f"no TypeDB server at {ADDRESS}")
    adapter = TypeDBAdapter(
        graph_database_url=ADDRESS,
        database_name=f"cognee_test_{uuid.uuid4().hex[:12]}",
    )
    yield adapter
    driver = adapter._get_driver()
    if driver.databases.contains(adapter.database_name):
        driver.databases.get(adapter.database_name).delete()
    await adapter.close()


@pytest.fixture
async def seeded(adapter):
    """The standard test graph: ml -> ai, dl -> ml (is_subset_of), dl -> ai (related_to)."""
    ai = Concept(name="artificial intelligence", description='the "broad" field')
    ml = Concept(name="machine learning")
    dl = Concept(name="deep learning")
    await adapter.add_nodes([ai, ml, dl], source_ref_key="ds:test", pipeline_run_id="run-1")
    await adapter.add_edges(
        [
            (str(ml.id), str(ai.id), "is_subset_of", {"weight": 1}),
            (str(dl.id), str(ml.id), "is_subset_of", {}),
            (str(dl.id), str(ai.id), "related_to", None),
        ],
        source_ref_key="ds:test",
        pipeline_run_id="run-1",
    )
    return SimpleNamespace(
        adapter=adapter,
        ai=str(ai.id),
        ml=str(ml.id),
        dl=str(dl.id),
        concepts={"ai": ai, "ml": ml, "dl": dl},
    )
