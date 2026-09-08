"""End-to-end tier: cognee's shared graph-backend test against TypeDB.

Runs cognee's own ``run_graph_db_test`` (add two documents -> cognify ->
GRAPH_COMPLETION / CHUNKS / SUMMARIES -> NodeSet filter -> prune) with the
TypeDB provider, plus dataset deletion through the per-dataset handler.
Needs a TypeDB server AND an LLM key; skipped otherwise.
"""

import os

import cognee
import pytest
from cognee.tests.e2e.postgres.test_graphdb_shared import run_graph_db_test
from support import ADDRESS, server_available

from cognee_community_graph_adapter_typedb import TypeDBAdapter, register

pytestmark = [
    pytest.mark.skipif(not server_available(), reason=f"no TypeDB server at {ADDRESS}"),
    pytest.mark.skipif(not os.environ.get("LLM_API_KEY"), reason="LLM_API_KEY not set"),
]


@pytest.fixture
def typedb_provider():
    register()
    cognee.config.set_graph_database_provider("typedb")
    cognee.config.set_graph_db_config(
        {
            "graph_database_url": ADDRESS,
            "graph_database_username": os.environ.get("GRAPH_DATABASE_USERNAME", "admin"),
            "graph_database_password": os.environ.get("GRAPH_DATABASE_PASSWORD", "password"),
        }
    )


async def test_shared_graph_db_suite(typedb_provider):
    await run_graph_db_test("typedb")


@pytest.mark.skipif(
    os.environ.get("ENABLE_BACKEND_ACCESS_CONTROL", "").lower() == "false",
    reason="dataset databases only exist with backend access control on",
)
async def test_delete_dataset_drops_its_typedb_database(typedb_provider):
    from cognee.modules.data.methods import delete_dataset, get_datasets_by_name
    from cognee.modules.users.methods import get_default_user

    dataset_name = "typedb_delete_me"
    await cognee.add(["TypeDB deletes the whole dataset database on request."], dataset_name)
    await cognee.cognify([dataset_name])

    user = await get_default_user()
    dataset = (await get_datasets_by_name([dataset_name], user.id))[0]
    database_name = f"cognee_{dataset.id.hex}"

    driver = TypeDBAdapter(graph_database_url=ADDRESS)._get_driver()
    try:
        assert driver.databases.contains(database_name)
        await delete_dataset(dataset)
        assert not driver.databases.contains(database_name)
    finally:
        driver.close()
