"""End-to-end tier: cognee's shared graph-backend test against TypeDB.

Runs cognee's own ``run_graph_db_test`` (add two documents -> cognify ->
GRAPH_COMPLETION / CHUNKS / SUMMARIES -> NodeSet filter -> prune) with the
TypeDB provider in BOTH access-control modes, plus dataset deletion through
the per-dataset handler. Spends LLM credits, so it is opt-in:
``RUN_E2E_TESTS=1`` plus a TypeDB server and an LLM key. Writes go to a
dedicated ``cognee_e2e`` database (or per-dataset databases), never to the
default ``cognee`` one.
"""

import os

import cognee
import pytest
from cognee.tests.e2e.postgres.test_graphdb_shared import run_graph_db_test
from support import ADDRESS, database_exists, graph_db_config, server_available

E2E_DATABASE = "cognee_e2e"

pytestmark = [
    pytest.mark.skipif(os.environ.get("RUN_E2E_TESTS") != "1", reason="set RUN_E2E_TESTS=1"),
    pytest.mark.skipif(not server_available(), reason=f"no TypeDB server at {ADDRESS}"),
    pytest.mark.skipif(not os.environ.get("LLM_API_KEY"), reason="LLM_API_KEY not set"),
]


@pytest.fixture
def e2e_config(typedb_config):
    cognee.config.set_graph_db_config(graph_db_config(graph_database_name=E2E_DATABASE))


@pytest.mark.parametrize("access_control", ["true", "false"])
async def test_shared_graph_db_suite(e2e_config, monkeypatch, access_control):
    # cognee reads this at call time, so it beats whatever .env loaded.
    monkeypatch.setenv("ENABLE_BACKEND_ACCESS_CONTROL", access_control)
    await run_graph_db_test("typedb")


async def test_delete_dataset_drops_its_typedb_database(e2e_config, monkeypatch):
    from cognee.context_global_variables import backend_access_control_enabled
    from cognee.modules.data.methods import delete_dataset, get_datasets_by_name
    from cognee.modules.users.methods import get_default_user

    monkeypatch.setenv("ENABLE_BACKEND_ACCESS_CONTROL", "true")
    assert backend_access_control_enabled()

    dataset_name = "typedb_delete_me"
    await cognee.add(["TypeDB deletes the whole dataset database on request."], dataset_name)
    await cognee.cognify([dataset_name])

    user = await get_default_user()
    dataset = (await get_datasets_by_name([dataset_name], user.id))[0]
    database_name = f"cognee_{dataset.id.hex}"

    assert await database_exists(database_name)
    await delete_dataset(dataset)
    assert not await database_exists(database_name)
