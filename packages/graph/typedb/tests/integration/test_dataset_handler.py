"""Per-dataset database handler: provisioning, isolation, and teardown."""

import uuid

import cognee
import pytest
from cognee.modules.users.models import DatasetDatabase
from support import ADDRESS, Concept, server_available

from cognee_community_graph_adapter_typedb import (
    TypeDBAdapter,
    TypeDBDatasetDatabaseHandler,
    register,
)

pytestmark = pytest.mark.skipif(not server_available(), reason=f"no TypeDB server at {ADDRESS}")


@pytest.fixture
def typedb_config():
    register()
    cognee.config.set_graph_database_provider("typedb")
    cognee.config.set_graph_db_config(
        {
            "graph_database_url": ADDRESS,
            "graph_database_username": "admin",
            "graph_database_password": "password",
        }
    )


def _row(info: dict) -> DatasetDatabase:
    row = DatasetDatabase()
    for key, value in info.items():
        setattr(row, key, value)
    return row


async def test_create_and_delete_dataset_database(typedb_config):
    dataset_id = uuid.uuid4()
    info = await TypeDBDatasetDatabaseHandler.create_dataset(dataset_id, None)

    assert info["graph_database_provider"] == "typedb"
    assert info["graph_dataset_database_handler"] == "typedb"
    assert info["graph_database_name"] == f"cognee_{dataset_id.hex}"
    assert info["graph_database_connection_info"] == {}  # no credentials persisted

    # The database exists with the cognee schema: the adapter can use it immediately.
    adapter = TypeDBAdapter(graph_database_url=ADDRESS, database_name=info["graph_database_name"])
    try:
        assert await adapter.is_empty()
        await adapter.add_nodes([Concept(name="tenant data")])
        assert not await adapter.is_empty()
    finally:
        await adapter.close()

    # Credentials are attached only at resolve time, from the live config.
    resolved = await TypeDBDatasetDatabaseHandler.resolve_dataset_connection_info(_row(info))
    assert resolved.graph_database_connection_info["graph_database_username"] == "admin"
    assert resolved.graph_database_connection_info["graph_database_password"] == "password"

    await TypeDBDatasetDatabaseHandler.delete_dataset(_row(info))
    driver = TypeDBAdapter(graph_database_url=ADDRESS)._get_driver()
    try:
        assert not driver.databases.contains(info["graph_database_name"])
    finally:
        driver.close()


async def test_datasets_are_isolated(typedb_config):
    first = await TypeDBDatasetDatabaseHandler.create_dataset(uuid.uuid4(), None)
    second = await TypeDBDatasetDatabaseHandler.create_dataset(uuid.uuid4(), None)
    a = TypeDBAdapter(graph_database_url=ADDRESS, database_name=first["graph_database_name"])
    b = TypeDBAdapter(graph_database_url=ADDRESS, database_name=second["graph_database_name"])
    try:
        node = Concept(name="only in a")
        await a.add_nodes([node])
        assert await a.has_node(str(node.id))
        assert not await b.has_node(str(node.id))
        assert await b.is_empty()
    finally:
        await a.close()
        await b.close()
        await TypeDBDatasetDatabaseHandler.delete_dataset(_row(first))
        await TypeDBDatasetDatabaseHandler.delete_dataset(_row(second))


async def test_delete_refuses_foreign_databases(typedb_config):
    with pytest.raises(ValueError):
        await TypeDBDatasetDatabaseHandler.delete_dataset(
            _row({"graph_database_name": "cognee", "graph_database_url": ADDRESS})
        )


async def test_create_requires_typedb_provider(typedb_config):
    cognee.config.set_graph_database_provider("networkx")
    try:
        with pytest.raises(ValueError):
            await TypeDBDatasetDatabaseHandler.create_dataset(uuid.uuid4(), None)
    finally:
        cognee.config.set_graph_database_provider("typedb")


async def test_delete_accepts_prune_style_row_mapping(typedb_config):
    """prune_system hands delete_dataset a read-only row mapping, not the ORM object."""
    from types import MappingProxyType

    info = await TypeDBDatasetDatabaseHandler.create_dataset(uuid.uuid4(), None)
    await TypeDBDatasetDatabaseHandler.delete_dataset(MappingProxyType(info))
    driver = TypeDBAdapter(graph_database_url=ADDRESS)._get_driver()
    try:
        assert not driver.databases.contains(info["graph_database_name"])
    finally:
        driver.close()
