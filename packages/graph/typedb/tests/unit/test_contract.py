"""Offline cognee conformance tests. No TypeDB server, no secrets."""

import pytest
from contract_suite import assert_graph_contract
from contract_suite.graph_contract import assert_registered

from cognee_community_graph_adapter_typedb import TypeDBAdapter, register


def test_conforms_to_cognee_graph_contract():
    assert_graph_contract(TypeDBAdapter)


def test_register_adds_typedb_provider():
    register()
    assert_registered("typedb", TypeDBAdapter)


def test_constructor_maps_cognee_config_to_typedb_address():
    adapter = TypeDBAdapter(
        graph_database_url="typedb://typedb.example.com",
        graph_database_username="user",
        graph_database_password="secret",
        graph_database_port=1730,
        graph_database_key="",
        database_name="my_db",
    )
    assert adapter.address == "typedb.example.com:1730"
    assert adapter.username == "user"
    assert adapter.password == "secret"
    assert adapter.database_name == "my_db"


def test_constructor_defaults():
    adapter = TypeDBAdapter()
    assert adapter.address == "127.0.0.1:1729"
    assert adapter.username == "admin"
    assert adapter.database_name == "cognee"


def test_register_module_import_does_not_shadow_the_function():
    import importlib

    # `import pkg.register as m` would bind the re-exported *function* (the
    # package attribute), so fetch the module object itself.
    register_module = importlib.import_module("cognee_community_graph_adapter_typedb.register")
    importlib.reload(register_module)  # re-runs the import-time registration
    assert_registered("typedb", TypeDBAdapter)
    from cognee_community_graph_adapter_typedb import register as exported

    assert callable(exported)


def test_register_adds_typedb_dataset_database_handler():
    from cognee.infrastructure.databases.dataset_database_handler import (
        supported_dataset_database_handlers,
    )

    from cognee_community_graph_adapter_typedb import TypeDBDatasetDatabaseHandler

    register()
    entry = supported_dataset_database_handlers["typedb"]
    assert entry["handler_instance"] is TypeDBDatasetDatabaseHandler
    assert entry["handler_provider"] == "typedb"


def test_dataset_database_name_derivation_and_validation():
    import uuid

    import pytest

    from cognee_community_graph_adapter_typedb import TypeDBDatasetDatabaseHandler

    dataset_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
    assert (
        TypeDBDatasetDatabaseHandler._database_name_for_dataset(dataset_id)
        == "cognee_12345678123456781234567812345678"
    )
    with pytest.raises(ValueError):
        TypeDBDatasetDatabaseHandler._database_name_for_dataset(None)
    for foreign in ("cognee", "typedb", "cognee_../x", "cognee_" + "a" * 70, ""):
        with pytest.raises(ValueError):
            TypeDBDatasetDatabaseHandler._validate_database_name(foreign)


def test_edge_key_is_unambiguous_for_ids_containing_separators():
    from cognee_community_graph_adapter_typedb.typedb_adapter import _edge_key

    assert _edge_key("a|b", "c", "r") != _edge_key("a", "b|c", "r")
    assert _edge_key("a", "b", "r") == _edge_key("a", "b", "r")


def test_created_at_mirror_rejects_bool_and_non_int_payload_values():
    from cognee_community_graph_adapter_typedb.typedb_adapter import TypeDBAdapter

    for value in (True, "2026-01-01", None, 1.5):
        created = TypeDBAdapter._row_from_properties("n", {"created_at": value}, "T")["created"]
        assert isinstance(created, int) and not isinstance(created, bool)
    assert TypeDBAdapter._row_from_properties("n", {"created_at": 42}, "T")["created"] == 42


def test_cypher_and_temporal_search_types_are_gated():
    """Cognee routes Cypher / temporal searches by these; both must refuse cleanly."""
    import asyncio

    from cognee.modules.retrieval.exceptions import SearchTypeNotSupported

    assert TypeDBAdapter.supports_cypher_queries is False
    adapter = TypeDBAdapter()
    with pytest.raises(SearchTypeNotSupported):
        asyncio.run(adapter.collect_time_ids(time_from=1, time_to=2))
    with pytest.raises(SearchTypeNotSupported):
        asyncio.run(adapter.collect_events(ids=["x"]))
