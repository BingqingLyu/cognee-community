"""Offline cognee conformance tests. No TypeDB server, no secrets."""

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


def test_register_module_import_registers_provider():
    # Run in a subprocess: importing the side-effect module shadows the
    # package-level register() function for the rest of the process.
    import subprocess
    import sys

    code = (
        "import cognee_community_graph_adapter_typedb.register\n"
        "from cognee.infrastructure.databases.graph.supported_databases import "
        "supported_databases\n"
        "from cognee_community_graph_adapter_typedb import TypeDBAdapter\n"
        "assert supported_databases['typedb'] is TypeDBAdapter\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
