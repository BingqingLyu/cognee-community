"""Per-dataset isolation handler for the TypeDB graph adapter.

Cognee's multi-tenant / backend-access-control mode maps every dataset to its
own graph database through a ``DatasetDatabaseHandlerInterface``. For TypeDB
that is one server-side database per dataset (``cognee_<dataset uuid hex>``),
created with the cognee schema on ``create_dataset`` and dropped on
``delete_dataset``. Credentials are never persisted: ``create_dataset`` stores
only the server address and database name, and
``resolve_dataset_connection_info`` re-derives username/password from the
live graph config right before a connection is opened.

Select it with ``GRAPH_DATASET_DATABASE_HANDLER=typedb`` (cognee's built-in
provider→handler derivation only knows in-tree providers).

Only writes provision a database. Reads on a missing database see an empty
graph, so an engine handle that outlives ``prune_system`` / dataset deletion
does not recreate the dropped database (cognee's shared e2e suite calls
``is_empty()`` on such a handle after its final prune).
"""

import re
from collections.abc import Mapping
from uuid import UUID

from cognee.infrastructure.databases.dataset_database_handler import (
    DatasetDatabaseHandlerInterface,
)
from cognee.infrastructure.databases.graph.config import get_graph_config
from cognee.infrastructure.databases.graph.get_graph_engine import graph_engine_cache
from cognee.modules.users.models import DatasetDatabase, User

from .typedb_adapter import TypeDBAdapter

TYPEDB_DATASET_DATABASE_HANDLER = "typedb"
TYPEDB_DATASET_DATABASE_PREFIX = "cognee_"
# TypeDB database names: keep to a conservative identifier alphabet.
TYPEDB_DATABASE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{2,62}$")


class TypeDBDatasetDatabaseHandler(DatasetDatabaseHandlerInterface):
    """One TypeDB database per cognee dataset."""

    @classmethod
    async def create_dataset(cls, dataset_id: UUID | None, user: User | None) -> dict:
        graph_config = get_graph_config()
        if graph_config.graph_database_provider != "typedb":
            raise ValueError(
                "TypeDBDatasetDatabaseHandler can only be used with the typedb "
                "graph database provider."
            )

        database_name = cls._database_name_for_dataset(dataset_id)

        # Provision the database and define the cognee schema now, so the first
        # pipeline write finds it ready (and so a bad address fails here, not
        # mid-cognify).
        adapter = cls._adapter(graph_config, database_name)
        try:
            await adapter._provision_database()
        finally:
            await adapter.close()

        return {
            "graph_database_provider": "typedb",
            "graph_database_url": graph_config.graph_database_url,
            "graph_database_name": database_name,
            "graph_database_key": graph_config.graph_database_key,
            "graph_dataset_database_handler": TYPEDB_DATASET_DATABASE_HANDLER,
            "graph_database_connection_info": {},
        }

    @classmethod
    async def resolve_dataset_connection_info(
        cls, dataset_database: DatasetDatabase
    ) -> DatasetDatabase:
        """Attach credentials from the live config; nothing is written back."""
        graph_config = get_graph_config()
        info = dict(dataset_database.graph_database_connection_info or {})
        info.setdefault("graph_database_username", graph_config.graph_database_username)
        info.setdefault("graph_database_password", graph_config.graph_database_password)
        dataset_database.graph_database_connection_info = info
        if not dataset_database.graph_database_url:
            dataset_database.graph_database_url = graph_config.graph_database_url
        return dataset_database

    @classmethod
    async def delete_dataset(cls, dataset_database) -> None:
        """Drop the dataset's database.

        Accepts the ``DatasetDatabase`` ORM object (dataset deletion) or the
        read-only row mapping ``prune_system`` iterates over.
        """
        graph_config = get_graph_config()
        database_name = cls._field(dataset_database, "graph_database_name")
        # Never drop a database this handler did not create.
        cls._validate_database_name(database_name)

        url = cls._field(dataset_database, "graph_database_url") or graph_config.graph_database_url
        info = dict(cls._field(dataset_database, "graph_database_connection_info") or {})
        username = info.get("graph_database_username") or graph_config.graph_database_username
        password = info.get("graph_database_password") or graph_config.graph_database_password

        # Evict by database name: cognee's cache key also carries the user-scoped
        # graph_file_path and subprocess flag, which a handler cannot rebuild.
        await graph_engine_cache.aevict_for_database(database_name)

        adapter = TypeDBAdapter(
            graph_database_url=url,
            graph_database_username=username,
            graph_database_password=password,
            database_name=database_name,
        )
        try:
            await adapter._run_sync(cls._drop_database_sync, adapter)
        finally:
            await adapter.close()

    # ------------------------------------------------------------------

    @staticmethod
    def _field(row, name: str):
        """Read a column from an ORM object or a dict-like row mapping."""
        if isinstance(row, Mapping):
            return row.get(name)
        return getattr(row, name, None)

    @staticmethod
    def _drop_database_sync(adapter: TypeDBAdapter) -> None:
        driver = adapter._get_driver()
        if driver.databases.contains(adapter.database_name):
            driver.databases.get(adapter.database_name).delete()

    @classmethod
    def _adapter(cls, graph_config, database_name: str) -> TypeDBAdapter:
        return TypeDBAdapter(
            graph_database_url=graph_config.graph_database_url,
            graph_database_username=graph_config.graph_database_username,
            graph_database_password=graph_config.graph_database_password,
            database_name=database_name,
        )

    @classmethod
    def _database_name_for_dataset(cls, dataset_id: UUID | None) -> str:
        if dataset_id is None:
            raise ValueError("dataset_id is required to create a TypeDB dataset database.")
        database_name = f"{TYPEDB_DATASET_DATABASE_PREFIX}{UUID(str(dataset_id)).hex}"
        cls._validate_database_name(database_name)
        return database_name

    @classmethod
    def _validate_database_name(cls, database_name: str) -> None:
        if not database_name or not database_name.startswith(TYPEDB_DATASET_DATABASE_PREFIX):
            raise ValueError(
                "Refusing to manage a TypeDB database that was not created by the "
                f"typedb dataset handler: {database_name!r}"
            )
        if not TYPEDB_DATABASE_NAME_PATTERN.fullmatch(database_name):
            raise ValueError(f"Invalid TypeDB dataset database name: {database_name!r}")
