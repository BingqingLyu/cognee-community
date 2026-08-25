"""TypeDB graph database adapter for cognee.

Maps cognee's property-graph model onto a generic, reified TypeDB schema:
one ``node`` entity type and one ``edge`` relation type, with cognee's
dynamic node labels and relationship names stored as attributes and
arbitrary properties serialized to a JSON string attribute
(see ``COGNEE_SCHEMA``). A typed per-DataPoint schema mode is a planned
follow-up.

The TypeDB Python driver is synchronous (the async Rust core stops at the
FFI boundary), while cognee's ``GraphDBInterface`` is fully async. Every
adapter method therefore scopes its driver work (open transaction ->
queries -> commit/close) inside a single ``asyncio.to_thread`` hop.
"""

import asyncio
import json
from typing import Any
from uuid import UUID

from cognee.infrastructure.databases.graph.graph_db_interface import (
    GraphDBInterface,
)
from cognee.infrastructure.engine import DataPoint
from cognee.modules.storage.utils import JSONEncoder
from cognee.shared.logging_utils import get_logger

logger = get_logger("TypeDBAdapter")

DEFAULT_ADDRESS = "127.0.0.1:1729"
DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "password"
DEFAULT_DATABASE = "cognee"

# Generic reified graph schema. Cognee's node labels (DataPoint type names)
# and edge relationship names are data here, not schema, so any pipeline
# output fits without runtime schema migration. `from` is a reserved TypeQL
# keyword, hence the `source`/`target` role names.
COGNEE_SCHEMA = """
define
  attribute node_id, value string;
  attribute node_type, value string;
  attribute node_name, value string;
  attribute relationship_name, value string;
  attribute properties_json, value string;
  attribute source_ref_key, value string;
  attribute pipeline_run_id, value string;
  attribute updated_at, value datetime;

  entity node,
    owns node_id @key,
    owns node_type,
    owns node_name,
    owns properties_json,
    owns source_ref_key,
    owns pipeline_run_id,
    owns updated_at,
    plays edge:source,
    plays edge:target;

  relation edge,
    relates source,
    relates target,
    owns relationship_name,
    owns properties_json,
    owns source_ref_key,
    owns pipeline_run_id,
    owns updated_at;
"""

_SCHEMA_KEYWORDS = ("define", "undefine", "redefine")
_WRITE_KEYWORDS = ("insert", "put", "update", "delete")


class TypeDBAdapter(GraphDBInterface):
    """Adapter for TypeDB as a cognee graph store."""

    def __init__(
        self,
        graph_database_url: str | None = None,
        graph_database_username: str | None = None,
        graph_database_password: str | None = None,
        graph_database_port: int | None = None,
        graph_database_key: str | None = None,
        database_name: str | None = None,
        **kwargs,
    ):
        address = graph_database_url or DEFAULT_ADDRESS
        # Cognee configs commonly carry scheme-prefixed URLs; TypeDB wants host:port.
        for scheme in ("typedb://", "bolt://", "grpc://", "http://", "https://"):
            if address.startswith(scheme):
                address = address[len(scheme) :]
                break
        if graph_database_port and ":" not in address:
            address = f"{address}:{graph_database_port}"

        self.address = address
        self.username = graph_database_username or DEFAULT_USERNAME
        self.password = graph_database_password or DEFAULT_PASSWORD
        self.database_name = database_name or DEFAULT_DATABASE

        self._driver = None
        self._schema_initialized = False
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Connection plumbing (synchronous; always called from worker threads)
    # ------------------------------------------------------------------

    def _get_driver(self):
        """Lazily open the (synchronous) TypeDB driver."""
        if self._driver is None:
            from typedb.driver import Credentials, DriverOptions, DriverTlsConfig, TypeDB

            self._driver = TypeDB.driver(
                self.address,
                Credentials(self.username, self.password),
                DriverOptions(DriverTlsConfig.disabled()),
            )
        return self._driver

    def _ensure_database_sync(self) -> None:
        """Create the database and define the cognee schema if needed."""
        from typedb.driver import TransactionType

        driver = self._get_driver()
        if not driver.databases.contains(self.database_name):
            driver.databases.create(self.database_name)
        with driver.transaction(self.database_name, TransactionType.SCHEMA) as tx:
            tx.query(COGNEE_SCHEMA).resolve()
            tx.commit()
        self._schema_initialized = True

    async def _ensure_database(self) -> None:
        if self._schema_initialized:
            return
        async with self._lock:
            if not self._schema_initialized:
                await asyncio.to_thread(self._ensure_database_sync)

    def _transaction_type_for(self, query_text: str):
        from typedb.driver import TransactionType

        first_word = query_text.lstrip().split(None, 1)[0].lower() if query_text.strip() else ""
        if first_word in _SCHEMA_KEYWORDS:
            return TransactionType.SCHEMA
        if first_word in _WRITE_KEYWORDS or any(
            keyword in query_text.lower() for keyword in _WRITE_KEYWORDS
        ):
            return TransactionType.WRITE
        return TransactionType.READ

    def _query_sync(self, query_text: str) -> list[dict[str, Any]]:
        """Run one TypeQL query in its own transaction and return plain data."""
        from typedb.driver import TransactionType

        driver = self._get_driver()
        transaction_type = self._transaction_type_for(query_text)
        with driver.transaction(self.database_name, transaction_type) as tx:
            answer = tx.query(query_text).resolve()
            results = self._collect_answer(answer)
            if transaction_type in (TransactionType.WRITE, TransactionType.SCHEMA):
                tx.commit()
        return results

    @staticmethod
    def _collect_answer(answer) -> list[dict[str, Any]]:
        """Convert a QueryAnswer into a list of plain dicts.

        Fetch queries yield JSON documents already; concept-row answers are
        flattened to {column: string} for now (richer concept decoding lands
        with the full implementation).
        """
        if answer.is_concept_documents():
            return list(answer.as_concept_documents())
        if answer.is_concept_rows():
            rows = []
            for row in answer.as_concept_rows():
                rows.append({name: str(row.get(name)) for name in row.column_names()})
            return rows
        return []

    # ------------------------------------------------------------------
    # GraphDBInterface — cognee 1.4.2 call surface
    # ------------------------------------------------------------------

    async def query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a raw TypeQL query.

        Note: cognee's Cypher-oriented search types pass Cypher here; this
        adapter executes TypeQL only (a TypeQL natural-language retriever is
        planned alongside this adapter).
        """
        await self._ensure_database()
        if params:
            raise NotImplementedError("TypeDBAdapter.query does not support query parameters yet")
        return await asyncio.to_thread(self._query_sync, query)

    async def has_node(self, node_id: str) -> bool:
        raise NotImplementedError("TypeDBAdapter.has_node is not implemented yet")

    async def add_node(self, node: DataPoint | str, properties: dict[str, Any] | None = None):
        raise NotImplementedError("TypeDBAdapter.add_node is not implemented yet")

    async def add_nodes(
        self,
        nodes: list[DataPoint],
        source_ref_key: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> None:
        raise NotImplementedError("TypeDBAdapter.add_nodes is not implemented yet")

    async def extract_node(self, node_id: str):
        raise NotImplementedError("TypeDBAdapter.extract_node is not implemented yet")

    async def extract_nodes(self, node_ids: list[str]):
        raise NotImplementedError("TypeDBAdapter.extract_nodes is not implemented yet")

    async def delete_node(self, node_id: str):
        raise NotImplementedError("TypeDBAdapter.delete_node is not implemented yet")

    async def delete_nodes(self, node_ids: list[str]) -> None:
        raise NotImplementedError("TypeDBAdapter.delete_nodes is not implemented yet")

    async def has_edge(
        self,
        source_id: str | UUID,
        target_id: str | UUID,
        relationship_name: str,
    ) -> bool:
        raise NotImplementedError("TypeDBAdapter.has_edge is not implemented yet")

    async def has_edges(self, edges):
        raise NotImplementedError("TypeDBAdapter.has_edges is not implemented yet")

    async def add_edge(
        self,
        source_id: str | UUID,
        target_id: str | UUID,
        relationship_name: str,
        properties: dict[str, Any] | None = None,
    ):
        raise NotImplementedError("TypeDBAdapter.add_edge is not implemented yet")

    async def add_edges(
        self,
        edges: list[tuple[str, str, str, dict[str, Any]]],
        source_ref_key: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> None:
        raise NotImplementedError("TypeDBAdapter.add_edges is not implemented yet")

    async def get_edges(self, node_id: str):
        raise NotImplementedError("TypeDBAdapter.get_edges is not implemented yet")

    async def get_disconnected_nodes(self) -> list[str]:
        raise NotImplementedError("TypeDBAdapter.get_disconnected_nodes is not implemented yet")

    async def get_predecessors(self, node_id: str, edge_label: str | None = None) -> list[str]:
        raise NotImplementedError("TypeDBAdapter.get_predecessors is not implemented yet")

    async def get_successors(self, node_id: str, edge_label: str | None = None) -> list[str]:
        raise NotImplementedError("TypeDBAdapter.get_successors is not implemented yet")

    async def get_neighbors(self, node_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError("TypeDBAdapter.get_neighbors is not implemented yet")

    async def get_neighborhood(
        self,
        node_ids: list[str],
        depth: int = 1,
        edge_types: list[str] | None = None,
    ) -> tuple[list[tuple[str, dict]], list[tuple[str, str, str, dict]]]:
        raise NotImplementedError("TypeDBAdapter.get_neighborhood is not implemented yet")

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        raise NotImplementedError("TypeDBAdapter.get_node is not implemented yet")

    async def get_nodes(self, node_ids: list[str]) -> list[dict[str, Any]]:
        raise NotImplementedError("TypeDBAdapter.get_nodes is not implemented yet")

    async def get_connections(self, node_id: str | UUID) -> list:
        raise NotImplementedError("TypeDBAdapter.get_connections is not implemented yet")

    async def remove_connection_to_predecessors_of(
        self, node_ids: list[str], edge_label: str
    ) -> None:
        raise NotImplementedError(
            "TypeDBAdapter.remove_connection_to_predecessors_of is not implemented yet"
        )

    async def remove_connection_to_successors_of(
        self, node_ids: list[str], edge_label: str
    ) -> None:
        raise NotImplementedError(
            "TypeDBAdapter.remove_connection_to_successors_of is not implemented yet"
        )

    async def delete_graph(self):
        raise NotImplementedError("TypeDBAdapter.delete_graph is not implemented yet")

    def serialize_properties(self, properties=None) -> dict[str, Any]:
        """Serialize property values so they round-trip through TypeDB.

        UUIDs become strings; nested dicts/lists become JSON strings (they are
        stored inside the `properties_json` attribute).
        """
        serialized = {}
        for key, value in (properties or {}).items():
            if isinstance(value, UUID):
                serialized[key] = str(value)
            elif isinstance(value, (dict, list)):
                serialized[key] = json.dumps(value, cls=JSONEncoder)
            else:
                serialized[key] = value
        return serialized

    async def get_model_independent_graph_data(self):
        raise NotImplementedError(
            "TypeDBAdapter.get_model_independent_graph_data is not implemented yet"
        )

    async def get_graph_data(self):
        raise NotImplementedError("TypeDBAdapter.get_graph_data is not implemented yet")

    async def get_nodeset_subgraph(
        self,
        node_type: type[Any],
        node_name: list[str],
        node_name_filter_operator: str = "OR",
    ) -> tuple[list[tuple[int, dict]], list[tuple[int, int, str, dict]]]:
        raise NotImplementedError("TypeDBAdapter.get_nodeset_subgraph is not implemented yet")

    async def get_filtered_graph_data(self, attribute_filters):
        raise NotImplementedError("TypeDBAdapter.get_filtered_graph_data is not implemented yet")

    async def get_graph_metrics(self, include_optional=False):
        raise NotImplementedError("TypeDBAdapter.get_graph_metrics is not implemented yet")

    async def is_empty(self) -> bool:
        raise NotImplementedError("TypeDBAdapter.is_empty is not implemented yet")
