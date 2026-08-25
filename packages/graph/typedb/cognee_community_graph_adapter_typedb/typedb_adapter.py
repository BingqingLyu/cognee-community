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
queries -> commit/close) inside a single ``asyncio.to_thread`` hop. Batch
methods pipeline all their queries through one transaction, using the
driver's promise API to avoid per-query round trips.

Not yet implemented (analytics tier): get_disconnected_nodes,
get_neighborhood, get_model_independent_graph_data, get_nodeset_subgraph,
get_filtered_graph_data, get_graph_metrics.
"""

import asyncio
import json
import re
from datetime import datetime, timezone
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
# Word-boundary match, applied only after string literals and comments are
# stripped, so reads over e.g. `updated_at` or values like "deleted" are not
# misclassified as writes.
_WRITE_STAGE_RE = re.compile(r"\b(insert|put|update|delete)\b")
_STRING_LITERAL_RE = re.compile(r'"(?:\\.|[^"\\])*"')
_COMMENT_RE = re.compile(r"#[^\n]*")


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

    def _close_sync(self) -> None:
        if self._driver is not None:
            self._driver.close()
            self._driver = None
            self._schema_initialized = False

    async def close(self) -> None:
        """Release the native TypeDB connection.

        Called by cognee's engine cache on eviction (prune, dataset deletion);
        without it the gRPC connection would leak until GC.
        """
        async with self._lock:
            await asyncio.to_thread(self._close_sync)

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

        bare = _COMMENT_RE.sub(" ", _STRING_LITERAL_RE.sub(" ", query_text))
        first_word = bare.lstrip().split(None, 1)[0].lower() if bare.strip() else ""
        if first_word in _SCHEMA_KEYWORDS:
            return TransactionType.SCHEMA
        if _WRITE_STAGE_RE.search(bare):
            return TransactionType.WRITE
        return TransactionType.READ

    def _execute_batch_sync(self, queries: list[str], transaction_type) -> list[list[dict]]:
        """Run queries in order within one transaction; commit unless READ.

        Query promises are all fired before any is resolved, so round trips
        are pipelined server-side while execution order is preserved. In a
        multi-query write batch the answers' row streams are not iterated:
        a later write in the same transaction interrupts earlier answer
        streams (TSV13), and write results are unused anyway — resolve()
        still surfaces per-query errors.
        """
        from typedb.driver import TransactionType

        driver = self._get_driver()
        collect_rows = transaction_type == TransactionType.READ or len(queries) == 1
        with driver.transaction(self.database_name, transaction_type) as tx:
            promises = [tx.query(query_text) for query_text in queries]
            answers = [promise.resolve() for promise in promises]
            results = (
                [self._collect_answer(answer) for answer in answers]
                if collect_rows
                else [[] for _ in answers]
            )
            if transaction_type != TransactionType.READ:
                tx.commit()
        return results

    async def _execute_batch(self, queries: list[str], write: bool = False) -> list[list[dict]]:
        from typedb.driver import TransactionType

        await self._ensure_database()
        transaction_type = TransactionType.WRITE if write else TransactionType.READ
        return await asyncio.to_thread(self._execute_batch_sync, queries, transaction_type)

    def _query_sync(self, query_text: str) -> list[dict[str, Any]]:
        """Run one TypeQL query in its own transaction and return plain data."""
        return self._execute_batch_sync([query_text], self._transaction_type_for(query_text))[0]

    @classmethod
    def _concept_to_value(cls, concept) -> Any:
        if concept is None:
            return None
        value = concept.try_get_value()
        if value is not None:
            return value
        if concept.is_type():
            return concept.get_label()
        return concept.try_get_iid()

    @classmethod
    def _collect_answer(cls, answer) -> list[dict[str, Any]]:
        """Convert a QueryAnswer into a list of plain dicts.

        Fetch queries yield JSON documents; concept rows are decoded to raw
        attribute/value payloads (types to labels, other instances to IIDs).
        """
        if answer.is_concept_documents():
            return list(answer.as_concept_documents())
        if answer.is_concept_rows():
            return [
                {name: cls._concept_to_value(row.get(name)) for name in row.column_names()}
                for row in answer.as_concept_rows()
            ]
        return []

    # ------------------------------------------------------------------
    # TypeQL construction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _quote(value: Any) -> str:
        """Render a value as a TypeQL string literal.

        Control characters are replaced (TypeQL string escapes cover quotes
        and backslashes; JSON-serialized payloads never contain raw newlines).
        """
        text = str(value)
        text = re.sub(r"[\x00-\x1f]", " ", text)
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'

    @staticmethod
    def _now_literal() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    def _node_upsert_query(
        self,
        node_id: str,
        node_type: str,
        node_name: str | None,
        properties_json: str,
        source_ref_key: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> str:
        updates = [
            f"  $n has node_type {self._quote(node_type)};",
            f"  $n has properties_json {self._quote(properties_json)};",
            f"  $n has updated_at {self._now_literal()};",
        ]
        if node_name is not None:
            updates.append(f"  $n has node_name {self._quote(node_name)};")
        if source_ref_key is not None:
            updates.append(f"  $n has source_ref_key {self._quote(source_ref_key)};")
        if pipeline_run_id is not None:
            updates.append(f"  $n has pipeline_run_id {self._quote(pipeline_run_id)};")
        return f"put $n isa node, has node_id {self._quote(node_id)};\nupdate\n" + "\n".join(
            updates
        )

    def _edge_upsert_query(
        self,
        source_id: str,
        target_id: str,
        relationship_name: str,
        properties_json: str,
        source_ref_key: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> str:
        updates = [
            f"  $e has properties_json {self._quote(properties_json)};",
            f"  $e has updated_at {self._now_literal()};",
        ]
        if source_ref_key is not None:
            updates.append(f"  $e has source_ref_key {self._quote(source_ref_key)};")
        if pipeline_run_id is not None:
            updates.append(f"  $e has pipeline_run_id {self._quote(pipeline_run_id)};")
        return (
            "match\n"
            f"  $s isa node, has node_id {self._quote(source_id)};\n"
            f"  $t isa node, has node_id {self._quote(target_id)};\n"
            "put\n"
            "  $e isa edge, links (source: $s, target: $t),"
            f" has relationship_name {self._quote(relationship_name)};\n"
            "update\n" + "\n".join(updates)
        )

    @staticmethod
    def _document_to_node_dict(document: dict[str, Any]) -> dict[str, Any]:
        """Rebuild the cognee node property dict from a fetched `{ $n.* }` doc."""
        properties = {}
        raw = document.get("properties_json")
        if raw:
            try:
                properties = json.loads(raw)
            except (TypeError, ValueError):
                logger.warning("Undecodable properties_json for node %s", document.get("node_id"))
        properties.setdefault("id", document.get("node_id"))
        if document.get("updated_at") is not None:
            properties.setdefault("updated_at", document["updated_at"])
        return properties

    def _serialize_datapoint(self, node: DataPoint) -> tuple[str, str, str | None, str]:
        properties = node.model_dump()
        node_id = str(node.id)
        node_type = str(properties.get("type") or type(node).__name__)
        name = properties.get("name")
        node_name = str(name) if name is not None else None
        properties_json = json.dumps(properties, cls=JSONEncoder)
        return node_id, node_type, node_name, properties_json

    # ------------------------------------------------------------------
    # GraphDBInterface — cognee 1.4.2 call surface
    # ------------------------------------------------------------------

    async def query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a raw TypeQL query.

        Note: cognee's Cypher-oriented search types pass Cypher here; this
        adapter executes TypeQL only (a TypeQL natural-language retriever is
        planned alongside this adapter). Query parameters are not supported:
        pass a fully-formed TypeQL string (the TypeDB `given` stage is the
        planned parameterization mechanism).
        """
        await self._ensure_database()
        if params:
            raise ValueError(
                "TypeDBAdapter.query takes a fully-formed TypeQL string and does not "
                "support query parameters; interpolate values before calling."
            )
        return await asyncio.to_thread(self._query_sync, query)

    async def has_node(self, node_id: str) -> bool:
        results = await self._execute_batch(
            [f"match $n isa node, has node_id {self._quote(str(node_id))}; reduce $count = count;"]
        )
        return bool(results[0] and results[0][0].get("count", 0) > 0)

    async def add_node(self, node: DataPoint | str, properties: dict[str, Any] | None = None):
        """Add (or update) a single node from a DataPoint or an id + properties."""
        if isinstance(node, DataPoint):
            node_id, node_type, node_name, properties_json = self._serialize_datapoint(node)
        else:
            node_props = dict(properties or {})
            node_id = str(node)
            node_props.setdefault("id", node_id)
            node_type = str(node_props.get("type", "node"))
            name = node_props.get("name")
            node_name = str(name) if name is not None else None
            properties_json = json.dumps(node_props, cls=JSONEncoder)

        await self._execute_batch(
            [self._node_upsert_query(node_id, node_type, node_name, properties_json)],
            write=True,
        )

    async def add_nodes(
        self,
        nodes: list[DataPoint],
        source_ref_key: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> None:
        """Upsert a batch of DataPoints in a single transaction."""
        if not nodes:
            return
        queries = []
        for node in nodes:
            node_id, node_type, node_name, properties_json = self._serialize_datapoint(node)
            queries.append(
                self._node_upsert_query(
                    node_id,
                    node_type,
                    node_name,
                    properties_json,
                    source_ref_key=source_ref_key,
                    pipeline_run_id=str(pipeline_run_id) if pipeline_run_id else None,
                )
            )
        await self._execute_batch(queries, write=True)

    async def extract_node(self, node_id: str):
        return await self.get_node(node_id)

    async def extract_nodes(self, node_ids: list[str]):
        return await self.get_nodes(node_ids)

    async def delete_node(self, node_id: str):
        await self.delete_nodes([node_id])

    async def delete_nodes(self, node_ids: list[str]) -> None:
        """Delete nodes and their incident edges in one transaction."""
        if not node_ids:
            return
        queries = []
        for node_id in node_ids:
            quoted = self._quote(str(node_id))
            # Edges first: deleting a player would leave a dangling edge.
            queries.append(
                f"match $n isa node, has node_id {quoted}; $e isa edge, links ($n); delete $e;"
            )
            queries.append(f"match $n isa node, has node_id {quoted}; delete $n;")
        await self._execute_batch(queries, write=True)

    def _has_edge_query(self, source_id, target_id, relationship_name: str) -> str:
        return (
            "match"
            f" $s isa node, has node_id {self._quote(str(source_id))};"
            f" $t isa node, has node_id {self._quote(str(target_id))};"
            " $e isa edge, links (source: $s, target: $t),"
            f" has relationship_name {self._quote(relationship_name)};"
            " reduce $count = count;"
        )

    async def has_edge(self, source_id, target_id, relationship_name: str) -> bool:
        results = await self._execute_batch(
            [self._has_edge_query(source_id, target_id, relationship_name)]
        )
        return bool(results[0] and results[0][0].get("count", 0) > 0)

    async def has_edges(self, edges):
        """Return the (source_id, target_id, relationship_name) tuples that exist.

        Cognee consumes this as a list of existing edge tuples (see
        retrieve_existing_edges), not as booleans.
        """
        if not edges:
            return []
        queries = [self._has_edge_query(edge[0], edge[1], edge[2]) for edge in edges]
        results = await self._execute_batch(queries)
        return [
            (str(edge[0]), str(edge[1]), edge[2])
            for edge, result in zip(edges, results, strict=True)
            if result and result[0].get("count", 0) > 0
        ]

    async def add_edge(
        self,
        source_id,
        target_id,
        relationship_name: str,
        properties: dict[str, Any] | None = None,
    ):
        await self.add_edges([(str(source_id), str(target_id), relationship_name, properties)])

    async def add_edges(
        self,
        edges: list[tuple[str, str, str, dict[str, Any]]],
        source_ref_key: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> None:
        """Upsert a batch of edges in a single transaction.

        Edge identity is (source, target, relationship_name); properties are
        replaced on re-add. Edges whose endpoints are missing are skipped
        (cognee adds nodes before edges).
        """
        if not edges:
            return
        queries = []
        for source_id, target_id, relationship_name, properties in edges:
            edge_properties = {
                **(properties or {}),
                "source_node_id": str(source_id),
                "target_node_id": str(target_id),
                "relationship_name": relationship_name,
            }
            queries.append(
                self._edge_upsert_query(
                    str(source_id),
                    str(target_id),
                    relationship_name,
                    json.dumps(edge_properties, cls=JSONEncoder),
                    source_ref_key=source_ref_key,
                    pipeline_run_id=str(pipeline_run_id) if pipeline_run_id else None,
                )
            )
        await self._execute_batch(queries, write=True)

    async def get_edges(self, node_id: str):
        """All edges incident to a node, as (source_id, target_id, {relationship_name})."""
        quoted = self._quote(str(node_id))
        results = await self._execute_batch(
            [
                "match\n"
                "  $e isa edge, links (source: $s, target: $t);\n"
                f"  {{ $s has node_id {quoted}; }} or {{ $t has node_id {quoted}; }};\n"
                "  $s has node_id $sid;\n"
                "  $t has node_id $tid;\n"
                "  $e has relationship_name $rel;\n"
                'fetch { "source": $sid, "target": $tid, "relationship_name": $rel };'
            ]
        )
        return [
            (
                document["source"],
                document["target"],
                {"relationship_name": document["relationship_name"]},
            )
            for document in results[0]
        ]

    def _neighbour_query(self, node_id: str, incoming: bool, edge_label: str | None = None) -> str:
        """Fetch neighbour nodes (+ relationship) on one side of a node."""
        anchor_role, neighbour_role = ("target", "source") if incoming else ("source", "target")
        label_constraint = (
            f", has relationship_name {self._quote(edge_label)}" if edge_label else ""
        )
        return (
            "match\n"
            f"  $n isa node, has node_id {self._quote(str(node_id))};\n"
            f"  $e isa edge, links ({anchor_role}: $n, {neighbour_role}: $m){label_constraint};\n"
            "  $e has relationship_name $rel;\n"
            'fetch { "neighbour": { $m.* }, "relationship_name": $rel, "node": { $n.* } };'
        )

    async def get_predecessors(self, node_id: str, edge_label: str | None = None) -> list:
        results = await self._execute_batch([self._neighbour_query(node_id, True, edge_label)])
        return [self._document_to_node_dict(doc["neighbour"]) for doc in results[0]]

    async def get_successors(self, node_id: str, edge_label: str | None = None) -> list:
        results = await self._execute_batch([self._neighbour_query(node_id, False, edge_label)])
        return [self._document_to_node_dict(doc["neighbour"]) for doc in results[0]]

    async def get_neighbors(self, node_id: str) -> list[dict[str, Any]]:
        results = await self._execute_batch(
            [
                self._neighbour_query(node_id, True),
                self._neighbour_query(node_id, False),
            ]
        )
        return [self._document_to_node_dict(doc["neighbour"]) for batch in results for doc in batch]

    async def get_neighborhood(
        self,
        node_ids: list[str],
        depth: int = 1,
        edge_types: list[str] | None = None,
    ) -> tuple[list[tuple[str, dict]], list[tuple[str, str, str, dict]]]:
        raise NotImplementedError("TypeDBAdapter.get_neighborhood is not implemented yet")

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        nodes = await self.get_nodes([node_id])
        return nodes[0] if nodes else None

    async def get_nodes(self, node_ids: list[str]) -> list[dict[str, Any]]:
        if not node_ids:
            return []
        queries = [
            (
                f"match $n isa node, has node_id {self._quote(str(node_id))};\n"
                'fetch { "node": { $n.* } };'
            )
            for node_id in node_ids
        ]
        results = await self._execute_batch(queries)
        return [
            self._document_to_node_dict(document["node"]) for batch in results for document in batch
        ]

    async def get_connections(self, node_id) -> list:
        """(neighbour, {relationship_name}, node) triples in edge direction order."""
        results = await self._execute_batch(
            [
                self._neighbour_query(str(node_id), True),
                self._neighbour_query(str(node_id), False),
            ]
        )
        connections = []
        for document in results[0]:  # incoming: neighbour -> node
            connections.append(
                (
                    self._document_to_node_dict(document["neighbour"]),
                    {"relationship_name": document["relationship_name"]},
                    self._document_to_node_dict(document["node"]),
                )
            )
        for document in results[1]:  # outgoing: node -> neighbour
            connections.append(
                (
                    self._document_to_node_dict(document["node"]),
                    {"relationship_name": document["relationship_name"]},
                    self._document_to_node_dict(document["neighbour"]),
                )
            )
        return connections

    def _remove_connections_query(self, node_id: str, incoming: bool, edge_label: str) -> str:
        anchor_role = "target" if incoming else "source"
        return (
            "match"
            f" $n isa node, has node_id {self._quote(str(node_id))};"
            f" $e isa edge, links ({anchor_role}: $n),"
            f" has relationship_name {self._quote(edge_label)};"
            " delete $e;"
        )

    async def remove_connection_to_predecessors_of(
        self, node_ids: list[str], edge_label: str
    ) -> None:
        if not node_ids:
            return
        await self._execute_batch(
            [self._remove_connections_query(node_id, True, edge_label) for node_id in node_ids],
            write=True,
        )

    async def remove_connection_to_successors_of(
        self, node_ids: list[str], edge_label: str
    ) -> None:
        if not node_ids:
            return
        await self._execute_batch(
            [self._remove_connections_query(node_id, False, edge_label) for node_id in node_ids],
            write=True,
        )

    async def delete_graph(self):
        """Remove all nodes and edges (the schema is kept)."""
        await self._execute_batch(
            [
                "match $e isa edge; delete $e;",
                "match $n isa node; delete $n;",
            ],
            write=True,
        )

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
        """All nodes and edges, keyed by cognee node id (UUID string)."""
        results = await self._execute_batch(
            [
                'match $n isa node; fetch { "node": { $n.* } };',
                "match\n"
                "  $e isa edge, links (source: $s, target: $t);\n"
                "  $s has node_id $sid;\n"
                "  $t has node_id $tid;\n"
                "  $e has relationship_name $rel;\n"
                'fetch { "source": $sid, "target": $tid, "relationship_name": $rel,'
                ' "edge": { $e.* } };',
            ]
        )
        nodes = [
            (document["node"]["node_id"], self._document_to_node_dict(document["node"]))
            for document in results[0]
        ]
        edges = []
        for document in results[1]:
            edge_properties = {}
            raw = document["edge"].get("properties_json")
            if raw:
                try:
                    edge_properties = json.loads(raw)
                except (TypeError, ValueError):
                    logger.warning("Undecodable properties_json for an edge")
            edges.append(
                (
                    document["source"],
                    document["target"],
                    document["relationship_name"],
                    edge_properties,
                )
            )
        return (nodes, edges)

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
        results = await self._execute_batch(["match $n isa node; limit 1; reduce $count = count;"])
        return bool(results[0] and results[0][0].get("count", 1) == 0)
