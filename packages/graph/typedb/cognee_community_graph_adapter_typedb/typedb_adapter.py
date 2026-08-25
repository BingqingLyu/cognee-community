"""TypeDB graph database adapter for cognee.

Maps cognee's property-graph model onto a generic, reified TypeDB schema:
one ``node`` entity type and one ``edge`` relation type (see
``COGNEE_SCHEMA``). Cognee's dynamic node labels and relationship names are
stored as attributes; the full property payload is serialized into the
``properties_json`` attribute, which is the canonical record — the promoted
attributes (``node_type``, ``node_name``, provenance) exist only as query
accelerators and are always written together with the JSON. A typed
per-DataPoint schema mode is a planned follow-up.

Values reach the server through the TypeQL ``given`` stage (driver
``given_rows``), never by string interpolation, so queries are compiled once
per batch and are injection-safe by construction.

The TypeDB Python driver is synchronous (the async Rust core stops at the
FFI boundary), while cognee's ``GraphDBInterface`` is fully async. Driver
work runs on a small dedicated thread pool, one transaction per adapter
call. Consequently each adapter method is atomic, but sequences of calls
(e.g. cognee's add_nodes followed by add_edges) are not — the same property
every sibling adapter has.
"""

import asyncio
import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor
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
# keyword, hence the `source`/`target` role names. `properties_json` is the
# canonical node/edge payload; the other attributes are query accelerators
# written in the same update and must never be edited independently.
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

# --- given-parameterized query templates -----------------------------------

_NODE_UPSERT = """
given $id: string, $type: string, $name: string, $props: string, $ref: string, $run: string;
put $n isa node, has node_id == $id;
update
  $n has node_type == $type;
  $n has node_name == $name;
  $n has properties_json == $props;
  $n has source_ref_key == $ref;
  $n has pipeline_run_id == $run;
  $n has updated_at {now};
"""

_EDGE_UPSERT = """
given $sid: string, $tid: string, $rel: string, $props: string, $ref: string, $run: string;
match
  $s isa node, has node_id == $sid;
  $t isa node, has node_id == $tid;
put
  $e isa edge, links (source: $s, target: $t), has relationship_name == $rel;
update
  $e has properties_json == $props;
  $e has source_ref_key == $ref;
  $e has pipeline_run_id == $run;
  $e has updated_at {now};
"""

_FETCH_NODES = """
given $id: string;
match $n isa node, has node_id == $id;
fetch { "node": { $n.* } };
"""

_HAS_EDGES = """
given $sid: string, $tid: string, $rel: string;
match
  $s isa node, has node_id == $sid;
  $t isa node, has node_id == $tid;
  $e isa edge, links (source: $s, target: $t), has relationship_name == $rel;
fetch { "source": $sid, "target": $tid, "relationship_name": $rel };
"""

_DELETE_INCIDENT_EDGES = """
given $id: string;
match $n isa node, has node_id == $id; $e isa edge, links ($n);
delete $e;
"""

_DELETE_NODES = """
given $id: string;
match $n isa node, has node_id == $id;
delete $n;
"""

_INCIDENT_EDGES = """
given $id: string;
match
  $e isa edge, links (source: $s, target: $t);
  { $s has node_id == $id; } or { $t has node_id == $id; };
  $s has node_id $sid;
  $t has node_id $tid;
  $e has relationship_name $rel;
fetch {
  "source": $sid, "target": $tid, "relationship_name": $rel,
  "edge": { $e.* }, "source_node": { $s.* }, "target_node": { $t.* }
};
"""

# incoming=True: neighbours pointing at the node; incoming=False: pointed to.
_NEIGHBOURS = """
given $id: string{label_decl};
match
  $n isa node, has node_id == $id;
  $e isa edge, links ({anchor_role}: $n, {neighbour_role}: $m){label_constraint};
  $e has relationship_name $rel;
fetch {{ "neighbour": {{ $m.* }}, "relationship_name": $rel, "node": {{ $n.* }} }};
"""

_REMOVE_LABELED_EDGES = """
given $id: string, $label: string;
match
  $n isa node, has node_id == $id;
  $e isa edge, links ({anchor_role}: $n), has relationship_name == $label;
delete $e;
"""

_ALL_NODE_IDS = "match $n isa node, has node_id $id; select $id;"
_ALL_EDGE_ENDPOINTS = """
match
  $e isa edge, links (source: $s, target: $t);
  $s has node_id $sid;
  $t has node_id $tid;
select $sid, $tid;
"""
_ALL_NODES = 'match $n isa node; fetch { "node": { $n.* } };'
_ALL_EDGES = """
match
  $e isa edge, links (source: $s, target: $t);
  $s has node_id $sid;
  $t has node_id $tid;
  $e has relationship_name $rel;
fetch { "source": $sid, "target": $tid, "relationship_name": $rel, "edge": { $e.* } };
"""
_COUNT_NODES = "match $n isa node; reduce $count = count;"
_COUNT_EDGES = "match $e isa edge; reduce $count = count;"


class TypeDBAdapter(GraphDBInterface):
    """Adapter for TypeDB as a cognee graph store."""

    # Cognee gates its Cypher-generating search types on this flag; TypeQL-only
    # adapters must opt out so those searches fail with SearchTypeNotSupported
    # instead of a TypeQL parse error.
    supports_cypher_queries: bool = False

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
        # Guards driver open/close across the worker threads.
        self._driver_lock = threading.Lock()
        # Small dedicated pool: makes the concurrency ceiling on the shared
        # native driver explicit instead of borrowing the default executor.
        self._executor: ThreadPoolExecutor | None = None

    # ------------------------------------------------------------------
    # Connection plumbing (synchronous; always called from worker threads)
    # ------------------------------------------------------------------

    def _get_executor(self) -> ThreadPoolExecutor:
        # Only called from the event loop thread, so no lock is needed.
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="typedb-adapter")
        return self._executor

    async def _run_sync(self, fn, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._get_executor(), fn, *args)

    def _get_driver(self):
        """Lazily open the (synchronous) TypeDB driver."""
        with self._driver_lock:
            if self._driver is None:
                from typedb.driver import Credentials, DriverOptions, DriverTlsConfig, TypeDB

                self._driver = TypeDB.driver(
                    self.address,
                    Credentials(self.username, self.password),
                    DriverOptions(DriverTlsConfig.disabled()),
                )
            return self._driver

    def _close_sync(self) -> None:
        with self._driver_lock:
            if self._driver is not None:
                self._driver.close()
                self._driver = None
                self._schema_initialized = False

    async def close(self) -> None:
        """Release the native TypeDB connection and worker threads.

        Called by cognee's engine cache on eviction (prune, dataset deletion);
        without it the gRPC connection would leak until GC. The adapter
        reopens lazily if used again afterwards.
        """
        async with self._lock:
            await asyncio.to_thread(self._close_sync)
            if self._executor is not None:
                executor = self._executor
                self._executor = None
                await asyncio.to_thread(executor.shutdown, True)

    def _ensure_database_sync(self) -> None:
        """Create the database and define the cognee schema if needed.

        The schema transaction (which takes an exclusive server-side lock) is
        skipped when the database already carries the cognee types.
        """
        from typedb.driver import TransactionType

        driver = self._get_driver()
        if driver.databases.contains(self.database_name):
            defined = "entity node" in driver.databases.get(self.database_name).schema()
        else:
            driver.databases.create(self.database_name)
            defined = False
        if not defined:
            with driver.transaction(self.database_name, TransactionType.SCHEMA) as tx:
                tx.query(COGNEE_SCHEMA).resolve()
                tx.commit()
        self._schema_initialized = True

    async def _ensure_database(self) -> None:
        if self._schema_initialized:
            return
        async with self._lock:
            if not self._schema_initialized:
                await self._run_sync(self._ensure_database_sync)

    def _transaction_type_for(self, query_text: str):
        from typedb.driver import TransactionType

        bare = _COMMENT_RE.sub(" ", _STRING_LITERAL_RE.sub(" ", query_text))
        first_word = bare.lstrip().split(None, 1)[0].lower() if bare.strip() else ""
        if first_word in _SCHEMA_KEYWORDS:
            return TransactionType.SCHEMA
        if _WRITE_STAGE_RE.search(bare):
            return TransactionType.WRITE
        return TransactionType.READ

    def _run_batch_sync(self, specs, transaction_type, collect_rows: bool):
        """Run (query, given_rows) specs in order in one transaction.

        Query promises are all fired before any is resolved, so round trips
        are pipelined server-side while execution order is preserved. In a
        multi-query write batch the answers' row streams must not be
        iterated: a later write in the same transaction interrupts earlier
        answer streams (TSV13) — resolve() still surfaces per-query errors.
        """
        from typedb.driver import TransactionType

        driver = self._get_driver()
        results = []
        with driver.transaction(self.database_name, transaction_type) as tx:
            promises = [
                (query_text, tx.query(query_text, given_rows=given_rows))
                for query_text, given_rows in specs
            ]
            for query_text, promise in promises:
                try:
                    answer = promise.resolve()
                    results.append(self._collect_answer(answer) if collect_rows else [])
                except Exception:
                    logger.error(
                        "TypeDB query failed (%s tx): %.300s", transaction_type, query_text
                    )
                    raise
            if transaction_type != TransactionType.READ:
                tx.commit()
        return results

    @staticmethod
    def _as_specs(queries) -> list[tuple[str, list | None]]:
        return [(query, None) if isinstance(query, str) else query for query in queries]

    async def _read_batch(self, queries) -> list[list[dict]]:
        """Run read queries in one READ transaction; returns rows per query."""
        from typedb.driver import TransactionType

        await self._ensure_database()
        return await self._run_sync(
            self._run_batch_sync, self._as_specs(queries), TransactionType.READ, True
        )

    async def _write_batch(self, queries) -> None:
        """Run write queries in one WRITE transaction; results are discarded."""
        from typedb.driver import TransactionType

        await self._ensure_database()
        specs = self._as_specs(queries)
        await self._run_sync(self._run_batch_sync, specs, TransactionType.WRITE, len(specs) == 1)

    def _query_sync(self, query_text: str, given_rows) -> list[dict[str, Any]]:
        """Run one raw query in its own transaction, inferring the type."""
        transaction_type = self._transaction_type_for(query_text)
        return self._run_batch_sync([(query_text, given_rows)], transaction_type, True)[0]

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
    # Serialization helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _now_literal() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

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

    @staticmethod
    def _document_to_edge_properties(document: dict[str, Any]) -> dict[str, Any]:
        raw = document.get("properties_json")
        if raw:
            try:
                return json.loads(raw)
            except (TypeError, ValueError):
                logger.warning("Undecodable properties_json for an edge")
        return {}

    def _node_row(
        self,
        node: DataPoint,
        source_ref_key: str | None,
        pipeline_run_id: str | None,
    ) -> dict[str, str]:
        properties = node.model_dump()
        name = properties.get("name")
        return {
            "id": str(node.id),
            "type": str(properties.get("type") or type(node).__name__),
            "name": str(name) if name is not None else "",
            "props": json.dumps(properties, cls=JSONEncoder),
            "ref": source_ref_key or "",
            "run": str(pipeline_run_id) if pipeline_run_id else "",
        }

    def _neighbours_spec(self, node_id: str, incoming: bool, edge_label: str | None = None):
        anchor_role, neighbour_role = ("target", "source") if incoming else ("source", "target")
        query = _NEIGHBOURS.format(
            label_decl=", $label: string" if edge_label is not None else "",
            anchor_role=anchor_role,
            neighbour_role=neighbour_role,
            label_constraint=", has relationship_name == $label" if edge_label is not None else "",
        )
        row: dict[str, Any] = {"id": str(node_id)}
        if edge_label is not None:
            row["label"] = edge_label
        return (query, [row])

    # ------------------------------------------------------------------
    # GraphDBInterface — cognee 1.4.2 call surface
    # ------------------------------------------------------------------

    async def query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        transaction_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a raw TypeQL query.

        ``params`` are forwarded as one ``given`` row, so a parameterized
        query declares a matching ``given`` stage, e.g.::

            query('given $name: string; match $n isa node, has node_name == $name; '
                  'fetch { "node": { $n.* } };', {"name": "cognee"})

        ``transaction_type`` ("read" | "write" | "schema") overrides the
        keyword-based inference for queries the heuristic would misjudge.

        Note: cognee's Cypher-oriented search types are disabled for this
        adapter via ``supports_cypher_queries = False``; a TypeQL
        natural-language retriever is planned alongside it.
        """
        from typedb.driver import TransactionType

        await self._ensure_database()
        given_rows = [params] if params else None
        if transaction_type is not None:
            explicit = TransactionType[transaction_type.upper()]
            return (
                await self._run_sync(self._run_batch_sync, [(query, given_rows)], explicit, True)
            )[0]
        return await self._run_sync(self._query_sync, query, given_rows)

    async def has_node(self, node_id: str) -> bool:
        results = await self._read_batch([(_FETCH_NODES, [{"id": str(node_id)}])])
        return bool(results[0])

    async def add_node(self, node: DataPoint | str, properties: dict[str, Any] | None = None):
        """Add (or update) a single node from a DataPoint or an id + properties."""
        if isinstance(node, DataPoint):
            row = self._node_row(node, None, None)
        else:
            node_props = dict(properties or {})
            node_props.setdefault("id", str(node))
            name = node_props.get("name")
            row = {
                "id": str(node),
                "type": str(node_props.get("type", "node")),
                "name": str(name) if name is not None else "",
                "props": json.dumps(node_props, cls=JSONEncoder),
                "ref": "",
                "run": "",
            }
        await self._write_batch([(_NODE_UPSERT.format(now=self._now_literal()), [row])])

    async def add_nodes(
        self,
        nodes: list[DataPoint],
        source_ref_key: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> None:
        """Upsert a batch of DataPoints: one compiled query, one transaction."""
        if not nodes:
            return
        rows = [self._node_row(node, source_ref_key, pipeline_run_id) for node in nodes]
        await self._write_batch([(_NODE_UPSERT.format(now=self._now_literal()), rows)])

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
        rows = [{"id": str(node_id)} for node_id in node_ids]
        # Edges first: deleting a player would leave a dangling edge.
        await self._write_batch([(_DELETE_INCIDENT_EDGES, rows), (_DELETE_NODES, rows)])

    async def has_edge(self, source_id, target_id, relationship_name: str) -> bool:
        matched = await self.has_edges([(source_id, target_id, relationship_name)])
        return bool(matched)

    async def has_edges(self, edges):
        """Return the (source_id, target_id, relationship_name) tuples that exist.

        Cognee consumes this as a list of existing edge tuples (see
        retrieve_existing_edges), not as booleans.
        """
        if not edges:
            return []
        rows = [{"sid": str(edge[0]), "tid": str(edge[1]), "rel": edge[2]} for edge in edges]
        results = await self._read_batch([(_HAS_EDGES, rows)])
        # De-duplicate while preserving first-seen order.
        return list(
            dict.fromkeys(
                (doc["source"], doc["target"], doc["relationship_name"]) for doc in results[0]
            )
        )

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
        """Upsert a batch of edges: one compiled query, one transaction.

        Edge identity is (source, target, relationship_name); properties are
        replaced on re-add. Edges whose endpoints are missing are skipped
        (cognee adds nodes before edges).
        """
        if not edges:
            return
        rows = []
        for source_id, target_id, relationship_name, properties in edges:
            edge_properties = {
                **(properties or {}),
                "source_node_id": str(source_id),
                "target_node_id": str(target_id),
                "relationship_name": relationship_name,
            }
            rows.append(
                {
                    "sid": str(source_id),
                    "tid": str(target_id),
                    "rel": relationship_name,
                    "props": json.dumps(edge_properties, cls=JSONEncoder),
                    "ref": source_ref_key or "",
                    "run": str(pipeline_run_id) if pipeline_run_id else "",
                }
            )
        await self._write_batch([(_EDGE_UPSERT.format(now=self._now_literal()), rows)])

    async def get_edges(self, node_id: str):
        """All edges incident to a node, as (source_id, target_id, {relationship_name})."""
        results = await self._read_batch([(_INCIDENT_EDGES, [{"id": str(node_id)}])])
        return [
            (
                document["source"],
                document["target"],
                {"relationship_name": document["relationship_name"]},
            )
            for document in results[0]
        ]

    async def get_predecessors(self, node_id: str, edge_label: str | None = None) -> list:
        results = await self._read_batch([self._neighbours_spec(node_id, True, edge_label)])
        return [self._document_to_node_dict(doc["neighbour"]) for doc in results[0]]

    async def get_successors(self, node_id: str, edge_label: str | None = None) -> list:
        results = await self._read_batch([self._neighbours_spec(node_id, False, edge_label)])
        return [self._document_to_node_dict(doc["neighbour"]) for doc in results[0]]

    async def get_neighbors(self, node_id: str) -> list[dict[str, Any]]:
        connections = await self.get_connections(node_id)
        anchor = str(node_id)
        return [
            source if target.get("id") == anchor else target for source, _, target in connections
        ]

    async def _induced_edges(
        self, node_ids, wanted_types: set[str] | None = None
    ) -> list[tuple[str, str, str, dict]]:
        """All edges whose both endpoints are in ``node_ids``.

        One incident sweep suffices: every such edge touches at least one of
        the nodes.
        """
        selected = set(node_ids)
        if not selected:
            return []
        incident = (
            await self._read_batch(
                [(_INCIDENT_EDGES, [{"id": node_id} for node_id in sorted(selected)])]
            )
        )[0]
        edges: dict[tuple[str, str, str], dict] = {}
        for document in incident:
            relationship = document["relationship_name"]
            if wanted_types is not None and relationship not in wanted_types:
                continue
            if document["source"] in selected and document["target"] in selected:
                key = (document["source"], document["target"], relationship)
                edges[key] = self._document_to_edge_properties(document["edge"])
        return [(source, target, rel, props) for (source, target, rel), props in edges.items()]

    async def get_neighborhood(
        self,
        node_ids: list[str],
        depth: int = 1,
        edge_types: list[str] | None = None,
    ) -> tuple[list[tuple[str, dict]], list[tuple[str, str, str, dict]]]:
        """K-hop neighborhood of the seed nodes, in get_graph_data() shape.

        Traversal follows only ``edge_types`` when given; the result is the
        induced subgraph over the reached nodes (every qualifying edge whose
        both endpoints were reached, not just the edges walked).
        """
        if not node_ids:
            return ([], [])
        wanted_types = set(edge_types) if edge_types else None

        seed_docs = await self._read_batch(
            [(_FETCH_NODES, [{"id": str(node_id)} for node_id in dict.fromkeys(node_ids)])]
        )
        nodes: dict[str, dict] = {
            doc["node"]["node_id"]: self._document_to_node_dict(doc["node"]) for doc in seed_docs[0]
        }

        frontier = set(nodes)
        for _ in range(max(depth, 0)):
            if not frontier:
                break
            results = await self._read_batch(
                [(_INCIDENT_EDGES, [{"id": node_id} for node_id in sorted(frontier)])]
            )
            next_frontier: set[str] = set()
            for document in results[0]:
                if wanted_types is not None and document["relationship_name"] not in wanted_types:
                    continue
                for endpoint, node_doc in (
                    (document["source"], document["source_node"]),
                    (document["target"], document["target_node"]),
                ):
                    if endpoint not in nodes:
                        nodes[endpoint] = self._document_to_node_dict(node_doc)
                        next_frontier.add(endpoint)
            frontier = next_frontier

        return (list(nodes.items()), await self._induced_edges(nodes, wanted_types))

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        nodes = await self.get_nodes([node_id])
        return nodes[0] if nodes else None

    async def get_nodes(self, node_ids: list[str]) -> list[dict[str, Any]]:
        if not node_ids:
            return []
        rows = [{"id": str(node_id)} for node_id in node_ids]
        results = await self._read_batch([(_FETCH_NODES, rows)])
        return [self._document_to_node_dict(document["node"]) for document in results[0]]

    async def get_connections(self, node_id) -> list:
        """(source_node, {relationship_name}, target_node) triples for a node."""
        results = await self._read_batch(
            [
                self._neighbours_spec(str(node_id), True),
                self._neighbours_spec(str(node_id), False),
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
        anchor = str(node_id)
        for document in results[1]:  # outgoing: node -> neighbour
            if document["neighbour"].get("node_id") == anchor:
                continue  # self-loop already reported by the incoming pass
            connections.append(
                (
                    self._document_to_node_dict(document["node"]),
                    {"relationship_name": document["relationship_name"]},
                    self._document_to_node_dict(document["neighbour"]),
                )
            )
        return connections

    async def remove_connection_to_predecessors_of(
        self, node_ids: list[str], edge_label: str
    ) -> None:
        await self._remove_labeled_edges(node_ids, edge_label, incoming=True)

    async def remove_connection_to_successors_of(
        self, node_ids: list[str], edge_label: str
    ) -> None:
        await self._remove_labeled_edges(node_ids, edge_label, incoming=False)

    async def _remove_labeled_edges(
        self, node_ids: list[str], edge_label: str, incoming: bool
    ) -> None:
        if not node_ids:
            return
        query = _REMOVE_LABELED_EDGES.format(anchor_role="target" if incoming else "source")
        rows = [{"id": str(node_id), "label": edge_label} for node_id in node_ids]
        await self._write_batch([(query, rows)])

    async def delete_graph(self):
        """Remove all nodes and edges (the schema is kept)."""
        await self._write_batch(
            [
                "match $e isa edge; delete $e;",
                "match $n isa node; delete $n;",
            ]
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

    # ------------------------------------------------------------------
    # Analytics tier
    # ------------------------------------------------------------------

    async def _get_all_graph_documents(self):
        return await self._read_batch([_ALL_NODES, _ALL_EDGES])

    async def get_model_independent_graph_data(self):
        """Nodes and (source, relationship, target) triples without model shaping."""
        node_docs, edge_docs = await self._get_all_graph_documents()
        nodes = [self._document_to_node_dict(document["node"]) for document in node_docs]
        elements = [
            [document["source"], document["relationship_name"], document["target"]]
            for document in edge_docs
        ]
        return ([{"nodes": nodes}], [{"elements": elements}])

    async def get_graph_data(self):
        """All nodes and edges, keyed by cognee node id (UUID string)."""
        node_docs, edge_docs = await self._get_all_graph_documents()
        nodes = [
            (document["node"]["node_id"], self._document_to_node_dict(document["node"]))
            for document in node_docs
        ]
        edges = [
            (
                document["source"],
                document["target"],
                document["relationship_name"],
                self._document_to_edge_properties(document["edge"]),
            )
            for document in edge_docs
        ]
        return (nodes, edges)

    async def get_nodeset_subgraph(
        self,
        node_type: type[Any],
        node_name: list[str],
        node_name_filter_operator: str = "OR",
    ) -> tuple[list[tuple[str, dict]], list[tuple[str, str, str, dict]]]:
        """Subgraph around nodes of ``node_type`` named in ``node_name``.

        "OR": seeds plus all their neighbours. "AND": seeds plus only the
        neighbours connected to every seed. Includes every edge whose two
        endpoints are in the selected set.
        """
        label = node_type.__name__
        seed_query = """
        given $label: string, $name: string;
        match $n isa node, has node_type == $label, has node_name == $name;
        fetch { "node": { $n.* } };
        """
        seed_rows = [{"label": label, "name": name} for name in node_name]
        seed_docs = (await self._read_batch([(seed_query, seed_rows)]))[0]
        seeds = {
            doc["node"]["node_id"]: self._document_to_node_dict(doc["node"]) for doc in seed_docs
        }
        if not seeds:
            return ([], [])

        incident = (
            await self._read_batch(
                [(_INCIDENT_EDGES, [{"id": node_id} for node_id in sorted(seeds)])]
            )
        )[0]

        neighbour_seeds: dict[str, set[str]] = {}
        neighbour_docs: dict[str, dict] = {}
        for document in incident:
            for anchor, other, other_doc in (
                (document["source"], document["target"], document["target_node"]),
                (document["target"], document["source"], document["source_node"]),
            ):
                if anchor in seeds and other not in seeds:
                    neighbour_seeds.setdefault(other, set()).add(anchor)
                    neighbour_docs[other] = other_doc

        if node_name_filter_operator == "AND":
            wanted = {
                node_id for node_id, connected in neighbour_seeds.items() if connected == set(seeds)
            }
        else:
            wanted = set(neighbour_seeds)

        nodes = dict(seeds)
        nodes.update(
            {node_id: self._document_to_node_dict(neighbour_docs[node_id]) for node_id in wanted}
        )

        return (list(nodes.items()), await self._induced_edges(nodes))

    async def get_filtered_graph_data(self, attribute_filters):
        """Nodes matching the attribute filters, and edges between them.

        ``attribute_filters`` is a list with one dict of {attribute: [values]};
        a node matches when every filtered attribute has an allowed value.
        Filtering runs client-side over the canonical properties_json payload.
        """
        filters = {attribute: set(values) for attribute, values in attribute_filters[0].items()}
        all_nodes, all_edges = await self.get_graph_data()
        nodes = [
            (node_id, properties)
            for node_id, properties in all_nodes
            if all(properties.get(attribute) in values for attribute, values in filters.items())
        ]
        selected = {node_id for node_id, _ in nodes}
        edges = [edge for edge in all_edges if edge[0] in selected and edge[1] in selected]
        return (nodes, edges)

    async def _edge_endpoint_pairs(self) -> tuple[list[str], list[tuple[str, str]]]:
        id_rows, endpoint_rows = await self._read_batch([_ALL_NODE_IDS, _ALL_EDGE_ENDPOINTS])
        node_ids = [row["id"] for row in id_rows]
        endpoints = [(row["sid"], row["tid"]) for row in endpoint_rows]
        return node_ids, endpoints

    @staticmethod
    def _connected_components(node_ids: list[str], endpoints: list[tuple[str, str]]):
        """Union-find over the edge list; returns {root: [member ids]}."""
        parent = {node_id: node_id for node_id in node_ids}

        def find(node_id: str) -> str:
            while parent[node_id] != node_id:
                parent[node_id] = parent[parent[node_id]]
                node_id = parent[node_id]
            return node_id

        for source, target in endpoints:
            if source in parent and target in parent:
                source_root, target_root = find(source), find(target)
                if source_root != target_root:
                    parent[target_root] = source_root

        components: dict[str, list[str]] = {}
        for node_id in parent:
            components.setdefault(find(node_id), []).append(node_id)
        return components

    async def get_disconnected_nodes(self) -> list[str]:
        """Ids of nodes outside the largest weakly connected component."""
        node_ids, endpoints = await self._edge_endpoint_pairs()
        if not node_ids:
            return []
        components = self._connected_components(node_ids, endpoints)
        largest = max(components.values(), key=len)
        largest_set = set(largest)
        return [node_id for node_id in node_ids if node_id not in largest_set]

    async def get_graph_metrics(self, include_optional=False):
        """Structural metrics; all-pairs metrics are reported unsupported (-1)."""
        try:
            counts = await self._read_batch([_COUNT_NODES, _COUNT_EDGES])
            num_nodes = counts[0][0].get("count", 0) if counts[0] else 0
            num_edges = counts[1][0].get("count", 0) if counts[1] else 0

            node_ids, endpoints = await self._edge_endpoint_pairs()
            components = self._connected_components(node_ids, endpoints)
            component_sizes = [len(members) for members in components.values()]

            metrics = {
                "num_nodes": num_nodes,
                "num_edges": num_edges,
                "mean_degree": (2 * num_edges) / num_nodes if num_nodes > 0 else 0,
                "edge_density": num_edges / (num_nodes * (num_nodes - 1)) if num_nodes > 1 else 0,
                "num_connected_components": len(component_sizes),
                "sizes_of_connected_components": component_sizes,
                "num_selfloops": (
                    sum(1 for source, target in endpoints if source == target)
                    if include_optional
                    else -1
                ),
                # All-pairs metrics need every shortest path, prohibitive on a
                # general graph; unsupported (-1) like the sibling adapters.
                "diameter": -1,
                "avg_shortest_path_length": -1,
                "avg_clustering": -1,
            }
            return metrics
        except Exception as error:
            logger.error("Failed to get graph metrics: %s", error)
            return {
                "num_nodes": 0,
                "num_edges": 0,
                "mean_degree": 0,
                "edge_density": 0,
                "num_connected_components": 0,
                "sizes_of_connected_components": [],
                "num_selfloops": -1,
                "diameter": -1,
                "avg_shortest_path_length": -1,
                "avg_clustering": -1,
            }

    async def is_empty(self) -> bool:
        results = await self._read_batch(["match $n isa node; limit 1; reduce $count = count;"])
        return bool(results[0] and results[0][0].get("count", 1) == 0)
