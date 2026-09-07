"""TypeDB graph database adapter for cognee.

Maps cognee's property-graph model onto the reified TypeDB schema in
``schema.tql``: one ``node`` entity type and one ``edge`` relation type
(roles ``source``/``target``). Cognee's dynamic node labels and relationship names
are stored as attributes; the full property payload is serialized into the
``properties-json`` attribute, which is the canonical record — the promoted
attributes (``node-type``, ``name``) exist only as query accelerators, mirror
the JSON, and are always written together with it. Edges carry an explicit
``edge-key`` (``"{source}|{target}|{relationship}"``) as their identity.

Provenance attributes (``source-ref-key``, ``source-run-id``) are
multi-valued stamps outside the payload: each write that provides a value
adds it, so a node touched by several pipeline runs keeps every run id, and
a write without provenance leaves existing stamps untouched. A node's
``created-at`` mirrors its DataPoint payload's ``created_at``; an edge's is
set once on first write; ``updated-at`` is the write time (all epoch ms).

Values reach the server through the TypeQL ``given`` stage (driver
``given_rows``), never by string interpolation, so queries are compiled once
per template and are injection-safe by construction.

The TypeDB Python driver is synchronous (the async Rust core stops at the
FFI boundary), while cognee's ``GraphDBInterface`` is fully async. Driver
work runs on a small dedicated thread pool. Batch writes (``add_nodes`` /
``add_edges``) run as chunked transactions of ``WRITE_CHUNK_ROWS`` rows with
``WRITE_CONCURRENCY`` in flight, so a batch is not atomic: on failure,
committed chunks stay committed and pending ones are cancelled — the same
property the sibling adapters' batches have. Commit isolation conflicts
(``[STC2]``) are retried on every write path; TypeDB rolls a failed commit
back entirely, so a retry never duplicates work.
"""

import asyncio
import json
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from functools import cache
from itertools import product
from pathlib import Path
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

# The schema is the single source of truth in schema.tql (shipped with the
# package). The define is idempotent and re-run on every fresh adapter, so
# additive schema evolution reaches existing databases; incompatible changes
# (e.g. new @key constraints) require a fresh database.
COGNEE_SCHEMA = (Path(__file__).parent / "schema.tql").read_text(encoding="utf-8")

_SCHEMA_KEYWORDS = ("define", "undefine", "redefine")
# Word-boundary match, applied only after string literals and comments are
# stripped, so reads over e.g. `updated-at` or values like "deleted" are not
# misclassified as writes.
_WRITE_STAGE_RE = re.compile(r"\b(insert|put|update|delete)\b")
_STRING_LITERAL_RE = re.compile(r'"(?:\\.|[^"\\])*"')
_COMMENT_RE = re.compile(r"#[^\n]*")

# --- given-parameterized query templates -----------------------------------


def _provenance_stages(var: str, with_ref: bool, with_run: bool) -> tuple[list[str], list[str]]:
    """Extra given declarations and insert statements for provenance stamps.

    The stamps are @card(0..) so they are inserted (accumulated), not updated.
    """
    given, inserts = [], []
    if with_ref:
        given.append("$ref: string")
        inserts.append(f"  {var} has source-ref-key == $ref;")
    if with_run:
        given.append("$run: string")
        inserts.append(f"  {var} has source-run-id == $run;")
    return given, inserts


@cache
def _node_upsert_template(with_ref: bool, with_run: bool) -> str:
    """Node upsert pipeline: put identity, replace mirrors (incl. created-at), stamp."""
    given = [
        "$id: string",
        "$type: string",
        "$name: string",
        "$props: string",
        "$created: integer",
        "$now: integer",
    ]
    extra_given, inserts = _provenance_stages("$n", with_ref, with_run)
    given += extra_given
    stages = [
        "given " + ", ".join(given) + ";",
        "put $n isa node, has node-id == $id;",
        "update",
        "  $n has node-type == $type;",
        "  $n has name == $name;",
        "  $n has properties-json == $props;",
        "  $n has created-at == $created;",
        "  $n has updated-at == $now;",
    ]
    if inserts:
        stages += ["insert", *inserts]
    return "\n".join(stages)


@cache
def _edge_upsert_template(with_ref: bool, with_run: bool) -> str:
    """Edge upsert pipeline keyed on edge-key; endpoints must already exist."""
    given = [
        "$key: string",
        "$sid: string",
        "$tid: string",
        "$rel: string",
        "$props: string",
        "$now: integer",
    ]
    extra_given, inserts = _provenance_stages("$e", with_ref, with_run)
    given += extra_given
    stages = [
        "given " + ", ".join(given) + ";",
        "match",
        "  $sa isa node-id == $sid; $s isa node, has $sa;",
        "  $ta isa node-id == $tid; $t isa node, has $ta;",
        "put",
        "  $e isa edge, links (source: $s, target: $t),",
        "    has edge-key == $key, has relationship-name == $rel;",
        "update",
        "  $e has properties-json == $props;",
        "  $e has updated-at == $now;",
    ]
    if inserts:
        stages += ["insert", *inserts]
    return "\n".join(stages)


# Edge created-at is set once via a separate statement (run after the upsert
# in the same transaction) rather than as trailing pipeline stages: a
# negation-only `match not {...}` stage after `update` did not filter rows
# that already carried created-at, re-stamping them on every write. Nodes
# need no such statement: their created-at mirrors the DataPoint payload's
# own created_at (see _row_from_properties).
_SET_EDGE_CREATED_AT = """
given $key: string, $now: integer;
match $k isa edge-key == $key; $e isa edge, has $k; not { $e has created-at $c; };
insert $e has created-at == $now;
"""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _edge_key(source_id: str, target_id: str, relationship_name: str) -> str:
    """Edge identity as a JSON-encoded triple, so ids containing '|' cannot collide."""
    return json.dumps([source_id, target_id, relationship_name], separators=(",", ":"))


_FETCH_NODES = """
given $id: string;
match $a isa node-id == $id; $n isa node, has $a;
fetch { "node": { $n.* } };
"""

_HAS_EDGES = """
given $key: string, $sid: string, $tid: string, $rel: string;
match $k isa edge-key == $key; $e isa edge, has $k;
fetch { "source": $sid, "target": $tid, "relationship_name": $rel };
"""

_DELETE_INCIDENT_EDGES = """
given $id: string;
match $a isa node-id == $id; $n isa node, has $a; $e isa edge, links ($n);
delete $e;
"""

_DELETE_NODES = """
given $id: string;
match $a isa node-id == $id; $n isa node, has $a;
delete $n;
"""

# Incident edges of an anchor node, one query per role the anchor plays (an
# `or` over the role is 12-18x slower than two directional queries — see
# benchmarks/README.md). Both produce the same document shape; self-loops
# appear in both and consumers de-duplicate by (source, target, rel). Only
# the far endpoint's document is fetched — the anchor is always already
# known to every consumer, and hub nodes would otherwise ship their payload
# once per incident edge.
_INCIDENT_EDGES_OUT = """
given $id: string;
match
  $a isa node-id == $id; $n isa node, has $a;
  $e isa edge, links (source: $n, target: $m);
  $m has node-id $mid;
  $e has relationship-name $rel;
fetch {
  "source": $id, "target": $mid, "relationship_name": $rel,
  "edge": { $e.* }, "source_node": { "node-id": $id }, "target_node": { $m.* }
};
"""

_INCIDENT_EDGES_IN = """
given $id: string;
match
  $a isa node-id == $id; $n isa node, has $a;
  $e isa edge, links (source: $m, target: $n);
  $m has node-id $mid;
  $e has relationship-name $rel;
fetch {
  "source": $mid, "target": $id, "relationship_name": $rel,
  "edge": { $e.* }, "source_node": { $m.* }, "target_node": { "node-id": $id }
};
"""

# incoming=True: neighbours pointing at the node; incoming=False: pointed to.
_NEIGHBOURS = """
given $id: string{label_decl};
match
  $a isa node-id == $id; $n isa node, has $a;
  $e isa edge, links ({anchor_role}: $n, {neighbour_role}: $m){label_constraint};
  $e has relationship-name $rel;
fetch {{ "neighbour": {{ $m.* }}, "relationship_name": $rel, "node": {{ $n.* }} }};
"""

_REMOVE_LABELED_EDGES = """
given $id: string, $label: string;
match
  $a isa node-id == $id; $n isa node, has $a;
  $e isa edge, links ({anchor_role}: $n), has relationship-name == $label;
delete $e;
"""

_ALL_NODE_IDS = "match $n isa node, has node-id $id; select $id;"
_ALL_EDGE_ENDPOINTS = """
match
  $e isa edge, links (source: $s, target: $t);
  $s has node-id $sid;
  $t has node-id $tid;
select $sid, $tid;
"""
_ALL_NODES = 'match $n isa node; fetch { "node": { $n.* } };'
_ALL_EDGES = """
match
  $e isa edge, links (source: $s, target: $t);
  $s has node-id $sid;
  $t has node-id $tid;
  $e has relationship-name $rel;
fetch { "source": $sid, "target": $tid, "relationship_name": $rel, "edge": { $e.* } };
"""
_ISOLATED_NODE_IDS = """
match
  $n isa node, has node-id $id;
  not { $e isa edge, links ($n); };
select $id;
"""

# Batch writes are split into transactions of this many rows, with up to
# WRITE_CONCURRENCY transactions in flight (see benchmarks/README.md: cost
# grows with rows per transaction, and concurrent transactions scale ~2-3x).
WRITE_CHUNK_ROWS = 200
WRITE_CONCURRENCY = 4
COMMIT_RETRIES = 6

# Filterable attributes promoted out of properties-json, usable server-side.
_PROMOTED_FILTER_ATTRS = {"type": "node-type", "name": "name"}


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
        # Cognee configs commonly carry scheme-prefixed URLs (any scheme —
        # the field is shared across graph providers); TypeDB wants host:port.
        address = (graph_database_url or DEFAULT_ADDRESS).split("://", 1)[-1]
        if graph_database_port and ":" not in address:
            address = f"{address}:{graph_database_port}"

        self.address = address
        self.username = graph_database_username or DEFAULT_USERNAME
        self.password = graph_database_password or DEFAULT_PASSWORD
        self.database_name = database_name or DEFAULT_DATABASE

        self._driver = None
        self._schema_initialized = False
        self._lock = asyncio.Lock()
        # Caps in-flight chunk transactions across ALL concurrent batch calls.
        self._write_semaphore = asyncio.Semaphore(WRITE_CONCURRENCY)
        # Guards driver open/close and executor creation across threads.
        self._state_lock = threading.Lock()
        # Small dedicated pool: makes the concurrency ceiling on the shared
        # native driver explicit instead of borrowing the default executor.
        self._executor: ThreadPoolExecutor | None = None

    # ------------------------------------------------------------------
    # Connection plumbing (synchronous; always called from worker threads)
    # ------------------------------------------------------------------

    def _get_executor(self) -> ThreadPoolExecutor:
        with self._state_lock:
            if self._executor is None:
                # One thread beyond the write concurrency so a read is never
                # queued behind a full set of in-flight write chunks.
                self._executor = ThreadPoolExecutor(
                    max_workers=WRITE_CONCURRENCY + 1, thread_name_prefix="typedb-adapter"
                )
            return self._executor

    async def _run_sync(self, fn, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._get_executor(), fn, *args)

    def _get_driver(self):
        """Lazily open the (synchronous) TypeDB driver."""
        with self._state_lock:
            if self._driver is None:
                from typedb.driver import Credentials, DriverOptions, DriverTlsConfig, TypeDB

                self._driver = TypeDB.driver(
                    self.address,
                    Credentials(self.username, self.password),
                    DriverOptions(DriverTlsConfig.disabled()),
                )
            return self._driver

    def _close_sync(self) -> None:
        with self._state_lock:
            if self._driver is not None:
                self._driver.close()
                self._driver = None
                self._schema_initialized = False

    async def close(self) -> None:
        """Release the native TypeDB connection and worker threads.

        Called by cognee's engine cache on eviction (prune, dataset deletion);
        without it the gRPC connection would leak until GC. The executor is
        drained BEFORE the driver closes so in-flight transactions finish on
        a live connection. The adapter reopens lazily if used again.
        """
        async with self._lock:
            with self._state_lock:
                executor = self._executor
                self._executor = None
            if executor is not None:
                await asyncio.to_thread(executor.shutdown, True)
            await asyncio.to_thread(self._close_sync)

    def _ensure_database_sync(self) -> None:
        """Create the database if missing and (re)define the cognee schema.

        The define always runs: it is idempotent, and re-running it applies
        additive schema evolution to pre-existing databases. (A presence
        check was tried and reverted: substring matching misfired on foreign
        types and silently froze the schema at its first version.)
        """
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

    @staticmethod
    def _is_commit_conflict(error: Exception) -> bool:
        """TypeDB [STC2]: commit lost an isolation conflict; nothing was committed."""
        return str(error).lstrip().startswith("[STC2]")

    @staticmethod
    async def _backoff(attempt: int) -> None:
        await asyncio.sleep(0.02 * (2**attempt) * (0.5 + random.random()))

    async def _write_batch(self, queries, retry: bool = True) -> None:
        """Run write queries in one WRITE transaction; results are discarded.

        Rows are never collected: errors surface via resolve()/commit(), and
        iterating write answers is pure FFI overhead (and forbidden anyway in
        multi-query batches, see _run_batch_sync). Commit isolation conflicts
        are retried with backoff: a failed commit rolls the whole transaction
        back, so replaying it can never duplicate work.
        """
        from typedb.driver import TransactionType

        await self._ensure_database()
        specs = self._as_specs(queries)
        attempts = COMMIT_RETRIES if retry else 1
        for attempt in range(attempts):
            try:
                await self._run_sync(self._run_batch_sync, specs, TransactionType.WRITE, False)
                return
            except Exception as error:
                if attempt + 1 == attempts or not self._is_commit_conflict(error):
                    raise
                await self._backoff(attempt)

    async def _write_rows(
        self,
        template: str,
        rows: list[dict],
        created_query: str | None = None,
        key: str | None = None,
    ):
        """Upsert rows as chunked transactions, WRITE_CONCURRENCY in flight.

        When ``created_query`` is given, each chunk's transaction also runs
        that set-once created-at statement after the upsert. The batch is not
        atomic: on the first failure, chunks not yet started are cancelled,
        chunks already committed stay committed, and chunks mid-transaction
        run to completion (a driver call on a worker thread cannot be
        interrupted) before the error propagates.
        """
        if not rows:
            return
        chunks = [rows[i : i + WRITE_CHUNK_ROWS] for i in range(0, len(rows), WRITE_CHUNK_ROWS)]
        tasks = [
            asyncio.create_task(self._write_chunk(template, chunk, created_query, key))
            for chunk in chunks
        ]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _write_chunk(self, template, chunk, created_query, key) -> None:
        specs = [(template, chunk)]
        if created_query is not None:
            specs.append((created_query, [{key: row[key], "now": row["now"]} for row in chunk]))
        for attempt in range(COMMIT_RETRIES):
            try:
                # The slot is held only for the attempt, never during backoff.
                async with self._write_semaphore:
                    await self._write_batch(specs, retry=False)
                return
            except Exception as error:
                if attempt + 1 == COMMIT_RETRIES or not self._is_commit_conflict(error):
                    raise
                await self._backoff(attempt)

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
    def _document_to_node_dict(document: dict[str, Any]) -> dict[str, Any]:
        """Rebuild the cognee node property dict from a fetched `{ $n.* }` doc."""
        properties = {}
        raw = document.get("properties-json")
        if raw:
            try:
                properties = json.loads(raw)
            except (TypeError, ValueError):
                logger.warning("Undecodable properties-json for node %s", document.get("node-id"))
        properties.setdefault("id", document.get("node-id"))
        for attribute, key in (("created-at", "created_at"), ("updated-at", "updated_at")):
            if document.get(attribute) is not None:
                properties.setdefault(key, document[attribute])
        return properties

    @staticmethod
    def _document_to_edge_properties(document: dict[str, Any]) -> dict[str, Any]:
        raw = document.get("properties-json")
        if raw:
            try:
                return json.loads(raw)
            except (TypeError, ValueError):
                logger.warning("Undecodable properties-json for an edge")
        return {}

    @staticmethod
    def _row_from_properties(
        node_id: str, properties: dict[str, Any], fallback_type: str
    ) -> dict[str, Any]:
        """The shared given-row shape for a node upsert (no provenance keys)."""
        name = properties.get("name")
        created = properties.get("created_at")
        return {
            "id": node_id,
            "type": str(properties.get("type") or fallback_type),
            "name": str(name) if name is not None else "",
            "props": json.dumps(properties, cls=JSONEncoder),
            # Mirrors DataPoint.created_at (epoch ms). Payloads without one
            # (add_node's dict form) get the write time.
            "created": (
                created if isinstance(created, int) and not isinstance(created, bool) else _now_ms()
            ),
        }

    def _node_row(self, node: DataPoint) -> dict[str, Any]:
        return self._row_from_properties(str(node.id), node.model_dump(), type(node).__name__)

    def _neighbours_spec(self, node_id: str, incoming: bool, edge_label: str | None = None):
        anchor_role, neighbour_role = ("target", "source") if incoming else ("source", "target")
        query = _NEIGHBOURS.format(
            label_decl=", $label: string" if edge_label is not None else "",
            anchor_role=anchor_role,
            neighbour_role=neighbour_role,
            label_constraint=", has relationship-name == $label" if edge_label is not None else "",
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

            query('given $name: string; match $n isa node, has name == $name; '
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
        resolved = (
            TransactionType[transaction_type.upper()]
            if transaction_type is not None
            else self._transaction_type_for(query)
        )
        specs = [(query, given_rows)]
        attempts = 1 if resolved == TransactionType.READ else COMMIT_RETRIES
        for attempt in range(attempts):
            try:
                return (await self._run_sync(self._run_batch_sync, specs, resolved, True))[0]
            except Exception as error:
                if attempt + 1 == attempts or not self._is_commit_conflict(error):
                    raise
                await self._backoff(attempt)

    async def has_node(self, node_id: str) -> bool:
        results = await self._read_batch([(_FETCH_NODES, [{"id": str(node_id)}])])
        return bool(results[0])

    async def add_node(self, node: DataPoint | str, properties: dict[str, Any] | None = None):
        """Add (or update) a single node from a DataPoint or an id + properties.

        Carries no provenance, so existing source-ref-key/source-run-id
        stamps on the node are left untouched.
        """
        if isinstance(node, DataPoint):
            row = self._node_row(node)
        else:
            node_props = dict(properties or {})
            node_props.setdefault("id", str(node))
            row = self._row_from_properties(str(node), node_props, "node")
        row["now"] = _now_ms()
        await self._write_batch([(_node_upsert_template(False, False), [row])])

    async def add_nodes(
        self,
        nodes: list[DataPoint],
        source_ref_key: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> None:
        """Upsert a batch of DataPoints in chunked, concurrent transactions.

        Provenance stamps are added when provided (they accumulate per node).
        Rows sharing a node id collapse to the last one, so a batch never
        races itself on the node-id key.
        """
        if not nodes:
            return
        now = _now_ms()
        rows: dict[str, dict[str, Any]] = {}
        for node in nodes:
            row = self._node_row(node)
            row["now"] = now
            if source_ref_key is not None:
                row["ref"] = source_ref_key
            if pipeline_run_id is not None:
                row["run"] = str(pipeline_run_id)
            rows[row["id"]] = row
        template = _node_upsert_template(source_ref_key is not None, pipeline_run_id is not None)
        await self._write_rows(template, list(rows.values()))

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
        retrieve_existing_edges), not as booleans. Lookup is by edge-key.
        """
        if not edges:
            return []
        rows = [
            {
                "key": _edge_key(str(edge[0]), str(edge[1]), edge[2]),
                "sid": str(edge[0]),
                "tid": str(edge[1]),
                "rel": edge[2],
            }
            for edge in edges
        ]
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
        """Upsert a batch of edges in chunked, concurrent transactions.

        Edge identity is the edge-key "{source}|{target}|{relationship}";
        properties are replaced on re-add and duplicate identities within a
        batch collapse to the last row. Provenance stamps are added when
        provided. Edges whose endpoints are missing are skipped (cognee adds
        nodes before edges).
        """
        if not edges:
            return
        now = _now_ms()
        rows: dict[str, dict[str, Any]] = {}
        for source_id, target_id, relationship_name, properties in edges:
            edge_properties = {
                **(properties or {}),
                "source_node_id": str(source_id),
                "target_node_id": str(target_id),
                "relationship_name": relationship_name,
            }
            key = _edge_key(str(source_id), str(target_id), relationship_name)
            row: dict[str, Any] = {
                "key": key,
                "sid": str(source_id),
                "tid": str(target_id),
                "rel": relationship_name,
                "props": json.dumps(edge_properties, cls=JSONEncoder),
                "now": now,
            }
            if source_ref_key is not None:
                row["ref"] = source_ref_key
            if pipeline_run_id is not None:
                row["run"] = str(pipeline_run_id)
            rows[key] = row
        template = _edge_upsert_template(source_ref_key is not None, pipeline_run_id is not None)
        await self._write_rows(template, list(rows.values()), _SET_EDGE_CREATED_AT, "key")

    async def get_edges(self, node_id: str):
        """Edges incident to a node, anchor-first: (node_id, neighbour_id, {...}).

        The de facto cognee adapter convention (and what format_edges in the
        memify pipeline assumes) is that slot 0 is the queried node and slot 1
        the neighbour, regardless of the edge's true direction.
        """
        anchor = str(node_id)
        seen: dict[tuple[str, str, str], None] = {}
        for document in await self._sweep_incident([anchor]):
            other = document["target"] if document["source"] == anchor else document["source"]
            seen[(anchor, other, document["relationship_name"])] = None
        return [
            (first, second, {"relationship_name": relationship})
            for first, second, relationship in seen
        ]

    async def get_predecessors(self, node_id: str, edge_label: str | None = None) -> list:
        results = await self._read_batch([self._neighbours_spec(node_id, True, edge_label)])
        return [self._document_to_node_dict(doc["neighbour"]) for doc in results[0]]

    async def get_successors(self, node_id: str, edge_label: str | None = None) -> list:
        results = await self._read_batch([self._neighbours_spec(node_id, False, edge_label)])
        return [self._document_to_node_dict(doc["neighbour"]) for doc in results[0]]

    async def get_neighbors(self, node_id: str) -> list[dict[str, Any]]:
        """Predecessors and successors combined, keyed on the node-id attribute
        (never the payload's "id", which callers may set independently)."""
        anchor = str(node_id)
        results = await self._read_batch(
            [
                self._neighbours_spec(anchor, True),
                self._neighbours_spec(anchor, False),
            ]
        )
        neighbours = [self._document_to_node_dict(doc["neighbour"]) for doc in results[0]]
        neighbours.extend(
            self._document_to_node_dict(doc["neighbour"])
            for doc in results[1]
            # A self-loop is already reported by the incoming pass.
            if doc["neighbour"].get("node-id") != anchor
        )
        return neighbours

    async def _sweep_incident(self, node_ids) -> list[dict[str, Any]]:
        """All edge documents incident to the given node ids (both directions)."""
        ids = sorted(set(node_ids))
        if not ids:
            return []
        rows = [{"id": i} for i in ids]
        outgoing, incoming = await self._read_batch(
            [(_INCIDENT_EDGES_OUT, rows), (_INCIDENT_EDGES_IN, rows)]
        )
        return outgoing + incoming

    @classmethod
    def _in_set_edges(
        cls, edge_docs, selected, wanted_types: set[str] | None = None
    ) -> list[tuple[str, str, str, dict]]:
        """Deduped (source, target, rel, props) with both endpoints in ``selected``."""
        edges: dict[tuple[str, str, str], dict] = {}
        for document in edge_docs:
            relationship = document["relationship_name"]
            if wanted_types is not None and relationship not in wanted_types:
                continue
            if document["source"] in selected and document["target"] in selected:
                key = (document["source"], document["target"], relationship)
                if key not in edges:
                    edges[key] = cls._document_to_edge_properties(document["edge"])
        return [(source, target, rel, props) for (source, target, rel), props in edges.items()]

    async def get_neighborhood(
        self,
        node_ids: list[str],
        depth: int = 1,
        edge_types: list[str] | None = None,
    ) -> tuple[list[tuple[str, dict]], list[tuple[str, str, str, dict]]]:
        """K-hop neighborhood of the seed nodes, in get_graph_data() shape.

        Traversal follows only ``edge_types`` when given; the result is the
        induced subgraph over the reached nodes. Edge docs are harvested from
        the BFS sweeps themselves (plus one sweep over the never-swept final
        frontier), so each node's incident edges cross the wire once.
        """
        if not node_ids:
            return ([], [])
        wanted_types = set(edge_types) if edge_types else None

        seed_docs = await self._read_batch(
            [(_FETCH_NODES, [{"id": str(node_id)} for node_id in dict.fromkeys(node_ids)])]
        )
        nodes: dict[str, dict] = {
            doc["node"]["node-id"]: self._document_to_node_dict(doc["node"]) for doc in seed_docs[0]
        }

        edge_docs: list[dict[str, Any]] = []
        swept: set[str] = set()
        frontier = set(nodes)
        for _ in range(max(depth, 0)):
            if not frontier:
                break
            documents = await self._sweep_incident(frontier)
            swept |= frontier
            edge_docs.extend(documents)
            next_frontier: set[str] = set()
            for document in documents:
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

        # Edges between nodes of the final frontier were never swept.
        edge_docs.extend(await self._sweep_incident(set(nodes) - swept))
        return (list(nodes.items()), self._in_set_edges(edge_docs, set(nodes), wanted_types))

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
            if document["neighbour"].get("node-id") == anchor:
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
        stored inside the `properties-json` attribute).
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
            (document["node"]["node-id"], self._document_to_node_dict(document["node"]))
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
        if not node_name:
            return ([], [])
        label = node_type.__name__
        seed_query = """
        given $label: string, $name: string;
        match $n isa node, has node-type == $label, has name == $name;
        fetch { "node": { $n.* } };
        """
        seed_rows = [{"label": label, "name": name} for name in node_name]
        seed_docs = (await self._read_batch([(seed_query, seed_rows)]))[0]
        seeds = {
            doc["node"]["node-id"]: self._document_to_node_dict(doc["node"]) for doc in seed_docs
        }
        if not seeds:
            return ([], [])

        seed_edge_docs = await self._sweep_incident(seeds)

        neighbour_seeds: dict[str, set[str]] = {}
        neighbour_docs: dict[str, dict] = {}
        for document in seed_edge_docs:
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

        # Seed-incident edges are already swept; only the neighbours' own
        # edges (e.g. neighbour-to-neighbour) still need one sweep.
        edge_docs = seed_edge_docs + await self._sweep_incident(wanted)
        return (list(nodes.items()), self._in_set_edges(edge_docs, set(nodes)))

    async def get_filtered_graph_data(self, attribute_filters):
        """Nodes matching the attribute filters, and edges between them.

        ``attribute_filters`` is a list with one dict of {attribute: [values]};
        a node matches when every filtered attribute has an allowed value.
        Filters over the promoted attributes ("type", "name") with string
        values run server-side; anything else falls back to a client-side
        scan of the canonical properties-json payload.
        """
        filters = {attribute: list(values) for attribute, values in attribute_filters[0].items()}
        promoted = (
            filters
            and all(attribute in _PROMOTED_FILTER_ATTRS for attribute in filters)
            and all(isinstance(value, str) for values in filters.values() for value in values)
        )

        if promoted:
            nodes_by_id = await self._filtered_nodes_server_side(filters)
            edge_docs = await self._sweep_incident(nodes_by_id)
            return (list(nodes_by_id.items()), self._in_set_edges(edge_docs, set(nodes_by_id)))

        all_nodes, all_edges = await self.get_graph_data()
        # Membership on lists compares by equality, so unhashable property
        # values (lists/dicts from the JSON payload) never raise.
        nodes = [
            (node_id, properties)
            for node_id, properties in all_nodes
            if all(properties.get(attribute) in values for attribute, values in filters.items())
        ]
        selected = {node_id for node_id, _ in nodes}
        edges = [edge for edge in all_edges if edge[0] in selected and edge[1] in selected]
        return (nodes, edges)

    async def _filtered_nodes_server_side(self, filters: dict[str, list]) -> dict[str, dict]:
        attributes = sorted(filters)
        given = ", ".join(f"$v{index}: string" for index in range(len(attributes)))
        constraints = "".join(
            f", has {_PROMOTED_FILTER_ATTRS[attribute]} == $v{index}"
            for index, attribute in enumerate(attributes)
        )
        query = f'given {given};\nmatch $n isa node{constraints};\nfetch {{ "node": {{ $n.* }} }};'
        rows = [
            {f"v{index}": value for index, value in enumerate(combination)}
            for combination in product(*(filters[attribute] for attribute in attributes))
        ]
        documents = (await self._read_batch([(query, rows)]))[0]
        return {
            doc["node"]["node-id"]: self._document_to_node_dict(doc["node"]) for doc in documents
        }

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
        """Ids of fully isolated nodes (no incident edges at all).

        This deliberately matches the reference (ladybug) adapter's
        degree-zero semantics, NOT "outside the largest component": cognee's
        remove_disconnected_chunks deletes every id returned here, so
        returning smaller-but-connected components would destroy real data.
        """
        rows = (await self._read_batch([_ISOLATED_NODE_IDS]))[0]
        return [row["id"] for row in rows]

    async def get_graph_metrics(self, include_optional=False):
        """Structural metrics; all-pairs metrics are reported unsupported (-1).

        Failures propagate (with the error logged) rather than returning a
        zeroed dict: cognee persists these metrics per pipeline run and would
        cache the zeros as fact.
        """
        try:
            node_ids, endpoints = await self._edge_endpoint_pairs()
            num_nodes = len(node_ids)
            num_edges = len(endpoints)
            components = self._connected_components(node_ids, endpoints)
            component_sizes = [len(members) for members in components.values()]

            return {
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
        except Exception as error:
            logger.error("Failed to get graph metrics: %s", error)
            raise

    async def is_empty(self) -> bool:
        results = await self._read_batch(["match $n isa node; limit 1; reduce $count = count;"])
        return bool(results[0] and results[0][0].get("count", 1) == 0)
