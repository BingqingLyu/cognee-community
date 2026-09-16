"""TypeDB graph database adapter for cognee.

Maps cognee's property-graph model onto the reified TypeDB schema in
``schema.tql``: one ``node`` entity type and one ``edge`` relation type
(roles ``source``/``target``). Cognee's dynamic node labels and relationship names
are stored as attributes; the full property payload is serialized into the
``properties-json`` attribute, which is the canonical record — the promoted
attributes (``node-type``, ``name``) exist only as query accelerators, mirror
the JSON, and are always written together with it. Edges carry an explicit
``edge-key`` (a JSON ``[source, target, relationship]`` triple) as their
identity and ``edge-object-id`` (cognee's deterministic feedback id).

Graph-native provenance follows cognee's contract exactly by delegating the
state transitions to ``provenance_after_attach`` / ``provenance_after_remove``
(Model A: a pipeline run is recorded against a source ref only when that ref
is newly attached). The record is relational: one ``source-ref`` entity per
key (owning the key and its dataset id) and one ``run-ref`` entity per run
ref (owning the ref and its run id), linked to their artifacts by
``sourced-from`` / ``run-attached`` relations whose ``position`` attribute
is the attach order (cognee asserts it, and TypeDB's multi-valued
attributes are unordered). The ref entities are ``put`` once per batch
before its chunks run, so a chunk only looks a ref up and links to it. That
matters because TypeDB conflicts concurrent inserts of *ownership* of the
same long string value (benchmarks/README.md): artifacts own nothing
shared, so provenance-carrying chunks run concurrently like any other
write, and ``add_nodes`` / ``add_edges`` fold the attach into each chunk's
transaction (upsert, read the links, apply the transition, link or unlink
for the difference, commit), so no artifact is ever visible without its
provenance, the invariant cognee's rollback and delete planners rely on.
Writers in other adapter instances that touch the same artifact conflict
at commit (``[STC2]``, verified in ``tests/integration/test_concurrency.py``
and ``test_stress.py``) and are retried against a re-read within a time
budget. Deleting an artifact must also delete its links: TypeDB keeps a
relation whose role player was deleted, so every delete path removes the
links first.

A node's ``created-at`` mirrors its DataPoint payload's ``created_at``; an
edge's is set once on first write; ``updated-at`` is the write time (all
epoch ms). Feedback weights and truth state live inside ``properties-json``.

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
import contextlib
import json
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from itertools import product
from pathlib import Path
from typing import Any
from uuid import UUID

from cognee.exceptions import CogneeValidationError
from cognee.infrastructure.databases.graph.graph_db_interface import (
    GraphDBInterface,
)
from cognee.infrastructure.databases.provenance import (
    EdgeDeleteData,
    EdgeIdentity,
    NodeDeleteData,
    get_dataset_id_from_source_ref_key,
    get_pipeline_run_id_from_source_run_ref,
    get_source_ref_key_from_source_run_ref,
    make_source_run_ref,
)
from cognee.infrastructure.databases.provenance.source_ref_state import (
    ProvenanceColumns,
    coerce_run_uuid,
    derive_dataset_ids,
    derive_run_ids,
    provenance_after_attach,
    provenance_after_remove,
)
from cognee.infrastructure.engine import DataPoint
from cognee.modules.engine.utils import generate_edge_object_id
from cognee.modules.retrieval.exceptions import SearchTypeNotSupported
from cognee.modules.storage.utils import JSONEncoder
from cognee.shared.logging_utils import get_logger

logger = get_logger("TypeDBAdapter")

DEFAULT_ADDRESS = "127.0.0.1:1729"
DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "password"
DEFAULT_DATABASE = "cognee"
# TLS is off by default (local/docker servers); TypeDB Cloud and hardened
# deployments need it. Read from the environment because cognee's graph config
# has no provider-specific fields.
TLS_ENV = "TYPEDB_TLS"
TLS_ROOT_CA_ENV = "TYPEDB_TLS_ROOT_CA"

# The schema is the single source of truth in schema.tql (shipped with the
# package). The define is idempotent and re-run on every fresh adapter, so
# additive schema evolution reaches existing databases; incompatible changes
# require a fresh database.
COGNEE_SCHEMA = (Path(__file__).parent / "schema.tql").read_text(encoding="utf-8")

_SCHEMA_KEYWORDS = ("define", "undefine", "redefine")
# Word-boundary match, applied only after string literals and comments are
# stripped, so reads over e.g. `updated-at` or values like "deleted" are not
# misclassified as writes.
_WRITE_STAGE_RE = re.compile(r"\b(insert|put|update|delete)\b")
_STRING_LITERAL_RE = re.compile(r'"(?:\\.|[^"\\])*"')
_COMMENT_RE = re.compile(r"#[^\n]*")

# --- given-parameterized query templates -----------------------------------


_NODE_UPSERT = """
given $id: string, $type: string, $name: string, $props: string, $created: integer, $now: integer;
put $n isa node, has node-id == $id;
update
  $n has node-type == $type;
  $n has name == $name;
  $n has properties-json == $props;
  $n has created-at == $created;
  $n has updated-at == $now;
"""

_EDGE_UPSERT = """
given $key: string, $sid: string, $tid: string, $rel: string, $eoid: string, $props: string,
  $now: integer;
match
  $s isa node, has node-id == $sid;
  $t isa node, has node-id == $tid;
put
  $e isa edge, links (source: $s, target: $t),
    has edge-key == $key, has relationship-name == $rel;
update
  $e has edge-object-id == $eoid;
  $e has properties-json == $props;
  $e has updated-at == $now;
"""

_SET_EDGE_CREATED_AT = """
given $key: string, $now: integer;
match $e isa edge, has edge-key == $key; not { $e has created-at $c; };
insert $e has created-at == $now;
"""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _edge_key(source_id: str, target_id: str, relationship_name: str) -> str:
    """Edge identity as a JSON-encoded triple, so ids containing '|' cannot collide."""
    return json.dumps([source_id, target_id, relationship_name], separators=(",", ":"))


_FETCH_NODES = """
given $id: string;
match $n isa node, has node-id == $id;
fetch { "node": { $n.* } };
"""

_HAS_EDGES = """
given $key: string, $sid: string, $tid: string, $rel: string;
match $e isa edge, has edge-key == $key;
fetch { "source": $sid, "target": $tid, "relationship_name": $rel };
"""

_DELETE_INCIDENT_EDGES = """
given $id: string;
match $n isa node, has node-id == $id; $e isa edge, links ($n);
delete $e;
"""

_DELETE_NODES = """
given $id: string;
match $n isa node, has node-id == $id;
delete $n;
"""

# Provenance links of artifacts about to be deleted (TypeDB does not remove a
# relation when a role player is deleted). One template per artifact
# selection, formatted with the link relation type.
_DELETE_LINKS_OF_NODES = """
given $id: string;
match $n isa node, has node-id == $id; $l isa {link}, links (artifact: $n);
delete $l;
"""
_DELETE_LINKS_OF_INCIDENT_EDGES = """
given $id: string;
match $n isa node, has node-id == $id; $e isa edge, links ($n); $l isa {link}, links (artifact: $e);
delete $l;
"""
_DELETE_LINKS_OF_EDGES_BY_KEY = """
given $id: string;
match $e isa edge, has edge-key == $id; $l isa {link}, links (artifact: $e);
delete $l;
"""
_DELETE_LINKS_OF_LABELED_EDGES = """
given $id: string, $label: string;
match
  $n isa node, has node-id == $id;
  $e isa edge, links ({anchor_role}: $n), has relationship-name == $label;
  $l isa {link}, links (artifact: $e);
delete $l;
"""
_LINK_TYPES = ("sourced-from", "run-attached")


def _link_deletes(template: str, **fields) -> list[str]:
    """The two link-deletion statements (one per link type) for a template."""
    return [template.format(link=link, **fields) for link in _LINK_TYPES]


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
  $n isa node, has node-id == $id;
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
  $n isa node, has node-id == $id;
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
  $n isa node, has node-id == $id;
  $e isa edge, links ({anchor_role}: $n, {neighbour_role}: $m){label_constraint};
  $e has relationship-name $rel;
fetch {{ "neighbour": {{ $m.* }}, "relationship_name": $rel, "node": {{ $n.* }} }};
"""

_REMOVE_LABELED_EDGES = """
given $id: string, $label: string;
match
  $n isa node, has node-id == $id;
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
# WRITE_CONCURRENCY transactions in flight. benchmarks/README.md (Phase 4
# sweep): per-row cost grows superlinearly with rows per transaction, 100-row
# chunks beat 200 by 12-32% at 2-4 in flight (equal at 1), 4 in flight is the knee.
# Defaults; per-adapter values come from TYPEDB_WRITE_CHUNK_ROWS /
# TYPEDB_WRITE_CONCURRENCY when set (read at construction).
WRITE_CHUNK_ROWS = 100
WRITE_CONCURRENCY = 4
# Commit conflicts ([STC2]) are retried for this long with capped, fully
# jittered backoff. A count would not do: under sustained contention from
# another writer on the same database (another process, or a second cached
# adapter for the same dataset) the loser keeps losing until the other
# batch ends, which can take longer than any short fixed budget.
COMMIT_RETRY_SECONDS = 30.0
COMMIT_BACKOFF_CAP_SECONDS = 0.25
COMMIT_RETRY_WARN_AFTER = 10
WRITE_CHUNK_ROWS_ENV = "TYPEDB_WRITE_CHUNK_ROWS"
WRITE_CONCURRENCY_ENV = "TYPEDB_WRITE_CONCURRENCY"


def _get_env_variable_as_positive_int(name: str, default: int, environ=os.environ) -> int:
    raw = environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(f"{name}={raw!r} is not an integer") from error
    if value < 1:
        raise ValueError(f"{name} must be >= 1, got {value}")
    return value


# --- provenance, weights, metadata, triplets -------------------------------

# Artifact match fragments: bind $x (node or edge) from a given $id.
_MATCH_BY_ID = {
    "node": "$x isa node, has node-id == $id;",
    "edge": "$x isa edge, has edge-key == $id;",
}
# Provenance link kinds: (ref entity type, its key attribute, its derived-id
# attribute, link relation type, the ref's role in it).
_PROVENANCE_LINKS = {
    "ref": ("source-ref", "source-ref-key", "source-dataset-id", "sourced-from", "ref"),
    "run": ("run-ref", "source-run-ref", "source-run-id", "run-attached", "run"),
}

# Ref entities are put once per batch, before the chunks that link to them.
_PUT_SOURCE_REF = """
given $k: string, $d: string;
put $r isa source-ref, has source-ref-key == $k;
update $r has source-dataset-id == $d;
"""
_PUT_RUN_REF = """
given $k: string, $run: string;
put $r isa run-ref, has source-run-ref == $k;
update $r has source-run-id == $run;
"""


def _links_read_query(kind: str, link: str) -> str:
    """One row per provenance link of each given artifact: its position and
    the ref's key. Written as a per-link fetch on purpose: a `select` that
    joins the key through the ref entity, followed by link inserts in the
    same transaction, runs ~70x slower (benchmarks/README.md)."""
    _entity, key_attr, _derived, relation, role = _PROVENANCE_LINKS[link]
    return (
        "given $id: string;\n"
        f"match {_MATCH_BY_ID[kind]} $l isa {relation}, links (artifact: $x, {role}: $r);\n"
        f'fetch {{ "id": $id, "p": $l.position, "k": $r.{key_attr} }};'
    )


def _links_fetch_list(var: str, link: str) -> str:
    """A fetch sub-query listing ``var``'s links of one kind (key + position)."""
    _entity, key_attr, _derived, relation, role = _PROVENANCE_LINKS[link]
    # Own variable names: the enclosing match may bind $l / $r to other types.
    return (
        f"[ match $pl_{link} isa {relation}, links (artifact: {var}, {role}: $pr_{link});"
        f' fetch {{ "k": $pr_{link}.{key_attr}, "p": $pl_{link}.position }}; ]'
    )


def _ref_exists_query(link: str) -> str:
    entity, key_attr, _derived, _relation, _role = _PROVENANCE_LINKS[link]
    return f'given $v: string;\nmatch $r isa {entity}, has {key_attr} == $v;\nfetch {{ "v": $v }};'


def _link_insert_query(kind: str, link: str) -> str:
    entity, key_attr, _derived, relation, role = _PROVENANCE_LINKS[link]
    return (
        "given $id: string, $v: string, $p: integer;\n"
        f"match {_MATCH_BY_ID[kind]} $r isa {entity}, has {key_attr} == $v;\n"
        # insert, not put: the read says the link is absent, and a `put`
        # would scan the ref entity's existing links (thousands on a hub).
        f"insert (artifact: $x, {role}: $r) isa {relation}, has position == $p;"
    )


def _touch_query(kind: str) -> str:
    """Bump the artifact's updated-at. Every provenance change does this for
    the artifacts it changed so that two writers changing one artifact
    conflict at commit (TypeDB flags concurrent writes to one owner) and
    the loser re-reads: without it, concurrent attaches of the same key
    would both see it as new and link it twice."""
    match = _MATCH_BY_ID[kind]
    return f"given $id: string, $now: integer;\nmatch {match}\nupdate $x has updated-at == $now;"


def _link_delete_query(kind: str, link: str) -> str:
    entity, key_attr, _derived, relation, role = _PROVENANCE_LINKS[link]
    return (
        "given $id: string, $v: string;\n"
        f"match {_MATCH_BY_ID[kind]} $r isa {entity}, has {key_attr} == $v;"
        f" $l isa {relation}, links (artifact: $x, {role}: $r);\n"
        "delete $l;"
    )


def _artifacts_by_ref_query(kind: str, link: str, by_derived: bool) -> str:
    """Artifacts linked to a ref entity, selected by its key or its derived id,
    with each artifact's full provenance links."""
    entity, key_attr, derived_attr, relation, role = _PROVENANCE_LINKS[link]
    attribute = derived_attr if by_derived else key_attr
    if kind == "node":
        return (
            "given $v: string;\n"
            f"match $r isa {entity}, has {attribute} == $v;"
            f" $l isa {relation}, links (artifact: $n, {role}: $r); $n isa node, has node-id $id;\n"
            f'fetch {{ "id": $id, "keys": {_links_fetch_list("$n", "ref")},'
            f' "runs": {_links_fetch_list("$n", "run")} }};'
        )
    return (
        "given $v: string;\n"
        f"match $r isa {entity}, has {attribute} == $v;"
        f" $l isa {relation}, links (artifact: $e, {role}: $r);"
        " $e isa edge, links (source: $s, target: $t);"
        " $s has node-id $sid; $t has node-id $tid; $e has relationship-name $rel;\n"
        'fetch { "source": $sid, "target": $tid, "relationship_name": $rel,'
        f' "keys": {_links_fetch_list("$e", "ref")}, "runs": {_links_fetch_list("$e", "run")} }};'
    )


def _properties_write_query(kind: str) -> str:
    match = _MATCH_BY_ID[kind]
    return (
        f"given $id: string, $v: string, $now: integer;\nmatch {match}\n"
        "update $x has properties-json == $v; $x has updated-at == $now;"
    )


def _properties_read_query(kind: str, all_artifacts: bool) -> str:
    if all_artifacts:
        key_attr = "node-id" if kind == "node" else "edge-key"
        artifact = "node" if kind == "node" else "edge"
        return (
            f"match $x isa {artifact}, has {key_attr} $id, has properties-json $p;\n"
            'fetch { "id": $id, "p": $p };'
        )
    return (
        "given $id: string;\n"
        f"match {_MATCH_BY_ID[kind]} $x has properties-json $p;\n"
        'fetch { "id": $id, "p": $p };'
    )


_NODE_DELETE_DATA = f"""
given $id: string;
match $n isa node, has node-id == $id;
fetch {{ "node": {{ $n.* }}, "keys": {_links_fetch_list("$n", "ref")},
        "runs": {_links_fetch_list("$n", "run")} }};
"""
_EDGE_DELETE_DATA = f"""
given $id: string;
match
  $e isa edge, has edge-key == $id, links (source: $s, target: $t);
  $s has node-id $sid; $t has node-id $tid; $e has relationship-name $rel;
fetch {{ "source": $sid, "target": $tid, "relationship_name": $rel, "edge": {{ $e.* }},
        "keys": {_links_fetch_list("$e", "ref")}, "runs": {_links_fetch_list("$e", "run")} }};
"""
_DELETE_EDGES_BY_KEY = """
given $id: string;
match $e isa edge, has edge-key == $id;
delete $e;
"""
_EDGES_BY_OBJECT_ID = """
given $v: string;
match $e isa edge, has edge-object-id == $v, has edge-key $k, has properties-json $p;
fetch { "eoid": $v, "key": $k, "p": $p };
"""
_METADATA_SET = """
given $k: string, $v: string;
put $m isa graph-metadata, has metadata-key == $k;
update $m has metadata-value == $v;
"""
_METADATA_GET = """
match $m isa graph-metadata, has metadata-key $k, has metadata-value $v;
fetch { "k": $k, "v": $v };
"""
_TRIPLETS_BATCH = """
match
  $e isa edge, links (source: $s, target: $t), has edge-key $k;
sort $k;
offset {offset};
limit {limit};
fetch {{ "start": {{ $s.* }}, "edge": {{ $e.* }}, "end": {{ $t.* }} }};
"""

# Filterable attributes promoted out of properties-json, usable server-side.
_PROMOTED_FILTER_ATTRS = {"type": "node-type", "name": "name"}


@dataclass(frozen=True)
class _ProvenanceAttach:
    """One provenance change: the transition to apply per artifact, plus the
    ref entities the change may link to (put before the chunks run)."""

    transition: Any
    keys: list[str] = field(default_factory=list)
    run_refs: list[str] = field(default_factory=list)

    @classmethod
    def for_keys(cls, keys: list[str], run: str | None) -> "_ProvenanceAttach":
        run_refs = [make_source_run_ref(run, key) for key in keys] if run else []
        return cls(
            lambda current, refs: provenance_after_attach(current, refs, keys, run),
            keys,
            run_refs,
        )

    @classmethod
    def removing(cls, keys: list[str]) -> "_ProvenanceAttach":
        return cls(lambda current, refs: provenance_after_remove(current, refs, keys))


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
        # _database_exists: a read has seen the database on the server.
        # _schema_initialized: a write has run the (idempotent) schema define.
        # Reads never provision — a stale engine handle used after the
        # database was dropped must see an empty graph, not recreate it.
        self._database_exists = False
        self._schema_initialized = False
        self._lock = asyncio.Lock()
        self._chunk_rows = _get_env_variable_as_positive_int(WRITE_CHUNK_ROWS_ENV, WRITE_CHUNK_ROWS)
        self._write_concurrency = _get_env_variable_as_positive_int(
            WRITE_CONCURRENCY_ENV, WRITE_CONCURRENCY
        )
        self._commit_retry_seconds = COMMIT_RETRY_SECONDS
        # Caps in-flight chunk transactions across ALL concurrent batch calls.
        self._write_semaphore = asyncio.Semaphore(self._write_concurrency)
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
                    max_workers=self._write_concurrency + 1, thread_name_prefix="typedb-adapter"
                )
            return self._executor

    async def _run_sync(self, fn, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._get_executor(), fn, *args)

    @staticmethod
    def _tls_config_from_env(environ=os.environ):
        """TLS settings from ``TYPEDB_TLS`` / ``TYPEDB_TLS_ROOT_CA``.

        ``TYPEDB_TLS`` unset or false: plaintext. True: TLS with the system's
        native trust roots, or with the PEM bundle at ``TYPEDB_TLS_ROOT_CA``
        when that is set (self-signed / private CA deployments). Any other
        value is an error rather than a silent fall back to plaintext.
        The mode is process-wide: cognee caches one engine per dataset, so a
        change to these variables takes effect on the next driver open.
        """
        from typedb.driver import DriverTlsConfig

        value = environ.get(TLS_ENV, "").strip().lower()
        if value in {"", "0", "false", "no", "off"}:
            return DriverTlsConfig.disabled()
        if value not in {"1", "true", "yes", "on"}:
            raise ValueError(f"{TLS_ENV}={value!r} is not a boolean (use true or false)")
        root_ca = environ.get(TLS_ROOT_CA_ENV, "").strip()
        if root_ca:
            return DriverTlsConfig.enabled_with_root_ca(root_ca)
        return DriverTlsConfig.enabled_with_native_root_ca()

    def _get_driver(self):
        """Lazily open the (synchronous) TypeDB driver."""
        with self._state_lock:
            if self._driver is None:
                from typedb.driver import Credentials, DriverOptions, TypeDB

                self._driver = TypeDB.driver(
                    self.address,
                    Credentials(self.username, self.password),
                    DriverOptions(self._tls_config_from_env()),
                )
            return self._driver

    def _detach_driver_and_executor(self):
        """Take ownership of the current executor and driver (if any) and reset
        the adapter to its unopened state, atomically. A call that arrives
        afterwards opens a fresh pair instead of racing the closing one."""
        with self._state_lock:
            executor, driver = self._executor, self._driver
            self._executor = None
            self._driver = None
            self._database_exists = False
            self._schema_initialized = False
        return executor, driver

    def _close_sync(self) -> None:
        executor, driver = self._detach_driver_and_executor()
        if executor is not None:
            executor.shutdown(wait=True)
        if driver is not None:
            driver.close()

    async def close(self) -> None:
        """Release the native TypeDB connection and worker threads.

        Called by cognee's engine cache on eviction (prune, dataset deletion);
        without it the gRPC connection would leak until GC. The executor is
        drained BEFORE the driver closes so in-flight transactions finish on
        a live connection. The adapter reopens lazily if used again.
        """
        async with self._lock:
            executor, driver = self._detach_driver_and_executor()
            if executor is not None:
                await asyncio.to_thread(executor.shutdown, True)
            if driver is not None:
                await asyncio.to_thread(driver.close)

    def _provision_database_sync(self) -> None:
        """Create the database if missing and (re)define the cognee schema.

        Write path only. The define always runs: it is idempotent, and
        re-running it applies additive schema evolution to pre-existing
        databases. (A presence check was tried and reverted: substring
        matching misfired on foreign types and froze the schema at v1.)
        """
        from typedb.driver import TransactionType

        driver = self._get_driver()
        if not driver.databases.contains(self.database_name):
            try:
                driver.databases.create(self.database_name)
            except Exception:
                # Lost a race with another creator (cognee's dataset lock is
                # per process; two workers can provision the same dataset).
                # The define below is idempotent, so proceed if it now exists.
                if not driver.databases.contains(self.database_name):
                    raise
        else:
            self._refuse_pre_link_schema_sync(driver)
        with driver.transaction(self.database_name, TransactionType.SCHEMA) as tx:
            tx.query(COGNEE_SCHEMA).resolve()
            tx.commit()
        self._database_exists = True
        self._schema_initialized = True

    def _refuse_pre_link_schema_sync(self, driver) -> None:
        """Refuse a database whose nodes still own ``provenance-json``: it was
        written by an earlier version that kept provenance as artifact
        attributes. The new define would succeed (it only adds types) and
        every existing artifact would then read as unowned, so cognee's
        dataset deletes and run rollbacks would silently remove nothing.
        """
        from typedb.driver import TransactionType

        try:
            with driver.transaction(self.database_name, TransactionType.READ) as tx:
                owned = {
                    row.get("a").get_label()
                    for row in tx.query("match $t label node; $t owns $a; select $a;").resolve()
                }
        except Exception:
            return  # no `node` type yet: an empty database, nothing to migrate
        if "provenance-json" in owned:
            raise RuntimeError(
                f"TypeDB database {self.database_name!r} was created by an earlier version "
                "of this adapter (provenance stored as artifact attributes). Its provenance "
                "cannot be read by the relational model; create a fresh database."
            )

    async def _provision_database(self) -> None:
        if self._schema_initialized:
            return
        async with self._lock:
            if not self._schema_initialized:
                await self._run_sync(self._provision_database_sync)

    def _database_exists_sync(self) -> bool:
        exists = self._get_driver().databases.contains(self.database_name)
        self._database_exists = exists
        return exists

    async def _database_available(self) -> bool:
        """Read-path gate: True if the database exists; never creates it."""
        if self._schema_initialized or self._database_exists:
            return True
        return await self._run_sync(self._database_exists_sync)

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
    def _normalize_query_specs(queries) -> list[tuple[str, list | None]]:
        return [(query, None) if isinstance(query, str) else query for query in queries]

    async def _read_batch(self, queries) -> list[list[dict]]:
        """Run read queries in one READ transaction; returns rows per query.

        A missing database yields empty results for every query rather than
        being created (see _database_available).
        """
        from typedb.driver import TransactionType

        if not await self._database_available():
            return [[] for _ in queries]
        return await self._run_sync(
            self._run_batch_sync, self._normalize_query_specs(queries), TransactionType.READ, True
        )

    @staticmethod
    def _is_commit_conflict(error: Exception) -> bool:
        """TypeDB [STC2]: commit lost an isolation conflict; nothing was committed."""
        return str(error).lstrip().startswith("[STC2]")

    @staticmethod
    async def _backoff(attempt: int) -> None:
        # Full jitter, exponential up to the cap: two losers never lock-step.
        await asyncio.sleep(random.random() * min(COMMIT_BACKOFF_CAP_SECONDS, 0.02 * 2**attempt))

    async def _retry_commit_conflicts(self, attempt, retry: bool = True):
        """Run ``attempt()`` until it succeeds or the conflict budget ends.

        Only [STC2] commit conflicts are retried (the failed commit rolled the
        whole transaction back, so replaying never duplicates work); any
        other error propagates at once. After COMMIT_RETRY_WARN_AFTER
        rounds a warning names the contention.
        """
        deadline = time.monotonic() + self._commit_retry_seconds
        rounds = 0
        while True:
            try:
                return await attempt()
            except Exception as error:
                if not retry or not self._is_commit_conflict(error):
                    raise
                if time.monotonic() >= deadline:
                    raise
                rounds += 1
                if rounds == COMMIT_RETRY_WARN_AFTER:
                    logger.warning(
                        "TypeDB commit conflicts on %s: %d retries so far, another writer "
                        "is active on this database (budget %.0fs)",
                        self.database_name,
                        rounds,
                        self._commit_retry_seconds,
                    )
                await self._backoff(rounds)

    async def _run_sync_shielded(self, fn, *args):
        """``_run_sync`` for write transactions: a cancelled caller waits for
        the in-flight transaction to finish (commit or fail) before the
        cancellation propagates, so no chunk can commit after cognee's
        rollback has already looked."""
        task = asyncio.ensure_future(self._run_sync(fn, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            with contextlib.suppress(BaseException):
                await asyncio.shield(task)
            raise

    async def _write_batch(self, queries, retry: bool = True) -> None:
        """Run write queries in one WRITE transaction; results are discarded.

        Rows are never collected: errors surface via resolve()/commit(), and
        iterating write answers is pure FFI overhead (and forbidden anyway in
        multi-query batches, see _run_batch_sync). Commit isolation conflicts
        are retried with backoff: a failed commit rolls the whole transaction
        back, so replaying it can never duplicate work.
        """
        from typedb.driver import TransactionType

        await self._provision_database()
        specs = self._normalize_query_specs(queries)

        async def attempt():
            await self._run_sync_shielded(self._run_batch_sync, specs, TransactionType.WRITE, False)

        await self._retry_commit_conflicts(attempt, retry)

    async def _write_rows(
        self,
        template: str,
        rows: list[dict],
        created_query: str | None = None,
        key: str | None = None,
        provenance=None,
    ):
        """Upsert rows as chunked transactions.

        ``created_query`` (with ``key``) adds the set-once created-at statement
        to each chunk's transaction. With ``provenance`` = (kind, id_field,
        attach) each chunk's transaction also attaches provenance to the rows
        it upserted (atomic per chunk); the ref entities it links to are put
        first, in their own transaction. Chunks run with WRITE_CONCURRENCY in
        flight either way. The batch is not atomic: on the first failure,
        chunks not yet started are cancelled and chunks already committed
        stay committed. A chunk whose transaction is in flight when the
        batch fails or the caller is cancelled runs to its end first (commit
        or roll back), so the caller never observes a chunk still landing.
        """
        if not rows:
            return
        size = self._chunk_rows
        chunks = [rows[i : i + size] for i in range(0, len(rows), size)]

        def specs_for(chunk):
            specs = [(template, chunk)]
            if created_query is not None:
                specs.append((created_query, [{key: row[key], "now": row["now"]} for row in chunk]))
            return specs

        def provenance_for(chunk):
            if provenance is None:
                return None
            kind, id_field, attach = provenance
            return (kind, [row[id_field] for row in chunk], attach.transition)

        if provenance is not None:
            await self._ensure_provenance_refs(provenance[2])

        tasks = [
            asyncio.create_task(self._write_chunk(specs_for(chunk), provenance_for(chunk)))
            for chunk in chunks
        ]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _write_chunk(self, specs, provenance=None) -> None:
        """One chunk transaction, retried on commit conflicts. With
        ``provenance`` = (kind, identities, transition) the transaction also
        applies the provenance transition to those artifacts after the specs."""

        async def attempt():
            # The slot is held only for the attempt, never during backoff.
            async with self._write_semaphore:
                if provenance is None:
                    await self._write_batch(specs, retry=False)
                else:
                    kind, identities, transition = provenance
                    await self._run_sync_shielded(
                        self._provenance_change_sync, kind, identities, transition, specs
                    )

        await self._retry_commit_conflicts(attempt)

    # ------------------------------------------------------------------
    # Read-modify-write primitives (one transaction each, retried on STC2)
    # ------------------------------------------------------------------

    @staticmethod
    def _ordered_links(entries) -> list[str]:
        """Keys of link entries ``{"k": key, "p": position}`` in attach order."""
        return [entry["k"] for entry in sorted(entries or [], key=lambda entry: entry["p"])]

    @classmethod
    def _decode_provenance(cls, document: dict) -> tuple[list[str], list[str]]:
        """(ordered source ref keys, ordered run refs) from a document carrying
        ``keys`` / ``runs`` link lists."""
        return cls._ordered_links(document.get("keys")), cls._ordered_links(document.get("runs"))

    @staticmethod
    def _ref_put_specs(keys, run_refs) -> list[tuple[str, list[dict]]]:
        specs = []
        if keys:
            specs.append(
                (
                    _PUT_SOURCE_REF,
                    [
                        {"k": key, "d": str(get_dataset_id_from_source_ref_key(key))}
                        for key in sorted(keys)
                    ],
                )
            )
        if run_refs:
            specs.append(
                (
                    _PUT_RUN_REF,
                    [
                        {"k": ref, "run": str(get_pipeline_run_id_from_source_run_ref(ref))}
                        for ref in sorted(run_refs)
                    ],
                )
            )
        return specs

    @classmethod
    def _require_refs(cls, tx, link: str, values: set[str]) -> None:
        """Raise if any ref entity a chunk is about to link to does not exist."""
        if not values:
            return
        rows = [{"v": value} for value in sorted(values)]
        answer = tx.query(_ref_exists_query(link), given_rows=rows).resolve()
        found = {document["v"] for document in cls._collect_answer(answer)}
        missing = sorted(values - found)
        if missing:
            raise RuntimeError(
                f"provenance ref entities missing for {link} links: {missing[:3]} "
                "(the batch put them; was the database emptied concurrently?)"
            )

    async def _ensure_provenance_refs(self, attach: "_ProvenanceAttach") -> None:
        """Put the ref entities a batch will link to, once, before its chunks
        (idempotent; retried, since two batches may create one key at once)."""
        specs = self._ref_put_specs(attach.keys, attach.run_refs)
        if specs:
            await self._write_batch(specs)

    def _provenance_change_sync(self, kind: str, identities: list[str], transition, pre_specs=()):
        """In one WRITE transaction: run ``pre_specs`` (a chunk's upserts),
        read each artifact's provenance links, apply the pure ``transition``,
        link/unlink the ref entities for the difference, commit.

        The reads happen after the pre-specs, so a folded chunk sees the
        artifacts it just upserted, and their streams are drained before
        any further write is issued (TSV13). New links get positions after
        the artifact's highest existing one, so order survives removals; a
        retry after a rolled-back commit re-reads and cannot drift.
        """
        from typedb.driver import TransactionType

        driver = self._get_driver()
        with driver.transaction(self.database_name, TransactionType.WRITE) as tx:
            for query_text, given_rows in pre_specs:
                tx.query(query_text, given_rows=given_rows).resolve()
            rows = [{"id": identity} for identity in identities]
            current: dict[str, dict[str, list]] = {
                identity: {"ref": [], "run": []} for identity in identities
            }
            for link in ("ref", "run"):
                for entry in self._collect_answer(
                    tx.query(_links_read_query(kind, link), given_rows=rows).resolve()
                ):
                    current[entry["id"]][link].append(entry)
            inserts: dict[str, list[dict]] = {"ref": [], "run": []}
            deletes: dict[str, list[dict]] = {"ref": [], "run": []}
            for identity, links in current.items():
                keys = self._ordered_links(links["ref"])
                run_refs = self._ordered_links(links["run"])
                columns = transition(keys, run_refs)
                for link, old, new in (
                    ("ref", links["ref"], columns.source_ref_keys),
                    ("run", links["run"], columns.source_run_refs),
                ):
                    old_keys = {entry["k"] for entry in old}
                    next_position = max((entry["p"] for entry in old), default=-1) + 1
                    for value in new:
                        if value not in old_keys:
                            inserts[link].append({"id": identity, "v": value, "p": next_position})
                            next_position += 1
                    for value in sorted(old_keys - set(new)):
                        deletes[link].append({"id": identity, "v": value})
            changed = {
                row["id"] for rows_ in (*deletes.values(), *inserts.values()) for row in rows_
            }
            for link, link_rows in deletes.items():
                if link_rows:
                    tx.query(_link_delete_query(kind, link), given_rows=link_rows).resolve()
            # A link insert whose ref entity is missing would match nothing
            # and commit an unowned artifact, so verify the refs this chunk
            # links to exist first. A read: re-putting them here made
            # concurrent chunks conflict on the shared key and tripled the
            # write time.
            for link, link_rows in inserts.items():
                self._require_refs(tx, link, {row["v"] for row in link_rows})
            for link, link_rows in inserts.items():
                if link_rows:
                    tx.query(_link_insert_query(kind, link), given_rows=link_rows).resolve()
            if changed:
                now = _now_ms()
                tx.query(
                    _touch_query(kind), given_rows=[{"id": i, "now": now} for i in sorted(changed)]
                ).resolve()
            tx.commit()

    async def _provenance_change(self, kind: str, identities, attach: "_ProvenanceAttach") -> None:
        """Explicit attach/remove: apply ``attach.transition`` to every
        artifact in ``identities`` in concurrent chunk-sized transactions,
        after putting any ref entities the change links to. Each chunk is
        retried on commit conflicts with other writers; the transition is
        idempotent, so a retried chunk cannot double-record."""
        identities = list(dict.fromkeys(str(identity) for identity in identities))
        if not identities:
            return
        await self._provision_database()
        await self._ensure_provenance_refs(attach)
        size = self._chunk_rows
        tasks = [
            asyncio.create_task(
                self._write_chunk([], (kind, identities[start : start + size], attach.transition))
            )
            for start in range(0, len(identities), size)
        ]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    def _mutate_properties_sync(self, kind: str, identities, mutate) -> set[str]:
        """In one WRITE transaction: read properties-json for ``identities``
        (all artifacts when None), apply ``mutate(identity, props) -> props |
        None``, write back the changed payloads, commit. Returns updated ids."""
        from typedb.driver import TransactionType

        driver = self._get_driver()
        with driver.transaction(self.database_name, TransactionType.WRITE) as tx:
            if identities is None:
                answer = tx.query(_properties_read_query(kind, True)).resolve()
            else:
                rows = [{"id": identity} for identity in dict.fromkeys(identities)]
                answer = tx.query(_properties_read_query(kind, False), given_rows=rows).resolve()
            documents = self._collect_answer(answer)
            now = _now_ms()
            updates = []
            for document in documents:
                try:
                    properties = json.loads(document["p"]) if document.get("p") else {}
                except (TypeError, ValueError):
                    continue
                changed = mutate(document["id"], properties)
                if changed is not None:
                    updates.append(
                        {
                            "id": document["id"],
                            "v": json.dumps(changed, cls=JSONEncoder),
                            "now": now,
                        }
                    )
            if updates:
                tx.query(_properties_write_query(kind), given_rows=updates).resolve()
            tx.commit()
        return {update["id"] for update in updates}

    async def _mutate_properties(self, kind: str, identities, mutate) -> set[str]:
        """Read-modify-write ``properties-json`` through ``mutate``; see the
        sync primitive. ``identities=None`` scans every artifact in a READ
        transaction first and writes only the candidates, so the write
        transaction (and any STC2 retry) touches the rows it changes."""
        if identities is None:
            documents = (await self._read_batch([_properties_read_query(kind, True)]))[0]
            identities = []
            for document in documents:
                try:
                    properties = json.loads(document["p"]) if document.get("p") else {}
                except (TypeError, ValueError):
                    continue
                if mutate(document["id"], properties) is not None:
                    identities.append(document["id"])
        if not identities:
            return set()
        await self._provision_database()
        return await self._retry_commit_conflicts(
            lambda: self._run_sync_shielded(self._mutate_properties_sync, kind, identities, mutate)
        )

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
    def _node_upsert_row_from_properties(
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

    def _node_upsert_row(self, node: DataPoint) -> dict[str, Any]:
        return self._node_upsert_row_from_properties(
            str(node.id), node.model_dump(), type(node).__name__
        )

    def _neighbour_query_spec(self, node_id: str, incoming: bool, edge_label: str | None = None):
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

        given_rows = [params] if params else None
        resolved = (
            TransactionType[transaction_type.upper()]
            if transaction_type is not None
            else self._transaction_type_for(query)
        )
        if resolved == TransactionType.READ:
            if not await self._database_available():
                return []
        else:
            await self._provision_database()
        specs = [(query, given_rows)]

        async def attempt():
            return (await self._run_sync(self._run_batch_sync, specs, resolved, True))[0]

        return await self._retry_commit_conflicts(attempt, resolved != TransactionType.READ)

    async def has_node(self, node_id: str) -> bool:
        results = await self._read_batch([(_FETCH_NODES, [{"id": str(node_id)}])])
        return bool(results[0])

    async def add_node(self, node: DataPoint | str, properties: dict[str, Any] | None = None):
        """Add (or update) a single node from a DataPoint or an id + properties.

        Carries no provenance, so existing source-ref-key/source-run-id
        stamps on the node are left untouched.
        """
        if isinstance(node, DataPoint):
            row = self._node_upsert_row(node)
        else:
            node_props = dict(properties or {})
            node_props.setdefault("id", str(node))
            row = self._node_upsert_row_from_properties(str(node), node_props, "node")
        row["now"] = _now_ms()
        await self._write_batch([(_NODE_UPSERT, [row])])

    @staticmethod
    def _validated_pipeline_run_id(pipeline_run_id) -> str | None:
        """Validate a pipeline run id (UUID or its string form) before any
        server contact; cognee's transition would otherwise fail mid-transaction."""
        if pipeline_run_id is None:
            return None
        try:
            return str(coerce_run_uuid(pipeline_run_id))
        except ValueError as error:
            raise ValueError(f"pipeline_run_id must be a UUID, got {pipeline_run_id!r}") from error

    @classmethod
    def _attach_for_batch(cls, source_ref_key: str | None, pipeline_run_id):
        """The provenance attach folded into add_nodes/add_edges, or None.

        Delegates to cognee's ``provenance_after_attach`` (Model A: a key's run
        mapping is recorded only when the key is new to the artifact).
        """
        if source_ref_key is None:
            return None
        try:
            get_dataset_id_from_source_ref_key(source_ref_key)
        except ValueError as error:
            raise ValueError(
                "source_ref_key must be built with cognee's make_source_ref_key()"
            ) from error
        run = cls._validated_pipeline_run_id(pipeline_run_id)
        return _ProvenanceAttach.for_keys([source_ref_key], run)

    async def add_nodes(
        self,
        nodes: list[DataPoint],
        source_ref_key: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> None:
        """Upsert a batch of DataPoints in chunked, concurrent transactions.

        With ``source_ref_key`` the provenance attach is folded into each
        chunk's transaction as links to ref entities (module docstring); the
        chunks run concurrently. Rows sharing a node id collapse to the last
        one.
        """
        if not nodes:
            return
        attach = self._attach_for_batch(source_ref_key, pipeline_run_id)
        now = _now_ms()
        rows: dict[str, dict[str, Any]] = {}
        for node in nodes:
            row = self._node_upsert_row(node)
            row["now"] = now
            rows[row["id"]] = row
        await self._write_rows(
            _NODE_UPSERT,
            list(rows.values()),
            provenance=("node", "id", attach) if attach else None,
        )

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
        rows = [{"id": node_id} for node_id in dict.fromkeys(str(i) for i in node_ids)]
        # Provenance links and edges first: deleting a player leaves a
        # dangling relation, whether that relation is an edge or a link.
        link_queries = _link_deletes(_DELETE_LINKS_OF_INCIDENT_EDGES) + _link_deletes(
            _DELETE_LINKS_OF_NODES
        )
        await self._write_batch(
            [(query, rows) for query in link_queries]
            + [(_DELETE_INCIDENT_EDGES, rows), (_DELETE_NODES, rows)]
        )

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

        Edge identity is the edge-key; properties are replaced on re-add and
        duplicate identities within a batch collapse to the last row. With
        ``source_ref_key`` the provenance attach is folded into each chunk's
        transaction as links to ref entities (module docstring); the chunks
        run concurrently. Edges whose endpoints are missing are skipped
        (cognee adds nodes before edges).
        """
        if not edges:
            return
        attach = self._attach_for_batch(source_ref_key, pipeline_run_id)
        now = _now_ms()
        rows: dict[str, dict[str, Any]] = {}
        for source_id, target_id, relationship_name, properties in edges:
            source_id, target_id = str(source_id), str(target_id)
            edge_properties = {
                **(properties or {}),
                "source_node_id": source_id,
                "target_node_id": target_id,
                "relationship_name": relationship_name,
            }
            key = _edge_key(source_id, target_id, relationship_name)
            # CogneeGraph reads edge_object_id from the projected properties,
            # so the derived id is written into the payload as well.
            edge_object_id = str(
                edge_properties.get("edge_object_id")
                or generate_edge_object_id(source_id, target_id, relationship_name)
            )
            edge_properties.setdefault("edge_object_id", edge_object_id)
            rows[key] = {
                "key": key,
                "sid": source_id,
                "tid": target_id,
                "rel": relationship_name,
                "eoid": edge_object_id,
                "props": json.dumps(edge_properties, cls=JSONEncoder),
                "now": now,
            }
        await self._write_rows(
            _EDGE_UPSERT,
            list(rows.values()),
            _SET_EDGE_CREATED_AT,
            "key",
            provenance=("edge", "key", attach) if attach else None,
        )

    async def get_edges(self, node_id: str):
        """Edges incident to a node, anchor-first: (node_id, neighbour_id, {...}).

        The de facto cognee adapter convention (and what format_edges in the
        memify pipeline assumes) is that slot 0 is the queried node and slot 1
        the neighbour, regardless of the edge's true direction.
        """
        anchor = str(node_id)
        seen: dict[tuple[str, str, str], None] = {}
        for document in await self._incident_edge_documents([anchor]):
            other = document["target"] if document["source"] == anchor else document["source"]
            seen[(anchor, other, document["relationship_name"])] = None
        return [
            (first, second, {"relationship_name": relationship})
            for first, second, relationship in seen
        ]

    async def get_predecessors(self, node_id: str, edge_label: str | None = None) -> list:
        results = await self._read_batch([self._neighbour_query_spec(node_id, True, edge_label)])
        return [self._document_to_node_dict(doc["neighbour"]) for doc in results[0]]

    async def get_successors(self, node_id: str, edge_label: str | None = None) -> list:
        results = await self._read_batch([self._neighbour_query_spec(node_id, False, edge_label)])
        return [self._document_to_node_dict(doc["neighbour"]) for doc in results[0]]

    async def get_neighbors(self, node_id: str) -> list[dict[str, Any]]:
        """Predecessors and successors combined, keyed on the node-id attribute
        (never the payload's "id", which callers may set independently)."""
        anchor = str(node_id)
        results = await self._read_batch(
            [
                self._neighbour_query_spec(anchor, True),
                self._neighbour_query_spec(anchor, False),
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

    async def _incident_edge_documents(self, node_ids) -> list[dict[str, Any]]:
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
    def _edges_within_node_set(
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

    async def _fetch_with_incident_edges(self, node_ids) -> tuple[list[dict], list[dict]]:
        """Node documents for ``node_ids`` plus all their incident edge documents,
        in a single read transaction (fetch + both directional sweeps)."""
        ids = sorted({str(node_id) for node_id in node_ids})
        if not ids:
            return [], []
        rows = [{"id": node_id} for node_id in ids]
        node_docs, outgoing, incoming = await self._read_batch(
            [(_FETCH_NODES, rows), (_INCIDENT_EDGES_OUT, rows), (_INCIDENT_EDGES_IN, rows)]
        )
        return node_docs, outgoing + incoming

    @classmethod
    def _add_unknown_endpoints(
        cls, edge_docs, nodes: dict[str, dict], wanted_types: set[str] | None = None
    ) -> set[str]:
        """Add the not-yet-known endpoints of ``edge_docs`` to ``nodes``.

        Returns the ids added (the next BFS frontier). Edges outside
        ``wanted_types`` are not followed.
        """
        added: set[str] = set()
        for document in edge_docs:
            if wanted_types is not None and document["relationship_name"] not in wanted_types:
                continue
            for endpoint, node_doc in (
                (document["source"], document["source_node"]),
                (document["target"], document["target_node"]),
            ):
                if endpoint not in nodes:
                    nodes[endpoint] = cls._document_to_node_dict(node_doc)
                    added.add(endpoint)
        return added

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

        node_docs, edge_docs = await self._fetch_with_incident_edges(node_ids)
        nodes: dict[str, dict] = {
            doc["node"]["node-id"]: self._document_to_node_dict(doc["node"]) for doc in node_docs
        }
        swept = set(nodes)
        frontier = (
            self._add_unknown_endpoints(edge_docs, nodes, wanted_types) if depth > 0 else set()
        )
        for _ in range(1, max(depth, 0)):
            if not frontier:
                break
            documents = await self._incident_edge_documents(frontier)
            swept |= frontier
            edge_docs.extend(documents)
            frontier = self._add_unknown_endpoints(documents, nodes, wanted_types)

        # Edges between nodes of the final frontier were never swept.
        edge_docs.extend(await self._incident_edge_documents(set(nodes) - swept))
        return (
            list(nodes.items()),
            self._edges_within_node_set(edge_docs, set(nodes), wanted_types),
        )

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
                self._neighbour_query_spec(str(node_id), True),
                self._neighbour_query_spec(str(node_id), False),
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
        anchor_role = "target" if incoming else "source"
        rows = [{"id": str(node_id), "label": edge_label} for node_id in node_ids]
        link_queries = _link_deletes(_DELETE_LINKS_OF_LABELED_EDGES, anchor_role=anchor_role)
        await self._write_batch(
            [(query, rows) for query in link_queries]
            + [(_REMOVE_LABELED_EDGES.format(anchor_role=anchor_role), rows)]
        )

    async def delete_graph(self):
        """Remove all nodes and edges (the schema is kept)."""
        await self._write_batch(
            [
                "match $l isa sourced-from; delete $l;",
                "match $l isa run-attached; delete $l;",
                "match $e isa edge; delete $e;",
                "match $n isa node; delete $n;",
                # Ref entities are kept: a concurrent writer may be between
                # putting them and linking to them, and unlinked refs are
                # invisible to every read path.
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

    async def get_id_filtered_graph_data(self, target_ids: list[str]):
        """Targets, their direct neighbours, and only the edges touching a target.

        Same shape as get_graph_data(). CogneeGraph prefers this over the
        whole-graph projection whenever an adapter provides it, which is what
        keeps GRAPH_COMPLETION search cost proportional to the search rather
        than to the graph. One read transaction.
        """
        if not target_ids:
            return ([], [])
        if not all(isinstance(target_id, str) for target_id in target_ids):
            raise CogneeValidationError("target_ids must be a list of strings")

        node_docs, edge_docs = await self._fetch_with_incident_edges(target_ids)
        nodes: dict[str, dict] = {
            doc["node"]["node-id"]: self._document_to_node_dict(doc["node"]) for doc in node_docs
        }
        if not nodes:
            return ([], [])
        self._add_unknown_endpoints(edge_docs, nodes)
        # Every swept edge touches a target, and both endpoints are now known.
        return (list(nodes.items()), self._edges_within_node_set(edge_docs, set(nodes)))

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

        seed_edge_docs = await self._incident_edge_documents(seeds)

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
        edge_docs = seed_edge_docs + await self._incident_edge_documents(wanted)
        return (list(nodes.items()), self._edges_within_node_set(edge_docs, set(nodes)))

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
            edge_docs = await self._incident_edge_documents(nodes_by_id)
            return (
                list(nodes_by_id.items()),
                self._edges_within_node_set(edge_docs, set(nodes_by_id)),
            )

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

    # cognee's TEMPORAL search type calls these two non-interface methods on
    # the graph engine (temporal_retriever.py) and would otherwise die with an
    # AttributeError; fail the way the CYPHER/NATURAL_LANGUAGE gates do. Real
    # implementations are planned (Phase 5 in the work plan).
    async def collect_time_ids(self, time_from=None, time_to=None):
        raise SearchTypeNotSupported(
            "Temporal search is not yet supported with the TypeDBAdapter graph backend."
        )

    async def collect_events(self, ids):
        raise SearchTypeNotSupported(
            "Temporal search is not yet supported with the TypeDBAdapter graph backend."
        )

    # ------------------------------------------------------------------
    # Graph provenance (cognee's 15-method contract) and parity methods
    # ------------------------------------------------------------------

    @staticmethod
    def _edge_identity_key(edge: EdgeIdentity) -> str:
        return _edge_key(str(edge.source_id), str(edge.target_id), edge.relationship_name)

    async def attach_node_source_refs(self, node_ids, source_ref_keys, pipeline_run_id=None):
        if not source_ref_keys:
            return
        run = self._validated_pipeline_run_id(pipeline_run_id)
        await self._provenance_change(
            "node", node_ids, _ProvenanceAttach.for_keys(list(source_ref_keys), run)
        )

    async def attach_edge_source_refs(self, edges, source_ref_keys, pipeline_run_id=None):
        if not source_ref_keys:
            return
        run = self._validated_pipeline_run_id(pipeline_run_id)
        await self._provenance_change(
            "edge",
            [self._edge_identity_key(edge) for edge in edges],
            _ProvenanceAttach.for_keys(list(source_ref_keys), run),
        )

    async def remove_node_source_refs(self, node_ids, source_ref_keys):
        if not source_ref_keys:
            return
        await self._provenance_change(
            "node", node_ids, _ProvenanceAttach.removing(list(source_ref_keys))
        )

    async def remove_edge_source_refs(self, edges, source_ref_keys):
        if not source_ref_keys:
            return
        await self._provenance_change(
            "edge",
            [self._edge_identity_key(edge) for edge in edges],
            _ProvenanceAttach.removing(list(source_ref_keys)),
        )

    async def delete_edge_triples(self, edges) -> None:
        """Delete the given edges only; their endpoint nodes are kept."""
        if not edges:
            return
        rows = [{"id": self._edge_identity_key(edge)} for edge in edges]
        await self._write_batch(
            [(query, rows) for query in _link_deletes(_DELETE_LINKS_OF_EDGES_BY_KEY)]
            + [(_DELETE_EDGES_BY_KEY, rows)]
        )

    def _provenance_columns_from_document(self, document: dict) -> ProvenanceColumns:
        keys, run_refs = self._decode_provenance(document)
        return ProvenanceColumns(keys, derive_dataset_ids(keys), derive_run_ids(run_refs), run_refs)

    async def get_node_delete_data(self, node_ids) -> dict[str, NodeDeleteData]:
        if not node_ids:
            return {}
        rows = [{"id": str(node_id)} for node_id in dict.fromkeys(node_ids)]
        documents = (await self._read_batch([(_NODE_DELETE_DATA, rows)]))[0]
        result: dict[str, NodeDeleteData] = {}
        for document in documents:
            node_doc = document["node"]
            node_id = node_doc["node-id"]
            properties = self._document_to_node_dict(node_doc)
            metadata = properties.get("metadata") or {}
            indexed_fields = (
                list(metadata.get("index_fields") or []) if isinstance(metadata, dict) else []
            )
            columns = self._provenance_columns_from_document(document)
            result[node_id] = NodeDeleteData(
                node_id=node_id,
                node_type=str(properties.get("type") or node_doc.get("node-type") or ""),
                indexed_fields=indexed_fields,
                node_properties=properties,
                source_ref_keys=columns.source_ref_keys,
                source_dataset_ids=columns.source_dataset_ids,
                source_run_ids=columns.source_run_ids,
                source_run_refs=columns.source_run_refs,
            )
        return result

    async def get_edge_delete_data(self, edges) -> dict[EdgeIdentity, EdgeDeleteData]:
        if not edges:
            return {}
        # Lazy import: the modules layer imports get_graph_engine at package
        # load, which would form a cycle with this adapter module.
        from cognee.modules.graph.utils.prepare_edges_for_storage import get_edge_retrieval_text

        rows = [{"id": self._edge_identity_key(edge)} for edge in edges]
        documents = (await self._read_batch([(_EDGE_DELETE_DATA, rows)]))[0]
        result: dict[EdgeIdentity, EdgeDeleteData] = {}
        for document in documents:
            edge = EdgeIdentity(
                document["source"], document["target"], document["relationship_name"]
            )
            properties = self._document_to_edge_properties(document["edge"])
            columns = self._provenance_columns_from_document(document)
            result[edge] = EdgeDeleteData(
                edge=edge,
                edge_text=get_edge_retrieval_text(
                    properties.get("edge_text"), edge.relationship_name
                ),
                edge_properties=properties,
                source_ref_keys=columns.source_ref_keys,
                source_dataset_ids=columns.source_dataset_ids,
                source_run_ids=columns.source_run_ids,
                source_run_refs=columns.source_run_refs,
            )
        return result

    async def _artifacts_by_ref(self, kind: str, link: str, by_derived: bool, value: str):
        """Artifact documents linked to a ref entity, one per artifact (an
        artifact linked to several refs of one dataset appears once)."""
        query = _artifacts_by_ref_query(kind, link, by_derived)
        documents = (await self._read_batch([(query, [{"v": value}])]))[0]
        unique: dict = {}
        for document in documents:
            identity = document["id"] if kind == "node" else self._edge_identity_of(document)
            unique.setdefault(identity, document)
        return unique

    @staticmethod
    def _edge_identity_of(document: dict) -> EdgeIdentity:
        return EdgeIdentity(document["source"], document["target"], document["relationship_name"])

    async def find_nodes_by_source_ref(self, source_ref_key: str) -> list[str]:
        return list(await self._artifacts_by_ref("node", "ref", False, source_ref_key))

    async def find_edges_by_source_ref(self, source_ref_key: str) -> list[EdgeIdentity]:
        return list(await self._artifacts_by_ref("edge", "ref", False, source_ref_key))

    def _keys_owned_by_dataset(self, document: dict, dataset_id: str) -> list[str]:
        keys, _refs = self._decode_provenance(document)
        return [key for key in keys if str(get_dataset_id_from_source_ref_key(key)) == dataset_id]

    def _keys_contributed_by_run(self, document: dict, pipeline_run_id: str) -> list[str]:
        _keys, run_refs = self._decode_provenance(document)
        return [
            get_source_ref_key_from_source_run_ref(ref)
            for ref in run_refs
            if str(get_pipeline_run_id_from_source_run_ref(ref)) == pipeline_run_id
        ]

    async def find_node_source_refs_by_dataset(self, dataset_id: str) -> dict[str, list[str]]:
        found = await self._artifacts_by_ref("node", "ref", True, dataset_id)
        result = {}
        for identity, doc in found.items():
            owned = self._keys_owned_by_dataset(doc, dataset_id)
            if owned:
                result[identity] = owned
        return result

    async def find_edge_source_refs_by_dataset(
        self, dataset_id: str
    ) -> dict[EdgeIdentity, list[str]]:
        found = await self._artifacts_by_ref("edge", "ref", True, dataset_id)
        result = {}
        for identity, doc in found.items():
            owned = self._keys_owned_by_dataset(doc, dataset_id)
            if owned:
                result[identity] = owned
        return result

    async def find_node_source_refs_by_pipeline_run(
        self, pipeline_run_id: str
    ) -> dict[str, list[str]]:
        found = await self._artifacts_by_ref("node", "run", True, pipeline_run_id)
        result = {}
        for identity, doc in found.items():
            contributed = self._keys_contributed_by_run(doc, pipeline_run_id)
            if contributed:
                result[identity] = contributed
        return result

    async def find_edge_source_refs_by_pipeline_run(
        self, pipeline_run_id: str
    ) -> dict[EdgeIdentity, list[str]]:
        found = await self._artifacts_by_ref("edge", "run", True, pipeline_run_id)
        result = {}
        for identity, doc in found.items():
            contributed = self._keys_contributed_by_run(doc, pipeline_run_id)
            if contributed:
                result[identity] = contributed
        return result

    async def set_graph_metadata(self, metadata: dict[str, str]) -> None:
        if not metadata:
            return
        rows = [{"k": str(key), "v": str(value)} for key, value in metadata.items()]
        await self._write_batch([(_METADATA_SET, rows)])

    async def get_graph_metadata(self) -> dict[str, str]:
        return {doc["k"]: doc["v"] for doc in (await self._read_batch([_METADATA_GET]))[0]}

    async def remove_belongs_to_set_tags(self, tags, node_ids=None) -> None:
        if not tags or (node_ids is not None and not node_ids):
            return None
        tag_set = set(tags)

        def mutate(_node_id, properties):
            current = properties.get("belongs_to_set")
            if not isinstance(current, list) or not any(tag in tag_set for tag in current):
                return None
            return {**properties, "belongs_to_set": [tag for tag in current if tag not in tag_set]}

        identities = None if node_ids is None else [str(node_id) for node_id in node_ids]
        await self._mutate_properties("node", identities, mutate)
        return None

    # --- feedback / truth weights (stored in properties-json, as Ladybug does;
    # CogneeGraph reads feedback_weight from the projected properties) ---

    @staticmethod
    def _non_empty_string_ids(ids) -> list[str]:
        return [identity for identity in ids if isinstance(identity, str) and identity]

    async def get_node_feedback_weights(self, node_ids) -> dict[str, float]:
        valid = self._non_empty_string_ids(node_ids)
        if not valid:
            return {}
        result = {}
        for node in await self.get_nodes(valid):
            try:
                result[node["id"]] = float(node.get("feedback_weight", 0.5))
            except (TypeError, ValueError):
                result[node["id"]] = 0.5
        return result

    async def set_node_feedback_weights(self, node_feedback_weights) -> dict[str, bool]:
        if not node_feedback_weights:
            return {}
        valid = self._non_empty_string_ids(node_feedback_weights)
        updated = set()
        if valid:
            updated = await self._mutate_properties(
                "node",
                valid,
                lambda node_id, props: {
                    **props,
                    "feedback_weight": float(node_feedback_weights[node_id]),
                },
            )
        return {node_id: node_id in updated for node_id in node_feedback_weights}

    async def get_node_truth_state(self, node_ids) -> dict[str, dict[str, Any]]:
        valid = self._non_empty_string_ids(node_ids)
        if not valid:
            return {}
        result = {}
        for node in await self.get_nodes(valid):
            alignment = node.get("truth_alignment", [])
            epoch = node.get("truth_epoch")
            try:
                truth_epoch = int(epoch) if epoch is not None else None
            except (TypeError, ValueError):
                truth_epoch = None
            result[node["id"]] = {
                "truth_alignment": list(alignment) if isinstance(alignment, (list, tuple)) else [],
                "truth_epoch": truth_epoch,
            }
        return result

    async def set_node_truth_state(self, node_truth_state) -> dict[str, bool]:
        if not node_truth_state:
            return {}
        valid = self._non_empty_string_ids(node_truth_state)

        def mutate(node_id, props):
            state = node_truth_state[node_id]
            updated = {**props, "truth_alignment": list(state.get("truth_alignment") or [])}
            if state.get("truth_epoch") is not None:
                updated["truth_epoch"] = int(state["truth_epoch"])
            return updated

        updated = await self._mutate_properties("node", valid, mutate) if valid else set()
        return {node_id: node_id in updated for node_id in node_truth_state}

    async def _edges_by_object_ids(self, edge_object_ids) -> list[dict]:
        rows = [{"v": edge_object_id} for edge_object_id in dict.fromkeys(edge_object_ids)]
        return (await self._read_batch([(_EDGES_BY_OBJECT_ID, rows)]))[0]

    async def get_edge_feedback_weights(self, edge_object_ids) -> dict[str, float]:
        valid = self._non_empty_string_ids(edge_object_ids)
        if not valid:
            return {}
        result = {}
        for document in await self._edges_by_object_ids(valid):
            properties = self._document_to_edge_properties({"properties-json": document.get("p")})
            try:
                result[document["eoid"]] = float(properties.get("feedback_weight", 0.5))
            except (TypeError, ValueError):
                result[document["eoid"]] = 0.5
        return result

    async def set_edge_feedback_weights(self, edge_feedback_weights) -> dict[str, bool]:
        if not edge_feedback_weights:
            return {}
        valid = self._non_empty_string_ids(edge_feedback_weights)
        found = (
            {doc["key"]: doc["eoid"] for doc in await self._edges_by_object_ids(valid)}
            if valid
            else {}
        )
        updated_keys = set()
        if found:
            updated_keys = await self._mutate_properties(
                "edge",
                list(found),
                lambda key, props: {
                    **props,
                    "feedback_weight": float(edge_feedback_weights[found[key]]),
                },
            )
        updated = {found[key] for key in updated_keys}
        return {
            edge_object_id: edge_object_id in updated for edge_object_id in edge_feedback_weights
        }

    async def get_triplets_batch(self, offset: int, limit: int) -> list[dict[str, Any]]:
        """Edges as {start_node, relationship_properties, end_node}, ordered by edge-key."""
        if offset < 0:
            raise ValueError(f"Offset must be non-negative, got {offset}")
        if limit < 0:
            raise ValueError(f"Limit must be non-negative, got {limit}")
        if limit == 0:
            return []
        query = _TRIPLETS_BATCH.format(offset=int(offset), limit=int(limit))
        triplets = []
        for document in (await self._read_batch([query]))[0]:
            edge_doc = document["edge"]
            triplets.append(
                {
                    "start_node": self._document_to_node_dict(document["start"]),
                    "relationship_properties": {
                        **self._document_to_edge_properties(edge_doc),
                        "relationship_name": edge_doc.get("relationship-name"),
                    },
                    "end_node": self._document_to_node_dict(document["end"]),
                }
            )
        return triplets

    async def is_empty(self) -> bool:
        results = await self._read_batch(["match $n isa node; limit 1; reduce $count = count;"])
        if not results[0]:
            return True  # no database (or no rows): nothing to search
        return results[0][0].get("count", 1) == 0
