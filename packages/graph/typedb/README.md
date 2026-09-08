# Cognee Community Graph Adapter - TypeDB

This package provides a [TypeDB](https://typedb.com) graph database adapter for the Cognee framework.

## Installation

```bash
pip install cognee-community-graph-adapter-typedb
```

Or locally from this directory:

```bash
uv sync --all-extras
# OR
poetry install
```

## Usage

```python
import asyncio

import cognee
from cognee.infrastructure.databases.graph import get_graph_engine
from cognee_community_graph_adapter_typedb import register


async def main():
    # Register the TypeDB adapter
    register()

    # Configure cognee to use TypeDB
    cognee.config.set_graph_database_provider("typedb")

    # Set up your TypeDB connection (TypeDB 3.x, default credentials shown)
    cognee.config.set_graph_db_config(
        {
            "graph_database_url": "127.0.0.1:1729",
            "graph_database_username": "admin",
            "graph_database_password": "password",
            # One TypeDB database per dataset for cognee's backend access
            # control (on by default). Equivalent env var below.
            "graph_dataset_database_handler": "typedb",
        }
    )

    await cognee.add(["TypeQL is TypeDB's declarative query language."], "my_dataset")
    await cognee.cognify(["my_dataset"])
    results = await cognee.search(
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        query_text="What is TypeQL?",
    )

    graph_engine = await get_graph_engine()
    nodes, edges = await graph_engine.get_graph_data()


if __name__ == "__main__":
    asyncio.run(main())
```

## Requirements

- Python >= 3.10, < 3.14
- TypeDB **3.12+** server (the adapter uses the TypeQL `given` stage)
- `typedb-driver` (installed automatically)
- An LLM API key for the full cognee pipeline (see the repository README)

## Configuration

Configure via `set_graph_db_config()`:

| Key | Default | Description |
|-----|---------|-------------|
| `graph_database_url` | `127.0.0.1:1729` | TypeDB server address as `host:port` (a URL scheme prefix is stripped) |
| `graph_database_port` | – | Appended to `graph_database_url` when the address carries no port |
| `graph_database_username` | `admin` | TypeDB user |
| `graph_database_password` | `password` | TypeDB password |
| `graph_database_name` | `cognee` | TypeDB database; created (with the cognee schema) on first write |
| `graph_dataset_database_handler` | – | Set to `typedb` for one database per dataset (backend access control) |

### Environment Variables

Set the following environment variables or pass them directly in the config:

```bash
export GRAPH_DATABASE_PROVIDER="typedb"
export GRAPH_DATABASE_URL="127.0.0.1:1729"
export GRAPH_DATABASE_USERNAME="admin"
export GRAPH_DATABASE_PASSWORD="password"
```

### Multi-tenant / backend access control

Cognee's backend access control (on by default in cognee 1.x) maps each
dataset to its own graph database through a dataset database handler. This
package registers one for TypeDB — one TypeDB database per dataset, named
`cognee_<dataset uuid>` — under the handler key `typedb`. Select it alongside
the provider, either in `set_graph_db_config()` (as in the Usage example) or
via the environment:

```bash
export GRAPH_DATABASE_PROVIDER="typedb"
export GRAPH_DATASET_DATABASE_HANDLER="typedb"
```

Without it, cognee falls back to its default (Ladybug) handler and refuses to
run pipelines against the TypeDB provider unless
`ENABLE_BACKEND_ACCESS_CONTROL=false`. Credentials are never stored in the
dataset registry; they are resolved from the live config when a connection is
opened.

See [`.env.example`](.env.example) for a complete template (including an
Anthropic + local-embeddings variant), or use the
[`.env.template`](https://github.com/topoteretes/cognee/blob/main/.env.template)
from the main cognee repository.

## Features

- Implements Cognee's full `GraphDBInterface`: node/edge CRUD, traversal,
  `get_graph_data`, the analytics tier (`get_graph_metrics`,
  `get_nodeset_subgraph`, `get_neighborhood`, `get_disconnected_nodes`,
  `get_filtered_graph_data`), graph-native provenance (source refs, dataset /
  pipeline-run lookups, graph metadata, `delete_edge_triples`), node/edge
  feedback weights, node truth state, and `get_triplets_batch`
- Async API; the synchronous TypeDB driver runs on a small dedicated thread pool
- Batched writes: one compiled TypeQL query per batch, values passed through
  the `given` stage (never string-interpolated)
- Raw TypeQL via `graph_engine.query()`, with `given`-based parameters
- Compatible with Cognee's add/cognify/search and graph visualization

### How the graph is modeled

TypeDB is schema-first while cognee's graph is a dynamic property graph, so
the adapter uses the reified schema in `schema.tql`: a single `node` entity
type and a single `edge` relation type (roles `source`/`target`). Cognee's node
labels and relationship names are stored as `node-type`/`relationship-name`
attributes, the full property payload is serialized into `properties-json`
(the canonical record), and each edge carries an explicit
`edge-key` (`"{source}|{target}|{relationship}"`) as its identity.
Timestamps are epoch milliseconds: a node's `created-at` mirrors its
DataPoint's own `created_at`, an edge's is set on first write, and
`updated-at` is the write time. A typed per-DataPoint schema mode is a
planned follow-up.

### Provenance

Cognee's graph-native provenance (the `attach_*_source_refs` /
`find_*_by_*` / `get_*_delete_data` family) is implemented with cognee's own
transition functions, so attach/remove semantics match the built-in adapters
exactly, including "Model A": a pipeline run is recorded against a source
ref only when that ref is newly attached. Each node and edge stores the
ordered provenance record in `provenance-json` (the canonical copy, since
TypeDB's multi-valued attributes are unordered and cognee asserts attach
order) and mirrors it into four multi-valued lookup attributes
(`source-ref-key`, `source-dataset-id`, `source-run-id`, `source-run-ref`).
`add_nodes` / `add_edges` fold the attach into each chunk's transaction:
upsert, read the current record, apply the transition, write the diff,
commit. Explicit attach/remove calls do the same in one transaction; all
provenance writes retry on TypeDB commit conflicts.

Feedback weights (`feedback_weight`) and truth state (`truth_alignment`,
`truth_epoch`) live inside `properties-json`, where `CogneeGraph` reads them
from the projected properties; edge weights are addressed by cognee's
`edge_object_id`, stored as the `edge-object-id` attribute.

The schema define is idempotent and re-applied on every fresh adapter, so
additive schema changes reach existing databases; incompatible changes need
a fresh database.

### Limitations

- Cypher-generating search types (`SearchType.CYPHER`,
  `SearchType.NATURAL_LANGUAGE`) are cleanly unsupported
  (`supports_cypher_queries = False`); a TypeQL natural-language retriever is
  planned.
- `SearchType.TEMPORAL` is not supported yet: queries containing a time range
  raise `SearchTypeNotSupported` (after cognee's date-extraction LLM call;
  cognee has no entry gate for this search type), while queries without one
  fall back to cognee's triplet search. Timestamp/Event retrieval is planned.
- Like most sibling adapters, the optional legacy-deletion methods
  `get_document_subgraph` / `get_degree_one_nodes` are not implemented; that
  path is only reachable for data ingested before cognee 1.4.x's relational
  provenance ledger.
- Frequency weights (`get_node_frequency_weights` /
  `get_edge_frequency_weights`) raise `NotImplementedError`, as on every
  cognee adapter.

## Example

See `examples/example.py` for a full workflow (add data, cognify, search,
graph visualization) against a local TypeDB server.

## Running tests

```bash
uv run pytest tests/unit -q           # offline contract tests, no server needed
uv run pytest tests/integration -q    # adapter against TypeDB on 127.0.0.1:1729
RUN_E2E_TESTS=1 uv run pytest tests/e2e -q   # cognee's shared suite, both access-control modes (+ LLM key)
```

## License

This project is licensed under the MIT License.
