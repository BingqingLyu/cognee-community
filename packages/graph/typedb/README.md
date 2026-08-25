# Cognee Community Graph Adapter — TypeDB

[TypeDB](https://typedb.com) graph database adapter for [cognee](https://github.com/topoteretes/cognee).

> **Status: work in progress.** The full `GraphDBInterface` surface is
> implemented — node/edge CRUD, traversal, raw TypeQL via `query()` (with
> `given`-based parameters), and the analytics tier (`get_graph_metrics`,
> `get_nodeset_subgraph`, `get_neighborhood`, `get_disconnected_nodes`,
> `get_filtered_graph_data`) — and integration-tested against a live
> TypeDB 3.x server. The full `add → cognify → search` pipeline has not yet
> been exercised end-to-end with an LLM; Cypher-generating search types are
> cleanly unsupported (`supports_cypher_queries = False`), with a TypeQL
> natural-language retriever planned.

## Requirements

- TypeDB **3.x** server (default `127.0.0.1:1729`, credentials `admin`/`password`)
- Python 3.10–3.13
- An LLM API key for the full cognee pipeline (see the repository README)

## Installation

```bash
uv pip install cognee-community-graph-adapter-typedb
```

Or locally from this directory:

```bash
uv sync
# OR
poetry install
```

## Usage

```python
import cognee
from cognee_community_graph_adapter_typedb import register

cognee.config.set_graph_database_provider("typedb")
register()

cognee.config.set_graph_db_config(
    {
        "graph_database_url": "127.0.0.1:1729",
        "graph_database_username": "admin",
        "graph_database_password": "password",
    }
)
```

See `examples/example.py` for the full `add → cognify → search` flow.

## How the graph is modeled

TypeDB is schema-first while cognee's graph is a dynamic property graph, so
the adapter uses a generic reified schema: a single `node` entity type and a
single `edge` relation type (roles `source`/`target`), with cognee's node
labels and relationship names stored as `node_type`/`relationship_name`
attributes and arbitrary properties serialized into a `properties_json`
string attribute. A typed per-DataPoint schema mode — enabling
natural-language-to-TypeQL search against real domain types — is a planned
follow-up.

## Running tests

```bash
uv run pytest tests/unit -q     # offline contract tests, no server needed
uv run pytest tests -q          # + CRUD integration tests (needs TypeDB on 127.0.0.1:1729)
```
