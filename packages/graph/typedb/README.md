# Cognee Community Graph Adapter — TypeDB

[TypeDB](https://typedb.com) graph database adapter for [cognee](https://github.com/topoteretes/cognee).

> **Status: work in progress.** The package scaffold, cognee contract
> conformance (registration, constructor, and full `GraphDBInterface` call
> surface), and connection plumbing are in place. The graph operations
> themselves are being implemented; methods that are not ready yet raise
> `NotImplementedError`.

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

cognee.config.set_graph_db_config({
    "graph_database_url": "127.0.0.1:1729",
    "graph_database_username": "admin",
    "graph_database_password": "password",
})
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
```
