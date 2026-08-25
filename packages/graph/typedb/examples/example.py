"""Example usage of the TypeDB community adapter for Cognee.

Requires a running TypeDB 3.x server (default: 127.0.0.1:1729) and an LLM
API key in the environment (see the repo README).
"""

import asyncio
import os
import pathlib

import cognee

# NOTE: Importing register lets cognee know it can use the TypeDB graph adapter
from cognee_community_graph_adapter_typedb import register


async def main():
    # Configure cognee to use TypeDB
    cognee.config.set_graph_database_provider("typedb")
    register()

    # Set up your TypeDB connection (TypeDB 3.x, default credentials shown)
    cognee.config.set_graph_db_config(
        {
            "graph_database_url": os.environ.get("GRAPH_DB_URL", "127.0.0.1:1729"),
            "graph_database_username": os.environ.get("GRAPH_DB_USERNAME", "admin"),
            "graph_database_password": os.environ.get("GRAPH_DB_PASSWORD", "password"),
        }
    )

    # Optional: Set custom data and system directories
    system_path = pathlib.Path(__file__).parent
    cognee.config.system_root_directory(os.path.join(system_path, ".cognee_system"))
    cognee.config.data_root_directory(os.path.join(system_path, ".data_storage"))

    sample_data = [
        "TypeDB is a polymorphic database with a conceptual data model.",
        "TypeQL is TypeDB's declarative query language.",
        "Knowledge graphs represent entities and the relationships between them.",
        "Cognee builds AI memory by combining knowledge graphs with vector search.",
    ]

    await cognee.prune.prune_data()
    await cognee.prune.prune_system(metadata=True)

    print("Adding data to Cognee...")
    await cognee.add(sample_data, "typedb_knowledge")

    print("Processing data with Cognee...")
    await cognee.cognify(["typedb_knowledge"])

    print("Searching for insights...")
    search_results = await cognee.search(
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        query_text="How does cognee use knowledge graphs?",
    )

    print(f"Found {len(search_results)} insights:")
    for index, result in enumerate(search_results, 1):
        print(f"{index}. {result}")

    print("\nVisualizing the graph...")
    await cognee.visualize_graph(system_path / "graph.html")
    print(f"Graph visualization saved to {system_path / 'graph.html'}")


if __name__ == "__main__":
    asyncio.run(main())
