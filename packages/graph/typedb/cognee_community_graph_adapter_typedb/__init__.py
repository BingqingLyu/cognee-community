"""Cognee Community Graph Adapter - TypeDB

This package provides a TypeDB graph database adapter for the Cognee framework.
"""

from .typedb_adapter import TypeDBAdapter
from .TypeDBDatasetDatabaseHandler import TypeDBDatasetDatabaseHandler

__version__ = "0.1.0"
__all__ = ["TypeDBAdapter", "TypeDBDatasetDatabaseHandler", "register"]


def register():
    """Register the TypeDB adapter and its per-dataset database handler.

    The handler backs cognee's backend-access-control mode (one TypeDB
    database per dataset); select it with GRAPH_DATASET_DATABASE_HANDLER=typedb.
    """
    try:
        from cognee.infrastructure.databases.dataset_database_handler import (
            use_dataset_database_handler,
        )
        from cognee.infrastructure.databases.graph import use_graph_adapter
    except ImportError as ie:
        raise ImportError(
            "cognee is not installed. Please install it with: pip install cognee"
        ) from ie

    use_graph_adapter("typedb", TypeDBAdapter)
    use_dataset_database_handler("typedb", TypeDBDatasetDatabaseHandler, "typedb")
