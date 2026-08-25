"""Cognee Community Graph Adapter - TypeDB

This package provides a TypeDB graph database adapter for the Cognee framework.
"""

from .typedb_adapter import TypeDBAdapter

__version__ = "0.1.0"
__all__ = ["TypeDBAdapter", "register"]


def register():
    """Register the TypeDB adapter with cognee's supported graph databases."""
    try:
        from cognee.infrastructure.databases.graph import use_graph_adapter
    except ImportError as ie:
        raise ImportError(
            "cognee is not installed. Please install it with: pip install cognee"
        ) from ie

    use_graph_adapter("typedb", TypeDBAdapter)
