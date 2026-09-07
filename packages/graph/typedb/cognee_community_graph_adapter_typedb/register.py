"""Register the TypeDB graph adapter with Cognee on import.

Importing this module is equivalent to calling ``register()`` from the
package root; both are supported so either style works::

    from cognee_community_graph_adapter_typedb import register; register()
    import cognee_community_graph_adapter_typedb.register  # noqa: F401
"""

from cognee.infrastructure.databases.dataset_database_handler import (
    use_dataset_database_handler,
)
from cognee.infrastructure.databases.graph import use_graph_adapter

from .typedb_adapter import TypeDBAdapter
from .TypeDBDatasetDatabaseHandler import TypeDBDatasetDatabaseHandler

use_graph_adapter("typedb", TypeDBAdapter)
use_dataset_database_handler("typedb", TypeDBDatasetDatabaseHandler, "typedb")
