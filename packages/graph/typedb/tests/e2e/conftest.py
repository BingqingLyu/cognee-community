"""Shared e2e fixtures: cognee roots isolated per test, TypeDB e2e database config."""

import cognee
import pytest


@pytest.fixture
def isolated_roots(tmp_path):
    """Point cognee's data/system roots at a temp dir (as run_graph_db_test does)
    so pruning never touches the installed package's default storage."""
    from cognee.base_config import get_base_config

    base_config = get_base_config()
    prev_data_root = base_config.data_root_directory
    prev_system_root = base_config.system_root_directory
    cognee.config.data_root_directory(str(tmp_path / "data"))
    cognee.config.system_root_directory(str(tmp_path / "system"))
    yield
    cognee.config.data_root_directory(prev_data_root)
    cognee.config.system_root_directory(prev_system_root)
