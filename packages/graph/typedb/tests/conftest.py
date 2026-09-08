"""Shared pytest setup: make packages/shared (contract_suite) importable."""

import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3] / "shared"))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))  # tests/support.py

# Per-dataset TypeDB databases for cognee's backend-access-control mode; read
# when cognee builds its graph config, so it must be set before any test does.
os.environ.setdefault("GRAPH_DATASET_DATABASE_HANDLER", "typedb")
