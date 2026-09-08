"""Helpers shared by the integration and e2e tiers (put on sys.path by tests/conftest.py)."""

import os
import socket

from cognee.infrastructure.engine import DataPoint

ADDRESS = os.environ.get("GRAPH_DATABASE_URL", "127.0.0.1:1729")


def server_available() -> bool:
    host, _, port = ADDRESS.rpartition(":")
    try:
        with socket.create_connection((host or "127.0.0.1", int(port)), timeout=2):
            return True
    except OSError:
        return False


class Concept(DataPoint):
    name: str
    description: str | None = None
    metadata: dict = {"index_fields": ["name"]}
