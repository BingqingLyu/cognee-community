"""Offline tests of the batch write/read paths (no server: the executor is faked)."""

import asyncio

import pytest

from cognee_community_graph_adapter_typedb.typedb_adapter import (
    COMMIT_RETRIES,
    WRITE_CHUNK_ROWS,
    WRITE_CONCURRENCY,
    TypeDBAdapter,
)

STC2 = "\n[STC2] Commit in database 'x' failed with isolation conflict: ..."


def _offline_adapter() -> TypeDBAdapter:
    adapter = TypeDBAdapter()
    adapter._schema_initialized = True  # skip the server round trip
    # The tests size their batches from the module defaults.
    adapter._chunk_rows, adapter._write_concurrency = WRITE_CHUNK_ROWS, WRITE_CONCURRENCY
    return adapter


async def test_write_batch_retries_commit_conflicts_then_succeeds():
    adapter = _offline_adapter()
    calls = []

    async def fake_run_sync(fn, *args):
        calls.append(args)
        if len(calls) < 3:
            raise RuntimeError(STC2)

    adapter._run_sync = fake_run_sync
    await adapter._write_batch(["match $n isa node; delete $n;"])
    assert len(calls) == 3


async def test_write_batch_does_not_retry_other_errors():
    adapter = _offline_adapter()
    calls = []

    async def fake_run_sync(fn, *args):
        calls.append(1)
        raise RuntimeError("[CNT5] Constraint violated")

    adapter._run_sync = fake_run_sync
    with pytest.raises(RuntimeError, match="CNT5"):
        await adapter._write_batch(["match $n isa node; delete $n;"])
    assert len(calls) == 1


async def test_write_batch_gives_up_after_commit_retries():
    adapter = _offline_adapter()
    calls = []

    async def fake_run_sync(fn, *args):
        calls.append(1)
        raise RuntimeError(STC2)

    adapter._run_sync = fake_run_sync
    with pytest.raises(RuntimeError, match="STC2"):
        await adapter._write_batch(["match $n isa node; delete $n;"])
    assert len(calls) == COMMIT_RETRIES


async def test_write_rows_cancels_pending_chunks_on_first_failure():
    adapter = _offline_adapter()
    started = []

    async def fake_write_batch(specs, retry=True):
        first_id = specs[0][1][0]["id"]
        started.append(first_id)
        if first_id == "0":
            raise RuntimeError("chunk 0 failed")
        await asyncio.sleep(0.2)  # in flight while chunk 0 fails

    adapter._write_batch = fake_write_batch
    rows = [{"id": str(i)} for i in range(WRITE_CHUNK_ROWS * (WRITE_CONCURRENCY + 2))]

    with pytest.raises(RuntimeError, match="chunk 0 failed"):
        await adapter._write_rows("template", rows)

    # The slot freed by the failing chunk can be grabbed by the next waiter in
    # the same loop tick before gather() cancels, so at most one extra chunk
    # starts; the remaining pending chunk must never run.
    assert len(started) <= WRITE_CONCURRENCY + 1
    assert str(WRITE_CHUNK_ROWS * (WRITE_CONCURRENCY + 1)) not in started


async def test_write_rows_retries_a_conflicting_chunk_without_holding_a_slot():
    adapter = _offline_adapter()
    attempts = {}

    async def fake_write_batch(specs, retry=True):
        first_id = specs[0][1][0]["id"]
        attempts[first_id] = attempts.get(first_id, 0) + 1
        if first_id == "0" and attempts[first_id] < 3:
            raise RuntimeError(STC2)

    adapter._write_batch = fake_write_batch
    rows = [{"id": str(i)} for i in range(WRITE_CHUNK_ROWS * 2)]
    await adapter._write_rows("template", rows)
    assert attempts["0"] == 3
    assert attempts[str(WRITE_CHUNK_ROWS)] == 1


async def test_reads_short_circuit_when_the_database_is_missing():
    """Reads must not provision: a missing database yields empty results."""
    adapter = TypeDBAdapter()
    calls = []

    async def fake_run_sync(fn, *args):
        calls.append(getattr(fn, "__name__", repr(fn)))
        if fn.__name__ == "_database_exists_sync":
            return False
        raise AssertionError("no transaction should run against a missing database")

    adapter._run_sync = fake_run_sync
    assert await adapter.is_empty() is True
    assert await adapter.has_node("x") is False
    assert await adapter.get_graph_data() == ([], [])
    assert await adapter.query("match $n isa node; reduce $c = count;") == []
    assert set(calls) == {"_database_exists_sync"}


async def test_provenance_chunks_run_serially_and_fold_each_chunk():
    """With provenance, every chunk is one transaction (upsert + attach) and
    chunks never overlap, whatever the write concurrency."""
    adapter = _offline_adapter()
    in_flight, max_in_flight, folded = 0, 0, []

    async def fake_run_sync(fn, *args):
        nonlocal in_flight, max_in_flight
        assert fn.__func__ is TypeDBAdapter._provenance_change_sync
        kind, identities, _transition, specs = args
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        folded.append((kind, identities, [row["id"] for row in specs[0][1]]))

    adapter._run_sync = fake_run_sync
    rows = [{"id": str(i)} for i in range(WRITE_CHUNK_ROWS * 3)]
    await adapter._write_rows("template", rows, provenance=("node", "id", lambda k, r: None))

    assert max_in_flight == 1
    assert [len(ids) for _, ids, _ in folded] == [WRITE_CHUNK_ROWS] * 3
    assert all(ids == upserted for _, ids, upserted in folded)  # attach covers the chunk's rows


async def test_provenance_chunk_failure_stops_the_batch():
    """A failing provenance chunk propagates; later chunks never start (so a
    partial batch is a prefix of committed, fully-stamped chunks)."""
    adapter = _offline_adapter()
    seen = []

    async def fake_run_sync(fn, *args):
        seen.append(args[1][0])
        if len(seen) == 2:
            raise RuntimeError("chunk 2 failed")

    adapter._run_sync = fake_run_sync
    rows = [{"id": str(i)} for i in range(WRITE_CHUNK_ROWS * 4)]
    with pytest.raises(RuntimeError, match="chunk 2 failed"):
        await adapter._write_rows("template", rows, provenance=("node", "id", lambda k, r: None))
    assert seen == ["0", str(WRITE_CHUNK_ROWS)]
