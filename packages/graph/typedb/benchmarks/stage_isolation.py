"""Phase 0 follow-up: where does superlinear write cost come from, and where is
the transaction-size knee?

bulk_insert.py showed per-row cost rising steeply with rows-per-TRANSACTION
(not rows-per-query). This script isolates the write pipeline stages at a fixed
row count and sweeps transaction sizes, including concurrent transactions.

Usage: uv run python benchmarks/stage_isolation.py [--rows 1000] [--address host:port]
"""

import argparse
import asyncio
import json
import random
import time
import uuid

from cognee.modules.engine.utils import generate_edge_object_id

from cognee_community_graph_adapter_typedb import TypeDBAdapter
from cognee_community_graph_adapter_typedb.queries import (
    _EDGE_UPSERT,
    _NODE_UPSERT,
    _SET_EDGE_CREATED_AT,
    _edge_key,
    _now_ms,
)

# The set-once node statement the adapter used to run (kept here so the
# measurement that motivated dropping it can be reproduced).
_SET_NODE_CREATED_AT = """
given $id: string, $now: integer;
match $a isa node-id == $id; $n isa node, has $a; not { $n has created-at $c; };
insert $n has created-at == $now;
"""

INSERT_NODES = """
given $id: string, $type: string, $name: string, $props: string, $created: integer, $now: integer;
insert $n isa node, has node-id == $id, has node-type == $type, has name == $name,
  has properties-json == $props, has updated-at == $now, has created-at == $created;
"""
PUT_ONLY = "given $id: string;\nput $n isa node, has node-id == $id;"
PUT_UPDATE = _NODE_UPSERT
INSERT_EDGES = """
given $key: string, $sid: string, $tid: string, $rel: string, $eoid: string, $props: string,
  $now: integer;
match $s isa node, has node-id == $sid; $t isa node, has node-id == $tid;
insert $e isa edge, links (source: $s, target: $t), has edge-key == $key,
  has relationship-name == $rel, has edge-object-id == $eoid, has properties-json == $props,
  has updated-at == $now, has created-at == $now;
"""
EDGE_PUT_UPDATE = _EDGE_UPSERT


def node_id(index: int) -> uuid.UUID:
    """Deterministic but random-looking ids (sequential UUIDs share a prefix,
    which turns TypeDB's string lookups into scans; see README)."""
    return uuid.UUID(int=random.Random(index).getrandbits(128), version=4)


def node_rows(count, now):
    # Per-row payloads: rows sharing one long attribute value conflict at
    # commit under concurrent writers (see README), which real data never does.
    payload = "x" * 600
    return [
        {
            "id": str(node_id(i)),
            "type": "Entity",
            "name": f"e{i}",
            "props": json.dumps({"description": payload, "i": i}),
            "created": now,
            "now": now,
        }
        for i in range(count)
    ]


def edge_rows(count, node_count, now):
    rows = []
    for i in range(count):
        sid, tid = (
            str(node_id(i % node_count)),
            str(node_id((i * 7 + 3) % node_count)),
        )
        rel = ["is_a", "contains", "related_to"][i % 3]
        rows.append(
            {
                "key": _edge_key(sid, tid, rel),
                "sid": sid,
                "tid": tid,
                "rel": rel,
                "eoid": generate_edge_object_id(sid, tid, rel),
                "props": json.dumps({"w": i}),
                "now": now,
            }
        )
    return rows


OPEN: list[TypeDBAdapter] = []  # benchmark databases not yet dropped


async def fresh(address):
    a = TypeDBAdapter(
        graph_database_url=address, database_name=f"cognee_bench_{uuid.uuid4().hex[:8]}"
    )
    await a._provision_database()
    OPEN.append(a)
    return a


async def drop(a):
    OPEN.remove(a)
    a._get_driver().databases.get(a.database_name).delete()
    await a.close()


async def drop_all_open():
    """Drop whatever a failed scenario left behind (runs from main's finally)."""
    for a in list(OPEN):
        await drop(a)


async def timed(label, rows, coro):
    t = time.perf_counter()
    await coro
    s = time.perf_counter() - t
    print(f"  {label:<44} {rows:>6,} rows {s:7.2f}s {rows / s:9,.0f} rows/s")


async def stages(address, n):
    print(f"\n=== stage isolation, {n:,} nodes in ONE transaction ===")
    now = _now_ms()
    nrows = node_rows(n, now)
    for label, specs in [
        ("nodes: plain insert (no put/update)", [(INSERT_NODES, nrows)]),
        ("nodes: put identity only", [(PUT_ONLY, [{"id": r["id"]} for r in nrows])]),
        ("nodes: put + update (no created-at)", [(PUT_UPDATE, nrows)]),
        (
            "nodes: put + update + created-at (current)",
            [
                (PUT_UPDATE, nrows),
                (_SET_NODE_CREATED_AT, [{"id": r["id"], "now": now} for r in nrows]),
            ],
        ),
    ]:
        a = await fresh(address)
        await timed(label, n, a._write_batch(specs))
        await drop(a)

    print(
        f"\n=== stage isolation, {int(n * 1.5):,} edges in ONE transaction (nodes pre-loaded) ==="
    )
    erows = edge_rows(int(n * 1.5), n, now)
    for label, specs in [
        ("edges: plain insert (match+insert)", [(INSERT_EDGES, erows)]),
        ("edges: put + update (no created-at)", [(EDGE_PUT_UPDATE, erows)]),
        (
            "edges: put + update + created-at (current)",
            [
                (EDGE_PUT_UPDATE, erows),
                (_SET_EDGE_CREATED_AT, [{"key": r["key"], "now": now} for r in erows]),
            ],
        ),
    ]:
        a = await fresh(address)
        await a._write_batch([(INSERT_NODES, nrows)])
        await timed(label, len(erows), a._write_batch(specs))
        await drop(a)


async def tx_sweep(address, n, sizes, concurrency):
    now = _now_ms()
    nrows, erows = node_rows(n, now), edge_rows(int(n * 1.5), n, now)
    # The shipped node shape: put + update only (the set-once created-at
    # statement was dropped from nodes; under 4 concurrent writers its
    # `match … not {}` read widened the conflict footprint enough to exhaust
    # the adapter's STC2 retry budget on this sweep).
    node_chunks = lambda size: [[(PUT_UPDATE, nrows[i : i + size])] for i in range(0, n, size)]
    edge_chunks = lambda size: [
        [
            (EDGE_PUT_UPDATE, erows[i : i + size]),
            (
                _SET_EDGE_CREATED_AT,
                [{"key": r["key"], "now": now} for r in erows[i : i + size]],
            ),
        ]
        for i in range(0, len(erows), size)
    ]

    async def run(a, chunks, parallel):
        if parallel == 1:
            for specs in chunks:
                await a._write_batch(specs)
        else:
            sem = asyncio.Semaphore(parallel)

            async def one(specs):
                async with sem:
                    await a._write_batch(specs)

            await asyncio.gather(*(one(c) for c in chunks))

    for parallel in (1, concurrency):
        print(
            f"\n=== transaction-size sweep, {n:,} nodes / {len(erows):,} edges, "
            f"{'sequential' if parallel == 1 else f'{parallel} concurrent'} transactions ==="
        )
        for size in sizes:
            a = await fresh(address)
            await timed(f"nodes: {size}-row transactions", n, run(a, node_chunks(size), parallel))
            await timed(
                f"edges: {size}-row transactions", len(erows), run(a, edge_chunks(size), parallel)
            )
            await drop(a)


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--address", default="127.0.0.1:1729")
    p.add_argument("--rows", type=int, default=1000)
    p.add_argument("--sizes", default="50,100,250,500")
    p.add_argument("--concurrency", type=int, default=4)
    args = p.parse_args()
    try:
        await stages(args.address, args.rows)
        await tx_sweep(
            args.address, args.rows, [int(s) for s in args.sizes.split(",")], args.concurrency
        )
        print("\nDone.")
    finally:
        await drop_all_open()


if __name__ == "__main__":
    asyncio.run(main())
