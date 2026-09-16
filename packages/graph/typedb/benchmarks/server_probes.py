"""Reproducible probes behind the server observations in README.md.

  python -u benchmarks/server_probes.py prefix-scan     # string lookups vs shared prefix
  python -u benchmarks/server_probes.py shared-value    # STC2 conflicts vs shared value length

Both use a throwaway schema and drop their databases. `--address` and
`--sizes` as in the other scripts.
"""

import argparse
import asyncio
import json
import random
import time
import uuid

from typedb.driver import Credentials, DriverOptions, DriverTlsConfig, TransactionType, TypeDB

from cognee_community_graph_adapter_typedb import TypeDBAdapter
from cognee_community_graph_adapter_typedb.queries import _NODE_UPSERT, _now_ms

PROBE_SCHEMA = "define attribute k value string; entity thing, owns k @key;"


def _driver(address):
    return TypeDB.driver(
        address, Credentials("admin", "password"), DriverOptions(DriverTlsConfig.disabled())
    )


def prefix_scan(address: str, sizes: list[int]) -> None:
    """200 key lookups per value shape; random values seek, shared prefixes scan."""
    driver = _driver(address)
    rng = random.Random(3)

    def rand(n):
        return "".join(rng.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=n))

    shapes = {
        "sequential uuid (UUID(int=i))": lambda i: str(uuid.UUID(int=i + 1)),
        "random uuid4": lambda i: str(uuid.uuid4()),
        "12 random chars": lambda i: rand(12),
        "12 chars, 8-char shared prefix": lambda i: "prefix00" + rand(4),
        "36 chars, 8-char shared prefix": lambda i: "shared-p" + rand(28),
        "36 chars, 20-char shared prefix": lambda i: "shared-prefix-000000" + rand(16),
    }
    for size in sizes:
        print(f"\n=== {size:,} values, 200 lookups each ===")
        for label, gen in shapes.items():
            name = f"cognee_bench_{uuid.uuid4().hex[:8]}"
            driver.databases.create(name)
            try:
                with driver.transaction(name, TransactionType.SCHEMA) as tx:
                    tx.query(PROBE_SCHEMA).resolve()
                    tx.commit()
                values = list(dict.fromkeys(gen(i) for i in range(size)))
                for start in range(0, len(values), 1000):
                    with driver.transaction(name, TransactionType.WRITE) as tx:
                        tx.query(
                            "given $v: string;\ninsert $t isa thing, has k == $v;",
                            given_rows=[{"v": v} for v in values[start : start + 1000]],
                        ).resolve()
                        tx.commit()
                sample = random.Random(1).sample(values, 200)
                with driver.transaction(name, TransactionType.READ) as tx:
                    started = time.perf_counter()
                    rows = list(
                        tx.query(
                            "given $v: string;\nmatch $t isa thing, has k == $v;\nselect $t;",
                            given_rows=[{"v": v} for v in sample],
                        ).resolve()
                    )
                    elapsed = time.perf_counter() - started
                print(f"  {label:<36} {len(rows) / elapsed:8.0f} lookups/s")
            finally:
                driver.databases.get(name).delete()
    driver.close()


async def shared_value(address: str, rows_n: int = 1000, chunk: int = 50) -> None:
    """20 concurrent 50-row transactions whose rows all own one `name` value."""
    for length in (16, 24, 32, 64, 600):
        shared = "s" * length
        name = f"cognee_bench_{uuid.uuid4().hex[:8]}"
        adapter = TypeDBAdapter(graph_database_url=address, database_name=name)
        await adapter._provision_database()
        now = _now_ms()
        rows = [
            {
                "id": str(uuid.uuid4()),
                "type": "T",
                "name": shared,
                "props": json.dumps({"i": i}),
                "created": now,
                "now": now,
            }
            for i in range(rows_n)
        ]
        chunks = [rows[i : i + chunk] for i in range(0, rows_n, chunk)]
        retries = 0
        original = adapter._backoff

        async def counting_backoff(attempt, original=original):
            nonlocal retries
            retries += 1
            await original(attempt)

        adapter._backoff = counting_backoff
        semaphore = asyncio.Semaphore(4)

        async def one(rows_chunk, adapter=adapter, semaphore=semaphore):
            async with semaphore:
                await adapter._write_batch([(_NODE_UPSERT, rows_chunk)])

        try:
            await asyncio.gather(*(one(c) for c in chunks))
            print(
                f"  shared name of {length:>3} chars: {retries:3d} STC2 retries / {len(chunks)} chunks"
            )
        finally:
            adapter._get_driver().databases.get(name).delete()
            await adapter.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("probe", choices=["prefix-scan", "shared-value"])
    parser.add_argument("--address", default="127.0.0.1:1729")
    parser.add_argument("--sizes", default="5000,20000")
    args = parser.parse_args()
    if args.probe == "prefix-scan":
        prefix_scan(args.address, [int(s) for s in args.sizes.split(",")])
    else:
        print("\n=== 1,000 rows, 20 concurrent 50-row transactions ===")
        await shared_value(args.address)
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
