#!/usr/bin/env python3
"""
Race-condition test: 50 users try to reserve seat_id=1 at the same instant.
Expected: exactly 1 x HTTP 201 and 49 x HTTP 409.

Requires:  pip install httpx
Usage:     python testconcurrency.py
Override:  BASE_URL=http://localhost:8002 python testconcurrency.py
"""

import asyncio
import os
import sys
import time
from collections import Counter

import httpx

BASE_URL = os.getenv("BASE_URL", "http://localhost:8002")
URL = f"{BASE_URL}/api/reservations"
SEAT_ID = 1
NUM_REQUESTS = 50


async def reserve(client, user_id):
    try:
        resp = await client.post(URL, json={"user_id": user_id, "seat_id": SEAT_ID})
        return user_id, resp.status_code, resp.text
    except httpx.HTTPError as exc:
        return user_id, -1, f"{type(exc).__name__}: {exc}"


async def main():
    limits = httpx.Limits(
        max_connections=NUM_REQUESTS, max_keepalive_connections=NUM_REQUESTS
    )
    async with httpx.AsyncClient(timeout=30.0, limits=limits) as client:
        # Fail early with a clear message if the API is not up.
        try:
            health = await client.get(f"{BASE_URL}/health")
            health.raise_for_status()
        except httpx.HTTPError as exc:
            print(f"FAIL: API not reachable at {BASE_URL}: {exc}")
            return 1

        print(
            f"Firing {NUM_REQUESTS} concurrent requests at seat_id={SEAT_ID} "
            f"(user_ids 1..{NUM_REQUESTS}) ..."
        )
        start = time.perf_counter()
        results = await asyncio.gather(
            *(reserve(client, uid) for uid in range(1, NUM_REQUESTS + 1))
        )
        elapsed = time.perf_counter() - start

    codes = Counter(code for _, code, _ in results)
    winners = [uid for uid, code, _ in results if code == 201]

    print(f"Completed in {elapsed:.3f}s")
    print(f"Status code distribution: {dict(sorted(codes.items()))}")

    created = codes.get(201, 0)
    conflicts = codes.get(409, 0)

    if created != 1 or conflicts != NUM_REQUESTS - 1:
        print("\nFAIL")
        print(f"  expected: 1 x 201 and {NUM_REQUESTS - 1} x 409")
        print(f"  actual:   {created} x 201 and {conflicts} x 409")
        others = [r for r in results if r[1] not in (201, 409)]
        for uid, code, body in others[:5]:
            print(f"  unexpected -> user_id={uid} status={code} body={body[:200]}")
        if created == 0 and conflicts == NUM_REQUESTS:
            print(
                "\nHint: seat 1 already has an active reservation from a previous run. "
                "Reset with:\n"
                "  docker compose exec app python seed.py"
            )
        return 1

    print("\nPASS: double-booking prevented by the database")
    print(f"  1 x 201 Created   (winner: user_id={winners[0]})")
    print(f"  {conflicts} x 409 Conflict")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))