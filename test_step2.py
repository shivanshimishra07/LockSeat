#!/usr/bin/env python3
"""
Step 2 verification: Redis fast-path, idempotency replay, graceful fallback.

Usage:
    python test_step2.py                # Test 1 + Test 2, then prints Test 3 instructions
    python test_step2.py --reset        # wipe reservations + Redis first (re-runnable)
    python test_step2.py --fallback     # Test 3 automated check (run with Redis STOPPED)

Environment:
    BASE_URL                 default http://localhost:8002
    MAX_MEDIAN_REJECT_SECS   default 1.0   (median latency allowed for rejected requests)

Requires: pip install httpx
The --reset flag and the DB row-count check use `docker exec` on the containers
reservation_db / reservation_redis; they are skipped gracefully if Docker is unavailable.
"""

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import time
from collections import Counter

import httpx

BASE_URL = os.getenv("BASE_URL", "http://localhost:8002")
URL = f"{BASE_URL}/api/reservations"
NUM_REQUESTS = 50
MAX_MEDIAN_REJECT_SECS = float(os.getenv("MAX_MEDIAN_REJECT_SECS", "1.0"))

FASTPATH_DETAIL = "Seat lock held (Redis fast-path)"
DB_DETAIL = "Seat already taken or held"

DB_CONTAINER = "reservation_db"
REDIS_CONTAINER = "reservation_redis"

IDEM_KEY = "test-key-123"
IDEM_SEAT_ID = 3


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def run_cmd(args):
    """Run a shell command; return (ok, stdout). Never raises."""
    try:
        proc = subprocess.run(args, capture_output=True, text=True, timeout=20)
        return proc.returncode == 0, proc.stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        return False, str(exc)


def reset_state():
    print("Resetting state (TRUNCATE reservations, FLUSHDB redis) ...")
    ok_db, out_db = run_cmd(
        ["docker", "exec", DB_CONTAINER, "psql", "-U", "postgres",
         "-d", "reservation_db", "-c", "TRUNCATE reservations;"]
    )
    ok_rd, out_rd = run_cmd(["docker", "exec", REDIS_CONTAINER, "redis-cli", "FLUSHDB"])
    print(f"  postgres: {'ok' if ok_db else 'SKIPPED/FAILED'} {out_db if not ok_db else ''}")
    print(f"  redis:    {'ok' if ok_rd else 'SKIPPED/FAILED'} {out_rd if not ok_rd else ''}")


def count_reservations_for_key(key):
    """Row count for an idempotency key via docker exec, or None if unavailable."""
    ok, out = run_cmd(
        ["docker", "exec", DB_CONTAINER, "psql", "-U", "postgres",
         "-d", "reservation_db", "-tA", "-c",
         f"SELECT COUNT(*) FROM reservations WHERE idempotency_key = '{key}';"]
    )
    if not ok:
        return None
    try:
        return int(out.strip())
    except ValueError:
        return None


def detail_of(text):
    try:
        return json.loads(text).get("detail")
    except (ValueError, AttributeError):
        return None


async def warm_connections(client):
    """Open all keep-alive connections up front so the burst is truly simultaneous."""
    await asyncio.gather(*(client.get(f"{BASE_URL}/health") for _ in range(NUM_REQUESTS)))


async def reserve(client, seat_id, user_id, headers=None):
    start = time.perf_counter()
    try:
        resp = await client.post(
            URL, json={"user_id": user_id, "seat_id": seat_id}, headers=headers
        )
        return user_id, resp.status_code, resp.text, time.perf_counter() - start
    except httpx.HTTPError as exc:
        return user_id, -1, f"{type(exc).__name__}: {exc}", time.perf_counter() - start


async def burst(client, seat_id):
    await warm_connections(client)
    started = time.perf_counter()
    results = await asyncio.gather(
        *(reserve(client, seat_id, uid) for uid in range(1, NUM_REQUESTS + 1))
    )
    return results, time.perf_counter() - started


def new_client():
    limits = httpx.Limits(
        max_connections=NUM_REQUESTS, max_keepalive_connections=NUM_REQUESTS
    )
    return httpx.AsyncClient(timeout=30.0, limits=limits)


# --------------------------------------------------------------------------- #
# Test 1: fast-path speed and rejection
# --------------------------------------------------------------------------- #
async def test_fast_path(client):
    seat_id = 2
    print(f"\n=== Test 1: Fast-path rejection ({NUM_REQUESTS} concurrent, seat_id={seat_id}) ===")
    results, elapsed = await burst(client, seat_id)

    codes = Counter(code for _, code, _, _ in results)
    created = codes.get(201, 0)
    conflicts = codes.get(409, 0)
    print(f"Completed in {elapsed:.3f}s | status codes: {dict(sorted(codes.items()))}")

    if created != 1 or conflicts != NUM_REQUESTS - 1:
        print(f"FAIL: expected 1 x 201 and {NUM_REQUESTS - 1} x 409, "
              f"got {created} x 201 and {conflicts} x 409")
        for uid, code, body, _ in [r for r in results if r[1] not in (201, 409)][:5]:
            print(f"  unexpected -> user_id={uid} status={code} body={body[:200]}")
        if created == 0 and conflicts == NUM_REQUESTS:
            print("Hint: seat 2 is already held from a previous run. "
                  "Re-run with: python test_step2.py --reset")
        return False

    rejected = [r for r in results if r[1] == 409]
    fast_path = sum(1 for r in rejected if detail_of(r[2]) == FASTPATH_DETAIL)
    reject_latencies = [r[3] for r in rejected]
    median = statistics.median(reject_latencies)
    print(f"Rejected via Redis fast-path: {fast_path}/{len(rejected)}")
    print(f"Reject latency: median={median * 1000:.1f}ms "
          f"max={max(reject_latencies) * 1000:.1f}ms "
          f"(limit on median: {MAX_MEDIAN_REJECT_SECS * 1000:.0f}ms)")

    ok = True
    if fast_path != len(rejected):
        print(f"FAIL: expected every rejection to say '{FASTPATH_DETAIL}'. "
              "Is Redis running? (If Redis is stopped, use --fallback instead.)")
        ok = False
    if median > MAX_MEDIAN_REJECT_SECS:
        print("FAIL: rejected requests were not fast enough")
        ok = False

    if ok:
        print("PASS: exactly 1 x 201, 49 x 409, all rejected by Redis before Postgres")
    return ok


# --------------------------------------------------------------------------- #
# Test 2: idempotency replay
# --------------------------------------------------------------------------- #
async def test_idempotency(client):
    print(f"\n=== Test 2: Idempotency-Key replay (seat_id={IDEM_SEAT_ID}, key={IDEM_KEY}) ===")
    headers = {"Idempotency-Key": IDEM_KEY}

    _, code1, body1, _ = await reserve(client, IDEM_SEAT_ID, 1, headers)
    _, code2, body2, _ = await reserve(client, IDEM_SEAT_ID, 1, headers)
    print(f"First : {code1} {body1[:160]}")
    print(f"Second: {code2} {body2[:160]}")

    ok = True
    if code1 != 201:
        print("FAIL: first request should return 201. "
              "Stale state from a previous run? Re-run with --reset")
        return False
    if code2 != code1:
        print(f"FAIL: replay status {code2} != original status {code1}")
        ok = False
    try:
        if json.loads(body1) != json.loads(body2):
            print("FAIL: replay body differs from the original body")
            ok = False
    except ValueError:
        print("FAIL: response bodies are not valid JSON")
        ok = False

    # Same key + different payload must be rejected, not silently replayed.
    _, code3, body3, _ = await reserve(client, IDEM_SEAT_ID, 2, headers)
    print(f"Same key, different user: {code3} {body3[:120]}")
    if code3 != 422:
        print("FAIL: reusing a key with a different payload should return 422")
        ok = False

    # A different key for the same seat must still be refused (no duplicate booking).
    _, code4, _, _ = await reserve(client, IDEM_SEAT_ID, 2, {"Idempotency-Key": "test-key-other"})
    print(f"Different key, same seat: {code4}")
    if code4 != 409:
        print("FAIL: a different key for an already-held seat should return 409")
        ok = False

    rows = count_reservations_for_key(IDEM_KEY)
    if rows is None:
        print("Note: DB row count skipped (docker exec unavailable); "
              "identical reservation id in both bodies shows no second row was created.")
    else:
        print(f"Reservation rows with idempotency_key='{IDEM_KEY}': {rows}")
        if rows != 1:
            print("FAIL: expected exactly 1 row for the idempotency key")
            ok = False

    if ok:
        print("PASS: replay returned identical status and body, no duplicate created")
    return ok


# --------------------------------------------------------------------------- #
# Test 3: graceful Redis fallback
# --------------------------------------------------------------------------- #
def print_fallback_instructions():
    print("\n=== Test 3: Graceful Redis fallback (manual) ===")
    print("  1. docker stop reservation_redis")
    print("  2. python test_concurrency.py        # expect 1 x 201, 49 x 409 (served by Postgres)")
    print("     (run `python test_step2.py --reset` first if seat 1 is already held,")
    print("      then stop Redis again, since --reset flushes Redis)")
    print("  3. docker logs reservation_app --tail 20")
    print("     # expect: 'Redis connection failed. Falling back to Postgres DB-only mode'")
    print("  4. docker start reservation_redis")
    print("     # within ~5s the log shows 'Redis connection restored. Fast-path re-enabled'")
    print("  Automated variant while Redis is stopped:  python test_step2.py --fallback")


async def test_fallback(client):
    seat_id = 4
    # Create a fresh client with a 60s timeout to handle heavy DB-only concurrency
    async with httpx.AsyncClient(timeout=60.0) as fallback_client:
        print(f"\n=== Test 3 (automated): DB-only mode ({NUM_REQUESTS} concurrent, seat_id={seat_id}) ===")
        print("Run this with Redis stopped:  docker stop reservation_redis")
        results, elapsed = await burst(fallback_client, seat_id)

        codes = Counter(code for _, code, _, _ in results)
        created = codes.get(201, 0)
        conflicts = codes.get(409, 0)
        print(f"Completed in {elapsed:.3f}s | status codes: {dict(sorted(codes.items()))}")

        if created != 1 or conflicts != NUM_REQUESTS - 1:
            print(f"FAIL: expected 1 x 201 and {NUM_REQUESTS - 1} x 409, "
                  f"got {created} x 201 and {conflicts} x 409")
            for uid, code, body, _ in [r for r in results if r[1] not in (201, 409)][:5]:
                print(f"  unexpected -> user_id={uid} status={code} body={body[:200]}")
            if created == 0 and conflicts == NUM_REQUESTS:
                print("Hint: seat 4 is already held. Reset (then stop Redis again): "
                      "python test_step2.py --reset")
            return False

        db_rejections = sum(1 for r in results if r[1] == 409 and detail_of(r[2]) == DB_DETAIL)
        print(f"Rejections decided by Postgres: {db_rejections}/{conflicts}")
        if db_rejections != conflicts:
            print("FAIL: some rejections came from Redis, so Redis is still running. "
                  "Run `docker stop reservation_redis` and retry.")
            return False

        print("PASS: single-booking invariant held with Redis down (Postgres-only mode)")
        return True


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
async def main():
    parser = argparse.ArgumentParser(description="Step 2 verification")
    parser.add_argument("--reset", action="store_true",
                        help="TRUNCATE reservations and FLUSHDB Redis before running")
    parser.add_argument("--fallback", action="store_true",
                        help="run only the automated Redis-down check (Test 3)")
    args = parser.parse_args()

    if args.reset:
        reset_state()

    async with new_client() as client:
        try:
            (await client.get(f"{BASE_URL}/health")).raise_for_status()
        except httpx.HTTPError as exc:
            print(f"FAIL: API not reachable at {BASE_URL}: {exc}")
            return 1

        if args.fallback:
            return 0 if await test_fallback(client) else 1

        results = [await test_fast_path(client), await test_idempotency(client)]

    print_fallback_instructions()

    print("\n=== Summary ===")
    print(f"Test 1 (fast-path):   {'PASS' if results[0] else 'FAIL'}")
    print(f"Test 2 (idempotency): {'PASS' if results[1] else 'FAIL'}")
    print("Test 3 (fallback):    manual / use --fallback")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))