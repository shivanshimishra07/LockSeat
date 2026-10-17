#!/usr/bin/env python3
"""
Step 3 verification: lifecycle (confirm/cancel), expiry sweeper, read APIs.

Usage:
    python test_step3.py            # run all 4 tests
    python test_step3.py --reset    # TRUNCATE reservations + FLUSHDB redis first

Prerequisites for Test 3 (sweeper): start the app with a short hold, e.g.
    HOLD_SECONDS=10 docker compose up -d
(or set HOLD_SECONDS=10 in .env and add it to the app service environment).

Environment:
    BASE_URL                  default http://localhost:8002
    SWEEPER_INTERVAL_SECONDS  default 10   (must match the app's setting)
    MAX_TEST_HOLD_SECONDS     default 30   (Test 3 fails fast if hold is longer)

The payment simulator is random (20% fail, 10% slow), so confirm calls are retried
on HTTP 402 exactly as a real client would.

Requires: pip install httpx
Redis lock checks use `docker exec reservation_redis`; skipped if Docker is unavailable.
"""

import argparse
import asyncio
import random
import subprocess
import sys
import time
import uuid
from datetime import datetime

import httpx

import os

BASE_URL = os.getenv("BASE_URL", "http://localhost:8002")
RES_URL = f"{BASE_URL}/api/reservations"
SWEEPER_INTERVAL = float(os.getenv("SWEEPER_INTERVAL_SECONDS", "10"))
MAX_TEST_HOLD = float(os.getenv("MAX_TEST_HOLD_SECONDS", "30"))

DB_CONTAINER = "reservation_db"
REDIS_CONTAINER = "reservation_redis"
USER_COUNT = 1000


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def run_cmd(args):
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
    print(f"  postgres: {'ok' if ok_db else 'SKIPPED/FAILED'} {'' if ok_db else out_db}")
    print(f"  redis:    {'ok' if ok_rd else 'SKIPPED/FAILED'} {'' if ok_rd else out_rd}")


def redis_lock_exists(seat_id):
    """True/False, or None when docker/redis-cli is unavailable."""
    ok, out = run_cmd(
        ["docker", "exec", REDIS_CONTAINER, "redis-cli", "EXISTS", f"seat:lock:{seat_id}"]
    )
    if not ok:
        return None
    return out.strip() == "1"


def pick_users(n):
    return random.sample(range(1, USER_COUNT + 1), n)


def parse_ts(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


async def reserve_any(client, user_id, attempts=10):
    """Reserve a random currently-available seat. Returns (seat_id, reservation_json)."""
    resp = await client.get(f"{BASE_URL}/api/seats", params={"status": "available"})
    resp.raise_for_status()
    seat_ids = [s["id"] for s in resp.json()]
    random.shuffle(seat_ids)
    for seat_id in seat_ids[:attempts]:
        r = await client.post(RES_URL, json={"user_id": user_id, "seat_id": seat_id})
        if r.status_code == 201:
            return seat_id, r.json()
    return None, None


async def get_reservation(client, rid):
    r = await client.get(f"{RES_URL}/{rid}")
    r.raise_for_status()
    return r.json()


async def confirm_with_retries(client, rid, max_attempts=10):
    """POST confirm, retrying on 402 (simulated payment failure). Returns (code, body, tries)."""
    code, body = None, None
    for attempt in range(1, max_attempts + 1):
        r = await client.post(f"{RES_URL}/{rid}/confirm")
        code, body = r.status_code, r.json()
        if code != 402:
            return code, body, attempt
    return code, body, max_attempts


def fail(msg):
    print(f"FAIL: {msg}")
    return False


# --------------------------------------------------------------------------- #
# Test 1: reserve -> confirm
# --------------------------------------------------------------------------- #
async def test_happy_path(client):
    print("\n=== Test 1: Happy path (Reserve -> Confirm) ===")
    (user,) = pick_users(1)

    confirmed_rid = None
    confirmed_seat = None
    for flow in range(1, 4):
        seat_id, res = await reserve_any(client, user)
        if res is None:
            return fail("could not reserve any available seat")
        rid = res["id"]
        print(f"Reserved seat_id={seat_id} reservation={rid} status={res['status']}")

        detail = await get_reservation(client, rid)
        if detail["status"] != "HELD" or detail["seconds_remaining"] <= 0:
            return fail(f"new reservation should be HELD with time remaining: {detail}")

        code, body, tries = await confirm_with_retries(client, rid)
        print(f"Confirm -> {code} {body} (payment attempts: {tries})")
        if code == 200 and body.get("status") == "CONFIRMED":
            confirmed_rid, confirmed_seat = rid, seat_id
            break
        if code == 409:
            print("  hold expired during slow/failed payments; retrying with a new seat")
            continue
        return fail(f"unexpected confirm response {code} {body}")

    if confirmed_rid is None:
        return fail("could not confirm within 3 flows (hold may be too short for retries)")

    detail = await get_reservation(client, confirmed_rid)
    if detail["status"] != "CONFIRMED":
        return fail(f"expected CONFIRMED, got {detail['status']}")

    reserved = await client.get(f"{BASE_URL}/api/seats", params={"status": "reserved"})
    if confirmed_seat not in {s["id"] for s in reserved.json()}:
        return fail("confirmed seat is not listed as 'reserved'")

    again = await client.post(f"{RES_URL}/{confirmed_rid}/confirm")
    if again.status_code != 200:
        return fail(f"re-confirming a CONFIRMED booking should be 200, got {again.status_code}")

    missing = await client.post(f"{RES_URL}/{uuid.uuid4()}/confirm")
    if missing.status_code != 404:
        return fail(f"confirming an unknown reservation should be 404, got {missing.status_code}")

    print("PASS: reserved, paid, CONFIRMED; seat listed as reserved; unknown id -> 404")
    return True


# --------------------------------------------------------------------------- #
# Test 2: reserve -> cancel -> re-reserve by another user
# --------------------------------------------------------------------------- #
async def test_cancel_lifecycle(client):
    print("\n=== Test 2: Cancel lifecycle (Reserve -> Cancel -> Re-reserve) ===")
    user_a, user_b = pick_users(2)

    seat_id, res = await reserve_any(client, user_a)
    if res is None:
        return fail("could not reserve any available seat")
    rid = res["id"]
    print(f"user {user_a} reserved seat_id={seat_id} reservation={rid}")

    blocked = await client.post(RES_URL, json={"user_id": user_b, "seat_id": seat_id})
    if blocked.status_code != 409:
        return fail(f"seat should be blocked while held, got {blocked.status_code}")

    cancel = await client.post(f"{RES_URL}/{rid}/cancel")
    print(f"Cancel -> {cancel.status_code} {cancel.text}")
    if cancel.status_code != 200 or cancel.json() != {"status": "CANCELLED"}:
        return fail("cancel should return 200 {'status': 'CANCELLED'}")

    detail = await get_reservation(client, rid)
    if detail["status"] != "CANCELLED":
        return fail(f"expected CANCELLED, got {detail['status']}")

    lock = redis_lock_exists(seat_id)
    if lock is True:
        return fail("Redis seat lock still present after cancel")
    print(f"Redis lock after cancel: {'absent' if lock is False else 'not checked (no docker)'}")

    rebook = await client.post(RES_URL, json={"user_id": user_b, "seat_id": seat_id})
    print(f"user {user_b} re-reserve -> {rebook.status_code}")
    if rebook.status_code != 201:
        return fail(f"seat should be immediately reservable again, got {rebook.status_code} {rebook.text}")

    second_cancel = await client.post(f"{RES_URL}/{rid}/cancel")
    if second_cancel.status_code != 409:
        return fail(f"cancelling twice should be 409, got {second_cancel.status_code}")

    await client.post(f"{RES_URL}/{rebook.json()['id']}/cancel")  # cleanup
    print("PASS: cancel freed the seat instantly for a different user")
    return True


# --------------------------------------------------------------------------- #
# Test 3: expiry sweeper
# --------------------------------------------------------------------------- #
async def test_sweeper(client):
    print("\n=== Test 3: Expiry sweeper ===")
    user_a, user_b = pick_users(2)

    seat_id, res = await reserve_any(client, user_a)
    if res is None:
        return fail("could not reserve any available seat")
    rid = res["id"]

    detail = await get_reservation(client, rid)
    hold = detail["seconds_remaining"]
    print(f"Reserved seat_id={seat_id} reservation={rid}; hold ~{hold}s")
    if hold > MAX_TEST_HOLD:
        await client.post(f"{RES_URL}/{rid}/cancel")
        return fail(
            f"hold is {hold}s. Restart the app with a short hold: "
            "HOLD_SECONDS=10 docker compose up -d"
        )

    deadline = time.monotonic() + hold + SWEEPER_INTERVAL + 8
    started = time.monotonic()
    status = detail["status"]
    while time.monotonic() < deadline:
        await asyncio.sleep(1.0)
        status = (await get_reservation(client, rid))["status"]
        if status == "EXPIRED":
            break
    waited = time.monotonic() - started
    print(f"Status after {waited:.1f}s: {status}")
    if status != "EXPIRED":
        return fail("sweeper did not mark the reservation EXPIRED in time")

    final = await get_reservation(client, rid)
    if final["seconds_remaining"] != 0:
        return fail(f"seconds_remaining should be 0, got {final['seconds_remaining']}")

    lock = redis_lock_exists(seat_id)
    if lock is True:
        return fail("Redis seat lock still present after expiry")
    print(f"Redis lock after expiry: {'absent' if lock is False else 'not checked (no docker)'}")

    rebook = await client.post(RES_URL, json={"user_id": user_b, "seat_id": seat_id})
    print(f"user {user_b} re-reserve expired seat -> {rebook.status_code}")
    if rebook.status_code != 201:
        return fail(f"expired seat should be reservable, got {rebook.status_code} {rebook.text}")
    await client.post(f"{RES_URL}/{rebook.json()['id']}/cancel")  # cleanup

    print("PASS: sweeper expired the hold, lock removed, seat bookable again")
    return True


# --------------------------------------------------------------------------- #
# Test 4: user bookings read API
# --------------------------------------------------------------------------- #
async def test_user_bookings(client):
    print("\n=== Test 4: GET /api/users/{id}/bookings ===")
    (user,) = pick_users(1)

    seat1, res1 = await reserve_any(client, user)
    if res1 is None:
        return fail("could not create first reservation")
    cancel = await client.post(f"{RES_URL}/{res1['id']}/cancel")
    if cancel.status_code != 200:
        return fail(f"cancel failed: {cancel.status_code}")

    await asyncio.sleep(0.05)  # guarantee distinct created_at ordering
    seat2, res2 = await reserve_any(client, user)
    if res2 is None:
        return fail("could not create second reservation")

    resp = await client.get(f"{BASE_URL}/api/users/{user}/bookings")
    if resp.status_code != 200:
        return fail(f"expected 200, got {resp.status_code} {resp.text}")
    bookings = resp.json()
    print(f"user {user} has {len(bookings)} booking(s)")

    if not isinstance(bookings, list) or not bookings:
        return fail("bookings should be a non-empty list")

    required = {"id", "user_id", "seat_id", "seat_number", "status",
                "expires_at", "seconds_remaining", "created_at"}
    for b in bookings:
        missing = required - set(b)
        if missing:
            return fail(f"booking is missing fields {sorted(missing)}: {b}")
        if b["user_id"] != user:
            return fail("response contains another user's booking")

    by_id = {b["id"]: b for b in bookings}
    if res1["id"] not in by_id or res2["id"] not in by_id:
        return fail("newly created reservations are missing from the history")
    if by_id[res1["id"]]["status"] != "CANCELLED":
        return fail("first reservation should be CANCELLED in history")
    if by_id[res2["id"]]["status"] != "HELD" or by_id[res2["id"]]["seconds_remaining"] <= 0:
        return fail("second reservation should be HELD with time remaining")

    stamps = [parse_ts(b["created_at"]) for b in bookings]
    if stamps != sorted(stamps, reverse=True):
        return fail("bookings are not ordered by created_at DESC")
    ids = [b["id"] for b in bookings]
    if ids.index(res2["id"]) > ids.index(res1["id"]):
        return fail("newest booking should come first")

    unknown = await client.get(f"{BASE_URL}/api/users/9999999/bookings")
    if unknown.status_code != 404:
        return fail(f"unknown user should be 404, got {unknown.status_code}")

    await client.post(f"{RES_URL}/{res2['id']}/cancel")  # cleanup
    print("PASS: history is complete, well-formed and ordered newest-first")
    return True


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
async def main():
    parser = argparse.ArgumentParser(description="Step 3 verification")
    parser.add_argument("--reset", action="store_true",
                        help="TRUNCATE reservations and FLUSHDB Redis before running")
    args = parser.parse_args()

    if args.reset:
        reset_state()

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            (await client.get(f"{BASE_URL}/health")).raise_for_status()
        except httpx.HTTPError as exc:
            print(f"FAIL: API not reachable at {BASE_URL}: {exc}")
            return 1

        results = {
            "Test 1 (happy path)": await test_happy_path(client),
            "Test 2 (cancel lifecycle)": await test_cancel_lifecycle(client),
            "Test 3 (expiry sweeper)": await test_sweeper(client),
            "Test 4 (user bookings)": await test_user_bookings(client),
        }

    print("\n=== Summary ===")
    for name, ok in results.items():
        print(f"{name:28s} {'PASS' if ok else 'FAIL'}")
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))