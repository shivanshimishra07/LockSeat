#!/usr/bin/env python3
"""
Idempotent seed script.

  - 500 seats : A1 .. A500 (Resets status to 'available' on conflict)
  - 1,000 users: user_1@example.com .. user_1000@example.com

Run inside the container (DATABASE_URL is provided by compose):
    docker compose exec app python seed.py

Run from the host (use the published host port):
    DATABASE_URL=postgresql://postgres:postgrespassword@localhost:5433/reservation_db python seed.py
"""

import asyncio
import os
import sys

import asyncpg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgrespassword@db:5432/reservation_db",
)

SEAT_COUNT = 500
USER_COUNT = 1000


async def connect_with_retry(dsn, attempts=10, delay=2.0):
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return await asyncpg.connect(dsn=dsn, timeout=10)
        except (OSError, asyncpg.PostgresError, asyncio.TimeoutError) as exc:
            last_exc = exc
            print(f"[seed] DB not ready (attempt {attempt}/{attempts}): {exc}")
            await asyncio.sleep(delay)
    raise RuntimeError(f"Could not connect to database: {last_exc}")


async def main():
    print(f"[seed] Connecting to {DATABASE_URL.split('@')[-1]} ...")
    conn = await connect_with_retry(DATABASE_URL)
    try:
        seats = [(f"A{i}",) for i in range(1, SEAT_COUNT + 1)]
        users = [
            (f"User {i}", f"user_{i}@example.com") for i in range(1, USER_COUNT + 1)
        ]

        async with conn.transaction():
            print(f"[seed] Inserting/Resetting {len(seats)} seats (A1..A{SEAT_COUNT}) ...")
            # ON CONFLICT UPDATE resets held/reserved seats back to 'available'
            await conn.executemany(
                """
                INSERT INTO seats (seat_number, status)
                VALUES ($1, 'available')
                ON CONFLICT (seat_number) DO UPDATE
                SET status = 'available'
                """,
                seats,
            )

            print(f"[seed] Inserting {len(users)} users ...")
            await conn.executemany(
                """
                INSERT INTO users (name, email)
                VALUES ($1, $2)
                ON CONFLICT (email) DO NOTHING
                """,
                users,
            )

        seat_total = await conn.fetchval("SELECT COUNT(*) FROM seats")
        user_total = await conn.fetchval("SELECT COUNT(*) FROM users")
        print(f"[seed] Done. seats={seat_total}, users={user_total}")
    finally:
        await conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        print(f"[seed] FAILED: {exc}", file=sys.stderr)
        sys.exit(1)