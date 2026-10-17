"""
FastAPI entrypoint for the high-concurrency event reservation system.

Step 1: Postgres partial unique index prevents double booking.
Step 2: Redis lock fast-path + idempotency + graceful fallback to Postgres.
Step 3: Payment simulator, confirm/cancel lifecycle, background expiry sweeper,
        read APIs.

Invariant: Postgres (partial unique index on reservations(seat_id) for HELD and
CONFIRMED rows) is the source of truth. Redis only sheds load ahead of it.
Database connections and locks are NEVER held while awaiting payment latency.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncIterator, Literal, Optional
from uuid import UUID

import asyncpg
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.db import (
    HOLD_SECONDS,
    SWEEPER_INTERVAL_SECONDS,
    TICKET_PRICE,
    close_db_pool,
    close_redis,
    get_db_conn,
    get_db_pool,
    get_redis_or_none,
    init_db_pool,
    init_redis,
    mark_redis_failure,
    mark_redis_success,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("app.main")

SeatStatus = Literal["available", "held", "reserved"]

# The seat lock mirrors the hold: it disappears when the hold expires.
SEAT_LOCK_TTL_SECONDS = max(1, HOLD_SECONDS)
# Once a booking is CONFIRMED the lock is refreshed to a long TTL; the DB
# partial index remains the real guard if Redis ever loses it.
CONFIRMED_LOCK_TTL_SECONDS = 86400
IDEMPOTENCY_INFLIGHT_TTL_SECONDS = 30
IDEMPOTENCY_RESPONSE_TTL_SECONDS = 86400
CONFIRM_GUARD_TTL_SECONDS = 30

LOCK_ACQUIRED = "acquired"
LOCK_HELD = "held"
LOCK_UNAVAILABLE = "unavailable"

IDEM_ACQUIRED = "acquired"
IDEM_IN_FLIGHT = "in_flight"
IDEM_UNAVAILABLE = "unavailable"

# Only delete the lock if we still own it (never free someone else's lock).
_RELEASE_LOCK_LUA = (
    "if redis.call('GET', KEYS[1]) == ARGV[1] then "
    "return redis.call('DEL', KEYS[1]) else return 0 end"
)

EXPIRED_DURING_PAYMENT_DETAIL = (
    "Reservation expired during payment process. Simulated refund initiated."
)


# --------------------------------------------------------------------------- #
# Schemas
# --------------------------------------------------------------------------- #
class SeatOut(BaseModel):
    id: int
    seat_number: str
    status: str
    created_at: datetime


class ReservationRequest(BaseModel):
    user_id: int = Field(..., gt=0)
    seat_id: int = Field(..., gt=0)


class ReservationDetail(BaseModel):
    id: UUID
    user_id: int
    seat_id: int
    seat_number: str
    status: str
    expires_at: Optional[datetime]
    seconds_remaining: int
    idempotency_key: Optional[str]
    created_at: datetime


# --------------------------------------------------------------------------- #
# Payment simulator (always called OUTSIDE any DB transaction / connection)
# --------------------------------------------------------------------------- #
async def simulate_payment(user_id: int, amount: float) -> str:
    """
    70%: SUCCESS (instant)
    20%: FAILED  (instant)
    10%: sleep 4-5s, then SUCCESS (slow gateway)
    """
    roll = random.random()
    if roll < 0.70:
        outcome = "SUCCESS"
    elif roll < 0.90:
        outcome = "FAILED"
    else:
        delay = random.uniform(4.0, 5.0)
        logger.info("Payment gateway slow: user_id=%s delay=%.2fs", user_id, delay)
        await asyncio.sleep(delay)
        outcome = "SUCCESS"
    logger.info("Payment result: user_id=%s amount=%.2f outcome=%s", user_id, amount, outcome)
    return outcome


# --------------------------------------------------------------------------- #
# Redis helpers: every one of these swallows Redis failures and degrades
# --------------------------------------------------------------------------- #
async def acquire_seat_lock(seat_id: int, user_id: int) -> str:
    """SET seat:lock:{seat_id} {user_id} NX EX <hold>."""
    client = get_redis_or_none()
    if client is None:
        return LOCK_UNAVAILABLE
    try:
        ok = await client.set(
            f"seat:lock:{seat_id}", str(user_id), nx=True, ex=SEAT_LOCK_TTL_SECONDS
        )
    except Exception as exc:  # includes redis.RedisError, timeouts, OSError
        mark_redis_failure("seat_lock_acquire", exc)
        return LOCK_UNAVAILABLE
    mark_redis_success()
    return LOCK_ACQUIRED if ok else LOCK_HELD


async def release_seat_lock(seat_id: int, user_id: int) -> None:
    """DEL seat:lock:{seat_id}, guarded so we only delete our own lock."""
    client = get_redis_or_none()
    if client is None:
        return
    try:
        await client.eval(_RELEASE_LOCK_LUA, 1, f"seat:lock:{seat_id}", str(user_id))
    except Exception as exc:
        mark_redis_failure("seat_lock_release", exc)


async def extend_seat_lock(seat_id: int, user_id: int) -> None:
    """After CONFIRMED: keep the fast-path lock for a long time (best effort)."""
    client = get_redis_or_none()
    if client is None:
        return
    try:
        await client.set(
            f"seat:lock:{seat_id}", str(user_id), ex=CONFIRMED_LOCK_TTL_SECONDS
        )
    except Exception as exc:
        mark_redis_failure("seat_lock_extend", exc)


async def idem_get_cached(key: str) -> Optional[dict[str, Any]]:
    client = get_redis_or_none()
    if client is None:
        return None
    try:
        raw = await client.get(f"idempotency:response:{key}")
    except Exception as exc:
        mark_redis_failure("idempotency_get_cached", exc)
        return None
    mark_redis_success()
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        logger.warning("Discarding corrupt idempotency cache entry for key=%s", key)
        return None


async def idem_begin(key: str) -> str:
    """SET idempotency:{key} PROCESSING NX EX 30."""
    client = get_redis_or_none()
    if client is None:
        return IDEM_UNAVAILABLE
    try:
        ok = await client.set(
            f"idempotency:{key}",
            "PROCESSING",
            nx=True,
            ex=IDEMPOTENCY_INFLIGHT_TTL_SECONDS,
        )
    except Exception as exc:
        mark_redis_failure("idempotency_begin", exc)
        return IDEM_UNAVAILABLE
    mark_redis_success()
    return IDEM_ACQUIRED if ok else IDEM_IN_FLIGHT


async def idem_store(
    key: str, status_code: int, body: dict[str, Any], fingerprint: str
) -> None:
    client = get_redis_or_none()
    if client is None:
        return
    payload = json.dumps(
        {"status_code": status_code, "body": body, "fingerprint": fingerprint}
    )
    try:
        await client.set(
            f"idempotency:response:{key}",
            payload,
            ex=IDEMPOTENCY_RESPONSE_TTL_SECONDS,
        )
    except Exception as exc:
        mark_redis_failure("idempotency_store", exc)


async def idem_release(key: str) -> None:
    client = get_redis_or_none()
    if client is None:
        return
    try:
        await client.delete(f"idempotency:{key}")
    except Exception as exc:
        mark_redis_failure("idempotency_release", exc)


async def confirm_guard_begin(reservation_id: UUID) -> str:
    """Best-effort guard so one reservation is not charged twice concurrently."""
    client = get_redis_or_none()
    if client is None:
        return IDEM_UNAVAILABLE
    try:
        ok = await client.set(
            f"confirm:inflight:{reservation_id}",
            "PROCESSING",
            nx=True,
            ex=CONFIRM_GUARD_TTL_SECONDS,
        )
    except Exception as exc:
        mark_redis_failure("confirm_guard_begin", exc)
        return IDEM_UNAVAILABLE
    mark_redis_success()
    return IDEM_ACQUIRED if ok else IDEM_IN_FLIGHT


async def confirm_guard_release(reservation_id: UUID) -> None:
    client = get_redis_or_none()
    if client is None:
        return
    try:
        await client.delete(f"confirm:inflight:{reservation_id}")
    except Exception as exc:
        mark_redis_failure("confirm_guard_release", exc)


# --------------------------------------------------------------------------- #
# Postgres helpers
# --------------------------------------------------------------------------- #
def affected_rows(command_tag: str) -> int:
    """Parse an asyncpg command tag such as 'UPDATE 1' into the row count."""
    try:
        return int(command_tag.split()[-1])
    except (ValueError, IndexError):
        return 0


async def insert_hold(
    payload: ReservationRequest, idempotency_key: Optional[str]
) -> asyncpg.Record:
    """
    Insert a HELD reservation. A connection is only acquired here, i.e. AFTER the
    Redis fast-path has let the request through.

    Lazily expires stale HELD rows for the seat first, so an abandoned hold does
    not block the seat through the partial unique index even before the sweeper runs.
    """
    pool = get_db_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE reservations
                SET status = 'EXPIRED'
                WHERE seat_id = $1
                  AND status = 'HELD'
                  AND expires_at <= NOW()
                """,
                payload.seat_id,
            )
            return await conn.fetchrow(
                """
                INSERT INTO reservations (user_id, seat_id, status, expires_at, idempotency_key)
                VALUES ($1, $2, 'HELD', NOW() + make_interval(secs => $4), $3)
                RETURNING id, user_id, seat_id, status, expires_at,
                          idempotency_key, created_at
                """,
                payload.user_id,
                payload.seat_id,
                idempotency_key,
                float(HOLD_SECONDS),
            )


async def fetch_reservation_by_key(idempotency_key: str) -> Optional[asyncpg.Record]:
    pool = get_db_pool()
    return await pool.fetchrow(
        """
        SELECT id, user_id, seat_id, status, expires_at, idempotency_key, created_at
        FROM reservations
        WHERE idempotency_key = $1
        """,
        idempotency_key,
    )


async def reserve_seat(
    payload: ReservationRequest, idempotency_key: Optional[str]
) -> dict[str, Any]:
    """Redis fast-path, then Postgres. Returns a JSON-safe reservation dict."""
    lock_state = await acquire_seat_lock(payload.seat_id, payload.user_id)

    if lock_state == LOCK_HELD:
        # Fast-path reject: no Postgres connection is touched.
        raise HTTPException(status_code=409, detail="Seat lock held (Redis fast-path)")

    lock_owned = lock_state == LOCK_ACQUIRED

    try:
        row = await insert_hold(payload, idempotency_key)

    except asyncpg.exceptions.UniqueViolationError:
        # Either the active-seat partial index or the idempotency_key constraint.
        if idempotency_key:
            existing = await fetch_reservation_by_key(idempotency_key)
            if existing is not None:
                if (
                    existing["user_id"] == payload.user_id
                    and existing["seat_id"] == payload.seat_id
                ):
                    # DB backstop replay: same key, same request.
                    return jsonable_encoder(dict(existing))
                if lock_owned:
                    await release_seat_lock(payload.seat_id, payload.user_id)
                raise HTTPException(
                    status_code=422,
                    detail="Idempotency-Key was already used with a different request",
                )
        if lock_owned:
            await release_seat_lock(payload.seat_id, payload.user_id)
        raise HTTPException(status_code=409, detail="Seat already taken or held")

    except asyncpg.exceptions.ForeignKeyViolationError:
        if lock_owned:
            await release_seat_lock(payload.seat_id, payload.user_id)
        raise HTTPException(status_code=404, detail="User or seat not found")

    except Exception:
        logger.exception(
            "Unexpected database error while reserving seat_id=%s user_id=%s",
            payload.seat_id,
            payload.user_id,
        )
        if lock_owned:
            await release_seat_lock(payload.seat_id, payload.user_id)
        raise HTTPException(status_code=503, detail="Database error, please retry")

    return jsonable_encoder(dict(row))


def replay_response(cached: dict[str, Any], fingerprint: str) -> JSONResponse:
    if cached.get("fingerprint") != fingerprint:
        raise HTTPException(
            status_code=422,
            detail="Idempotency-Key was already used with a different request",
        )
    return JSONResponse(
        status_code=int(cached["status_code"]),
        content=cached["body"],
        headers={"Idempotent-Replay": "true"},
    )


# --------------------------------------------------------------------------- #
# Background expiry sweeper
# --------------------------------------------------------------------------- #
async def expire_stale_holds() -> int:
    """Flip expired HELD rows to EXPIRED and drop their Redis seat locks."""
    pool = get_db_pool()
    rows = await pool.fetch(
        """
        UPDATE reservations
        SET status = 'EXPIRED'
        WHERE status = 'HELD' AND expires_at < NOW()
        RETURNING seat_id, user_id
        """
    )
    for row in rows:
        # Compare-and-delete: never remove a lock a newer holder has since taken.
        await release_seat_lock(row["seat_id"], row["user_id"])
    return len(rows)


async def sweeper_loop() -> None:
    logger.info(
        "Expiry sweeper started (interval=%.1fs, hold=%ds)",
        SWEEPER_INTERVAL_SECONDS,
        HOLD_SECONDS,
    )
    while True:
        try:
            expired = await expire_stale_holds()
            logger.info("Sweeper run complete: expired_reservations=%d", expired)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Sweeper run failed; will retry next cycle")
        await asyncio.sleep(SWEEPER_INTERVAL_SECONDS)


# --------------------------------------------------------------------------- #
# Lifespan
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Manage DB/Redis pools and the sweeper task for the lifetime of the app."""
    logger.info("Starting up: initialising connection pools")
    await init_db_pool()
    await init_redis()  # never raises if Redis is down
    sweeper_task = asyncio.create_task(sweeper_loop(), name="expiry-sweeper")
    try:
        yield
    finally:
        logger.info("Shutting down: stopping sweeper and closing connection pools")
        sweeper_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sweeper_task
        await close_redis()
        await close_db_pool()


app = FastAPI(
    title="Event Reservation System",
    version="0.3.0",
    lifespan=lifespan,
)


# --------------------------------------------------------------------------- #
# Routes: health and seats
# --------------------------------------------------------------------------- #
@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/seats", response_model=list[SeatOut])
async def list_seats(
    status: Optional[SeatStatus] = Query(
        default=None,
        description="Filter by effective seat status: available | held | reserved",
    ),
    conn: asyncpg.Connection = Depends(get_db_conn),
) -> list[dict]:
    """
    Seats with an effective status derived from the reservations table:
    reserved = CONFIRMED reservation, held = unexpired HELD reservation,
    otherwise available.
    """
    rows = await conn.fetch(
        """
        SELECT t.id, t.seat_number, t.status, t.created_at
        FROM (
            SELECT s.id,
                   s.seat_number,
                   s.created_at,
                   CASE
                       WHEN EXISTS (
                           SELECT 1 FROM reservations r
                           WHERE r.seat_id = s.id AND r.status = 'CONFIRMED'
                       ) THEN 'reserved'
                       WHEN EXISTS (
                           SELECT 1 FROM reservations r
                           WHERE r.seat_id = s.id
                             AND r.status = 'HELD'
                             AND r.expires_at > NOW()
                       ) THEN 'held'
                       ELSE 'available'
                   END AS status
            FROM seats s
        ) t
        WHERE ($1::text IS NULL OR t.status = $1)
        ORDER BY t.id
        """,
        status,
    )
    return [dict(r) for r in rows]


# --------------------------------------------------------------------------- #
# Routes: create reservation (Step 2 behaviour, hold duration now configurable)
# --------------------------------------------------------------------------- #
@app.post("/api/reservations", status_code=201)
async def create_reservation(
    payload: ReservationRequest,
    idempotency_key: Optional[str] = Header(
        default=None, alias="Idempotency-Key", max_length=255
    ),
):
    key = (idempotency_key or "").strip() or None
    fingerprint = f"{payload.user_id}:{payload.seat_id}"
    owns_inflight = False

    if key:
        # 1. Completed before? Replay the cached response.
        cached = await idem_get_cached(key)
        if cached is not None:
            return replay_response(cached, fingerprint)

        # 2. Is another request with this key running right now?
        state = await idem_begin(key)
        if state == IDEM_IN_FLIGHT:
            cached = await idem_get_cached(key)  # it may have just finished
            if cached is not None:
                return replay_response(cached, fingerprint)
            raise HTTPException(status_code=409, detail="Duplicate request in progress")
        owns_inflight = state == IDEM_ACQUIRED
        # IDEM_UNAVAILABLE: Redis is down; the DB unique constraint on
        # reservations.idempotency_key is the backstop.

    try:
        body = await reserve_seat(payload, key)
        if key:
            await idem_store(key, 201, body, fingerprint)
        return JSONResponse(status_code=201, content=body)
    finally:
        if key and owns_inflight:
            await idem_release(key)


# --------------------------------------------------------------------------- #
# Routes: confirm / cancel
# --------------------------------------------------------------------------- #
@app.post("/api/reservations/{reservation_id}/confirm")
async def confirm_reservation(reservation_id: UUID):
    pool = get_db_pool()

    # Short single-statement read: the connection returns to the pool immediately.
    row = await pool.fetchrow(
        """
        SELECT id, user_id, seat_id, status,
               COALESCE(expires_at > NOW(), FALSE) AS still_valid
        FROM reservations
        WHERE id = $1
        """,
        reservation_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Reservation not found")
    if row["status"] == "CONFIRMED":
        return {"status": "CONFIRMED", "message": "Booking already confirmed"}
    if row["status"] != "HELD":
        raise HTTPException(
            status_code=400,
            detail=f"Reservation is {row['status']} and cannot be confirmed",
        )
    if not row["still_valid"]:
        raise HTTPException(status_code=400, detail="Reservation hold has expired")

    guard = await confirm_guard_begin(reservation_id)
    if guard == IDEM_IN_FLIGHT:
        raise HTTPException(
            status_code=409, detail="Payment already in progress for this reservation"
        )

    try:
        # Payment runs OUTSIDE any DB connection, transaction or lock.
        outcome = await simulate_payment(row["user_id"], TICKET_PRICE)

        if outcome != "SUCCESS":
            raise HTTPException(
                status_code=402,
                detail="Payment failed. Reservation remains HELD for retry.",
            )

        try:
            tag = await pool.execute(
                """
                UPDATE reservations
                SET status = 'CONFIRMED'
                WHERE id = $1 AND status = 'HELD' AND expires_at > NOW()
                """,
                reservation_id,
            )
            updated = affected_rows(tag)
            current_status = None
            if updated == 0:
                current_status = await pool.fetchval(
                    "SELECT status FROM reservations WHERE id = $1", reservation_id
                )
        except asyncpg.PostgresError:
            logger.exception("Database error confirming reservation %s", reservation_id)
            raise HTTPException(status_code=503, detail="Database error, please retry")

        if updated == 1:
            await extend_seat_lock(row["seat_id"], row["user_id"])
            return {"status": "CONFIRMED", "message": "Booking successful"}

        if current_status == "CONFIRMED":
            # A concurrent confirm won the race; the outcome is the same.
            return {"status": "CONFIRMED", "message": "Booking successful"}
        if current_status == "CANCELLED":
            raise HTTPException(
                status_code=409,
                detail="Reservation was cancelled during payment process. "
                "Simulated refund initiated.",
            )
        raise HTTPException(status_code=409, detail=EXPIRED_DURING_PAYMENT_DETAIL)
    finally:
        if guard == IDEM_ACQUIRED:
            await confirm_guard_release(reservation_id)


@app.post("/api/reservations/{reservation_id}/cancel")
async def cancel_reservation(reservation_id: UUID):
    pool = get_db_pool()
    try:
        row = await pool.fetchrow(
            """
            UPDATE reservations
            SET status = 'CANCELLED'
            WHERE id = $1 AND status IN ('HELD', 'CONFIRMED')
            RETURNING seat_id, user_id
            """,
            reservation_id,
        )
        if row is None:
            current = await pool.fetchval(
                "SELECT status FROM reservations WHERE id = $1", reservation_id
            )
    except asyncpg.PostgresError:
        logger.exception("Database error cancelling reservation %s", reservation_id)
        raise HTTPException(status_code=503, detail="Database error, please retry")

    if row is None:
        if current is None:
            raise HTTPException(status_code=404, detail="Reservation not found")
        raise HTTPException(
            status_code=409,
            detail=f"Reservation is {current} and cannot be cancelled",
        )

    # The partial unique index no longer covers this row, so the seat is free in
    # Postgres already. Drop the Redis lock so the fast-path stops rejecting it.
    await release_seat_lock(row["seat_id"], row["user_id"])
    return {"status": "CANCELLED"}


# --------------------------------------------------------------------------- #
# Routes: read APIs
# --------------------------------------------------------------------------- #
_RESERVATION_SELECT = """
    SELECT r.id,
           r.user_id,
           r.seat_id,
           s.seat_number,
           r.status,
           r.expires_at,
           r.idempotency_key,
           r.created_at,
           CASE
               WHEN r.status = 'HELD' AND r.expires_at IS NOT NULL
               THEN GREATEST(0, CEIL(EXTRACT(EPOCH FROM (r.expires_at - NOW()))))::int
               ELSE 0
           END AS seconds_remaining
    FROM reservations r
    JOIN seats s ON s.id = r.seat_id
"""


@app.get("/api/reservations/{reservation_id}", response_model=ReservationDetail)
async def get_reservation(
    reservation_id: UUID, conn: asyncpg.Connection = Depends(get_db_conn)
):
    row = await conn.fetchrow(_RESERVATION_SELECT + " WHERE r.id = $1", reservation_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Reservation not found")
    return dict(row)


@app.get("/api/users/{user_id}/bookings", response_model=list[ReservationDetail])
async def get_user_bookings(
    user_id: int, conn: asyncpg.Connection = Depends(get_db_conn)
):
    exists = await conn.fetchval("SELECT 1 FROM users WHERE id = $1", user_id)
    if exists is None:
        raise HTTPException(status_code=404, detail="User not found")
    rows = await conn.fetch(
        _RESERVATION_SELECT + " WHERE r.user_id = $1 ORDER BY r.created_at DESC",
        user_id,
    )
    return [dict(r) for r in rows]