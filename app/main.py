"""
FastAPI entrypoint for the high-concurrency event reservation system.

Step 2: Redis lock fast-path + idempotency + graceful fallback to Postgres.

Invariant: Postgres (partial unique index on reservations(seat_id) for active
rows) is the source of truth for "one seat, one active reservation". Redis only
sheds load ahead of it. If Redis is down, behaviour is identical to Step 1.
"""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncIterator, Literal, Optional

import asyncpg
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.db import (
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

SEAT_LOCK_TTL_SECONDS = 300
IDEMPOTENCY_INFLIGHT_TTL_SECONDS = 30
IDEMPOTENCY_RESPONSE_TTL_SECONDS = 86400

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


# --------------------------------------------------------------------------- #
# Lifespan
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Manage DB and Redis pools for the lifetime of the app."""
    logger.info("Starting up: initialising connection pools")
    await init_db_pool()
    await init_redis()  # never raises if Redis is down
    try:
        yield
    finally:
        logger.info("Shutting down: closing connection pools")
        await close_redis()
        await close_db_pool()


app = FastAPI(
    title="Event Reservation System",
    version="0.2.0",
    lifespan=lifespan,
)


# --------------------------------------------------------------------------- #
# Redis helpers: every one of these swallows Redis failures and degrades
# --------------------------------------------------------------------------- #
async def acquire_seat_lock(seat_id: int, user_id: int) -> str:
    """SET seat:lock:{seat_id} {user_id} NX EX 300."""
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


# --------------------------------------------------------------------------- #
# Postgres helpers
# --------------------------------------------------------------------------- #
async def insert_hold(
    payload: ReservationRequest, idempotency_key: Optional[str]
) -> asyncpg.Record:
    """
    Insert a HELD reservation. A connection is only acquired here, i.e. AFTER the
    Redis fast-path has let the request through.

    Lazily expires stale HELD rows for the seat first, so an abandoned hold does
    not block the seat forever through the partial unique index.
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
                VALUES ($1, $2, 'HELD', NOW() + INTERVAL '5 minutes', $3)
                RETURNING id, user_id, seat_id, status, expires_at,
                          idempotency_key, created_at
                """,
                payload.user_id,
                payload.seat_id,
                idempotency_key,
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
                    # DB backstop replay: same key, same request. The reservation
                    # is real, so the seat lock is kept.
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
# Routes
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
        # 1. Completed before? Replay the cached response (checked first so a
        #    replay inside the 30s in-flight window is not mistaken for a duplicate).
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