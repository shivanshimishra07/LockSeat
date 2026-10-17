"""
FastAPI entrypoint for the high-concurrency event reservation system.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncIterator, Literal

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel

from app.db import (
    close_db_pool,
    close_redis,
    get_db_conn,
    init_db_pool,
    init_redis,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("app.main")

SeatStatus = Literal["available", "held", "reserved"]


class SeatOut(BaseModel):
    id: int
    seat_number: str
    status: str
    created_at: datetime


class ReservationRequest(BaseModel):
    user_id: int
    seat_id: int


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Manage DB and Redis pools for the lifetime of the app."""
    logger.info("Starting up: initialising connection pools")
    await init_db_pool()
    await init_redis()
    try:
        yield
    finally:
        logger.info("Shutting down: closing connection pools")
        await close_redis()
        await close_db_pool()


app = FastAPI(
    title="Event Reservation System",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/seats", response_model=list[SeatOut])
async def list_seats(
    status: SeatStatus | None = Query(
        default=None,
        description="Filter by seat status: available | held | reserved",
    ),
    conn: asyncpg.Connection = Depends(get_db_conn),
) -> list[dict]:
    """Return seats, optionally filtered by status. Ordered by id (A1..A50, B1..B50, ...)."""
    if status is None:
        rows = await conn.fetch(
            "SELECT id, seat_number, status, created_at FROM seats ORDER BY id"
        )
    else:
        rows = await conn.fetch(
            """
            SELECT id, seat_number, status, created_at
            FROM seats
            WHERE status = $1
            ORDER BY id
            """,
            status,
        )
    return [dict(r) for r in rows]


@app.post("/api/reservations", status_code=status.HTTP_201_CREATED)
async def create_reservation(
    payload: ReservationRequest,
    conn: asyncpg.Connection = Depends(get_db_conn),
):
    """Atomically reserve a seat for a user using an UPDATE row lock."""
    row = await conn.fetchrow(
        """
        UPDATE seats
        SET status = 'held'
        WHERE id = $1 AND status = 'available'
        RETURNING id, seat_number, status
        """,
        payload.seat_id,
    )

    if not row:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Seat is already held or reserved",
        )

    return {"message": "Seat held successfully", "reservation": dict(row)}