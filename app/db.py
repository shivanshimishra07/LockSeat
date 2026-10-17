import logging
import os
import time

import asyncpg
import redis.asyncio as aioredis

logger = logging.getLogger("app.db")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgrespassword@db:5432/reservation_db",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

DB_POOL_MIN_SIZE = int(os.getenv("DB_POOL_MIN_SIZE", "10"))
DB_POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE", "50"))
DB_COMMAND_TIMEOUT = float(os.getenv("DB_COMMAND_TIMEOUT", "30"))
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "100"))

# Redis is an optimisation, so it must never stall a request for long.
REDIS_SOCKET_TIMEOUT = float(os.getenv("REDIS_SOCKET_TIMEOUT", "0.1"))
# After a Redis failure, skip Redis entirely for this many seconds (circuit breaker).
REDIS_COOLDOWN_SECONDS = float(os.getenv("REDIS_COOLDOWN_SECONDS", "5"))

# --- Step 3 lifecycle configuration ---------------------------------------- #
# How long a HELD reservation lasts before it expires (set to 10 for testing).
HOLD_SECONDS = int(os.getenv("HOLD_SECONDS", "300"))
# How often the background sweeper runs.
SWEEPER_INTERVAL_SECONDS = float(os.getenv("SWEEPER_INTERVAL_SECONDS", "10"))
# Simulated ticket price passed to the payment simulator.
TICKET_PRICE = float(os.getenv("TICKET_PRICE", "49.99"))

_db_pool = None
_redis_client = None
_redis_down_until = 0.0  # time.monotonic() deadline; 0 means "healthy"


# --------------------------------------------------------------------------- #
# PostgreSQL
# --------------------------------------------------------------------------- #
async def init_db_pool():
    """Create the global asyncpg pool (idempotent)."""
    global _db_pool
    if _db_pool is None:
        _db_pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=DB_POOL_MIN_SIZE,
            max_size=DB_POOL_MAX_SIZE,
            command_timeout=DB_COMMAND_TIMEOUT,
        )
        async with _db_pool.acquire() as conn:
            await conn.execute("SELECT 1")
        logger.info(
            "PostgreSQL pool ready (min=%d, max=%d)",
            DB_POOL_MIN_SIZE,
            DB_POOL_MAX_SIZE,
        )
    return _db_pool


async def close_db_pool():
    """Gracefully close the asyncpg pool."""
    global _db_pool
    if _db_pool is not None:
        await _db_pool.close()
        _db_pool = None
        logger.info("PostgreSQL pool closed")


def get_db_pool():
    """Return the global pool; raises if called before startup."""
    if _db_pool is None:
        raise RuntimeError("Database pool is not initialised")
    return _db_pool


async def get_db_conn():
    """FastAPI dependency that yields an asyncpg database connection from the pool."""
    pool = get_db_pool()
    async with pool.acquire() as conn:
        yield conn


# --------------------------------------------------------------------------- #
# Redis (optional dependency: failures degrade to Postgres-only mode)
# --------------------------------------------------------------------------- #
async def init_redis():
    """
    Create the global Redis client (idempotent).

    Does NOT raise if Redis is unreachable at startup: the app must be able to
    boot and serve traffic in Postgres-only mode. redis-py connects lazily, so
    the client starts working as soon as Redis comes back.
    """
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.from_url(
            REDIS_URL,
            max_connections=REDIS_MAX_CONNECTIONS,
            decode_responses=True,
            socket_timeout=REDIS_SOCKET_TIMEOUT,
            socket_connect_timeout=REDIS_SOCKET_TIMEOUT,
            health_check_interval=15,
        )
        try:
            await _redis_client.ping()
            logger.info("Redis ready (max_connections=%d)", REDIS_MAX_CONNECTIONS)
        except Exception as exc:
            mark_redis_failure("startup_ping", exc)
    return _redis_client


async def close_redis():
    """Gracefully close the Redis client and its pool."""
    global _redis_client
    if _redis_client is not None:
        try:
            await _redis_client.aclose()
        except Exception as exc:
            logger.warning("Error while closing Redis: %s", exc)
        _redis_client = None
        logger.info("Redis closed")


def get_redis():
    """Return the global Redis client; raises if called before startup."""
    if _redis_client is None:
        raise RuntimeError("Redis client is not initialised")
    return _redis_client


def get_redis_or_none():
    """
    Return the Redis client, or None when Redis should be bypassed
    (not initialised, or inside the post-failure cooldown window).
    """
    if _redis_client is None:
        return None
    if _redis_down_until and time.monotonic() < _redis_down_until:
        return None
    return _redis_client


def mark_redis_failure(operation, exc):
    """Record a Redis failure, open the cooldown window and log a structured warning."""
    global _redis_down_until
    _redis_down_until = time.monotonic() + REDIS_COOLDOWN_SECONDS
    logger.warning(
        "Redis connection failed. Falling back to Postgres DB-only mode "
        "(operation=%s error_type=%s error=%s cooldown_s=%.1f)",
        operation,
        type(exc).__name__,
        exc,
        REDIS_COOLDOWN_SECONDS,
    )


def mark_redis_success():
    """Record a successful Redis call; closes the cooldown window if it was open."""
    global _redis_down_until
    if _redis_down_until:
        _redis_down_until = 0.0
        logger.info("Redis connection restored. Fast-path re-enabled")