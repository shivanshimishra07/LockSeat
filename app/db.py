import logging
import os

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

_db_pool = None
_redis_client = None


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


async def init_redis():
    """Create the global Redis client with a bounded connection pool (idempotent)."""
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.from_url(
            REDIS_URL,
            max_connections=REDIS_MAX_CONNECTIONS,
            decode_responses=True,
        )
        await _redis_client.ping()
        logger.info("Redis ready (max_connections=%d)", REDIS_MAX_CONNECTIONS)
    return _redis_client


async def close_redis():
    """Gracefully close the Redis client and its pool."""
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
        logger.info("Redis closed")


def get_redis():
    """Return the global Redis client; raises if called before startup."""
    if _redis_client is None:
        raise RuntimeError("Redis client is not initialised")
    return _redis_client

async def get_db_conn():
    """FastAPI dependency that yields an asyncpg database connection from the pool."""
    pool = get_db_pool()
    async with pool.acquire() as conn:
        yield conn