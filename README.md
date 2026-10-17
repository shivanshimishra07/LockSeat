# The Last Seat

A high-concurrency event seat reservation backend. Many users can race for the same seat at the same moment, and at most one holds it. A partial unique index in Postgres enforces that, and Redis sheds most of the losing traffic before it reaches the database.

## Architecture

```
Client ──► FastAPI (Uvicorn, 4 workers)
              │
              ├─► Redis: seat lock (SET NX EX), idempotency cache   ← sheds losing requests
              │        │ unavailable? skipped, never a hard dependency
              ▼        ▼
           PostgreSQL: reservations
           UNIQUE (seat_id) WHERE status IN ('HELD','CONFIRMED')     ← source of truth
```

1. **Redis fast-path.** `SET seat:lock:{seat_id} {user_id} NX EX <hold>`. If the key exists, the request gets a 409 without touching Postgres.
2. **Postgres partial unique index.** At most one `HELD` or `CONFIRMED` reservation per seat. This is what prevents double booking, even when Redis is down, flushed or inconsistent.

Redis is an optimization. If it fails, the API logs a warning, skips Redis for a short cooldown and serves the request from Postgres.

### Lifecycle

```
POST /api/reservations ─► HELD ──confirm (payment OK)──► CONFIRMED
                            │                               │
                            ├─ expires_at passes ─► EXPIRED  │
                            └─ cancel ──────────► CANCELLED ◄┘ (cancel)
```

`EXPIRED` and `CANCELLED` rows leave the partial index, which frees the seat.

## Tech Stack

| Component | Technology |
|---|---|
| API | Python 3.11, FastAPI, Uvicorn (4 workers) |
| DB driver | AsyncPG (pool per worker) |
| Database | PostgreSQL 15 (`max_connections=300`) |
| Cache / locks | Redis 7 (redis-py asyncio) |
| Orchestration | Docker Compose |
| Load testing | Grafana k6 |

## Features

- Reserve, confirm, cancel, view seats, view one reservation, view a user's bookings.
- Two-tier concurrency control (Redis lock plus Postgres partial unique index).
- Auto-expiring holds via a background sweeper in each worker (default every 10 s). Stale holds are also cleared inside the reserve transaction.
- Payment simulation: 70% instant success, 20% instant failure, 10% success after 4 to 5 s. Payment runs outside any DB transaction or connection. A failed payment leaves the reservation `HELD` so the user can retry.
- Idempotency keys on `POST /api/reservations`: the first request runs and its response is cached for 24 h; a concurrent duplicate gets `409 Duplicate request in progress`; a replay returns the cached response with `Idempotent-Replay: true`; the same key with a different payload returns `422`. `reservations.idempotency_key` is `UNIQUE` as a backstop when Redis is down.

## Setup

**Prerequisites:** Docker with Compose v2. Optional: Python 3.11+ with `httpx` for the test scripts, and k6 for load tests.

### 1. Environment

`docker-compose.yml` reads host ports from `.env` and has no defaults for them, so the file must exist:

```bash
cp .env.example .env
```

Typical values:

```bash
DB_HOST_PORT=5433
REDIS_HOST_PORT=6379
APP_HOST_PORT=8002
HOLD_SECONDS=300             # use 10 for the sweeper test
SWEEPER_INTERVAL_SECONDS=10
```

### 2. Start

```bash
docker compose up --build -d
docker compose ps      # db and redis should be "healthy"
```

`schema.sql` runs only on the first boot of an empty volume. After editing it, run `docker compose down -v` first.

### 3. Seed

```bash
docker compose exec app python seed.py
```

Inserts 500 seats (`A1` to `A500`) and 1,000 users (`user_1@example.com` to `user_1000@example.com`). It is safe to run repeatedly.

### 4. Check

```bash
curl http://localhost:8002/health        # {"status":"ok"}
curl -s "http://localhost:8002/api/seats?status=available" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))"   # 500 on a fresh database
```

### Resetting state between test runs

`seed.py` does **not** release held seats. Availability comes from the `reservations` table, so clear it and the Redis locks:

```bash
docker exec reservation_db psql -U postgres -d reservation_db -c "TRUNCATE reservations;"
docker exec reservation_redis redis-cli FLUSHDB
```

### Configuration

| Variable | Default | Meaning |
|---|---|---|
| `HOLD_SECONDS` | `300` | Lifetime of a `HELD` reservation |
| `SWEEPER_INTERVAL_SECONDS` | `10` | Sweeper period |
| `DB_POOL_MIN_SIZE` / `DB_POOL_MAX_SIZE` | `10` / `40` in compose | AsyncPG pool size per worker |
| `REDIS_SOCKET_TIMEOUT` | `0.1` | Seconds before a Redis call is abandoned |
| `REDIS_COOLDOWN_SECONDS` | `5` | How long Redis is skipped after a failure |
| `TICKET_PRICE` | `49.99` | Amount passed to the payment simulator |

## API

Base URL `http://localhost:8002`. Interactive docs at `/docs`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness |
| `GET` | `/api/seats?status=` | `available`, `held` or `reserved` (optional filter) |
| `POST` | `/api/reservations` | Hold a seat; optional `Idempotency-Key` header |
| `POST` | `/api/reservations/{id}/confirm` | Pay and confirm |
| `POST` | `/api/reservations/{id}/cancel` | Cancel a `HELD` or `CONFIRMED` reservation |
| `GET` | `/api/reservations/{id}` | Details with `seconds_remaining` |
| `GET` | `/api/users/{user_id}/bookings` | User history, newest first |

### Reserve

```bash
curl -i -X POST http://localhost:8002/api/reservations \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-key-001" \
  -d '{"user_id": 1, "seat_id": 10}'
```

`201` on success; `404` unknown user or seat; `409` seat taken or duplicate in progress; `422` invalid body or key reused with a different payload; `503` database error (retry).

### Confirm

```bash
curl -i -X POST http://localhost:8002/api/reservations/<reservation_id>/confirm
```

`200` `{"status":"CONFIRMED","message":"Booking successful"}`; `402` payment failed (reservation stays `HELD`, retry); `400` expired or not `HELD`; `404` unknown; `409` expired or cancelled during payment, or payment already in progress.

### Cancel

```bash
curl -i -X POST http://localhost:8002/api/reservations/<reservation_id>/cancel
# {"status":"CANCELLED"}
```

`404` unknown id; `409` if already `CANCELLED` or `EXPIRED`.

### List seats and read endpoints

```bash
curl -s "http://localhost:8002/api/seats?status=available"
curl -s http://localhost:8002/api/reservations/<reservation_id>
curl -s http://localhost:8002/api/users/1/bookings
```

## Concurrency and Scaling

**Redis lock shedding.** The first request on a seat wins `SET NX` and goes on to Postgres. The others get `409 Seat lock held (Redis fast-path)` after one Redis round trip, with no pooled DB connection used. The lock TTL equals `HOLD_SECONDS`; release is a compare-and-delete script so a request frees only its own lock.

**Partial unique index.** Whatever passes Redis is settled by `uq_reservations_active_seat`. Concurrent INSERTs for one seat race on the index; one commits and the rest raise `UniqueViolationError`, returned as 409. This holds across workers and when Redis is down.

**Connection pooling.** Each worker has its own pool (10 to 40 connections). With 4 workers that is at most 160 connections against Postgres `max_connections=300`, leaving room for `psql`, the seed script and the sweepers. Payment never holds a connection, and the reserve path acquires one only after Redis lets the request through.

**Multi-worker.** All shared state is in Postgres and Redis, so workers need no coordination. Each runs its own sweeper; this is redundant but safe, because `UPDATE ... RETURNING` hands each expired row to one worker. If you change the worker count, change pool size and `max_connections` together.

## Testing

```bash
pip install httpx
python testconcurrency.py        # 50 concurrent requests on one seat
python test_step2.py --reset     # fast-path and idempotency
python test_step3.py --reset     # confirm, cancel, sweeper, bookings (needs HOLD_SECONDS=10)
```

Redis fallback: `docker stop reservation_redis`, then `python test_step2.py --fallback`, then `docker start reservation_redis`.

## Load Testing

`loadtest/k6_script.js` runs three scenarios back to back: a 500-VU spike on one seat, a 10-VU idempotency-key stampede, and a 2,000-VU, 30 s mixed workload (80% reserve, 10% confirm, 10% list).

```bash
ulimit -n 65535
docker exec reservation_db psql -U postgres -d reservation_db -c "TRUNCATE reservations;"
docker exec reservation_redis redis-cli FLUSHDB

k6 run -e BASE_URL=http://localhost:8002 loadtest/k6_script.js

sleep 15
docker exec reservation_db psql -U postgres -d reservation_db -v ON_ERROR_STOP=1 -f /loadtest/verify.sql
```

`./loadtest` is mounted read-only at `/loadtest` in the db container, so `verify.sql` can run in place. It exits non-zero if any invariant is violated.

### Recorded run (single machine, Docker Desktop, 4 vCPU / 8 GB)

| Metric | Value |
|---|---|
| Duration | 86.8 s |
| Iterations | 30,071 (30,096 HTTP requests) |
| Peak VUs | 2,000 |
| Throughput | about 347 req/s averaged over the run |
| 5xx / network errors | **0** |
| Latency, all requests (p95 / p99) | 7.2 s / 11.7 s |
| Latency, reserve (p95) | 7.2 s |
| Latency, list seats (p95) | 7.5 s |

**Caveats on this run.**
- It produced no successful reservations, so the single-winner assertions were not met (0 x 201 in the spike; idempotency stampede unresolved). This points to seats left held by an earlier run rather than a logic fault, but the 351 spike responses and 16,228 reserve responses that were neither 201, 409 nor 5xx have not been classified.
- The latency gates (reserve p95 < 1 s, list p95 < 2 s) failed. k6, the app, Postgres and Redis shared one VM.
- `verify.sql` has not been run against this data, so the zero-double-booking result is **not yet demonstrated by the load test**. The functional scripts above do show it for 50-way bursts, with and without Redis.

A clean re-run (reset, run k6 from a separate machine, run `verify.sql`) is the next step. `loadtest/RESULTS.md` is the template for recording it.

## Project Structure

```
.
├── app/
│   ├── __init__.py
│   ├── db.py                # pools, Redis client, circuit breaker, config
│   └── main.py              # API, Redis helpers, payment simulator, sweeper
├── loadtest/
│   ├── k6_script.js
│   ├── verify.sql
│   └── RESULTS.md
├── schema.sql
├── seed.py
├── testconcurrency.py
├── test_step2.py
├── test_step3.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── DECISIONS.md
└── README.md
```

See [DECISIONS.md](DECISIONS.md) for trade-offs and known limitations.
