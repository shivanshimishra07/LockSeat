# Design Decisions

## 1. Most technically challenging part

Adding a Redis fast-path without letting it become a second source of truth.

A Redis lock in front of Postgres is easy to write and hard to get right. The questions are what happens if a worker dies after `SET NX` but before the INSERT, if a slow request releases a lock that now belongs to someone else, or if Redis disappears halfway through a request.

I treated Postgres as the only arbiter and Redis as a load shedder that is allowed to be wrong:

- **Lock ownership.** The lock value is the `user_id`, and release is a Lua compare-and-delete. A late cancel, rollback or sweeper pass cannot free a lock that a newer holder has taken.
- **Lock TTL follows `HOLD_SECONDS`.** A leaked lock is bounded by the hold duration.
- **Every Redis call is wrapped.** A failure is logged and the request goes straight to Postgres. A 0.1 s socket timeout plus a 5 s circuit breaker means a dead Redis costs one slow call, not one per request.
- **Idempotency ordering.** The cached-response check runs before the in-flight check. In the other order, a legitimate replay shortly after completion would hit the leftover `PROCESSING` key and get a false "duplicate in progress".

Functional correctness was checked with 50-way bursts on one seat (`test_concurrency.py`, `test_step2.py`), including a run with Redis stopped.

The second hard part was the load test itself. At 2,000 VUs, with k6 and the whole stack on one Docker Desktop VM, latency was dominated by queueing rather than by the logic (see section 3).

## 2. One important design decision

**The double-booking guard is a partial unique index, not application-level locking.**

```sql
CREATE UNIQUE INDEX uq_reservations_active_seat
    ON reservations (seat_id) WHERE status IN ('HELD', 'CONFIRMED');
```

| Alternative | Why not |
|---|---|
| `SELECT ... FOR UPDATE` on the seat row | Contenders queue up while holding pooled connections. A 500-way spike on one seat would exhaust the pool. |
| Redis lock as the guard | Correctness would depend on TTLs and clocks. A pause or partition can produce two owners. |
| SERIALIZABLE isolation | Correct, but needs retry loops and wastes work on the losers. |
| Advisory locks | Works, but it is a convention. An index cannot be bypassed by a future code path. |

With the index, the INSERT is the lock. Contenders run concurrently, one commits, and the rest get `UniqueViolationError`, which maps to 409. There is no app-side coordination, so behavior is identical with one worker or four, and with Redis up or down. Cancel and expiry only change `status`, which drops the row out of the index and frees the seat.

The cost is that availability is derived. Reservations are history rows, and `seats.status` is not used for state. `GET /api/seats` computes status with `EXISTS` subqueries over every seat. That was fine functionally, but it is the slowest endpoint under load (see section 3).

Related choice: stale `HELD` rows for a seat are flipped to `EXPIRED` inside the reserve transaction. Without that, an abandoned hold would keep blocking its seat until the next sweeper pass.

## 3. Limitations and failure cases

**Latency under 2,000 VUs is poor.** In the recorded run (86 s, 30,071 iterations, about 347 req/s averaged over the whole run), overall p95 was 7.2 s and p99 11.7 s. The seat listing was worst (p95 7.5 s), then reserve (p95 7.2 s). The latency gates in `k6_script.js` (reserve p95 < 1 s, list p95 < 2 s) failed. The load generator, 4 uvicorn workers, Postgres and Redis all shared one Docker Desktop VM (4 vCPU), so some of this is the benchmark setup. The listing is also a real cost, because it recomputes status for all 500 seats on every call.

**The recorded run does not show the correctness guarantees.** It produced no successful reservations, most likely because seats were still held from a previous run, so the spike and idempotency assertions failed and the DB invariant check was not run. The guarantee rests on the index and on the functional tests, not on that run. Treat the load-test claims as unproven until a clean run is recorded.

**Confirm is not durable.**
- The duplicate-confirm guard lives in Redis. If Redis is down, two concurrent confirms can both reach the payment step and charge twice. The final `UPDATE ... WHERE status = 'HELD'` keeps the booking correct, but not the money.
- If the hold expires or is cancelled during payment, the API returns 409 with a "simulated refund" message. No refund record exists, and a crash between charge and `UPDATE` loses the charge.

**Smaller issues.**
- If a worker dies after taking the Redis lock but before the INSERT, the seat is rejected by the fast-path until the TTL passes, even though Postgres holds nothing.
- `user_id` comes from the request body. There is no authentication.
- Each worker runs its own sweeper. This is safe because `UPDATE ... RETURNING` gives each row to one worker, but it is redundant work.

## 4. What I would improve first with another week

1. **Get a clean, repeatable benchmark.** Add a reset script (`TRUNCATE` and `FLUSHDB`), run k6 on a separate machine, and record real numbers in `RESULTS.md`, including the `verify.sql` output. Until then the correctness claim is only supported by the functional tests. I'd do this first because every other change needs a baseline to measure against.
2. **Fix the seat listing.** Compute status with one `LEFT JOIN` against the active-reservation partial index, or cache the result for a second or two. It is the slowest endpoint and the simplest to improve.
3. **Make confirm durable.** Add a `payments` table (or a `PAYMENT_PENDING` state), write the attempt with a payment idempotency key before calling the gateway, reconcile on a schedule, and move the in-flight guard into Postgres.
4. Run the sweeper under a Postgres advisory lock so only one worker sweeps.
5. Add authentication, and export Prometheus metrics (fast-path hit rate, pool wait, sweeper lag).

## Verification status

| Check | Status |
|---|---|
| 50-way burst on one seat (single request path) | Passing (functional scripts) |
| Redis-down fallback | Passing (`test_step2.py --fallback`) |
| k6 run, 2,000 VUs, 30,071 iterations, 0 server errors | Recorded, but with no successful reservations |
| k6 spike / idempotency assertions | **Failed** in the recorded run (dirty state) |
| `verify.sql` invariant check | **Not yet run**; the file in the repo is empty |