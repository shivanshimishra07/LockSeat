# Load Test Results

Fill in one copy of this file per test campaign. Commit it next to the raw `summary.json`
files so results are reproducible.

## 1. Run metadata

| Field | Value |
|---|---|
| Date / time (UTC) | 2026-10-17 09:30:00 UTC |
| Git commit | `a4f891b` (feat: Step 4 load testing & verification) |
| Tester | Shivanshi |
| k6 version (`k6 version`) | v0.49.0 (via Docker `grafana/k6`) |
| Load generator host (CPU / RAM / OS) | Windows 11 / x86_64 / Docker Desktop (WSL2 backend) |
| App host (CPU / RAM / OS, Docker CPU+memory limits) | Docker Desktop (4 vCPU / 8 GB RAM allocation) |
| Same machine as load generator? | yes |
| `ulimit -n` on the generator | 65535 |

## 2. System configuration under test

| Setting | Run A (baseline) | Run B (multi-worker) | Run C (Redis down) |
|---|---|---|---|
| uvicorn workers | 1 | **4** | 4 |
| `DB_POOL_MIN_SIZE` / `DB_POOL_MAX_SIZE` per worker | 10 / 60 | **10 / 40** | 10 / 40 |
| Postgres `max_connections` | 100 (default) | **300** | 300 |
| `HOLD_SECONDS` | 300 | **300** | 300 |
| `SWEEPER_INTERVAL_SECONDS` | 10 | **10** | 10 |
| Redis | up | **up** | stopped |
| `EXPECT_REDIS` passed to k6 | true | **true** | false |

Connection budget check: `workers x DB_POOL_MAX_SIZE` must stay below Postgres `max_connections` (minus ~3 reserved). 4 x 40 = 160 < 300 active limits, leaving ample headroom for `psql` connection maintenance.

### Multi-worker override (`docker-compose.multiworker.yml`)

```yaml
services:
  db:
    command: ["postgres", "-c", "max_connections=300"]
  app:
    command: ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
    environment:
      DB_POOL_MIN_SIZE: "10"
      DB_POOL_MAX_SIZE: "40"
