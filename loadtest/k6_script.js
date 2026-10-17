/**
 * k6 load-testing suite for the high-concurrency seat reservation engine.
 *
 * Scenarios (run back-to-back, never overlapping, so each scenario's assertions
 * are not polluted by the others and k6 can reuse the same VU pool):
 *
 *   1. spike_single_seat      500 VUs, one request each, all for seat_id=42
 *                             => exactly 1 x 201 and 499 x 409, zero 5xx
 *   2. idempotency_stampede   10 VUs, same Idempotency-Key, seat_id=50
 *      + idempotency_verify   => exactly 1 execution, 9 replays, 1 DB row
 *   3. distributed_mix        2000 VUs for 30s over seats 1..500
 *                             => 80% reserve / 10% confirm / 10% list, p95/p99 + RPS
 *
 * Usage (from repo root):
 *   k6 run loadtest/k6_script.js
 *   k6 run -e BASE_URL=http://localhost:8002 loadtest/k6_script.js
 *   k6 run -e SCENARIOS=spike loadtest/k6_script.js            # subset
 *
 * Reset state before every run (seat 42 / seat 50 / the idempotency key must be fresh):
 *   docker exec reservation_db psql -U postgres -d reservation_db -c "TRUNCATE reservations;"
 *   docker exec reservation_redis redis-cli FLUSHDB
 *
 * Environment variables (all optional):
 *   BASE_URL              default http://localhost:8002
 *   SCENARIOS             comma list of: spike,idempotency,distributed (default: all)
 *   EXPECT_REDIS          "true" (default) | "false"  (Redis stopped => relaxes idempotency expectations)
 *   SKIP_PREFLIGHT        "true" to skip the clean-state check in setup()
 *   SPIKE_VUS             default 500
 *   SPIKE_SEAT_ID         default 42
 *   IDEM_VUS              default 10
 *   IDEM_SEAT_ID          default 50
 *   IDEM_USER_ID          default 1
 *   IDEM_KEY              default k6-stampede-test-42
 *   DIST_VUS              default 2000
 *   DIST_DURATION_SECONDS default 30
 *   SEAT_COUNT            default 500
 *   USER_COUNT            default 1000
 *   THINK_MAX_SECONDS     default 0.1  (random think time upper bound per iteration)
 *   RESERVE_P95_MS        default 1000
 *   RESERVE_P99_MS        default 2500
 *   LIST_P95_MS           default 2000
 *   CONFIRM_P95_MS        default 7000 (payment simulator sleeps 4-5s on 10% of calls)
 *   MIN_RPS               default 0    (minimum acceptable throughput in scenario 3)
 *   SUMMARY_PATH          default loadtest/summary.json
 *
 * OS note: 2000 VUs need many sockets. Run `ulimit -n 65535` first (macOS defaults to 256).
 */

import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Counter, Gauge } from 'k6/metrics';

// --------------------------------------------------------------------------- //
// Configuration
// --------------------------------------------------------------------------- //
function num(name, fallback) {
  const raw = __ENV[name];
  if (raw === undefined || raw === '') {
    return fallback;
  }
  const value = Number(raw);
  return Number.isFinite(value) ? value : fallback;
}

const BASE_URL = (__ENV.BASE_URL || 'http://localhost:8002').replace(/\/+$/, '');
const RESERVATIONS_URL = `${BASE_URL}/api/reservations`;

const ENABLED = (__ENV.SCENARIOS || 'spike,idempotency,distributed')
  .split(',')
  .map((s) => s.trim().toLowerCase())
  .filter((s) => s.length > 0);
const RUN_SPIKE = ENABLED.indexOf('spike') !== -1;
const RUN_IDEM = ENABLED.indexOf('idempotency') !== -1;
const RUN_DIST = ENABLED.indexOf('distributed') !== -1;

const EXPECT_REDIS = (__ENV.EXPECT_REDIS || 'true').toLowerCase() !== 'false';
const SKIP_PREFLIGHT = (__ENV.SKIP_PREFLIGHT || 'false').toLowerCase() === 'true';

const SPIKE_VUS = num('SPIKE_VUS', 500);
const SPIKE_SEAT_ID = num('SPIKE_SEAT_ID', 42);

const IDEM_VUS = num('IDEM_VUS', 10);
const IDEM_SEAT_ID = num('IDEM_SEAT_ID', 50);
const IDEM_USER_ID = num('IDEM_USER_ID', 1);
const IDEM_KEY = __ENV.IDEM_KEY || 'k6-stampede-test-42';
const IDEM_MAX_ATTEMPTS = 10;
const IDEM_RETRY_SLEEP_SECONDS = 0.3;

const DIST_VUS = num('DIST_VUS', 2000);
const DIST_SECONDS = num('DIST_DURATION_SECONDS', 30);
const SEAT_COUNT = num('SEAT_COUNT', 500);
const USER_COUNT = num('USER_COUNT', 1000);
const THINK_MAX_SECONDS = num('THINK_MAX_SECONDS', 0.1);

const RESERVE_P95_MS = num('RESERVE_P95_MS', 1000);
const RESERVE_P99_MS = num('RESERVE_P99_MS', 2500);
const LIST_P95_MS = num('LIST_P95_MS', 2000);
const CONFIRM_P95_MS = num('CONFIRM_P95_MS', 7000);
const MIN_RPS = num('MIN_RPS', 0);

const SUMMARY_PATH = __ENV.SUMMARY_PATH || 'loadtest/summary.json';

const JSON_HEADERS = { 'Content-Type': 'application/json' };

const DETAIL_IN_PROGRESS = 'Duplicate request in progress';
const DETAIL_SEAT_TAKEN = 'Seat already taken or held';
const DETAIL_FASTPATH = 'Redis fast-path';

// 4xx responses are legitimate business outcomes (409 conflict, 402 payment failed,
// 400 expired...). Only 5xx and network errors count towards http_req_failed.
http.setResponseCallback(http.expectedStatuses({ min: 200, max: 499 }));

// --------------------------------------------------------------------------- //
// Custom metrics
// --------------------------------------------------------------------------- //
// Global
const serverErrors = new Counter('server_errors');

// Scenario 1
const spikeCreated = new Counter('spike_created');
const spikeConflict = new Counter('spike_conflict');
const spikeConflictRedis = new Counter('spike_conflict_redis_fastpath');
const spikeConflictPostgres = new Counter('spike_conflict_postgres');
const spikeServerError = new Counter('spike_server_error');
const spikeOther = new Counter('spike_other');

// Scenario 2
const idemCreatedTotal = new Counter('idem_created_total');
const idemOriginal = new Counter('idem_original');
const idemReplay = new Counter('idem_replay');
const idemInProgress = new Counter('idem_in_progress_responses');
const idemUnresolved = new Counter('idem_unresolved');
const idemUnexpected = new Counter('idem_unexpected');
const idemServerError = new Counter('idem_server_error');
const idemDbRows = new Gauge('idem_db_rows');

// Scenario 3
const opsReserve = new Counter('dist_ops_reserve');
const opsConfirm = new Counter('dist_ops_confirm');
const opsList = new Counter('dist_ops_list');
const confirmFallback = new Counter('dist_confirm_fallback_to_reserve');
const reserve201 = new Counter('dist_reserve_201');
const reserve409 = new Counter('dist_reserve_409');
const reserveOther = new Counter('dist_reserve_other');
const confirm200 = new Counter('dist_confirm_200');
const confirm402 = new Counter('dist_confirm_402');
const confirm409 = new Counter('dist_confirm_409');
const confirmOther = new Counter('dist_confirm_other');
const listOk = new Counter('dist_list_200');
const listBad = new Counter('dist_list_bad');

// --------------------------------------------------------------------------- //
// Scenario timeline (sequential, non-overlapping)
// --------------------------------------------------------------------------- //
let cursorSeconds = 0;
function slot(seconds) {
  const start = cursorSeconds;
  cursorSeconds += seconds;
  return `${start}s`;
}

const scenarios = {};
const thresholds = {
  // Only 5xx / network errors count as failures (see setResponseCallback above).
  http_req_failed: ['rate<0.01'],
  server_errors: ['count==0'],
  checks: ['rate>0.99'],
};

if (RUN_SPIKE) {
  scenarios.spike_single_seat = {
    executor: 'per-vu-iterations',
    exec: 'spikeSingleSeat',
    vus: SPIKE_VUS,
    iterations: 1,
    startTime: slot(25),
    maxDuration: '25s',
    gracefulStop: '0s',
  };
  thresholds.spike_created = ['count==1'];
  thresholds.spike_conflict = [`count==${SPIKE_VUS - 1}`];
  thresholds.spike_server_error = ['count==0'];
  thresholds.spike_other = ['count==0'];
  thresholds['http_req_duration{endpoint:spike_reserve}'] = ['p(95)>=0'];
}

if (RUN_IDEM) {
  scenarios.idempotency_stampede = {
    executor: 'per-vu-iterations',
    exec: 'idempotencyStampede',
    vus: IDEM_VUS,
    iterations: 1,
    startTime: slot(20),
    maxDuration: '20s',
    gracefulStop: '0s',
  };
  scenarios.idempotency_verify = {
    executor: 'per-vu-iterations',
    exec: 'idempotencyVerify',
    vus: 1,
    iterations: 1,
    startTime: slot(10),
    maxDuration: '10s',
    gracefulStop: '0s',
  };
  thresholds.idem_created_total = [`count==${IDEM_VUS}`];
  thresholds.idem_unresolved = ['count==0'];
  thresholds.idem_unexpected = ['count==0'];
  thresholds.idem_server_error = ['count==0'];
  thresholds.idem_db_rows = ['value==1'];
  if (EXPECT_REDIS) {
    thresholds.idem_original = ['count==1'];
    thresholds.idem_replay = [`count==${IDEM_VUS - 1}`];
  }
}

if (RUN_DIST) {
  scenarios.distributed_mix = {
    executor: 'constant-vus',
    exec: 'distributedMix',
    vus: DIST_VUS,
    duration: `${DIST_SECONDS}s`,
    startTime: slot(DIST_SECONDS + 15),
    gracefulStop: '15s',
  };
  thresholds['http_req_duration{scenario:distributed_mix}'] = ['p(95)>=0'];
  thresholds['http_reqs{scenario:distributed_mix}'] = [
    `count>=${Math.floor(MIN_RPS * DIST_SECONDS)}`,
  ];
  thresholds['http_req_duration{endpoint:dist_reserve}'] = [
    `p(95)<${RESERVE_P95_MS}`,
    `p(99)<${RESERVE_P99_MS}`,
  ];
  thresholds['http_req_duration{endpoint:dist_list}'] = [`p(95)<${LIST_P95_MS}`];
  thresholds['http_req_duration{endpoint:dist_confirm}'] = [`p(95)<${CONFIRM_P95_MS}`];
}

export const options = {
  scenarios: scenarios,
  thresholds: thresholds,
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  noConnectionReuse: false,
};

// --------------------------------------------------------------------------- //
// Helpers
// --------------------------------------------------------------------------- //
function isServerError(res) {
  return res.status === 0 || res.status >= 500;
}

function recordServerError(res) {
  serverErrors.add(1);
  console.error(
    `5xx/network error: status=${res.status} url=${res.url} body=${String(res.body).slice(0, 160)}`
  );
}

function jsonBody(res) {
  try {
    return res.json();
  } catch (e) {
    return null;
  }
}

function detailOf(res) {
  const body = jsonBody(res);
  return body && typeof body.detail === 'string' ? body.detail : null;
}

function headerOf(res, name) {
  const wanted = name.toLowerCase();
  const headers = res.headers || {};
  const keys = Object.keys(headers);
  for (let i = 0; i < keys.length; i++) {
    if (keys[i].toLowerCase() === wanted) {
      return headers[keys[i]];
    }
  }
  return null;
}

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// --------------------------------------------------------------------------- //
// setup(): fail fast on a dead API or dirty state
// --------------------------------------------------------------------------- //
export function setup() {
  const health = http.get(`${BASE_URL}/health`, { tags: { endpoint: 'setup' } });
  if (health.status !== 200) {
    throw new Error(`API not healthy at ${BASE_URL}: status=${health.status}`);
  }

  if (SKIP_PREFLIGHT) {
    return {};
  }

  const listing = http.get(`${BASE_URL}/api/seats?status=available`, {
    tags: { endpoint: 'setup' },
    timeout: '60s',
  });
  if (listing.status !== 200) {
    throw new Error(`Preflight GET /api/seats failed: status=${listing.status}`);
  }
  const available = {};
  (jsonBody(listing) || []).forEach((seat) => {
    available[seat.id] = true;
  });

  const problems = [];
  if (RUN_SPIKE && !available[SPIKE_SEAT_ID]) {
    problems.push(`seat ${SPIKE_SEAT_ID} is not available`);
  }
  if (RUN_IDEM && !available[IDEM_SEAT_ID]) {
    problems.push(`seat ${IDEM_SEAT_ID} is not available`);
  }
  if (problems.length > 0) {
    throw new Error(
      `Dirty state: ${problems.join('; ')}. Reset first:\n` +
        '  docker exec reservation_db psql -U postgres -d reservation_db -c "TRUNCATE reservations;"\n' +
        '  docker exec reservation_redis redis-cli FLUSHDB\n' +
        '(or run with -e SKIP_PREFLIGHT=true)'
    );
  }
  return {};
}

// --------------------------------------------------------------------------- //
// Scenario 1: spike on a single seat
// --------------------------------------------------------------------------- //
export function spikeSingleSeat() {
  // iterationInInstance is unique per iteration across the scenario (0..SPIKE_VUS-1).
  const userId = (exec.scenario.iterationInInstance % USER_COUNT) + 1;

  const res = http.post(
    RESERVATIONS_URL,
    JSON.stringify({ user_id: userId, seat_id: SPIKE_SEAT_ID }),
    {
      headers: JSON_HEADERS,
      tags: { endpoint: 'spike_reserve', name: 'POST /api/reservations (spike)' },
    }
  );

  if (res.status === 201) {
    spikeCreated.add(1);
  } else if (res.status === 409) {
    spikeConflict.add(1);
    const detail = detailOf(res) || '';
    if (detail.indexOf(DETAIL_FASTPATH) !== -1) {
      spikeConflictRedis.add(1);
    } else {
      spikeConflictPostgres.add(1);
    }
  } else if (isServerError(res)) {
    spikeServerError.add(1);
    recordServerError(res);
  } else {
    spikeOther.add(1);
    console.error(`spike: unexpected status ${res.status} body=${String(res.body).slice(0, 160)}`);
  }

  check(res, {
    'spike: status is 201 or 409': (r) => r.status === 201 || r.status === 409,
    'spike: no 5xx': (r) => r.status < 500 && r.status !== 0,
  });
}

// --------------------------------------------------------------------------- //
// Scenario 2: idempotency-key stampede
//
// All VUs send the SAME key AND the same payload (the API fingerprints
// user_id:seat_id, so a different payload would correctly be a 422).
// Requests that arrive while the first one is still executing get
// 409 "Duplicate request in progress"; a real client retries those, so each VU
// retries until it receives a 201 (the original or a cached replay).
// --------------------------------------------------------------------------- //
export function idempotencyStampede() {
  const body = JSON.stringify({ user_id: IDEM_USER_ID, seat_id: IDEM_SEAT_ID });
  const params = {
    headers: Object.assign({}, JSON_HEADERS, { 'Idempotency-Key': IDEM_KEY }),
    tags: { endpoint: 'idem', name: 'POST /api/reservations (idempotency)' },
  };

  let resolved = false;

  for (let attempt = 1; attempt <= IDEM_MAX_ATTEMPTS && !resolved; attempt++) {
    const res = http.post(RESERVATIONS_URL, body, params);

    if (isServerError(res)) {
      idemServerError.add(1);
      recordServerError(res);
      break;
    }

    if (res.status === 201) {
      idemCreatedTotal.add(1);
      const isReplay = (headerOf(res, 'Idempotent-Replay') || '').toLowerCase() === 'true';
      if (isReplay) {
        idemReplay.add(1);
      } else {
        idemOriginal.add(1);
      }
      const parsed = jsonBody(res);
      check(res, {
        'idem: body has reservation id': () => parsed !== null && typeof parsed.id === 'string',
        'idem: body is the requested seat': () => parsed !== null && parsed.seat_id === IDEM_SEAT_ID,
        'idem: body carries the idempotency key': () =>
          parsed !== null && parsed.idempotency_key === IDEM_KEY,
      });
      resolved = true;
      break;
    }

    if (res.status === 409) {
      const detail = detailOf(res);
      const retryable =
        detail === DETAIL_IN_PROGRESS || (!EXPECT_REDIS && detail === DETAIL_SEAT_TAKEN);
      if (retryable) {
        idemInProgress.add(1);
        sleep(IDEM_RETRY_SLEEP_SECONDS);
        continue;
      }
    }

    idemUnexpected.add(1);
    console.error(`idem: unexpected response ${res.status} body=${String(res.body).slice(0, 160)}`);
    break;
  }

  if (!resolved) {
    idemUnresolved.add(1);
  }
}

// Runs after the stampede: exactly ONE row may exist for the key.
export function idempotencyVerify() {
  const res = http.get(`${BASE_URL}/api/users/${IDEM_USER_ID}/bookings`, {
    tags: { endpoint: 'idem_verify', name: 'GET /api/users/{id}/bookings (verify)' },
  });

  let rows = -1;
  if (res.status === 200) {
    const bookings = jsonBody(res) || [];
    const matching = bookings.filter((b) => b.idempotency_key === IDEM_KEY);
    rows = matching.length;
    check(matching, {
      'idem verify: row is for the stampede seat': (m) =>
        m.length === 0 || m.every((b) => b.seat_id === IDEM_SEAT_ID),
    });
  } else if (isServerError(res)) {
    recordServerError(res);
  }
  idemDbRows.add(rows);
}

// --------------------------------------------------------------------------- //
// Scenario 3: distributed load (80% reserve / 10% confirm / 10% list)
// --------------------------------------------------------------------------- //
// Module-level state is per-VU in k6: each VU remembers its own HELD reservations.
const heldReservationIds = [];
const MAX_REMEMBERED_HOLDS = 20;

function doReserve() {
  const seatId = randInt(1, SEAT_COUNT);
  const userId = randInt(1, USER_COUNT);

  opsReserve.add(1);
  const res = http.post(
    RESERVATIONS_URL,
    JSON.stringify({ user_id: userId, seat_id: seatId }),
    {
      headers: JSON_HEADERS,
      tags: { endpoint: 'dist_reserve', name: 'POST /api/reservations' },
    }
  );

  if (res.status === 201) {
    reserve201.add(1);
    const parsed = jsonBody(res);
    if (parsed && parsed.id && heldReservationIds.length < MAX_REMEMBERED_HOLDS) {
      heldReservationIds.push(parsed.id);
    }
  } else if (res.status === 409) {
    reserve409.add(1);
  } else if (isServerError(res)) {
    recordServerError(res);
  } else {
    reserveOther.add(1);
  }

  check(res, { 'reserve: 201 or 409': (r) => r.status === 201 || r.status === 409 });
}

function doConfirm() {
  if (heldReservationIds.length === 0) {
    // Nothing to confirm yet: do a reserve instead so the request is not wasted.
    confirmFallback.add(1);
    doReserve();
    return;
  }

  const reservationId = heldReservationIds.shift();
  opsConfirm.add(1);
  const res = http.post(`${RESERVATIONS_URL}/${reservationId}/confirm`, null, {
    tags: { endpoint: 'dist_confirm', name: 'POST /api/reservations/{id}/confirm' },
    timeout: '60s',
  });

  if (res.status === 200) {
    confirm200.add(1);
  } else if (res.status === 402) {
    confirm402.add(1);
    heldReservationIds.push(reservationId); // payment failed: hold remains, retry later
  } else if (res.status === 409) {
    confirm409.add(1);
  } else if (isServerError(res)) {
    recordServerError(res);
  } else {
    confirmOther.add(1);
  }

  check(res, {
    'confirm: 200/400/402/409 (business outcome)': (r) =>
      r.status === 200 || r.status === 400 || r.status === 402 || r.status === 409,
  });
}

function doList() {
  opsList.add(1);
  const res = http.get(`${BASE_URL}/api/seats?status=available`, {
    tags: { endpoint: 'dist_list', name: 'GET /api/seats?status=available' },
    timeout: '60s',
  });

  if (res.status === 200) {
    listOk.add(1);
  } else if (isServerError(res)) {
    recordServerError(res);
  } else {
    listBad.add(1);
  }
  check(res, { 'list: 200': (r) => r.status === 200 });
}

export function distributedMix() {
  const roll = Math.random();
  if (roll < 0.8) {
    doReserve();
  } else if (roll < 0.9) {
    doConfirm();
  } else {
    doList();
  }
  sleep(Math.random() * THINK_MAX_SECONDS);
}

// --------------------------------------------------------------------------- //
// Summary
// --------------------------------------------------------------------------- //
function values(data, name) {
  return data.metrics[name] ? data.metrics[name].values : null;
}

function count(data, name) {
  const v = values(data, name);
  return v && v.count !== undefined ? v.count : 0;
}

function ms(v) {
  return v === undefined || v === null ? 'n/a' : `${v.toFixed(1)}ms`;
}

function latency(data, name) {
  const v = values(data, name);
  if (!v) {
    return 'n/a';
  }
  return (
    `avg=${ms(v.avg)} med=${ms(v.med)} p90=${ms(v['p(90)'])} ` +
    `p95=${ms(v['p(95)'])} p99=${ms(v['p(99)'])} max=${ms(v.max)}`
  );
}

function failedThresholds(data) {
  const failed = [];
  Object.keys(data.metrics).forEach((name) => {
    const thresholdMap = data.metrics[name].thresholds;
    if (!thresholdMap) {
      return;
    }
    Object.keys(thresholdMap).forEach((expr) => {
      const result = thresholdMap[expr];
      if (result && typeof result === 'object' && result.ok === false) {
        failed.push(`${name}  ${expr}`);
      }
    });
  });
  return failed;
}

export function handleSummary(data) {
  const lines = [];
  lines.push('');
  lines.push('==================== LOAD TEST SUMMARY ====================');
  lines.push(`target: ${BASE_URL}   scenarios: ${ENABLED.join(', ')}   expect_redis: ${EXPECT_REDIS}`);

  if (RUN_SPIKE) {
    lines.push('');
    lines.push(`--- Scenario 1: spike on seat ${SPIKE_SEAT_ID} (${SPIKE_VUS} VUs) ---`);
    lines.push(`201 Created : ${count(data, 'spike_created')}   (expected 1)`);
    lines.push(
      `409 Conflict: ${count(data, 'spike_conflict')}   (expected ${SPIKE_VUS - 1})` +
        `  [redis fast-path=${count(data, 'spike_conflict_redis_fastpath')}, ` +
        `postgres=${count(data, 'spike_conflict_postgres')}]`
    );
    lines.push(`5xx errors  : ${count(data, 'spike_server_error')}   (expected 0)`);
    lines.push(`other codes : ${count(data, 'spike_other')}   (expected 0)`);
    lines.push(`latency     : ${latency(data, 'http_req_duration{endpoint:spike_reserve}')}`);
  }

  if (RUN_IDEM) {
    lines.push('');
    lines.push(`--- Scenario 2: idempotency stampede (${IDEM_VUS} VUs, key=${IDEM_KEY}) ---`);
    lines.push(`201 total            : ${count(data, 'idem_created_total')}   (expected ${IDEM_VUS})`);
    lines.push(
      `original execution   : ${count(data, 'idem_original')}   ` +
        `(expected ${EXPECT_REDIS ? 1 : 'n/a, Redis down'})`
    );
    lines.push(
      `cached replays       : ${count(data, 'idem_replay')}   ` +
        `(expected ${EXPECT_REDIS ? IDEM_VUS - 1 : 'n/a, Redis down'})`
    );
    lines.push(`in-progress 409s seen: ${count(data, 'idem_in_progress_responses')}   (retried by the client)`);
    lines.push(`unresolved / unexpected / 5xx: ${count(data, 'idem_unresolved')} / ${count(data, 'idem_unexpected')} / ${count(data, 'idem_server_error')}   (expected 0 / 0 / 0)`);
    const rows = values(data, 'idem_db_rows');
    lines.push(`DB rows for the key  : ${rows ? rows.value : 'n/a'}   (expected 1)`);
  }

  if (RUN_DIST) {
    const total = count(data, 'http_reqs{scenario:distributed_mix}');
    lines.push('');
    lines.push(`--- Scenario 3: distributed mix (${DIST_VUS} VUs, ${DIST_SECONDS}s) ---`);
    lines.push(
      `requests: ${total}   throughput: ${(total / DIST_SECONDS).toFixed(1)} req/s ` +
        `(measured over the ${DIST_SECONDS}s window)`
    );
    lines.push(
      `ops mix : reserve=${count(data, 'dist_ops_reserve')} confirm=${count(data, 'dist_ops_confirm')} ` +
        `list=${count(data, 'dist_ops_list')}  (confirm->reserve fallbacks=${count(data, 'dist_confirm_fallback_to_reserve')})`
    );
    lines.push(
      `reserve : 201=${count(data, 'dist_reserve_201')} 409=${count(data, 'dist_reserve_409')} other=${count(data, 'dist_reserve_other')}`
    );
    lines.push(
      `confirm : 200=${count(data, 'dist_confirm_200')} 402=${count(data, 'dist_confirm_402')} ` +
        `409=${count(data, 'dist_confirm_409')} other=${count(data, 'dist_confirm_other')}`
    );
    lines.push(`list    : 200=${count(data, 'dist_list_200')} bad=${count(data, 'dist_list_bad')}`);
    lines.push(`latency (all)    : ${latency(data, 'http_req_duration{scenario:distributed_mix}')}`);
    lines.push(`latency (reserve): ${latency(data, 'http_req_duration{endpoint:dist_reserve}')}`);
    lines.push(`latency (confirm): ${latency(data, 'http_req_duration{endpoint:dist_confirm}')}`);
    lines.push(`latency (list)   : ${latency(data, 'http_req_duration{endpoint:dist_list}')}`);
  }

  const failed = failedThresholds(data);
  lines.push('');
  lines.push(`server_errors (all scenarios): ${count(data, 'server_errors')}   (expected 0)`);
  lines.push('');
  if (failed.length === 0) {
    lines.push('THRESHOLDS: ALL PASSED');
  } else {
    lines.push(`THRESHOLDS: ${failed.length} FAILED`);
    failed.forEach((f) => lines.push(`  x ${f}`));
  }
  lines.push('');
  lines.push('Next: run loadtest/verify.sql to assert database invariants.');
  lines.push('===========================================================');
  lines.push('');

  const out = { stdout: lines.join('\n') };
  out[SUMMARY_PATH] = JSON.stringify(data, null, 2);
  return out;
}