CREATE TABLE IF NOT EXISTS users (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    email       VARCHAR(255) NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS seats (
    id           SERIAL PRIMARY KEY,
    seat_number  VARCHAR(16) NOT NULL UNIQUE,
    status       VARCHAR(16) NOT NULL DEFAULT 'AVAILABLE',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reservations (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          INT NOT NULL REFERENCES users(id),
    seat_id          INT NOT NULL REFERENCES seats(id),
    status           VARCHAR(16) NOT NULL,
    expires_at       TIMESTAMPTZ,
    idempotency_key  VARCHAR(255) UNIQUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT reservations_status_check
        CHECK (status IN ('HELD', 'CONFIRMED', 'EXPIRED', 'CANCELLED'))
);

-- CRITICAL: database-level double-booking guard.
-- At most ONE active (HELD or CONFIRMED) reservation can exist per seat.
CREATE UNIQUE INDEX IF NOT EXISTS uq_reservations_active_seat
    ON reservations (seat_id)
    WHERE status IN ('HELD', 'CONFIRMED');

CREATE INDEX IF NOT EXISTS idx_reservations_user_id ON reservations (user_id);
CREATE INDEX IF NOT EXISTS idx_seats_status ON seats (status);