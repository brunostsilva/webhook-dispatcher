create table subscriptions (
    id          uuid        primary key,
    url         text        not null,
    secret      text        not null,
    event_types text[]      not null,
    created_at  timestamptz not null
);

create table events (
    id              uuid        primary key,
    idempotency_key text        not null unique,
    -- SHA-256 of the request, to tell a retried request from a different one reusing the key.
    request_hash    text        not null,
    type            text        not null,
    -- The exact JSON sent (and signed) on every attempt.
    payload         text        not null,
    created_at      timestamptz not null
);

-- The outbox: one row per event and subscription, written in the same transaction as the event.
create table deliveries (
    id               uuid        primary key,
    event_id         uuid        not null references events (id),
    subscription_id  uuid        not null references subscriptions (id),
    status           text        not null check (status in ('PENDING', 'IN_FLIGHT', 'DELIVERED', 'FAILED', 'DEAD')),
    attempts         int         not null default 0,
    -- Attempts allowed before giving up; a manual retry raises it.
    attempt_limit    int         not null,
    next_attempt_at  timestamptz not null,
    -- Until when the worker that claimed an IN_FLIGHT delivery owns it; after that it can be claimed again.
    lease_until      timestamptz,
    -- Token of the current claim: only the worker holding it can renew the lease or record the outcome.
    claim_id         uuid,
    last_status_code int,
    last_error       text,
    created_at       timestamptz not null,
    updated_at       timestamptz not null,
    unique (event_id, subscription_id)
);

create index deliveries_due on deliveries (next_attempt_at) where status = 'PENDING';
create index deliveries_leased on deliveries (lease_until) where status = 'IN_FLIGHT';

create table delivery_attempts (
    id           bigint generated always as identity primary key,
    delivery_id  uuid        not null references deliveries (id),
    attempt      int         not null,
    status_code  int,
    error        text,
    duration_ms  bigint      not null,
    attempted_at timestamptz not null
);

create index delivery_attempts_delivery on delivery_attempts (delivery_id, attempt);
