-- Schema for es/pgstore. The append-only event log plus one row per
-- projector/worker checkpoint. See TASK.md for the reasoning behind this
-- design (no broker needed: events already live in Postgres, LISTEN/NOTIFY
-- + poll fallback wakes projectors, advisory locks elect a single leader).

CREATE TABLE IF NOT EXISTS events (
    global_id         BIGSERIAL PRIMARY KEY,
    id                UUID NOT NULL,
    aggregate_id      UUID NOT NULL,
    aggregate_version BIGINT NOT NULL,
    version           INT NOT NULL DEFAULT 1,
    name              TEXT NOT NULL,
    payload           JSONB NOT NULL,
    occurred_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (aggregate_id, aggregate_version)
);

CREATE INDEX IF NOT EXISTS events_aggregate_id_idx ON events (aggregate_id, aggregate_version);

CREATE TABLE IF NOT EXISTS checkpoints (
    name           TEXT PRIMARY KEY,
    last_global_id BIGINT NOT NULL DEFAULT 0
);

-- Optional: notify listeners as soon as an event is committed. Store.StreamAll
-- LISTENs on this channel but always falls back to polling, since NOTIFY is
-- best-effort and delivers nothing to a listener that wasn't connected yet.
CREATE OR REPLACE FUNCTION notify_events() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('events_channel', NEW.global_id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS events_notify ON events;
CREATE TRIGGER events_notify
    AFTER INSERT ON events
    FOR EACH ROW EXECUTE FUNCTION notify_events();
