CREATE SCHEMA IF NOT EXISTS source;

CREATE TABLE IF NOT EXISTS source.events (
    event_id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    source_system VARCHAR(100) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO source.events (event_type, source_system, payload)
VALUES
    ('platform_started', 'postgres-source', '{"status": "ready"}')
ON CONFLICT DO NOTHING;
