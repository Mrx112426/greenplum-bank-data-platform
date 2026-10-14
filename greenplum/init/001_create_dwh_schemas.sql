CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS dm;
CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS dq.platform_status (
    component text NOT NULL,
    status text NOT NULL,
    checked_at timestamptz NOT NULL DEFAULT current_timestamp
)
DISTRIBUTED REPLICATED;

DELETE FROM dq.platform_status
WHERE component = 'greenplum';

INSERT INTO dq.platform_status (component, status)
VALUES ('greenplum', 'initialized');
