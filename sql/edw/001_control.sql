CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS dm;
CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS dq.pipeline_runs (
    batch_id text NOT NULL, layer text NOT NULL, status text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT current_timestamp,
    finished_at timestamptz, details jsonb, error_message text
) DISTRIBUTED BY (batch_id);

CREATE TABLE IF NOT EXISTS dq.layer_state (
    layer text NOT NULL, batch_id text NOT NULL,
    published_at timestamptz NOT NULL DEFAULT current_timestamp
) DISTRIBUTED REPLICATED;

CREATE TABLE IF NOT EXISTS dq.check_results (
    audit_id text NOT NULL, batch_id text NOT NULL, layer text NOT NULL,
    table_name text NOT NULL, check_name text NOT NULL, severity text NOT NULL,
    status text NOT NULL, failed_rows bigint NOT NULL, details jsonb,
    checked_at timestamptz NOT NULL DEFAULT current_timestamp
) DISTRIBUTED BY (batch_id);

CREATE TABLE IF NOT EXISTS dq.rejected_records (
    batch_id text NOT NULL, layer text NOT NULL, table_name text NOT NULL,
    reason text NOT NULL, record_data jsonb NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT current_timestamp
) DISTRIBUTED BY (batch_id);

CREATE TABLE IF NOT EXISTS staging.exchange_rates (
    request_date date NOT NULL, rate_date date, currency_code text,
    nominal integer, source_value numeric(24,8), rate_value numeric(24,12),
    source_key text NOT NULL, source_sha256 text NOT NULL,
    batch_id text NOT NULL, loaded_at timestamptz NOT NULL DEFAULT current_timestamp
) DISTRIBUTED BY (currency_code);

CREATE TABLE IF NOT EXISTS core.exchange_rates
(LIKE staging.exchange_rates INCLUDING DEFAULTS)
DISTRIBUTED BY (currency_code);

CREATE TABLE IF NOT EXISTS dm.exchange_rates_daily (
    request_date date NOT NULL, rate_date date NOT NULL, currency_code text NOT NULL,
    rate_value numeric(24,12) NOT NULL, previous_rate numeric(24,12),
    change_pct numeric(24,8), batch_id text NOT NULL
) DISTRIBUTED BY (currency_code);

CREATE TABLE IF NOT EXISTS dm.ticket_flight_details (
    ticket_no text NOT NULL, flight_id integer NOT NULL, book_ref text NOT NULL,
    book_date timestamptz NOT NULL, passenger_id text NOT NULL, passenger_name text NOT NULL,
    flight_no text NOT NULL, scheduled_departure timestamptz NOT NULL,
    scheduled_arrival timestamptz NOT NULL, departure_airport text NOT NULL,
    departure_city text NOT NULL, arrival_airport text NOT NULL, arrival_city text NOT NULL,
    flight_status text NOT NULL, aircraft_code text NOT NULL, fare_conditions text NOT NULL,
    amount_rub numeric(18,2) NOT NULL, seat_no text, batch_id text NOT NULL
) WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=1)
DISTRIBUTED BY (ticket_no);

CREATE TABLE IF NOT EXISTS dm.flight_airport_daily (
    flight_date date NOT NULL, departure_airport text NOT NULL, arrival_airport text NOT NULL,
    flight_count bigint NOT NULL, cancelled_flight_count bigint NOT NULL,
    ticket_count bigint NOT NULL, ticket_amount_rub numeric(24,2) NOT NULL, batch_id text NOT NULL
) DISTRIBUTED BY (departure_airport);

CREATE TABLE IF NOT EXISTS dm.booking_sales_daily (
    booking_date date NOT NULL, booking_count bigint NOT NULL,
    booking_amount_rub numeric(24,2) NOT NULL, ticket_count bigint NOT NULL,
    ticket_flight_count bigint NOT NULL, ticket_flight_amount_rub numeric(24,2) NOT NULL,
    batch_id text NOT NULL
) DISTRIBUTED BY (booking_date);
