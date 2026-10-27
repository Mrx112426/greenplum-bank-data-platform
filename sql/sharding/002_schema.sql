-- Run only through scripts/sharding/run.sh: target database guard is mandatory.
CREATE SCHEMA stage5_meta;
CREATE TABLE stage5_meta.manifest (id integer, document jsonb NOT NULL) DISTRIBUTED REPLICATED;
CREATE SCHEMA stage5_base;
CREATE SCHEMA stage5_work;
CREATE TABLE stage5_base.tickets (ticket_no text,book_ref text) WITH (appendoptimized=true,orientation=column,compresstype=zstd,compresslevel=1) DISTRIBUTED BY (ticket_no);
CREATE TABLE stage5_base.bookings (book_ref text,book_date timestamptz,total_amount numeric(18,2)) WITH (appendoptimized=true,orientation=column,compresstype=zstd,compresslevel=1) DISTRIBUTED BY (book_ref);
CREATE TABLE stage5_base.ticket_flights (ticket_no text,flight_id integer,fare_conditions text,amount numeric(18,2)) WITH (appendoptimized=true,orientation=column,compresstype=zstd,compresslevel=1) DISTRIBUTED BY (ticket_no);
CREATE TABLE stage5_base.boarding_passes (ticket_no text,flight_id integer,boarding_no integer,seat_no text) WITH (appendoptimized=true,orientation=column,compresstype=zstd,compresslevel=1) DISTRIBUTED BY (ticket_no);
CREATE TABLE stage5_base.flights (flight_id integer,scheduled_departure timestamptz,departure_airport text,arrival_airport text,aircraft_code text) WITH (appendoptimized=true,orientation=column,compresstype=zstd,compresslevel=1) DISTRIBUTED BY (flight_id);
CREATE TABLE stage5_base.airports (airport_code text,city text) DISTRIBUTED REPLICATED;
CREATE TABLE stage5_base.exchange_rates (request_date date,rate_date date,currency_code text,rate_value numeric(24,12)) DISTRIBUTED BY (currency_code);
CREATE TABLE stage5_base.flight_airport_daily (flight_date date,departure_airport text,arrival_airport text,flight_count bigint,ticket_amount_rub numeric(24,2)) DISTRIBUTED BY (departure_airport);
CREATE TABLE stage5_base.booking_sales_daily (booking_date date,booking_count bigint,booking_amount_rub numeric(24,2)) DISTRIBUTED BY (booking_date);
