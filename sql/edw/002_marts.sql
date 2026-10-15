TRUNCATE dm.ticket_flight_details, dm.flight_airport_daily, dm.booking_sales_daily, dm.exchange_rates_daily;

INSERT INTO dm.ticket_flight_details
SELECT tf.ticket_no,tf.flight_id,t.book_ref,b.book_date,t.passenger_id,t.passenger_name,
       f.flight_no,f.scheduled_departure,f.scheduled_arrival,f.departure_airport,dep.city,
       f.arrival_airport,arr.city,f.status,f.aircraft_code,tf.fare_conditions,tf.amount,
       bp.seat_no,%(batch_id)s
FROM core.ticket_flights tf
JOIN core.tickets t ON t.ticket_no=tf.ticket_no
JOIN core.bookings b ON b.book_ref=t.book_ref
JOIN core.flights f ON f.flight_id=tf.flight_id
JOIN core.airports dep ON dep.airport_code=f.departure_airport
JOIN core.airports arr ON arr.airport_code=f.arrival_airport
LEFT JOIN core.boarding_passes bp ON bp.ticket_no=tf.ticket_no AND bp.flight_id=tf.flight_id;

INSERT INTO dm.flight_airport_daily
SELECT f.scheduled_departure::date,f.departure_airport,f.arrival_airport,
       count(*),count(*) FILTER (WHERE f.status='Cancelled'),
       sum(coalesce(t.ticket_count,0)),sum(coalesce(t.amount,0)),%(batch_id)s
FROM core.flights f
LEFT JOIN (
    SELECT flight_id,count(*) AS ticket_count,sum(amount) AS amount
    FROM core.ticket_flights GROUP BY flight_id
) t ON t.flight_id=f.flight_id
GROUP BY f.scheduled_departure::date,f.departure_airport,f.arrival_airport;

-- Aggregate tickets and flight legs before joining bookings to avoid multiplying booking amounts.
INSERT INTO dm.booking_sales_daily
SELECT b.book_date::date,count(*),sum(b.total_amount),
       sum(coalesce(t.ticket_count,0)),sum(coalesce(t.leg_count,0)),sum(coalesce(t.amount,0)),%(batch_id)s
FROM core.bookings b
LEFT JOIN (
    SELECT t.book_ref,count(*) AS ticket_count,
           sum(coalesce(tf.leg_count,0)) AS leg_count,sum(coalesce(tf.amount,0)) AS amount
    FROM core.tickets t
    LEFT JOIN (
        SELECT ticket_no,count(*) AS leg_count,sum(amount) AS amount
        FROM core.ticket_flights GROUP BY ticket_no
    ) tf ON tf.ticket_no=t.ticket_no
    GROUP BY t.book_ref
) t ON t.book_ref=b.book_ref
GROUP BY b.book_date::date;

INSERT INTO dm.exchange_rates_daily
SELECT request_date,rate_date,currency_code,rate_value,previous_rate,
       CASE WHEN previous_rate > 0 THEN round((rate_value/previous_rate-1)*100,8) END,%(batch_id)s
FROM (
    SELECT request_date,rate_date,currency_code,rate_value,
           lag(rate_value) OVER (PARTITION BY currency_code ORDER BY request_date) AS previous_rate
    FROM core.exchange_rates
) rates;
