-- name: ticket_join
SELECT t.book_ref,count(*) AS legs,sum(tf.amount) AS amount
FROM stage5_work.tf tf JOIN stage5_work.tickets t USING (ticket_no)
GROUP BY t.book_ref ORDER BY amount DESC,t.book_ref LIMIT 20;
-- name: flight_join
SELECT f.departure_airport,count(*) AS legs,sum(tf.amount) AS amount
FROM stage5_work.tf tf JOIN stage5_base.flights f USING (flight_id)
GROUP BY f.departure_airport ORDER BY f.departure_airport;
-- name: boarding_join
SELECT tf.fare_conditions,count(*) AS boarded,sum(tf.amount) AS amount
FROM stage5_work.tf tf JOIN stage5_base.boarding_passes b USING (ticket_no,flight_id)
GROUP BY tf.fare_conditions ORDER BY tf.fare_conditions;
-- name: ticket_filter
SELECT count(*) AS legs,sum(amount) AS amount FROM stage5_work.tf
WHERE ticket_no=%(ticket_no)s;
-- name: flight_filter
SELECT count(*) AS legs,sum(amount) AS amount FROM stage5_work.tf WHERE flight_id=%(flight_id)s;
-- name: booking_join
SELECT b.book_date::date AS day,count(*) AS bookings,sum(b.total_amount) AS booking_amount,sum(t.ticket_count) AS tickets
FROM (SELECT book_ref,count(*) AS ticket_count FROM stage5_work.tickets GROUP BY book_ref) t
JOIN stage5_base.bookings b USING (book_ref)
GROUP BY b.book_date::date ORDER BY day;
-- name: airport_join
SELECT a.city,count(*) AS flights FROM stage5_base.flights f
JOIN stage5_work.airports a ON a.airport_code=f.departure_airport
GROUP BY a.city ORDER BY a.city;
-- name: rates_window
SELECT currency_code,request_date,rate_value,
lag(rate_value) OVER (PARTITION BY currency_code ORDER BY request_date) AS previous
FROM stage5_work.rates ORDER BY currency_code,request_date;
-- name: daily_group
SELECT flight_date,sum(flight_count) AS flights,sum(ticket_amount_rub) AS amount
FROM stage5_work.daily GROUP BY flight_date ORDER BY flight_date;
-- name: partition_filter
SELECT fare_conditions,count(*) AS legs,sum(amount) AS amount FROM stage5_work.fact
WHERE flight_date>='2016-10-01' AND flight_date<'2016-10-08'
GROUP BY fare_conditions ORDER BY fare_conditions;
-- name: partition_all
SELECT fare_conditions,count(*) AS legs,sum(amount) AS amount FROM stage5_work.fact
GROUP BY fare_conditions ORDER BY fare_conditions;
