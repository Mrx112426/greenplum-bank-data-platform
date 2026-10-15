SELECT layer,batch_id,status,started_at,finished_at,
       finished_at-started_at AS elapsed,details,error_message
FROM dq.pipeline_runs ORDER BY finished_at DESC;

SELECT layer,batch_id,published_at FROM dq.layer_state ORDER BY layer;

SELECT audit_id,layer,status,count(*) AS checks
FROM dq.check_results GROUP BY audit_id,layer,status ORDER BY audit_id,layer,status;

SELECT table_name,check_name,status,failed_rows,details,checked_at
FROM dq.check_results WHERE status<>'PASS' ORDER BY checked_at DESC;

SELECT batch_id,table_name,reason,count(*) AS rejected_rows
FROM dq.rejected_records GROUP BY batch_id,table_name,reason;

SELECT 'ticket_flight_details' AS mart,count(*) AS rows FROM dm.ticket_flight_details
UNION ALL SELECT 'flight_airport_daily',count(*) FROM dm.flight_airport_daily
UNION ALL SELECT 'booking_sales_daily',count(*) FROM dm.booking_sales_daily
UNION ALL SELECT 'exchange_rates_daily',count(*) FROM dm.exchange_rates_daily;

SELECT min(request_date),max(request_date),count(DISTINCT request_date) AS dates,count(*) AS rate_rows
FROM core.exchange_rates;

SELECT currency_code,request_date,count(*)
FROM core.exchange_rates GROUP BY currency_code,request_date HAVING count(*)>1;

SELECT 'core' AS layer,sum(amount) AS ticket_flight_amount_rub FROM core.ticket_flights
UNION ALL SELECT 'dm',sum(amount_rub) FROM dm.ticket_flight_details;

SELECT booking_date,booking_count,booking_amount_rub,ticket_count,ticket_flight_count
FROM dm.booking_sales_daily ORDER BY booking_date DESC LIMIT 10;

SELECT * FROM dm.exchange_rates_daily ORDER BY request_date DESC,currency_code LIMIT 15;
