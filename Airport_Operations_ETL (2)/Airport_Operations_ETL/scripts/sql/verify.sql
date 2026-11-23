USE airport_etl;

SELECT COUNT(*) AS total_flights, COUNT(DISTINCT FlightID) AS distinct_flight_ids
FROM Flight;

SELECT service_date, COUNT(*) AS total_flights, COUNT(is_delayed) AS actually_departed,
       SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_flights,
       ROUND(100.0 * SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END)
             / NULLIF(COUNT(is_delayed), 0), 2) AS delayed_pct_of_departed,
       ROUND(AVG(CASE WHEN is_delayed = 1 THEN delay_minutes END), 2) AS avg_minutes_among_delayed
FROM flight_departure_metrics
GROUP BY service_date
ORDER BY service_date;

SELECT FlightID, `Status`, DepartureTime, ActualDepartureTime, delay_minutes
FROM flight_departure_metrics
ORDER BY service_date, FlightID;

SELECT run_id, step, attempt, outcome, metrics, message, event_at AS event_at_utc
FROM etl_log
ORDER BY log_id DESC
LIMIT 30;

SELECT run_id, reason_code, COUNT(*) AS rejected_rows
FROM flight_reject
GROUP BY run_id, reason_code
ORDER BY run_id, reason_code;

-- Trace each rejected observation to the source, using run ID and CSV line number.
SELECT rejected.run_id, rejected.source_line, rejected.reason_code, rejected.detail,
       JSON_UNQUOTE(JSON_EXTRACT(raw.payload, '$.fields.FlightID')) AS source_flight_id
FROM flight_reject AS rejected
JOIN flight_raw AS raw USING (run_id, source_line)
ORDER BY rejected.run_id, rejected.source_line
LIMIT 40;
