"""Transactional MySQL steps and monitoring for the airport DAG."""

import json
import logging
import os
import re
from contextlib import contextmanager
from pathlib import Path

from airport_etl.cleaning import COLUMNS, clean_rows, read_source

LOGGER = logging.getLogger(__name__)
COLUMN_SQL = ", ".join(f"`{column}`" for column in COLUMNS)
FLIGHT_PARAMS = ", ".join(["%s"] * len(COLUMNS))
UPDATES_SQL = ",\n".join(
    f"`{column}` = IF(incoming.SourceUpdatedAt >= Flight.SourceUpdatedAt, incoming.`{column}`, Flight.`{column}`)"
    for column in COLUMNS[1:-1]
)
UPSERT_SQL = f"""
    INSERT INTO Flight ({COLUMN_SQL}) VALUES ({FLIGHT_PARAMS}) AS incoming
    ON DUPLICATE KEY UPDATE
        {UPDATES_SQL},
        SourceUpdatedAt = GREATEST(incoming.SourceUpdatedAt, Flight.SourceUpdatedAt)
"""
MATCH_SQL = " AND ".join(f"(target.`{column}` <=> clean.`{column}`)" for column in COLUMNS[1:])


@contextmanager
def transaction():
    import pymysql

    connection = pymysql.connect(
        host=os.environ["AIRPORT_MYSQL_HOST"],
        user=os.environ["AIRPORT_MYSQL_USER"],
        password=os.environ["AIRPORT_MYSQL_PASSWORD"],
        database=os.environ["AIRPORT_MYSQL_DATABASE"],
        charset="utf8mb4",
        connect_timeout=10,
        read_timeout=30,
        write_timeout=30,
        init_command="SET time_zone = '+00:00'",
        autocommit=False,
    )
    try:
        with connection.cursor() as cursor:
            yield cursor
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def write_log(cursor, run_id, step, attempt, outcome, message, metrics=None):
    cursor.execute(
        """
        INSERT INTO etl_log (run_id, step, attempt, outcome, metrics, message)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (run_id, step, attempt, outcome, json.dumps(metrics or {}, default=str), message[:1000]),
    )


def execute_step(step, operation, run_id, attempt):
    try:
        with transaction() as cursor:
            write_log(cursor, run_id, step, attempt, "STARTED", "Task started")
        with transaction() as cursor:
            metrics = operation(cursor, run_id)
            write_log(cursor, run_id, step, attempt, "SUCCESS", "Task completed", metrics)
    except Exception as exc:
        LOGGER.exception("%s failed for run %s", step, run_id)
        try:
            with transaction() as cursor:
                write_log(cursor, run_id, step, attempt, "FAILED", f"{type(exc).__name__}: {exc}")
        except Exception:
            LOGGER.exception("MySQL log unavailable for %s. See this Airflow task log.", step)
        raise

    LOGGER.info("%s run=%s metrics=%s", step, run_id, metrics)


def ingest(cursor, run_id, source_file):
    if not isinstance(source_file, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}\.csv", source_file):
        raise ValueError("source_file must be a CSV filename inside the input folder")
    source_dir = Path(os.environ["AIRPORT_SOURCE_DIR"]).resolve()
    path = (source_dir / source_file).resolve()
    if path.parent != source_dir:
        raise ValueError("CSV must be inside the input folder")
    if not path.is_file():
        raise FileNotFoundError(f"Input file {source_file} was not found")
    rows = read_source(path)
    cursor.execute("DELETE FROM flight_raw WHERE run_id = %s", (run_id,))
    cursor.executemany(
        "INSERT INTO flight_raw (run_id, source_line, payload) VALUES (%s, %s, %s)",
        [(run_id, line, json.dumps(payload, ensure_ascii=False)) for line, payload in rows],
    )
    return {"source_file": source_file, "input_rows": len(rows)}


def read_codes(cursor, query):
    cursor.execute(query)
    return {row[0] for row in cursor.fetchall()}


def clean(cursor, run_id):
    cursor.execute(
        "SELECT source_line, payload FROM flight_raw WHERE run_id = %s ORDER BY source_line", (run_id,)
    )
    rows = [(line, json.loads(payload)) for line, payload in cursor.fetchall()]
    airports = read_codes(cursor, "SELECT AirportCode FROM Airport")
    references = {
        "DepartureAirportCode": airports,
        "ArrivalAirportCode": airports,
        "AircraftID": read_codes(cursor, "SELECT AircraftID FROM Aircraft"),
        "GateID": read_codes(cursor, "SELECT GateID FROM Gate"),
    }
    accepted, rejected = clean_rows(rows, references)
    cursor.execute("DELETE FROM flight_clean WHERE run_id = %s", (run_id,))
    cursor.execute("DELETE FROM flight_reject WHERE run_id = %s", (run_id,))
    cursor.executemany(
        f"INSERT INTO flight_clean (run_id, {COLUMN_SQL}) VALUES (%s, {FLIGHT_PARAMS})",
        [(run_id, *flight) for flight in accepted],
    )
    cursor.executemany(
        "INSERT INTO flight_reject (run_id, source_line, reason_code, detail) VALUES (%s,%s,%s,%s)",
        [(run_id, *reject) for reject in rejected],
    )
    duplicates = sum(reason == "DUPLICATE" for _, reason, _ in rejected)
    return {
        "input_rows": len(rows), "accepted_rows": len(accepted),
        "duplicate_rows": duplicates, "invalid_rows": len(rejected) - duplicates,
    }


def load(cursor, run_id):
    cursor.execute(
        f"SELECT {COLUMN_SQL} FROM flight_clean WHERE run_id = %s ORDER BY FlightID", (run_id,)
    )
    flights = cursor.fetchall()
    # Report rows considered, since MySQL distinguishes inserts, updates and unchanged rows in rowcount.
    cursor.executemany(UPSERT_SQL, flights)
    return {"upsert_candidates": len(flights)}


def check(cursor, run_id):
    cursor.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM flight_raw WHERE run_id = %s),
          (SELECT COUNT(*) FROM flight_clean WHERE run_id = %s),
          (SELECT COUNT(*) FROM flight_reject WHERE run_id = %s)
        """,
        (run_id, run_id, run_id),
    )
    raw, accepted, rejected = cursor.fetchone()
    if raw != accepted + rejected:
        raise ValueError(f"Row counts do not reconcile: {raw=}, {accepted=}, {rejected=}")
    if not accepted:
        raise ValueError(f"No valid flights to verify: {raw=}, {rejected=}")

    cursor.execute(
        f"""
        SELECT COUNT(CASE WHEN target.FlightID IS NULL OR NOT (
                   target.SourceUpdatedAt > clean.SourceUpdatedAt OR ({MATCH_SQL})
               ) THEN 1 END),
               COUNT(CASE WHEN target.SourceUpdatedAt > clean.SourceUpdatedAt THEN 1 END)
        FROM flight_clean AS clean
        LEFT JOIN Flight AS target ON target.FlightID = clean.FlightID
        WHERE clean.run_id = %s
        """,
        (run_id,),
    )
    mismatches, newer = cursor.fetchone()
    if mismatches:
        raise ValueError(f"{mismatches} accepted observations are missing or differ in Flight")

    cursor.execute(
        """
        SELECT metrics.service_date, COUNT(*), SUM(metrics.is_delayed = 1),
               ROUND(AVG(CASE WHEN metrics.is_delayed = 1 THEN metrics.delay_minutes END), 2)
        FROM flight_departure_metrics AS metrics
        JOIN flight_clean AS clean ON clean.FlightID = metrics.FlightID
        WHERE clean.run_id = %s
        GROUP BY metrics.service_date
        ORDER BY metrics.service_date
        """,
        (run_id,),
    )
    daily = [
        {"date": day.isoformat(), "flights": count, "delayed": int(delayed or 0),
         "avg_delay_minutes": float(average) if average is not None else None}
        for day, count, delayed, average in cursor.fetchall()
    ]
    return {
        "input_rows": raw, "accepted_rows": accepted, "rejected_rows": rejected,
        "newer_target_rows": newer, "verified_rows": accepted, "daily_for_batch": daily,
    }
