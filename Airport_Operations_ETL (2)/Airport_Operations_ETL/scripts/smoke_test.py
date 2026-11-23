"""Exercise the running Docker Compose stack from the host using the Airflow API and MySQL."""

import argparse
import base64
import json
import os
import subprocess
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = "/api/v1/dags/airport_flight_departure_etl"
DEMO_IDS = ", ".join(f"'ETL{i:03d}'" for i in range(1, 9))


def api(base, token, path, method="GET", payload=None):
    request = Request(
        base + path,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Authorization": f"Basic {token}", "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urlopen(request, timeout=10) as response:
            return json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"Airflow API {exc.code}: {exc.read().decode(errors='replace')}") from exc


def run_dag(base, token, filename, label, expected="success"):
    run_id = f"demo_{label}_{uuid4().hex[:10]}"
    url = f"{DAG_PATH}/dagRuns"
    api(base, token, url, "POST", {"dag_run_id": run_id, "conf": {"source_file": filename}})
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        state = api(base, token, f"{url}/{run_id}")["state"]
        if state in {"success", "failed"}:
            if state != expected:
                raise RuntimeError(f"{run_id}: expected {expected}, got {state}. See Airflow task logs.")
            print(f"{run_id}: {state}", flush=True)
            return run_id
        time.sleep(3)
    raise TimeoutError(f"{run_id}: Airflow did not finish within 240 seconds")


def mysql(query):
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "mysql", "sh", "-c",
         'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u"$MYSQL_USER" --batch --raw --skip-column-names "$MYSQL_DATABASE"'],
        input=query,
        text=True,
        stdout=subprocess.PIPE,
        check=True,
        timeout=30,
        cwd=ROOT,
    )
    return result.stdout.strip()


def event(run_id, step, outcome):
    result = mysql(
        f"SELECT metrics FROM etl_log WHERE run_id = '{run_id}' "
        f"AND step = '{step}' AND outcome = '{outcome}' ORDER BY log_id DESC LIMIT 1;"
    )
    if not result:
        raise RuntimeError(f"Missing {outcome} SQL log for {run_id} / {step}")
    return json.loads(result)


def verify(condition, message):
    if not condition:
        raise RuntimeError(message)


def target_snapshot():
    return mysql(
        "SELECT FlightID, `Status`, ActualDepartureTime, SourceUpdatedAt "
        f"FROM Flight WHERE FlightID IN ({DEMO_IDS}) ORDER BY FlightID;"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--password", default=os.environ.get("AIRFLOW_PASSWORD", "airflow"))
    args = parser.parse_args()
    base = f"http://127.0.0.1:{args.port}"
    token = base64.b64encode(f"airflow:{args.password}".encode()).decode()
    api(base, token, DAG_PATH, "PATCH", {"is_paused": False})

    first = run_dag(base, token, "flights.csv", "initial")
    verify(event(first, "clean", "SUCCESS") == {
        "input_rows": 16, "accepted_rows": 8, "duplicate_rows": 2, "invalid_rows": 6,
    }, "Unexpected cleaning totals")
    verify(event(first, "check", "SUCCESS")["verified_rows"] == 8, "Expected eight verified rows")
    event(first, "pipeline", "SUCCESS")
    snapshot = target_snapshot()
    verify(len(snapshot.splitlines()) == 8, "Expected eight demo flights")

    run_dag(base, token, "flights.csv", "replay")
    verify(target_snapshot() == snapshot, "Replay changed the flight targets")

    run_dag(base, token, "flights_update.csv", "update")
    updated = target_snapshot()
    verify("ETL003\tDeparted\t2026-11-20 10:20:00" in updated, "ETL003 was not updated")
    verify("ETL007\tDeparted\t2026-11-21 08:30:00" in updated, "ETL007 was not updated")

    replay = run_dag(base, token, "flights.csv", "older_replay")
    verify(event(replay, "check", "SUCCESS")["newer_target_rows"] == 2, "Expected two protected targets")
    verify(target_snapshot() == updated, "Old observations overwrote newer targets")
    kpis = mysql(
        "SELECT service_date, COUNT(*), SUM(is_delayed = 1), "
        "ROUND(AVG(CASE WHEN is_delayed = 1 THEN delay_minutes END), 2) "
        f"FROM flight_departure_metrics WHERE FlightID IN ({DEMO_IDS}) GROUP BY service_date ORDER BY service_date;"
    )
    verify(kpis == "2026-11-20\t5\t3\t20.00\n2026-11-21\t3\t2\t40.00", f"Unexpected KPIs:\n{kpis}")

    failed = run_dag(base, token, "flights_bad_schema.csv", "invalid_header", "failed")
    event(failed, "ingest", "FAILED")
    event(failed, "pipeline", "FAILED")
    verify(target_snapshot() == updated, "A failed ingest changed target data")
    print("Passed: ETL, row totals, replay, late updates, KPIs and error logging.")


if __name__ == "__main__":
    main()
