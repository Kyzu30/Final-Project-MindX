"""Regression checks for the demo dataset and flight validation rules."""

import sys
import tempfile
import unittest
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))

from airport_etl.cleaning import COLUMNS, clean_rows, read_source

AIRPORTS = {"SGN", "HAN", "DAD", "CXR", "PQC"}
REFERENCES = {
    "DepartureAirportCode": AIRPORTS,
    "ArrivalAirportCode": AIRPORTS,
    "AircraftID": {"AC001", "AC004", "AC006"},
    "GateID": {"G01", "G02", "G03", "G04"},
}


class FlightCleaningTests(unittest.TestCase):
    def test_demo_records_reconcile_and_expected_reasons_are_recorded(self):
        raw = read_source(ROOT / "data/flights.csv")
        accepted, rejected = clean_rows(raw, REFERENCES)
        self.assertEqual((len(raw), len(accepted), len(rejected)), (16, 8, 8))
        self.assertEqual(
            Counter(reason for _, reason, _ in rejected),
            Counter(DUPLICATE=2, MISSING_REQUIRED=1, INVALID_DATETIME=1, INVALID_STATUS=1,
                    UNKNOWN_REFERENCE=1, INVALID_DURATION=1, INVALID_ACTUAL_TIME=1),
        )

    def test_latest_observation_is_kept_when_an_older_duplicate_appears_later(self):
        raw = read_source(ROOT / "data/flights.csv")
        accepted, rejected = clean_rows(raw, REFERENCES)
        by_id = {flight[0]: flight for flight in accepted}
        self.assertEqual(by_id["ETL001"][3], datetime(2026, 11, 20, 8, 25))
        self.assertIn((10, "DUPLICATE", "Kept the observation on line 2"), rejected)

    def test_pending_and_cancelled_flights_can_have_no_actual_departure(self):
        accepted, _ = clean_rows(read_source(ROOT / "data/flights.csv"), REFERENCES)
        by_id = {flight[0]: flight for flight in accepted}
        self.assertIsNone(by_id["ETL003"][3])
        self.assertIsNone(by_id["ETL004"][3])
        self.assertIsNone(by_id["ETL007"][3])
        self.assertEqual(by_id["ETL002"][6:8], ("HAN", "SGN"))
        self.assertEqual(by_id["ETL005"][5], "Arrived")

    def test_malformed_header_is_reported(self):
        with self.assertRaisesRegex(ValueError, "Invalid CSV header"):
            read_source(ROOT / "data/flights_bad_schema.csv")

    def test_short_line_is_quarantined_and_keeps_the_csv_line_number(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "short.csv"
            path.write_text(",".join(COLUMNS) + "\n\nETL900,2026-11-21\n", encoding="utf-8-sig")
            accepted, rejected = clean_rows(read_source(path), REFERENCES)
        self.assertEqual(accepted, [])
        self.assertEqual(rejected, [(3, "INVALID_COLUMN_COUNT", "Expected 11 fields")])

    def test_update_file_carries_actual_departures_and_newer_source_times(self):
        original, _ = clean_rows(read_source(ROOT / "data/flights.csv"), REFERENCES)
        updates, rejected = clean_rows(read_source(ROOT / "data/flights_update.csv"), REFERENCES)
        self.assertEqual(rejected, [])
        original_by_id = {flight[0]: flight for flight in original}
        self.assertEqual([flight[0] for flight in updates], ["ETL003", "ETL007"])
        for flight in updates:
            self.assertIsNotNone(flight[3])
            self.assertGreater(flight[-1], original_by_id[flight[0]][-1])
