"""Pure CSV and flight validation functions used by the Airflow tasks."""

import csv
import re
from datetime import date, datetime

COLUMNS = (
    "FlightID", "Date", "DepartureTime", "ActualDepartureTime", "ArrivalTime",
    "Status", "DepartureAirportCode", "ArrivalAirportCode", "AircraftID",
    "GateID", "SourceUpdatedAt",
)
STATUSES = {
    status.casefold(): status
    for status in ("Scheduled", "Delayed", "Departed", "Arrived", "Cancelled")
}
DATE_FORMAT = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
DATETIME_FORMAT = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}")


class InvalidFlight(ValueError):
    def __init__(self, code, detail):
        super().__init__(detail)
        self.code = code


def read_source(path):
    """Read a UTF-8 file and retain the starting physical line of each record."""
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        header = [name.strip() for name in next(reader, [])]
        if len(header) != len(COLUMNS) or set(header) != set(COLUMNS):
            missing = ", ".join(sorted(set(COLUMNS) - set(header))) or "none"
            raise ValueError(f"Invalid CSV header. Missing columns: {missing}. Expected: {', '.join(COLUMNS)}")
        rows = []
        while True:
            row_number = reader.line_num + 1
            values = next(reader, None)
            if values is None:
                break
            if values:
                rows.append((row_number, {
                    "fields": dict(zip(header, values)),
                    "width": len(values),
                    "extra_fields": values[len(header):],
                }))
        return rows


def parse_timestamp(text, field):
    if not DATETIME_FORMAT.fullmatch(text):
        raise InvalidFlight("INVALID_DATETIME", f"{field} must be YYYY-MM-DD HH:MM:SS")
    try:
        stamp = datetime.fromisoformat(text)
        if stamp.year < 1000:
            raise ValueError("year out of range")
    except ValueError as exc:
        raise InvalidFlight("INVALID_DATETIME", f"{field} is outside the valid datetime range") from exc
    return stamp


def clean_flight(payload, references):
    if payload["width"] != len(COLUMNS):
        raise InvalidFlight("INVALID_COLUMN_COUNT", f"Expected {len(COLUMNS)} fields")
    values = {column: payload["fields"][column].strip() for column in COLUMNS}
    for column in COLUMNS:
        if column != "ActualDepartureTime" and not values[column]:
            raise InvalidFlight("MISSING_REQUIRED", f"{column} is empty")

    for column, pattern in {
        "FlightID": r"[A-Z0-9][A-Z0-9-]{0,19}",
        "DepartureAirportCode": r"[A-Z]{3}",
        "ArrivalAirportCode": r"[A-Z]{3}",
        "AircraftID": r"[A-Z0-9][A-Z0-9-]{0,9}",
        "GateID": r"[A-Z0-9][A-Z0-9-]{0,9}",
    }.items():
        values[column] = values[column].upper()
        if not re.fullmatch(pattern, values[column]):
            raise InvalidFlight("INVALID_CODE", f"{column} has an invalid format")

    status = STATUSES.get(values["Status"].casefold())
    if status is None:
        raise InvalidFlight("INVALID_STATUS", "Status is outside the allowed list")
    values["Status"] = status

    try:
        if not DATE_FORMAT.fullmatch(values["Date"]):
            raise ValueError("bad format")
        service_date = date.fromisoformat(values["Date"])
        if service_date.year < 1000:
            raise ValueError("year out of range")
    except ValueError as exc:
        raise InvalidFlight("INVALID_DATE", "Date must be a valid YYYY-MM-DD date") from exc
    values["Date"] = service_date
    for field in ("DepartureTime", "ArrivalTime", "SourceUpdatedAt"):
        values[field] = parse_timestamp(values[field], field)
    actual = values["ActualDepartureTime"]
    values["ActualDepartureTime"] = parse_timestamp(actual, "ActualDepartureTime") if actual else None

    if values["DepartureTime"].date() != service_date:
        raise InvalidFlight("DATE_MISMATCH", "Date must equal the scheduled departure date")
    if values["ArrivalTime"] <= values["DepartureTime"]:
        raise InvalidFlight("INVALID_DURATION", "Scheduled arrival must be after scheduled departure")
    if (status in {"Departed", "Arrived"}) != (values["ActualDepartureTime"] is not None):
        raise InvalidFlight("INVALID_ACTUAL_TIME", "Only Departed or Arrived requires an actual departure")
    if values["ActualDepartureTime"] and values["SourceUpdatedAt"] < values["ActualDepartureTime"]:
        raise InvalidFlight("INVALID_SOURCE_TIME", "SourceUpdatedAt precedes actual departure")
    if values["DepartureAirportCode"] == values["ArrivalAirportCode"]:
        raise InvalidFlight("SAME_AIRPORT", "Departure and arrival airports must differ")
    for column, allowed_codes in references.items():
        if values[column] not in allowed_codes:
            raise InvalidFlight("UNKNOWN_REFERENCE", f"{column} is missing from the lookup table")

    return tuple(values[column] for column in COLUMNS)


def clean_rows(rows, references):
    """Keep the newest valid observation per flight, breaking timestamp ties by file order."""
    accepted = {}
    rejected = []
    for row_number, payload in rows:
        try:
            flight = clean_flight(payload, references)
        except InvalidFlight as exc:
            rejected.append((row_number, exc.code, str(exc)))
            continue

        flight_id = flight[0]
        if flight_id in accepted:
            kept_row, kept_flight = accepted[flight_id]
            if flight[-1] >= kept_flight[-1]:
                accepted[flight_id] = (row_number, flight)
                rejected.append((kept_row, "DUPLICATE", f"Kept the observation on line {row_number}"))
            else:
                rejected.append((row_number, "DUPLICATE", f"Kept the observation on line {kept_row}"))
        else:
            accepted[flight_id] = (row_number, flight)

    return [flight for _, flight in accepted.values()], sorted(rejected)
