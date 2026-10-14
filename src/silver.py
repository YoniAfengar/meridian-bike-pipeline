import csv
import hashlib
import io
import json

from src.db import connect


FIELD_NAMES = {
    "start_station_id": ("start_station_id", "start station id"),
    "end_station_id": ("end_station_id", "end station id"),
    "started_at": ("started_at", "starttime", "start time"),
    "ended_at": ("ended_at", "stoptime", "stop time"),
}


def _normalized_name(name):
    return name.strip().lower()


def _validate_headers(fieldnames):
    available = {
        _normalized_name(name)
        for name in (fieldnames or [])
        if name is not None
    }

    missing = [
        field
        for field, aliases in FIELD_NAMES.items()
        if not any(alias in available for alias in aliases)
    ]

    if missing:
        raise ValueError(
            f"missing required CSV columns: {', '.join(missing)}"
        )


def _value(row, field):
    normalized = {
        _normalized_name(name): value
        for name, value in row.items()
        if name is not None
    }

    for name in FIELD_NAMES[field]:
        value = normalized.get(name)
        if value is not None:
            return value.strip()

    return ""


def _conform(row):
    conformed = {
        field: _value(row, field)
        for field in FIELD_NAMES
    }

    missing = [
        field
        for field, value in conformed.items()
        if not value
    ]

    if missing:
        return None, f"missing {', '.join(missing)}"

    return conformed, None


def _read_rows(member_name, raw_bytes):
    text = io.TextIOWrapper(
        io.BytesIO(bytes(raw_bytes)),
        encoding="utf-8-sig",
        errors="replace",
        newline="",
    )

    reader = csv.DictReader(text)
    _validate_headers(reader.fieldnames)

    for row_number, row in enumerate(reader, start=2):
        trip_key = hashlib.sha256(
            f"{member_name}:{row_number}".encode()
        ).hexdigest()

        yield trip_key, row


def _write_trips(cur, rows):
    with cur.copy(
        """
        COPY silver_trips (
            market,
            window_key,
            trip_key,
            start_station_id,
            end_station_id,
            started_at,
            ended_at
        )
        FROM STDIN
        """
    ) as copy:
        for row in rows:
            copy.write_row(row)


def _write_rejects(cur, rows):
    with cur.copy(
        """
        COPY silver_rejects (
            market,
            window_key,
            trip_key,
            reason,
            raw_row
        )
        FROM STDIN
        """
    ) as copy:
        for row in rows:
            copy.write_row(row)


def transform(market, window):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT member_name, raw_bytes
                FROM bronze_objects
                WHERE market = %s AND window_key = %s
                ORDER BY member_name
                """,
                (market, window),
            )
            objects = cur.fetchall()

            cur.execute(
                """
                DELETE FROM silver_trips
                WHERE market = %s AND window_key = %s
                """,
                (market, window),
            )
            cur.execute(
                """
                DELETE FROM silver_rejects
                WHERE market = %s AND window_key = %s
                """,
                (market, window),
            )

            trips = []
            rejects = []

            for member_name, raw_bytes in objects:
                for trip_key, row in _read_rows(member_name, raw_bytes):
                    conformed, reason = _conform(row)

                    if reason:
                        rejects.append(
                            (
                                market,
                                window,
                                trip_key,
                                reason,
                                json.dumps(row),
                            )
                        )
                        continue

                    trips.append(
                        (
                            market,
                            window,
                            trip_key,
                            conformed["start_station_id"],
                            conformed["end_station_id"],
                            conformed["started_at"],
                            conformed["ended_at"],
                        )
                    )

            _write_trips(cur, trips)
            _write_rejects(cur, rejects)