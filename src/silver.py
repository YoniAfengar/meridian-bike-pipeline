import csv
import hashlib
import io
import json

from src.db import connect


FIELD_NAMES = {
    "start_station_id": ("start_station_id", "start station id"),
    "end_station_id": ("end_station_id", "end station id"),
    "started_at": ("started_at", "starttime"),
    "ended_at": ("ended_at", "stoptime"),
}


def _value(row, field):
    for name in FIELD_NAMES[field]:
        value = row.get(name)
        if value is not None:
            return value.strip()
    return ""


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
                "DELETE FROM silver_trips WHERE market = %s AND window_key = %s",
                (market, window),
            )
            cur.execute(
                "DELETE FROM silver_rejects WHERE market = %s AND window_key = %s",
                (market, window),
            )

            for member_name, raw_bytes in objects:
                text = io.TextIOWrapper(
                    io.BytesIO(bytes(raw_bytes)),
                    encoding="utf-8-sig",
                    errors="replace",
                    newline="",
                )

                reader = csv.DictReader(text)

                for row_number, row in enumerate(reader, start=2):
                    start_station_id = _value(row, "start_station_id")
                    end_station_id = _value(row, "end_station_id")
                    started_at = _value(row, "started_at")
                    ended_at = _value(row, "ended_at")

                    trip_key = hashlib.sha256(
                        f"{member_name}:{row_number}".encode()
                    ).hexdigest()

                    if not (
                        start_station_id
                        and end_station_id
                        and started_at
                        and ended_at
                    ):
                        cur.execute(
                            """
                            INSERT INTO silver_rejects (
                                market, window_key, trip_key, reason, raw_row
                            )
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (
                                market,
                                window,
                                trip_key,
                                "never docked",
                                json.dumps(row),
                            ),
                        )
                        continue

                    cur.execute(
                        """
                        INSERT INTO silver_trips (
                            market,
                            window_key,
                            trip_key,
                            start_station_id,
                            end_station_id,
                            started_at,
                            ended_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            market,
                            window,
                            trip_key,
                            start_station_id,
                            end_station_id,
                            started_at,
                            ended_at,
                        ),
                    )
