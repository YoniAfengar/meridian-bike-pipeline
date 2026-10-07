import json

from src.db import connect


def inspect_bronze(job, market, window):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(DISTINCT object_key), COALESCE(SUM(row_count), 0)
                FROM bronze_objects
                WHERE market = %s AND window_key = %s
                """,
                (market, window),
            )
            objects, rows = cur.fetchone()

    print(json.dumps({
        "layer": "bronze",
        "job": job,
        "window": window,
        "objects": objects,
        "rows": int(rows),
    }, separators=(",", ":")))


def inspect_silver(job, market, window):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM silver_trips
                WHERE market = %s AND window_key = %s
                """,
                (market, window),
            )
            rows = cur.fetchone()[0]

            cur.execute(
                """
                SELECT reason, COUNT(*)
                FROM silver_rejects
                WHERE market = %s AND window_key = %s
                GROUP BY reason
                ORDER BY reason
                """,
                (market, window),
            )
            reason_rows = cur.fetchall()

    reasons = {reason: count for reason, count in reason_rows}
    rejects = sum(reasons.values())

    print(json.dumps({
        "layer": "silver",
        "job": job,
        "window": window,
        "rows": rows,
        "rejects": rejects,
        "reasons": reasons,
    }, separators=(",", ":")))
