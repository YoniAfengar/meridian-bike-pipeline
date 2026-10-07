from src.config import EARLIEST_WINDOWS
from src.db import connect


def schedule_enabled(market):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT enabled
                FROM operational_schedules
                WHERE market = %s
                """,
                (market,),
            )
            row = cur.fetchone()

    return bool(row and row[0])


def set_schedule(market, enabled):
    if market not in EARLIEST_WINDOWS:
        raise ValueError(f"invalid market: {market}")

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO operational_schedules (market, enabled)
                VALUES (%s, %s)
                ON CONFLICT (market)
                DO UPDATE SET enabled = EXCLUDED.enabled
                """,
                (market, enabled),
            )

    return {"market": market, "schedule": "on" if enabled else "off"}


def disable_all_schedules():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE operational_schedules SET enabled = FALSE"
            )
