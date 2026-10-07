import calendar
from datetime import date

from src.config import EARLIEST_WINDOWS
from src.db import connect


def record_load(job, market, window):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO operational_loads (
                    job, market, window_key, loaded_at
                )
                VALUES (%s, %s, %s, clock_timestamp())
                ON CONFLICT (job, market, window_key)
                DO UPDATE SET loaded_at = EXCLUDED.loaded_at
                """,
                (job, market, window),
            )


def is_loaded(job, market, window):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM operational_loads
                    WHERE job = %s
                      AND market = %s
                      AND window_key = %s
                )
                """,
                (job, market, window),
            )
            return cur.fetchone()[0]


def days_in_month(window):
    year, month = map(int, window.split("-"))
    day_count = calendar.monthrange(year, month)[1]

    return [
        date(year, month, day).isoformat()
        for day in range(1, day_count + 1)
    ]


def next_month(window):
    year, month = map(int, window.split("-"))

    if month == 12:
        return f"{year + 1:04d}-01"

    return f"{year:04d}-{month + 1:02d}"


def complete_months(market):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT job, window_key, loaded_at
                FROM operational_loads
                WHERE market = %s
                  AND job IN (
                      'transform-to-silver',
                      'transform-to-gold'
                  )
                """,
                (market,),
            )
            rows = cur.fetchall()

    silver = {
        window: loaded_at
        for job, window, loaded_at in rows
        if job == "transform-to-silver"
    }
    gold = {
        window: loaded_at
        for job, window, loaded_at in rows
        if job == "transform-to-gold"
    }

    return {
        window
        for window, silver_time in silver.items()
        if all(
            day in gold and gold[day] > silver_time
            for day in days_in_month(window)
        )
    }


def month_complete(market, window):
    return window in complete_months(market)


def newest_complete_month(market):
    return max(complete_months(market), default=None)


def coverage(market):
    earliest = EARLIEST_WINDOWS[market]
    completed = {
        window
        for window in complete_months(market)
        if window >= earliest
    }

    next_due = earliest
    watermark = None

    while next_due in completed:
        watermark = next_due
        next_due = next_month(next_due)

    gaps = []
    newest = max(completed, default=None)
    window = earliest

    while newest is not None and window <= newest:
        if window not in completed:
            gaps.append(window)
        window = next_month(window)

    return {
        "earliest": earliest,
        "watermark": watermark,
        "complete": len(completed),
        "gaps": gaps,
        "next": next_due,
    }


def gold_day_complete(market, day):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM operational_loads AS gold
                    JOIN operational_loads AS silver
                      ON silver.market = gold.market
                     AND silver.job = 'transform-to-silver'
                     AND silver.window_key = %s
                    WHERE gold.job = 'transform-to-gold'
                      AND gold.market = %s
                      AND gold.window_key = %s
                      AND gold.loaded_at > silver.loaded_at
                )
                """,
                (day[:7], market, day),
            )
            return cur.fetchone()[0]


def run_gold_batch(market, window):
    from src.gold import transform as transform_gold

    days = days_in_month(window)
    failed_days = []

    for day in days:
        try:
            if gold_day_complete(market, day):
                continue

            with connect() as lock_conn:
                with lock_conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                        (f"meridian:gold:{day}",),
                    )

                    if gold_day_complete(market, day):
                        continue

                    transform_gold(day)
                    record_load("transform-to-gold", market, day)
        except Exception:
            failed_days.append(day)

    if failed_days:
        raise RuntimeError(
            f"gold failed for days: {','.join(failed_days)}"
        )

    return {
        "days": len(days),
        "failed_days": [],
    }


def run_bronze(market, window):
    from src.ingest import ingest

    ingest(market, window)
    record_load("ingest-to-bronze", market, window)


def record_silver(market, window):
    record_load("transform-to-silver", market, window)