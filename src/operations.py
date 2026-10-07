import calendar
from datetime import date

from src.db import connect

def record_load(job, market, window):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO operational_loads (
                    job,
                    market,
                    window_key
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (job, market, window_key)
                DO NOTHING
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

def month_complete(market, window):
    if not is_loaded("transform-to-silver", market, window):
        return False

    return all(
        is_loaded("transform-to-gold", market, day)
        for day in days_in_month(window)
    )

def next_month(window):
    year, month = map(int, window.split("-"))

    if month == 12:
        return f"{year + 1}-01"

    return f"{year}-{month + 1:02d}"

def newest_complete_month(market):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT window_key
                FROM operational_loads
                WHERE job = 'transform-to-silver'
                  AND market = %s
                  AND window_key ~ '^\\d{4}-\\d{2}$'
                ORDER BY window_key DESC
                """,
                (market,),
            )
            months = [row[0] for row in cur.fetchall()]

    for window in months:
        if month_complete(market, window):
            return window

    return None

def coverage(market):
    from src.config import EARLIEST_WINDOWS

    earliest = EARLIEST_WINDOWS[market]
    newest = newest_complete_month(market)

    complete = 0
    gaps = []
    watermark = None
    next_due = None

    window = earliest

    if newest is not None:
        while window <= newest:
            if month_complete(market, window):
                complete += 1

                if not gaps:
                    watermark = window
            else:
                gaps.append(window)

                if next_due is None:
                    next_due = window

            window = next_month(window)

    if next_due is None:
        window = earliest

        while month_complete(market, window):
            window = next_month(window)

        next_due = window

    return {
        "earliest": earliest,
        "watermark": watermark,
        "complete": complete,
        "gaps": gaps,
        "next": next_due,
    }

