import json
import threading
from unittest.mock import patch

from src.db import connect
from src.gold import transform
from src.operations import days_in_month, gold_day_complete, month_complete
from src.pipeline import gold_task, silver_task


market = "jc"
month = "2026-06"
failed_day = "2026-06-02"
days = days_in_month(month)


def snapshot():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT station_id, day, departures, arrivals
                FROM gold_station_daily
                WHERE market = %s
                  AND day >= %s
                  AND day < %s
                ORDER BY day, station_id
                """,
                (market, "2026-06-01", "2026-07-01"),
            )
            return cur.fetchall()


baseline = snapshot()
assert baseline, "FAIL: baseline gold data is empty"
assert month_complete(market, month), "FAIL: baseline month is incomplete"

silver_task(market, month)
assert not month_complete(market, month), (
    "FAIL: silver reload did not invalidate completion"
)

calls = []
failure_injected = False
stop = threading.Event()
observations = []
errors = []


def observe():
    while not stop.is_set():
        try:
            current = snapshot()
            observations.append(1)
            if current != baseline:
                errors.append("Gold data changed or became incomplete")
        except Exception as exc:
            errors.append(str(exc))
        stop.wait(0.05)


def flaky_transform(day):
    global failure_injected
    calls.append(day)

    if day == failed_day and not failure_injected:
        failure_injected = True
        raise RuntimeError(f"Injected failure for {day}")

    transform(day)


observer = threading.Thread(target=observe, daemon=True)
observer.start()

try:
    with patch("src.gold.transform", side_effect=flaky_transform):
        try:
            gold_task(market, month)
        except RuntimeError as exc:
            assert failed_day in str(exc), (
                f"FAIL: batch did not name the failed day: {exc}"
            )
        else:
            raise AssertionError("FAIL: first gold batch did not fail")

        assert calls == days, (
            "FAIL: the batch did not attempt every day"
        )
        recorded = [
            day for day in days if gold_day_complete(market, day)
        ]
        assert recorded == [
            day for day in days if day != failed_day
        ], "FAIL: successful days were not recorded correctly"

        assert not month_complete(market, month), (
            "FAIL: month is complete despite a failed gold day"
        )

        calls.clear()
        result = gold_task(market, month)

        assert calls == [failed_day], (
            f"FAIL: retry ran unexpected days: {calls}"
        )
        assert result == {"days": 30, "failed_days": []}
        assert month_complete(market, month), (
            "FAIL: month is incomplete after retry"
        )
finally:
    stop.set()
    observer.join(timeout=5)

assert observations, "FAIL: no concurrent read was performed"
assert not errors, f"FAIL: concurrent reads: {errors}"
assert snapshot() == baseline, "FAIL: final gold answers changed"

print(json.dumps({
    "test": "gold_day_failure_and_retry",
    "passed": True,
    "first_attempt_days": 30,
    "successful_days_before_retry": 29,
    "retry_days": [failed_day],
    "concurrent_reads": len(observations),
    "answers_unchanged": True,
}))
