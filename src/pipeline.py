import os
import re
import subprocess
from contextlib import contextmanager
from datetime import date

from src.config import EARLIEST_WINDOWS
from src.db import connect
from src.ingest import _choose_object
from src.operations import (
    coverage,
    month_complete,
    record_silver,
    run_bronze,
    run_gold_batch,
)


def validate_month(market, window):
    from airflow.exceptions import AirflowFailException

    if market not in EARLIEST_WINDOWS:
        raise AirflowFailException(f"invalid market: {market}")

    if not isinstance(window, str) or not re.fullmatch(
        r"\d{4}-\d{2}", window
    ):
        raise AirflowFailException(f"invalid month: {window}")

    try:
        date.fromisoformat(f"{window}-01")
    except ValueError as exc:
        raise AirflowFailException(f"invalid month: {window}") from exc

    if window < EARLIEST_WINDOWS[market]:
        raise AirflowFailException(
            f"{market} history starts at {EARLIEST_WINDOWS[market]}: "
            f"{window} is too early"
        )

    return window


def source_published(market, window):
    if window is None:
        return False

    try:
        _choose_object(market, window)
    except ValueError:
        return False

    return True


def select_month(market):
    from airflow.sdk import get_current_context

    context = get_current_context()
    dag_run = context["dag_run"]
    requested = (dag_run.conf or {}).get("month")

    if requested is not None:
        return validate_month(market, requested)

    if dag_run.run_type == "manual":
        window = validate_month(market, coverage(market)["next"])

        if not source_published(market, window):
            return None

        return window

    window = context["data_interval_start"].strftime("%Y-%m")
    return validate_month(market, window)


def work_due(market, window):
    from airflow.sdk import get_current_context

    if window is None:
        return False

    dag_run = get_current_context()["dag_run"]

    if dag_run.run_type == "scheduled":
        return not month_complete(market, window)

    return True


@contextmanager
def month_lock(market, window):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"meridian:month:{market}:{window}",),
            )
            yield


def bronze_task(market, window):
    with month_lock(market, window):
        run_bronze(market, window)


def silver_task(market, window):
    project_dir = os.environ["MERIDIAN_PROJECT_DIR"]

    with month_lock(market, window):
        subprocess.run(
            [
                "just",
                "run",
                "transform-to-silver",
                f"trips:{market}",
                window,
            ],
            cwd=project_dir,
            check=True,
        )
        record_silver(market, window)


def gold_task(market, window):
    with month_lock(market, window):
        return run_gold_batch(market, window)
