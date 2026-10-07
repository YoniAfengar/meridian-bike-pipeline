from psycopg.rows import dict_row

from src.config import EARLIEST_WINDOWS
from src.db import connect
from src.operations import coverage, days_in_month, gold_day_complete


def require_market(market):
    if market not in EARLIEST_WINDOWS:
        raise ValueError(f"invalid market: {market}")


def inspect_coverage(job):
    if job not in ("trips:jc", "trips:nyc"):
        raise ValueError(f"invalid job: {job}")

    market = job.split(":", 1)[1]
    return {"job": job, **coverage(market)}


def run_records(market, request_id=None):
    require_market(market)

    with connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    r.id,
                    r.run_id,
                    r.run_type,
                    r.state,
                    r.conf,
                    r.data_interval_start,
                    (
                        SELECT x.value
                        FROM xcom x
                        WHERE x.dag_run_id = r.id
                          AND x.task_id = 'which_month'
                          AND x.key = 'return_value'
                          AND x.map_index = -1
                    ) AS selected_month,
                    (
                        SELECT x.value
                        FROM xcom x
                        WHERE x.dag_run_id = r.id
                          AND x.task_id = 'transform_to_gold'
                          AND x.key = 'return_value'
                          AND x.map_index = -1
                    ) AS gold_result,
                    (
                        SELECT COALESCE(
                            jsonb_object_agg(
                                t.task_id,
                                jsonb_build_object(
                                    'state', t.state,
                                    'tries', t.try_number
                                )
                            ),
                            '{}'::jsonb
                        )
                        FROM task_instance t
                        WHERE t.dag_id = r.dag_id
                          AND t.run_id = r.run_id
                    ) AS tasks
                FROM dag_run r
                WHERE r.dag_id = %s
                  AND (
                      %s::text IS NULL
                      OR r.conf ->> 'operational_request_id' = %s
                  )
                ORDER BY r.queued_at DESC NULLS LAST, r.id DESC
                """,
                (f"pipeline_{market}", request_id, request_id),
            )
            return cur.fetchall()


def run_month(record):
    if record["selected_month"] is not None:
        return record["selected_month"]

    requested = (record["conf"] or {}).get("month")
    if requested is not None:
        return requested

    interval = record["data_interval_start"]
    if interval is not None:
        return interval.strftime("%Y-%m")

    return None


def format_run(market, record):
    month = run_month(record)
    raw_tasks = record["tasks"]
    tasks = {}

    for task_id, layer in (
        ("ingest_to_bronze", "ingest-to-bronze"),
        ("transform_to_silver", "transform-to-silver"),
    ):
        name = f"{layer} trips:{market}"
        if month is not None:
            name = f"{name} {month}"

        tasks[name] = raw_tasks.get(
            task_id,
            {"state": None, "tries": 0},
        )

    gold = dict(
        raw_tasks.get(
            "transform_to_gold",
            {"state": None, "tries": 0},
        )
    )
    result = record["gold_result"]

    if isinstance(result, dict):
        gold.update(result)
    else:
        active = gold["state"] in (
            "running",
            "success",
            "failed",
            "up_for_retry",
        )
        days = days_in_month(month) if month and active and gold["tries"] > 0 else []
        gold["days"] = len(days)
        gold["failed_days"] = (
            [
                day
                for day in days
                if not gold_day_complete(market, day)
            ]
            if gold["state"] in ("failed", "up_for_retry")
            else []
        )

    tasks["transform-to-gold station-daily"] = gold

    return {
        "month": month,
        "type": record["run_type"],
        "state": record["state"],
        "tasks": tasks,
    }


def inspect_runs(market):
    return {
        "market": market,
        "runs": [
            format_run(market, record)
            for record in run_records(market)
        ],
    }
