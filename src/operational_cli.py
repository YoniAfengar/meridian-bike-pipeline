import argparse
import json
import re
import subprocess
import sys
import time
import uuid
from datetime import date

from src.db import connect
from src.operational_inspect import (
    format_run,
    inspect_coverage,
    inspect_runs,
    require_market,
    run_records,
)
from src.operations import next_month
from src.schedules import disable_all_schedules, set_schedule


def emit(value):
    print(json.dumps(value, separators=(",", ":")), flush=True)


def require_month(window):
    if not re.fullmatch(r"\d{4}-\d{2}", window):
        raise ValueError(f"invalid month: {window}")

    date.fromisoformat(f"{window}-01")
    return window


def airflow_command(arguments):
    result = subprocess.run(
        ["airflow", *arguments],
        capture_output=True,
        text=True,
    )

    if result.returncode:
        raise RuntimeError(
            result.stderr.strip()
            or result.stdout.strip()
            or "Airflow command failed"
        )


def backfill_details(request_id):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM backfill
                WHERE dag_run_conf ->> 'operational_request_id' = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (request_id,),
            )
            row = cur.fetchone()

            if row is None:
                raise RuntimeError("Airflow did not create the backfill")

            backfill_id = row[0]
            cur.execute(
                "SELECT id FROM dag_run WHERE backfill_id = %s",
                (backfill_id,),
            )
            return {item[0] for item in cur.fetchall()}


def wait_for_runs(market, request_id, expected, backfill_ids=None):
    while True:
        if backfill_ids is None:
            records = run_records(market, request_id)
        else:
            records = [
                record
                for record in run_records(market)
                if record["id"] in backfill_ids
            ]

        if len(records) == expected and all(
            record["state"] in ("success", "failed")
            for record in records
        ):
            return records

        time.sleep(2)


def run_pipeline(market, windows):
    require_market(market)

    if len(windows) > 2:
        raise ValueError("pipeline requires zero, one or two months")

    for window in windows:
        require_month(window)

    request_id = uuid.uuid4().hex
    configuration = {"operational_request_id": request_id}
    dag_id = f"pipeline_{market}"
    backfill_ids = None
    expected = 1

    if len(windows) < 2:
        if windows:
            configuration["month"] = windows[0]

        airflow_command(
            [
                "dags",
                "trigger",
                dag_id,
                "--run-id",
                f"manual__meridian_{request_id}",
                "--conf",
                json.dumps(configuration),
            ]
        )
    else:
        first, last = windows

        if first > last:
            raise ValueError("first month must not be after last month")

        expected = 0
        window = first

        while window <= last:
            expected += 1
            window = next_month(window)

        code = (
            "import json, sys, pendulum; "
            "from airflow.models.backfill import ReprocessBehavior, _create_backfill; "
            "_create_backfill("
            "dag_id=sys.argv[1], "
            "from_date=pendulum.parse(sys.argv[2], tz='UTC'), "
            "to_date=pendulum.parse(sys.argv[3], tz='UTC'), "
            "max_active_runs=16, "
            "reverse=False, "
            "dag_run_conf=json.loads(sys.argv[4]), "
            "triggering_user_name='meridian-cli', "
            "reprocess_behavior=ReprocessBehavior.COMPLETED)"
        )
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                code,
                dag_id,
                f"{first}-01",
                f"{last}-01",
                json.dumps(configuration),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode:
            raise RuntimeError(
                result.stderr.strip()
                or result.stdout.strip()
                or "Airflow backfill creation failed"
            )

        backfill_ids = backfill_details(request_id)

        if len(backfill_ids) != expected:
            raise RuntimeError(
                f"expected {expected} backfill runs, "
                f"Airflow created {len(backfill_ids)}; "
                "check for existing active runs"
            )

    records = wait_for_runs(
        market,
        request_id,
        expected,
        backfill_ids,
    )
    formatted = [format_run(market, record) for record in records]

    if len(windows) == 2:
        emit({"market": market, "runs": formatted})
    else:
        result = {"market": market, **formatted[0]}

        if records[0]["tasks"].get(
            "ingest_to_bronze", {}
        ).get("state") == "skipped":
            result["work"] = "skipped"

        emit(result)

    return 1 if any(
        record["state"] == "failed" for record in records
    ) else 0


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run")
    run.add_argument("market")
    run.add_argument("months", nargs="*")

    coverage = commands.add_parser("coverage")
    coverage.add_argument("job")

    runs = commands.add_parser("runs")
    runs.add_argument("market")

    schedule = commands.add_parser("schedule")
    schedule.add_argument("market")
    schedule.add_argument("state", choices=("on", "off"))

    commands.add_parser("schedules-off")
    args = parser.parse_args()

    try:
        if args.command == "run":
            return run_pipeline(args.market, args.months)

        if args.command == "coverage":
            emit(inspect_coverage(args.job))
        elif args.command == "runs":
            emit(inspect_runs(args.market))
        elif args.command == "schedule":
            emit(set_schedule(args.market, args.state == "on"))
        elif args.command == "schedules-off":
            disable_all_schedules()

        return 0
    except (ValueError, RuntimeError) as exc:
        emit({"error": str(exc)})
        return 1


if __name__ == "__main__":
    sys.exit(main())
