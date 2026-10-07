import json
import os
import subprocess
import time


environment = {
    **os.environ,
    "COMPOSE_PROJECT_NAME": "meridian-night-check",
    "AIRFLOW_PORT": "8081",
}


def command(arguments):
    result = subprocess.run(
        arguments,
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def coverage():
    return json.loads(command([
        "just", "inspect", "coverage", "trips:jc",
    ]))


def run_counts():
    statement = """
        SELECT json_build_object(
            'total', COUNT(*),
            'scheduled', COUNT(*) FILTER (
                WHERE run_type = 'scheduled'
            ),
            'other_types', COUNT(*) FILTER (
                WHERE run_type <> 'scheduled'
            ),
            'failed', COUNT(*) FILTER (
                WHERE state = 'failed'
            ),
            'success', COUNT(*) FILTER (
                WHERE state = 'success'
            )
        )
        FROM dag_run
        WHERE dag_id = 'pipeline_jc';
    """
    return json.loads(command([
        "docker", "compose", "exec", "-T", "db",
        "psql", "-U", "meridian", "-d", "meridian",
        "-X", "-q", "-A", "-t",
        "-v", "ON_ERROR_STOP=1", "-c", statement,
    ]))


initial = coverage()
assert initial["complete"] == 0
assert initial["watermark"] is None
assert run_counts()["total"] == 0, (
    "FAIL: this test requires a fresh Airflow history"
)

started = time.monotonic()
command(["just", "schedule", "jc", "on"])

august_seconds = None
last_progress = None
last_print = 0
passed = False

try:
    while time.monotonic() - started <= 1800:
        current = coverage()
        counts = run_counts()
        elapsed = time.monotonic() - started
        watermark = current["watermark"]

        assert counts["other_types"] == 0, (
            f"FAIL: non-scheduled runs appeared: {counts}"
        )
        assert counts["failed"] == 0, (
            f"FAIL: scheduled runs failed: {counts}"
        )

        if (
            watermark is not None
            and watermark >= "2026-08"
            and current["complete"] >= 68
            and not current["gaps"]
            and august_seconds is None
        ):
            august_seconds = round(elapsed, 2)

        progress = (
            current["complete"],
            watermark,
            counts["success"],
        )
        if progress != last_progress or elapsed - last_print >= 30:
            print(json.dumps({
                "status": "running",
                "elapsed_seconds": round(elapsed, 2),
                "complete": current["complete"],
                "watermark": watermark,
                "gap_count": len(current["gaps"]),
                "runs": counts,
            }), flush=True)
            last_progress = progress
            last_print = elapsed

        if (
            watermark is not None
            and watermark >= "2026-09"
            and current["complete"] >= 69
            and not current["gaps"]
            and counts["success"] == counts["total"]
        ):
            assert august_seconds is not None
            passed = True
            print(json.dumps({
                "test": "fresh_stack_scheduled_history",
                "passed": True,
                "seconds_to_required_68_months": august_seconds,
                "total_seconds": round(elapsed, 2),
                "complete": current["complete"],
                "watermark": watermark,
                "gaps": current["gaps"],
                "run_types": "scheduled",
            }), flush=True)
            break

        time.sleep(10)

    assert passed, "FAIL: history did not finish within 30 minutes"
finally:
    if not passed:
        subprocess.run(
            ["just", "schedule", "jc", "off"],
            env=environment,
            capture_output=True,
            text=True,
        )
