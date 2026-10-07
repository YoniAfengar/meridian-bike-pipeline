import json
import subprocess
import time
import uuid


run_id = f"manual__interrupted_{uuid.uuid4().hex}"
scheduler_stopped = False
holder = None


def command(arguments):
    return subprocess.run(
        arguments,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def sql(statement):
    return command([
        "docker", "compose", "exec", "-T", "db",
        "psql", "-U", "meridian", "-d", "meridian",
        "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1",
        "-c", statement,
    ])


def coverage():
    code = (
        "import json; from src.operations import coverage; "
        "print(json.dumps(coverage('jc')))"
    )
    return json.loads(command([
        "docker", "compose", "exec", "-T",
        "airflow-api-server", "python", "-c", code,
    ]))


baseline = coverage()
assert baseline["watermark"] >= "2021-03", (
    "FAIL: January through March must already be complete"
)

lock_code = """
import sys
from src.db import connect

with connect() as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            ("meridian:gold:2021-03-01",),
        )
        print("READY", flush=True)
        sys.stdin.readline()
"""

try:
    holder = subprocess.Popen(
        [
            "docker", "compose", "exec", "-T",
            "airflow-api-server", "python", "-c", lock_code,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert holder.stdout.readline().strip() == "READY"

    command([
        "docker", "compose", "exec", "-T", "airflow-scheduler",
        "airflow", "dags", "trigger", "pipeline_jc",
        "--run-id", run_id,
        "--conf", '{"month":"2021-03"}',
    ])

    deadline = time.monotonic() + 180
    blocked = False

    while time.monotonic() < deadline:
        tasks = json.loads(sql(f"""
            SELECT COALESCE(
                jsonb_object_agg(task_id, state), '{{}}'::jsonb
            )
            FROM task_instance
            WHERE dag_id = 'pipeline_jc'
              AND run_id = '{run_id}';
        """))
        if (
            tasks.get("transform_to_silver") == "success"
            and tasks.get("transform_to_gold") == "running"
        ):
            blocked = True
            break
        time.sleep(0.5)

    assert blocked, "FAIL: did not reach gold after successful silver"

    command(["docker", "compose", "stop", "airflow-scheduler"])
    scheduler_stopped = True

    sql(f"""
        BEGIN;
        UPDATE task_instance
        SET state = 'failed', end_date = NOW()
        WHERE dag_id = 'pipeline_jc'
          AND run_id = '{run_id}'
          AND (
              state IS NULL
              OR state NOT IN (
                  'success', 'failed', 'skipped',
                  'upstream_failed', 'removed'
              )
          );
        UPDATE dag_run
        SET state = 'failed', end_date = NOW()
        WHERE dag_id = 'pipeline_jc' AND run_id = '{run_id}';
        COMMIT;
    """)

    interrupted = coverage()
    assert interrupted["next"] == "2021-03", (
        f"FAIL: interrupted month is not next: {interrupted}"
    )
    assert interrupted["watermark"] == "2021-02"
    assert interrupted["complete"] == baseline["complete"] - 1

    holder.communicate(input="\n", timeout=30)
    holder = None

    command(["docker", "compose", "start", "airflow-scheduler"])
    scheduler_stopped = False

    recovered = json.loads(command(["just", "run", "pipeline", "jc"]))
    assert recovered["month"] == "2021-03"
    assert recovered["state"] == "success"
    assert coverage() == baseline, (
        "FAIL: recovery did not restore baseline coverage"
    )

    print(json.dumps({
        "test": "interrupted_named_rerun",
        "passed": True,
        "silver_completed_before_stop": True,
        "next_after_stop": interrupted["next"],
        "watermark_after_stop": interrupted["watermark"],
        "recovered_month": recovered["month"],
        "coverage_restored": True,
    }))
finally:
    if holder is not None:
        holder.communicate(input="\n", timeout=30)
    if scheduler_stopped:
        command(["docker", "compose", "start", "airflow-scheduler"])
