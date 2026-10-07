import json
import subprocess


market = "jc"
month = "2026-06"


def json_command(arguments):
    result = subprocess.run(
        arguments,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def gold_snapshot():
    code = """
import json
from src.db import connect

with connect() as conn:
    with conn.cursor() as cur:
        cur.execute(
            '''
            SELECT market, station_id, day, departures, arrivals
            FROM gold_station_daily
            WHERE day >= %s AND day < %s
            ORDER BY market, day, station_id
            ''',
            ('2026-06-01', '2026-07-01'),
        )
        print(json.dumps(cur.fetchall(), default=str))
"""
    return json_command([
        "docker", "compose", "exec", "-T",
        "airflow-scheduler", "python", "-c", code,
    ])


report_command = [
    "just", "report", "daily-station-trips",
    market, "JC115", "2026-06-02",
]
coverage_command = ["just", "inspect", "coverage", f"trips:{market}"]

before_report = json_command(report_command)
before_coverage = json_command(coverage_command)
before_gold = gold_snapshot()
assert before_gold, "FAIL: baseline gold data is empty"

processes = [
    subprocess.Popen(
        ["just", "run", "pipeline", market, month],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    for _ in range(2)
]

runs = []

for process in processes:
    stdout, stderr = process.communicate(timeout=600)
    assert process.returncode == 0, (
        f"FAIL: pipeline failed\n{stdout}\n{stderr}"
    )
    run = json.loads(stdout)
    assert run["month"] == month
    assert run["state"] == "success"
    assert all(
        task["state"] == "success"
        for task in run["tasks"].values()
    ), f"FAIL: a job task did not succeed: {run}"
    runs.append(run)

assert json_command(report_command) == before_report, (
    "FAIL: report changed"
)
assert json_command(coverage_command) == before_coverage, (
    "FAIL: coverage changed"
)
assert gold_snapshot() == before_gold, (
    "FAIL: gold data changed"
)

print(json.dumps({
    "test": "concurrent_same_month_runs",
    "passed": True,
    "successful_runs": len(runs),
    "report_unchanged": True,
    "coverage_unchanged": True,
    "gold_rows_unchanged": len(before_gold),
}))
