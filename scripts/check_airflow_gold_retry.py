import json
import subprocess
import time
import uuid


token = uuid.uuid4().hex
run_id = f"manual__gold_retry_{token}"
sequence = f"test_gold_sequence_{token}"
function = f"test_gold_function_{token}"
trigger = f"test_gold_trigger_{token}"


def sql(statement):
    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "db",
            "psql", "-U", "meridian", "-d", "meridian",
            "-X", "-q", "-A", "-t",
            "-v", "ON_ERROR_STOP=1",
            "-c", statement,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def status():
    value = sql(f"""
        SELECT json_build_object(
            'state', r.state,
            'gold_state', t.state,
            'tries', t.try_number
        )
        FROM dag_run r
        LEFT JOIN task_instance t
          ON t.dag_id = r.dag_id
         AND t.run_id = r.run_id
         AND t.task_id = 'transform_to_gold'
        WHERE r.dag_id = 'pipeline_jc'
          AND r.run_id = '{run_id}';
    """)
    return json.loads(value) if value else None


def recorded_days():
    return json.loads(sql("""
        SELECT COALESCE(
            jsonb_object_agg(gold.window_key, gold.loaded_at),
            '{}'::jsonb
        )
        FROM operational_loads gold
        JOIN operational_loads silver
          ON silver.market = gold.market
         AND silver.job = 'transform-to-silver'
         AND silver.window_key = '2026-06'
        WHERE gold.market = 'jc'
          AND gold.job = 'transform-to-gold'
          AND gold.window_key LIKE '2026-06-%'
          AND gold.loaded_at > silver.loaded_at;
    """))


before_retry = None

try:
    sql(f"""
        BEGIN;
        CREATE SEQUENCE {sequence};
        CREATE FUNCTION {function}() RETURNS trigger AS $$
        BEGIN
            IF nextval('{sequence}') = 1 THEN
                RAISE EXCEPTION 'Injected gold failure for 2026-06-02';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        CREATE TRIGGER {trigger}
        BEFORE INSERT ON gold_station_daily
        FOR EACH ROW
        WHEN (NEW.market = 'jc' AND NEW.day = DATE '2026-06-02')
        EXECUTE FUNCTION {function}();
        COMMIT;
    """)

    subprocess.run(
        [
            "docker", "compose", "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "trigger", "pipeline_jc",
            "--run-id", run_id,
            "--conf", '{"month":"2026-06"}',
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    deadline = time.monotonic() + 600
    final = None

    while time.monotonic() < deadline:
        current = status()

        if current and current["gold_state"] == "up_for_retry":
            if before_retry is None:
                before_retry = recorded_days()
                assert len(before_retry) == 29, (
                    f"FAIL: expected 29 recorded days: {before_retry}"
                )
                assert "2026-06-02" not in before_retry

        if current and current["state"] in ("success", "failed"):
            final = current
            break

        time.sleep(0.5)

    assert final is not None, "FAIL: timed out waiting for Airflow"
    assert final["state"] == "success", f"FAIL: {final}"
    assert final["gold_state"] == "success", f"FAIL: {final}"
    assert final["tries"] >= 2, f"FAIL: no gold retry: {final}"
    assert before_retry is not None, "FAIL: retry state was not observed"

    after_retry = recorded_days()
    assert len(after_retry) == 30
    assert "2026-06-02" in after_retry
    assert all(
        after_retry[day] == timestamp
        for day, timestamp in before_retry.items()
    ), "FAIL: retry repeated already successful gold days"

    print(json.dumps({
        "test": "airflow_gold_automatic_retry",
        "passed": True,
        "gold_tries": final["tries"],
        "successful_days_before_retry": 29,
        "successful_days_after_retry": 30,
        "other_day_timestamps_unchanged": True,
        "run_state": final["state"],
    }))
finally:
    sql(f"""
        DROP TRIGGER IF EXISTS {trigger} ON gold_station_daily;
        DROP FUNCTION IF EXISTS {function}();
        DROP SEQUENCE IF EXISTS {sequence};
    """)
