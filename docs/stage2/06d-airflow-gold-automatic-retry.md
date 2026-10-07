# Stage 2 — Checkpoint 6d: Automatic gold retry in Airflow

## Test

Command: `python3 scripts/check_airflow_gold_retry.py`

The test installs a temporary PostgreSQL trigger and sequence that cause
one insert for jc 2026-06-02 to fail. It then triggers a real Airflow run
and observes the gold task entering up_for_retry.

No Stage 1 job file is modified. Temporary database objects are removed
in the test's finally block.

## Verified behavior

- After the first gold attempt, 29 days were recorded after silver.
- The failed day, 2026-06-02, was not among those records.
- Airflow retried the gold task automatically.
- Gold succeeded with try_number 2.
- All 30 days were complete after retry.
- The other 29 recording timestamps remained unchanged.
- The final Airflow run state was success.

This completes the automatic gold retry check left open in checkpoint 6b.
