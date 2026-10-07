# Stage 2 — Checkpoint 6b: Gold day failure and retry

## Test

Run inside the Airflow scheduler:

`python "$MERIDIAN_PROJECT_DIR/scripts/check_gold_retry.py"`

The test uses the existing June 2026 data and reloads silver through its
operational wrapper. It injects a one-time failure for 2026-06-02 using
an in-memory mock; no Stage 1 job file is modified.

## Verified behavior

- Recording the new successful silver load makes the month incomplete.
- The first gold batch attempts all 30 days.
- The batch records the other 29 days despite the injected failure.
- The batch fails and names 2026-06-02.
- A second invocation runs only 2026-06-02.
- After retry, the month is complete.
- Final gold data is identical to the baseline.
- Twenty concurrent snapshots of all June gold rows for jc matched
  the baseline while gold was rebuilding.

## Scope

The test exercises the real silver subprocess and gold batch wrappers
against PostgreSQL. The retry is a second direct invocation of the gold
wrapper, not an automatically retried Airflow task.

Automatic silver retry was verified separately.
The automatic gold task retry remains an integration check.
