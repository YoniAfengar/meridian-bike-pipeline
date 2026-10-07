# Stage 2 — Checkpoint 7a: Fresh scheduled history

## Setup

- Fresh Compose project: meridian-night-check.
- Separate PostgreSQL and Airflow log volumes.
- Airflow host port: 8081.
- Initial coverage: zero complete months, null watermark.
- Initial Airflow run history: empty.
- Both automatic schedule switches initially off.

Compose now accepts COMPOSE_PROJECT_NAME and AIRFLOW_PORT overrides.
The original project name and port remain the defaults.
Nested silver subprocesses inherit the selected Compose project name.

## Test

Command: `python3 scripts/check_scheduled_history.py`

The test enables jc scheduling, polls coverage and Airflow run counts,
and measures time from the schedule-on command.

## Results on 2026-10-07

Required history through August 2026:
- Complete months: 68.
- Watermark: 2026-08.
- Gaps: none.
- Elapsed time: 223.44 seconds.
- Required limit: 1,800 seconds.

September 2026 was also present in the vendor listing:
- Complete months: 69.
- Final watermark: 2026-09.
- Gaps: none.
- Total elapsed time: 233.89 seconds.
- Airflow runs: 69 scheduled, 69 successful.
- Failed runs: zero.
- Manual or backfill runs: zero.

The original requirement of 68 months through August within 30 minutes
passed. The additional published September month also loaded successfully.

## Remaining scheduling checks

- A complete scheduled month skips work when reprocessed.
- A windowless request with no published month due returns month null.
- Schedule off prevents further automatic runs.
