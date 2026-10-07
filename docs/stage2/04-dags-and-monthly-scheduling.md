# Stage 2 — Checkpoint 4: DAGs and monthly scheduling

## Changes

- Define one DAG per market: pipeline_jc and pipeline_nyc.
- Wire month selection, due check, publication wait and the three job tasks.
- Use a monthly data-interval timetable with cron `0 0 1 * *`.
- Keep automatic schedule switches separate from DAG pause state.
- Permit bounded backfills while automatic scheduling is disabled.
- Register the custom timetable through an Airflow plugin.
- Use a reschedule-mode publication sensor with a seven-day timeout.
- Configure job retries and allow multiple active monthly runs.
- Keep job execution and recording logic outside the DAG file.

## Validation performed

These checks were performed on the working tree before splitting the commits:

- Airflow discovered both market DAGs after PYTHONPATH was configured.
- The manual June 2026 run completed successfully.
- The corrected backfill dry run produced exactly:
  - 2021-01-01
  - 2021-02-01
- The actual January and February backfill completed successfully.
- Both runs were reported as type backfill.
- January reported 31 gold days and February reported 28.
- The runs exercised the vendor schema change across these months.

## Corrected scheduling issue

The initial cron expression had six fields and generated 61 runs.
The scheduler and DAG processor were stopped.
The erroneous backfill was paused and its unfinished runs marked failed.
No application containers remained active.
The expression was corrected to five fields, checked with a dry run,
and the intended two-month backfill then succeeded.

## Pending validation

- Enabling a schedule loads the full required history within 30 minutes.
- Disabling a schedule prevents new automatic work.
- A scheduled run for a complete month skips its job tasks.
- An unpublished named month waits and eventually times out.
- Failure injection and concurrent-run acceptance cases.
