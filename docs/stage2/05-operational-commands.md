# Stage 2 — Checkpoint 5: Operational commands

## Changes

- Extend just recipes while preserving existing Stage 1 command routing.
- Make up initialize operational tables and disable both schedules.
- Add per-market schedule on/off commands.
- Add blocking pipeline commands for the next month, a named month or a range.
- Use Airflow manual runs for individual pipeline requests.
- Use Airflow backfills for month ranges.
- Wait for terminal run states and return non-zero if any run failed.
- Add JSON coverage and run inspection.
- Report Airflow's own run types, task states and attempt counts.
- Order runs by queued timestamp rather than database ID.

## Validation performed

These checks were performed on the working tree before this commit:

- `just inspect coverage trips:jc` returned the expected coverage.
- `just run pipeline jc` selected 2021-03 and waited for success.
- `just run pipeline jc 2021-01 2021-02` waited for two successful backfill runs.
- The gold batches reported 31 and 28 days respectively.
- `just inspect runs jc` returned valid JSON with the latest runs first.
- `just up` completed and disabled both schedule switches.
- `just schedule jc off` returned schedule off.

Attempt counts are Airflow's stored values; reprocessing existing runs can
increase those counts.

## Corrected backfill configuration issue

Airflow 3.1.0's backfill CLI stored the supplied configuration as a JSON string.
The controller now invokes Airflow's backfill creation function in an isolated
Python subprocess, passing a decoded dictionary. The range command then passed.
This integration is tied to the project's pinned Airflow 3.1.0 version.

## Pending acceptance checks

- Named reruns preserve answers and final coverage.
- Unpublished windowless requests return success with no work.
- Failed DAG runs produce a non-zero pipeline command exit.
- Automatic scheduling, failure injection and concurrent runs.
- Stage 1 commands and checks with Airflow stopped.
