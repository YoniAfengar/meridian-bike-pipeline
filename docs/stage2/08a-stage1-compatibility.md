# Checkpoint 8a — Stage 1 compatibility

## Unchanged files

Git comparison against the commit before Stage 2 runtime changes showed
no differences in:
- src/cli.py
- src/db.py
- src/ingest.py
- src/silver.py
- src/gold.py
- src/inspect.py
- sql/init.sql

## Manual commands with Airflow stopped

Stopped the API server, scheduler and DAG processor in the main stack.

The June 2026 JC inspections returned:
- Bronze: 109897 rows, 1 object.
- Silver: 109510 rows, 387 rejects.
- Reject reason: never docked.

All three Stage 1 jobs then ran successfully by hand:
- ingest-to-bronze trips:jc 2026-06
- transform-to-silver trips:jc 2026-06
- transform-to-gold station-daily 2026-06-02

The report for JC115 on 2026-06-02 matched the expected compact JSON,
including its trailing newline: 216 departures and 209 arrivals.

## Pipeline comparison

Restarted Airflow using just up and ran pipeline jc 2026-06.

Observed:
- Run state: success.
- All three job tasks succeeded with one attempt.
- Gold batch reported 30 days and no failed days.
- Report output was byte-identical to the saved manual Stage 1 report.

## Scope

This checkpoint verifies the June JC compatibility case.
Historical datasets and invalid-grain checks remain separate checks.
No automated Stage 1 test suite was found in the initial repository search.
