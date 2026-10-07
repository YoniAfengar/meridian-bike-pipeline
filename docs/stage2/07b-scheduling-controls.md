# Checkpoint 7b — Scheduling controls

Validated on the isolated meridian-night-check stack.

## No published month due

After loading all published months through September 2026, two windowless
pipeline requests returned:
- month: null
- state: success
- work: skipped
- All three job tasks skipped with zero attempts.
- Command exit code: 0.

## Complete scheduled month skips work

Cleared the existing January 2021 scheduled run using Airflow's task CLI.

Observed:
- Run type remained scheduled.
- Final run state: success.
- Month selection and due check succeeded.
- Publication sensor and all three job tasks were skipped.
- January's operational load records, including timestamps, were byte-identical
  before and after the run.

Skipped tasks retained their previous Airflow attempt counts.

## Schedule off

The schedule command returned schedule: off.
The stored flag was false.
A timetable constructed from that flag returned no next automatic run
for an unbounded scheduling restriction.

This checks the stored setting and timetable behavior directly.
It does not test cancellation of already queued or running tasks.
