# Stage 2 — Checkpoint 6a: Pre-history failure and silver retry

## Pre-history month

Command: `just run pipeline jc 2019-06`

Observed:
- Run state: failed.
- Pipeline command exited with code 1.
- which_month failed with try_number 1.
- All downstream tasks were upstream_failed with zero attempts.
- No operational load records existed for jc 2019-06.

## Killed silver container

Command: `python3 scripts/check_silver_retry.py`

The test starts a named June 2026 run, identifies its silver container,
kills it, waits for the pipeline command, and checks the final state
and silver attempt count.

Observed:
- A matching silver container was killed successfully.
- Silver attempts: 2.
- Final run state: success.
- Test result: passed.

This satisfies the requirement that a silver container killed mid-work
is retried and the run succeeds with at least two attempts.

## Remaining stage 6 checks

- Gold continues after a failed day and retries only failed days.
- Reports remain complete while gold rebuilds.
- Interrupted named reruns invalidate completion after silver.
- Concurrent runs for the same month preserve the expected result.
