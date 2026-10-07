# Stage 2 — Checkpoint 6e: Interrupted named rerun

## Test

Command: `python3 scripts/check_interrupted_rerun.py`

The test holds an advisory lock on the first gold day of March 2021.
It starts a named Airflow rerun and waits until silver succeeds and
gold is running. It then stops the scheduler, terminating the worker,
and marks that specific interrupted run failed to prevent automatic recovery.

The lock is released and the scheduler restarted. A windowless pipeline
request is then used to recover the incomplete month.

## Verified behavior

- Silver completed before the worker was stopped.
- March became incomplete.
- The next month due became 2021-03.
- The watermark moved back to 2021-02.
- A windowless request selected 2021-03 and succeeded.
- Coverage returned exactly to its baseline.

No Stage 1 job file was changed.
The test releases its lock and restarts the scheduler in its cleanup path.
