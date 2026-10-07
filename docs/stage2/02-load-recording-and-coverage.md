# Stage 2 — Checkpoint 2: Load recording and coverage

## Changes

- Add repeatable SQL setup for operational load and schedule tables.
- Record loads only after successful job execution.
- Keep one control-table row per job, market and window.
- Refresh the timestamp when a successful load is repeated.
- Require every gold day to be recorded after the month's latest silver load.
- Compute complete months, watermark, gaps and next month from load records.
- Run gold as a monthly batch, recording each successful day.
- Skip days already recorded after the latest silver load.
- Collect failed days and fail the batch after attempting the remaining days.
- Add a database advisory lock around each gold day.

Stage 1 job files are unchanged.

## Validation performed

These checks were performed on the working tree before splitting the commits:

- Initially both markets had zero complete months and null watermarks.
- The initial next months were jc 2021-01 and nyc 2026-01.
- After loading jc 2021-01, 2021-02 and 2026-06:
  - complete: 3
  - watermark: 2021-02
  - next: 2021-03
  - gaps covered 2021-03 through 2026-05
- January's successful gold batch reported 31 days.
- February's successful gold batch reported 28 days.
- June's successful gold batch reported 30 days.

## Pending validation

- Hand-only loads do not change operational coverage.
- Interrupted reruns invalidate completion after silver is recorded.
- Failed gold days are retried without repeating successful days.
- Concurrent runs preserve correct answers and load records.
