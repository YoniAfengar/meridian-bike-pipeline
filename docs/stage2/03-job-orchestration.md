# Stage 2 — Checkpoint 3: Job orchestration

## Changes

- Select an explicit month, a scheduled interval or the next incomplete month.
- Validate month syntax and the market's earliest window.
- Check whether the vendor has published the source object.
- Skip completed months for scheduled runs.
- Call bronze in-process and record its successful load.
- Run silver through a subprocess invoking the existing just recipe.
- Record silver only after the subprocess succeeds.
- Call the monthly gold batch in-process.
- Serialize job execution for the same market and month with an advisory lock.

Stage 1 job implementations and CLI are unchanged.

## Validation performed

These checks were performed on the working tree before splitting the commits:

- Source publication lookup returned true for jc 2026-06.
- A manual Airflow run for jc 2026-06 completed all job tasks.
- Its report returned 216 departures and 209 arrivals for JC115 on 2026-06-02.
- A windowless run selected jc 2021-03 and completed successfully.
- That run reported 31 gold days.
- Silver ran through just and Docker Compose against the existing warehouse.

## Pending validation

- Invalid and pre-history months fail with no job writes.
- Unpublished windowless runs succeed without work.
- Silver failure triggers a retry.
- Concurrent runs for the same month preserve the expected result.
