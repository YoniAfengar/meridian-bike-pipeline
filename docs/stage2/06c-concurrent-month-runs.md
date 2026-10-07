# Stage 2 — Checkpoint 6c: Concurrent runs for the same month

## Test

Command: `python3 scripts/check_concurrent_runs.py`

The test captures baseline coverage, the JC115 report for 2026-06-02,
and all June gold rows. It starts two named pipeline requests for
jc 2026-06 without waiting between them, then waits for both results.

## Verified behavior

- Both runs finished successfully.
- All three job tasks succeeded in each run.
- The report matched its baseline.
- Coverage matched its baseline.
- All 3,513 June gold rows matched the baseline exactly.

This verifies the requirement that firing the same month twice at once
preserves the result of a successful single run.

## Scope

The comparison includes all markets present in June's gold table.
Report comparison uses parsed JSON; byte-identical report output is
checked separately during Stage 1 compatibility validation.
