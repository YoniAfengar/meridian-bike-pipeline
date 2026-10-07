# Checkpoint 8e — Coverage gaps and next-month selection

Validated on the isolated meridian-progress-check stack.

## January, February and April complete

After January was complete, ran named pipelines for February and April.

Both succeeded:
- February Gold days: 28.
- April Gold days: 30.

Coverage matched the specification's example exactly:

{"job":"trips:jc","earliest":"2021-01","watermark":"2021-02","complete":3,"gaps":["2021-03"],"next":"2021-03"}

## Windowless requests fill the earliest gap

Executed two windowless pipeline requests sequentially.

Observed:
- First selected 2021-03 and succeeded with 31 Gold days.
- Second selected 2021-05 and succeeded with 31 Gold days.

Final coverage:

{"job":"trips:jc","earliest":"2021-01","watermark":"2021-05","complete":5,"gaps":[],"next":"2021-06"}

This verifies that selection fills the earliest incomplete month,
then advances past months that were already complete.
