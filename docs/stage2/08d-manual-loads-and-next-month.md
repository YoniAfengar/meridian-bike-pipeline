# Checkpoint 8d — Manual loads do not advance coverage

Validated on a fresh isolated meridian-progress-check stack,
using port 8082 and disabled schedules.

## Initial coverage

- earliest: 2021-01
- watermark: null
- complete: 0
- gaps: []
- next: 2021-01

## Entire month loaded by hand

Executed Stage 1 commands for:
- JC January 2021 Bronze ingestion.
- JC January 2021 Silver transformation.
- All 31 January Gold days.

All 33 commands succeeded.

Coverage remained identical to its initial value:
complete was 0, watermark was null, and next was 2021-01.

## Windowless pipeline request

Ran just run pipeline jc on this stack.

Observed:
- Selected month: 2021-01.
- Run type: manual.
- Run state: success.
- All three job tasks succeeded with one attempt.
- Gold reported 31 days and no failed days.

This verifies that a fully hand-loaded month remains due until
the operational pipeline processes and records it.
