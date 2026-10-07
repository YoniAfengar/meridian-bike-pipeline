# Checkpoint 8c — Stage 1 window validation

Executed each command through the existing just run recipe.

| Job | Invalid window | Exit code | Expected error observed |
| --- | --- | ---: | --- |
| ingest-to-bronze trips:jc | 2026-06-02 | 2 | window must be an ISO month |
| transform-to-silver trips:jc | 2026-06-02 | 2 | window must be an ISO month |
| transform-to-gold station-daily | 2026-06 | 2 | window must be an ISO day |

All three checks passed. Each command rejected the incorrect processing
grain through Stage 1's unchanged CLI.
