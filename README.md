# Meridian Bike Pipeline

A containerized Citi Bike data pipeline built with Python, PostgreSQL, Docker Compose, and `just`.

## Architecture

The pipeline follows a Bronze → Silver → Gold architecture.

- **Bronze** — preserves published Citi Bike source data and provenance.
- **Silver** — conforms old and new Citi Bike schemas, validates required trip fields, and quarantines rejected rows.
- **Gold** — produces daily station-level departure and arrival counts.

## Commands

Start PostgreSQL:

    just up

Stop the stack:

    just down

Run a pipeline job:

    just run <layer> <job> <window>

Inspect Bronze or Silver:

    just inspect <layer> <job> <window>

Report daily station trips:

    just report daily-station-trips <market> <station> <day>

## Examples

    just run ingest-to-bronze trips:jc 2026-06
    just run transform-to-silver trips:jc 2026-06
    just run transform-to-gold station-daily 2026-06-02

    just inspect bronze trips:jc 2026-06
    just inspect silver trips:jc 2026-06

    just report daily-station-trips jc JC115 2026-06-02

## Window Grains

Trip jobs require an ISO month:

    YYYY-MM

Gold station-daily jobs require an ISO day:

    YYYY-MM-DD

Invalid grains fail with a non-zero exit code and an error on stderr.

## Data Quality

A trip is countable when all four fields are present:

- start station identifier
- end station identifier
- started_at
- ended_at

Rows missing a required field are preserved in the Silver reject quarantine with a reason.

## Reproducibility

Jobs are independently runnable and idempotent. Silver is rebuilt from Bronze, and Gold is rebuilt from Silver.
