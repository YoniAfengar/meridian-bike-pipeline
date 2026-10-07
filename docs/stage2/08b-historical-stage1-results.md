# Checkpoint 8b — Historical Stage 1 results

The historical windows were initially absent from the main warehouse.
Loaded each using Stage 1's manual Bronze and Silver commands.

## JC — June 2019

- Bronze: 1 object, 39430 rows.
- Silver: 39430 rows.
- Rejects: 0.

## NYC — April 2018

- Bronze: 1 object, 1307543 rows.
- Silver: 1307543 rows.
- Rejects: 0.

Both datasets matched the documented Stage 1 counts.

## Manual loads are not recorded

Queried operational_loads for both historical windows, including
their daily windows. The recorded load count was 0.

Stage 1 manual jobs accepted these historical windows, while the
operational pipeline separately enforces its configured earliest months.

## Scope

These historical checks ran with Airflow services available and schedules
disabled. Manual operation with Airflow stopped was verified for June 2026
JC in checkpoint 8a.
