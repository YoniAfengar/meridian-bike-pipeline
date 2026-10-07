# Checkpoint 7c — Unpublished month and sensor timeout

Validated on the isolated meridian-night-check stack.

## Named unpublished month waits

Triggered pipeline_jc manually with month 2026-10.

Observed:
- Month selection and due check succeeded.
- The publication sensor entered up_for_reschedule with try_number 1.
- All three job tasks remained unstarted with zero attempts.
- No October operational loads were recorded.

The reschedule state releases the worker between publication checks.

## Timeout fails the run

To test the seven-day timeout without waiting seven days, updated only
this test run's task_reschedule record:
- Set start_date to eight days earlier.
- Made the next publication check immediately due.

The sensor then failed through its normal execution path.

Observed:
- Log exception: AirflowSensorTimeout.
- Reported elapsed time: 691201.435621 seconds.
- Configured timeout: 604800 seconds.
- Sensor state: failed, try_number 1.
- DAG run state: failed.
- All three job tasks: upstream_failed, zero attempts.
- October operational load count: 0.

This validates timeout handling using a simulated elapsed duration,
rather than an actual seven-day wait. No job implementation was changed.
