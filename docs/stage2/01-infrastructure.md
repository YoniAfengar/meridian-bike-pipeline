# Stage 2 — Checkpoint 1: Airflow infrastructure

## Changes

- Add Docker CLI, Docker Compose and just to the application image.
- Add the standard Airflow operators and sensors provider.
- Share configuration across Airflow services.
- Configure LocalExecutor, the execution API URL and shared authentication keys.
- Make the project package importable through PYTHONPATH.
- Connect Airflow to the host Docker daemon and the same Compose project.
- Mount project files, DAGs, plugins and shared Airflow logs.
- Run database migration without masking migration failures.

## Validation performed

These checks were performed on the working tree before splitting the commits:

- `docker compose build` completed successfully.
- Airflow services started and `airflow-init` exited with code 0.
- Runtime tools reported:
  - just 1.58.0
  - Docker 29.8.2
  - Docker Compose v5.5.1
- Docker Compose invoked inside Airflow saw the existing project containers.
- The Stage 1 report invoked inside Airflow returned:
  `{"market":"jc","station":"JC115","day":"2026-06-02","departures":216,"arrivals":209}`

## Scope

This checkpoint covers infrastructure only.
Scheduling behavior, job recording and pipeline commands are documented separately.
Full acceptance tests and Stage 1 checks with Airflow stopped remain pending.
