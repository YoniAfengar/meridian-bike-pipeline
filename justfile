up:
    docker compose up -d --wait db
    docker compose exec -T db psql -U meridian -d meridian -v ON_ERROR_STOP=1 < sql/operations.sql
    docker compose exec -T db psql -U meridian -d meridian -v ON_ERROR_STOP=1 -c "UPDATE operational_schedules SET enabled = FALSE;"
    touch airflow/dags/pipelines.py
    docker compose up -d airflow-api-server airflow-scheduler airflow-dag-processor

down:
    docker compose down

schedule market state:
    docker compose exec -T airflow-scheduler python -m src.operational_cli schedule "{{market}}" "{{state}}"
    touch airflow/dags/pipelines.py

run layer job window="" last="":
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ "{{layer}}" == "pipeline" ]]; then
        args=("{{job}}")
        if [[ -n "{{window}}" ]]; then
            args+=("{{window}}")
        fi
        if [[ -n "{{last}}" ]]; then
            args+=("{{last}}")
        fi
        docker compose exec -T airflow-scheduler python -m src.operational_cli run "${args[@]}"
    else
        if [[ -n "{{last}}" ]]; then
            echo "Stage 1 run requires: <layer> <job> <window>" >&2
            exit 2
        fi
        docker compose run --rm app run "{{layer}}" "{{job}}" "{{window}}"
    fi

inspect layer job window="":
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ "{{layer}}" == "coverage" || "{{layer}}" == "runs" ]]; then
        if [[ -n "{{window}}" ]]; then
            echo "Operational inspect requires: <coverage|runs> <job|market>" >&2
            exit 2
        fi
        docker compose exec -T airflow-scheduler python -m src.operational_cli "{{layer}}" "{{job}}"
    else
        docker compose run --rm app inspect "{{layer}}" "{{job}}" "{{window}}"
    fi

report question market station day:
    docker compose run --rm app report "{{question}}" "{{market}}" "{{station}}" "{{day}}"
