up:
    docker compose up -d db airflow-api-server airflow-scheduler airflow-dag-processor

down:
    docker compose down

run layer job window:
    docker compose run --rm app run "{{layer}}" "{{job}}" "{{window}}"

inspect layer job window:
    docker compose run --rm app inspect "{{layer}}" "{{job}}" "{{window}}"

report question market station day:
    docker compose run --rm app report "{{question}}" "{{market}}" "{{station}}" "{{day}}"
