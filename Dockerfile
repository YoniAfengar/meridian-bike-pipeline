FROM docker:29-cli AS docker_tools

FROM python:3.13-slim

WORKDIR /app

COPY --from=docker_tools /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker_tools /usr/local/libexec/docker/cli-plugins/ /usr/local/libexec/docker/cli-plugins/
COPY --from=ghcr.io/casey/just:latest /just /usr/local/bin/just

COPY requirements.txt .

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.13.txt" \
    -r requirements.txt

COPY src ./src
COPY sql ./sql

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "src.cli"]