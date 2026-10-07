FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.13.txt" \
    -r requirements.txt

COPY src ./src
COPY sql ./sql

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "src.cli"]