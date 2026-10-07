FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY sql ./sql

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "src.cli"]
