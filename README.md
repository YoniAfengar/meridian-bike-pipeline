# 🚲 Meridian Bike Pipeline

> A production-style data engineering pipeline for ingesting, validating, transforming, and analyzing Citi Bike trip data across multiple markets and schema generations.

Built with **Python, PostgreSQL, Docker Compose, and Just**.

---

## 🎯 Project Overview

Citi Bike publishes years of trip data, but building a reliable pipeline over that history is not as simple as downloading CSV files.

The source data contains several real-world data engineering challenges:

- Different schemas across historical and modern datasets
- Different publishing conventions for NYC and Jersey City
- Changing file extensions and object naming patterns
- Duplicate historical exports
- Invalid trips that must be quarantined rather than deleted
- Large monthly datasets with more than one million rows
- Pipelines that must remain reproducible and idempotent

This project handles those problems through a **Bronze → Silver → Gold** architecture while keeping every pipeline layer independently runnable.

### Current Status

**Stage 1 — The Shape of the Job ✅**

The first stage implements the complete ingestion and transformation path:

`Citi Bike S3 → Bronze → Silver → Gold → Station Daily Report`

Future stages will extend the platform with additional production-grade capabilities.

---
## 🏗️ Architecture

The project follows a **Medallion Architecture**, separating raw source data, validated analytical data, and business-level aggregates.

```text
                       Citi Bike S3
                            │
                            │  XML object listing
                            │  + published ZIP/CSV data
                            ▼
                 ┌─────────────────────┐
                 │       BRONZE        │
                 │─────────────────────│
                 │ Raw published bytes │
                 │ Source provenance   │
                 │ No data cleaning    │
                 └──────────┬──────────┘
                            │
                            │ schema detection
                            │ validation
                            ▼
                 ┌─────────────────────┐
                 │       SILVER        │
                 │─────────────────────│
                 │ Conformed trips     │
                 │ Typed fields        │
                 │ Valid trip records  │
                 └──────────┬──────────┘
                            │
                  invalid   │   valid
                   rows     │
                    ┌───────┘
                    ▼
          ┌─────────────────────┐
          │  REJECT QUARANTINE  │
          │─────────────────────│
          │ Original row        │
          │ Rejection reason    │
          └─────────────────────┘

                            │
                            │ daily aggregation
                            ▼
                 ┌─────────────────────┐
                 │        GOLD         │
                 │─────────────────────│
                 │ Station + Day grain │
                 │ Departures          │
                 │ Arrivals            │
                 └──────────┬──────────┘
                            │
                            ▼
                 Daily Station Report
```

### Layer Responsibilities

| Layer | Responsibility | Rebuildable? |
|---|---|---|
| 🥉 **Bronze** | Preserve the published source data and its provenance | No |
| 🥈 **Silver** | Normalize schemas, validate trips, and quarantine invalid records | Yes — from Bronze |
| 🥇 **Gold** | Produce business-ready daily station metrics | Yes — from Silver |

### Pipeline Jobs

```text
ingest-to-bronze
        ↓
transform-to-silver
        ↓
transform-to-gold
        ↓
daily-station-trips
```

Each transformation layer reads only from the layer directly before it. Network access is required only during ingestion.

---
## 🧠 Engineering Challenges

This project is intentionally built around real data engineering problems rather than a perfectly clean dataset.

### 1. Schema Evolution

Citi Bike changed its trip schema over time.

Historical datasets use fields such as:

`start station id`, `starttime`, `stoptime`

Modern datasets use:

`start_station_id`, `started_at`, `ended_at`

Instead of relying on a hardcoded global cutover date, the Silver transformation detects the available columns and maps both schema generations into one consistent model.

### 2. NYC April 2018 — Multiple Published Exports

The NYC 2018 archive contains April data more than once.

Blindly processing every matching CSV would double-count trips and corrupt downstream aggregates.

The ingestion layer deliberately selects the publisher-backed April export under `4_April/`, producing exactly:

**1,307,543 trips**

No raw-row deduplication is used to hide the publishing problem.

### 3. Data Quality Without Data Loss

A trip is considered countable only when it contains:

- Start station identifier
- End station identifier
- Start timestamp
- End timestamp

Invalid rows are **never silently dropped**.

They are written to a reject quarantine together with the original row and a human-readable reason, allowing the pipeline to preserve bad data for investigation.

For Jersey City, June 2026:

- **109,897** published rows
- **109,510** valid Silver trips
- **387** quarantined rows

### 4. Idempotent Processing

Every pipeline job can be rerun for the same window without creating duplicate records.

Silver windows are rebuilt deterministically from Bronze, while Gold daily aggregates are rebuilt from Silver.

This makes retries safe and keeps pipeline outputs reproducible.

### 5. Explicit Processing Grains

Trip jobs operate on exact monthly windows:

`YYYY-MM`

Station aggregates operate on exact daily windows:

`YYYY-MM-DD`

Invalid grains fail immediately with a non-zero exit code instead of producing partial or ambiguous results.

---

## ✅ Verified Results

The pipeline was validated against known Citi Bike publishing counts and business outputs.

| Dataset | Bronze Rows | Silver Rows | Rejects |
| --- | ---: | ---: | ---: |
| JC — 2019-06 | 39,430 | 39,430 | 0 |
| JC — 2026-06 | 109,897 | 109,510 | 387 |
| NYC — 2018-04 | 1,307,543 | 1,307,543 | 0 |

### Gold Validation

For Jersey City station `JC115` on `2026-06-02`:

```json
{
  "market": "jc",
  "station": "JC115",
  "day": "2026-06-02",
  "departures": 216,
  "arrivals": 209
}
```

### Reliability Checks

- ✅ Idempotent Silver reruns
- ✅ Idempotent Gold reruns
- ✅ Fresh database initialization
- ✅ Explicit failure on incorrect window grains
- ✅ Silver rebuilds from Bronze
- ✅ Gold rebuilds from Silver
- ✅ Empty-safe inspection before data is loaded

---

## 🚀 Quick Start

### 1. Start the database

```bash
just up
```

### 2. Ingest a monthly dataset into Bronze

```bash
just run ingest-to-bronze trips:jc 2026-06
```

### 3. Transform Bronze → Silver

```bash
just run transform-to-silver trips:jc 2026-06
```

### 4. Build the daily Gold aggregate

```bash
just run transform-to-gold station-daily 2026-06-02
```

### 5. Inspect pipeline layers

```bash
just inspect bronze trips:jc 2026-06
just inspect silver trips:jc 2026-06
```

### 6. Query the business result

```bash
just report daily-station-trips jc JC115 2026-06-02
```

Expected output:

```json
{
  "market": "jc",
  "station": "JC115",
  "day": "2026-06-02",
  "departures": 216,
  "arrivals": 209
}
```

### 7. Stop the stack

```bash
just down
```
---
## 🛠️ Tech Stack

| Technology | Role |
| --- | --- |
| **Python** | Ingestion, transformation, validation, and CLI |
| **PostgreSQL** | Persistent storage for Bronze, Silver, rejects, and Gold |
| **Docker** | Reproducible application runtime |
| **Docker Compose** | Database and application orchestration |
| **Just** | Simple and consistent pipeline command interface |
| **Citi Bike S3** | Public source dataset |

---

## 📁 Project Structure

```text
meridian-bike-pipeline/
├── compose.yaml
├── Dockerfile
├── justfile
├── requirements.txt
├── README.md
│
├── sql/
│   └── init.sql
│
└── src/
    ├── __init__.py
    ├── cli.py
    ├── db.py
    ├── ingest.py
    ├── silver.py
    ├── gold.py
    └── inspect.py
```

### Main Components

- `ingest.py` — discovers Citi Bike objects and loads published data into Bronze.
- `silver.py` — handles schema evolution, validation, conformance, and reject quarantine.
- `gold.py` — builds daily station-level business aggregates and reports.
- `inspect.py` — exposes machine-readable Bronze and Silver inspection results.
- `cli.py` — validates commands and processing grains and routes pipeline jobs.
- `init.sql` — defines the PostgreSQL storage model.

---

## 🗺️ Roadmap

This repository is being developed incrementally as a multi-stage data engineering project.

### Stage 1 — The Shape of the Job ✅

- Bronze → Silver → Gold pipeline
- Historical and modern schema support
- NYC and Jersey City market handling
- Data quality quarantine
- Idempotent processing
- Daily station-level analytics
- Containerized local environment
- Reproducible command interface

### Stage 2 — Coming Next 🚧

The next stage will extend the platform with additional data engineering requirements while preserving the contracts established in Stage 1.

### Future Stages

As the project evolves, each stage will build on the existing architecture rather than replacing it.

The goal is to progressively turn a working data pipeline into a more complete production-style data platform.