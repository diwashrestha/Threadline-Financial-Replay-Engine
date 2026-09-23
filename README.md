# Threadline Financial Replay Engine

Threadline is a local project for checking whether an online store's orders, payments, refunds, fees, and payouts agree. It keeps the source records so a late refund or corrected payment can be replayed without losing the earlier evidence.

> **Under construction:** The financial ingestion and recovery flow is being built and tested. Some end-to-end and concurrency tests are still being validated.

## How it works

A source report arrives as a JSON file with a manifest. Threadline checks the file, stores its records in PostgreSQL, and chooses the current version of each order, payment, or other financial entity. It then calculates transaction and payout results. If a correction arrives later, a recovery run recalculates the result.

```mermaid
flowchart LR
    A["JSON reports<br/>+ manifests"] --> B["Check file<br/>and schema"]
    B --> C["Store receipts<br/>and version history"]
    C --> D["Choose current<br/>records"]
    D --> E["Reconcile orders<br/>and payouts"]
    E --> F["Publish current<br/>financial result"]

    C --> G["Late data or<br/>correction"]
    G --> H["Recovery request"]
    H --> I["Airflow recovery run"]
    I --> E

    classDef files fill:#fff0b3,stroke:#b7791f,color:#382900
    classDef checks fill:#d9f2ff,stroke:#1683ad,color:#073b50
    classDef storage fill:#e5dcff,stroke:#7255bd,color:#302057
    classDef finance fill:#d8f5df,stroke:#298449,color:#123d24
    classDef recovery fill:#ffe0e0,stroke:#bf5050,color:#5a2020

    class A files
    class B checks
    class C,D storage
    class E,F finance
    class G,H,I recovery
```

A duplicate should not change the financial totals. A higher source version replaces an older one. If two different records have the same identity and version, Threadline retains both as evidence and marks the entity as conflicted.

## Tools

![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![pytest](https://img.shields.io/badge/pytest-0A9EDC?logo=pytest&logoColor=white)

Python handles validation and reconciliation. PostgreSQL stores financial evidence and published results. Airflow schedules recovery work; Docker Compose runs the local services. Alembic manages database migrations, and pytest checks replay and failure behavior.

## Run locally

Development uses WSL2 and Docker Compose. Before starting, set `AIRFLOW_FERNET_KEY` and `AIRFLOW_UID` in `.env`.

From the project root:

```bash
docker build -t etl-airflow:local .
docker build -t etl-generator:local ./generator

docker compose up -d postgres threadline-postgres
docker compose run --rm threadline-migrate
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler generator
```

Open Airflow at <http://localhost:8080>. Check that its DAGs load:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

The included `generator` comes from the original ecommerce event pipeline. Its clickstream events are separate from Threadline's financial source reports.

## Run tests

Use the project's Python virtual environment:

```bash
./.venv/bin/python -m pytest -q
```

PostgreSQL integration tests need the dedicated test database:

```bash
docker compose --profile test up -d --wait threadline-test-postgres

export THREADLINE_TEST_DATABASE_URL='postgresql://threadline_test:threadline_test@127.0.0.1:5434/threadline_test'

./.venv/bin/python -m pytest -q -m integration tests/integration
```

The integration tests reset the **test** database schema, so do not point `THREADLINE_TEST_DATABASE_URL` at the development database.
