"""Airflow DAG: ingest JSON ecommerce events from a shared volume into Postgres."""
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from event_ingest import has_pending_json_files, run_ingest

log = logging.getLogger(__name__)


def ingest_raw_events(**context):
    """Task entrypoint: validate files, load raw.events, archive to processed/."""
    run_ingest()


# Demo: start_date in the past keeps the DAG active; catchup=False avoids historical backfill.
DAG_START_DATE = datetime(2025, 1, 1)


def alert_on_failure(context):
    """Demo alert: log to task stdout. In prod, use email_on_failure + SMTP in airflow.cfg."""
    ti = context['task_instance']
    message = (
        f'ALERT task failed: dag_id={ti.dag_id}, task_id={ti.task_id}, '
        f'run_id={context.get("run_id")}, try_number={ti.try_number}'
    )
    print(message)
    log.error(message)


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "start_date": DAG_START_DATE,
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id='raw_events_processing',
    default_args=default_args,
    description='Ingest raw JSON events into raw.events',
    schedule=timedelta(seconds=30),  # short interval for local/demo runs
    catchup=False,
    max_active_runs=1,
    tags=['raw'],
) as dag:
    wait_for_pending_files = PythonSensor(
        task_id='wait_for_pending_files',
        python_callable=has_pending_json_files,
        poke_interval=30,
        timeout=180,
        mode='poke',
        # No files this interval isn't a failure — skip cleanly instead of
        # retrying and backing up the next scheduled runs (max_active_runs=1).
        soft_fail=True,
    )

    ingest_task = PythonOperator(
        task_id='ingest_raw_events',
        python_callable=ingest_raw_events,
    )

    wait_for_pending_files >> ingest_task
