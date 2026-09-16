"""Periodically process Threadline's durable full-rebuild recovery queue."""

from __future__ import annotations

import logging
import os

from dataclasses import asdict
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


LOGGER = logging.getLogger(__name__)


def process_recovery_queue():
    # Import application dependencies during task execution.
    # DAG parsing does not connect to PostgreSQL.
    from threadline.recovery_runner import drain_recovery_queue
    from threadline.recovery_runtime import create_recovery_worker

    max_requests = int(
        os.environ.get(
            "THREADLINE_RECOVERY_MAX_REQUESTS",
            "10",
        )
    )

    max_seconds = float(
        os.environ.get(
            "THREADLINE_RECOVERY_MAX_SECONDS",
            "120",
        )
    )

    worker = create_recovery_worker()

    summary = drain_recovery_queue(
        worker,
        max_requests=max_requests,
        max_seconds=max_seconds,
    )

    LOGGER.info(
        "Recovery drain finished: processed=%s stop_reason=%s "
        "elapsed_seconds=%s last_request_id=%s last_run_id=%s",
        summary.processed_count,
        summary.stop_reason,
        summary.elapsed_seconds,
        summary.last_request_id,
        summary.last_run_id,
    )

    # Small operational summary only.
    # Financial documents stay in PostgreSQL.
    return asdict(summary)


with DAG(
    dag_id="threadline_recovery",
    description="Drain eligible durable full-rebuild recovery requests",
    start_date=pendulum.datetime(
        2024,
        1,
        1,
        tz="UTC",
    ),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=12),
    default_args={
        "owner": "data_engineer",

        # PostgreSQL owns request retry timing and attempt history.
        # The next scheduled DAG run checks eligibility again.
        "retries": 0,
    },
    tags=[
        "threadline",
        "recovery",
        "financial",
    ],
) as dag:
    recover_financial_state = PythonOperator(
        task_id="recover_financial_state",
        python_callable=process_recovery_queue,
        pool="threadline_recovery",
        pool_slots=1,
        execution_timeout=timedelta(minutes=10),
        do_xcom_push=True,
    )