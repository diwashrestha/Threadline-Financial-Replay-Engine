"""Airflow DAG: build marts from raw.events and optionally emit purchase reports."""
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

log = logging.getLogger(__name__)

REPORTS_DIR = '/data/reports'


def _data_interval(context):
    """Current run window (replaces deprecated execution_date)."""
    return context['data_interval_start'], context['data_interval_end']


def cart_snapshot(**context):
    """Upsert add_to_cart counts per user for the run interval."""
    start_date, end_date = _data_interval(context)
    hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = """
        INSERT INTO marts.cart_snapshot (user_id, snapshot_hour, add_to_cart_count)
        SELECT user_id, %s::timestamp AS snapshot_hour, COUNT(*)
        FROM raw.events
        WHERE event_type = 'add_to_cart'
          AND event_timestamp >= %s AND event_timestamp < %s
        GROUP BY user_id
        ON CONFLICT (user_id, snapshot_hour) DO UPDATE
        SET add_to_cart_count = EXCLUDED.add_to_cart_count;
    """
    hook.run(sql, parameters=(start_date, start_date, end_date))
    log.info('Updated marts.cart_snapshot for interval [%s, %s)', start_date, end_date)


def quantity_of_purchases(**context):
    """Upsert purchase event count for the run interval."""
    start_date, end_date = _data_interval(context)
    hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = """
        INSERT INTO marts.quantity_of_purchases (operation_timestamp, total_quantity)
        SELECT %s, COUNT(*)
        FROM raw.events
        WHERE event_type = 'purchase'
          AND event_timestamp >= %s AND event_timestamp < %s
        ON CONFLICT (operation_timestamp) DO UPDATE
        SET total_quantity = EXCLUDED.total_quantity;
    """
    hook.run(sql, parameters=(start_date, start_date, end_date))
    log.info('Updated marts.quantity_of_purchases for interval [%s, %s)', start_date, end_date)


def branch_decision(**context):
    """Skip report when no purchases in the interval."""
    start_date, _ = _data_interval(context)
    hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = """
        SELECT total_quantity
        FROM marts.quantity_of_purchases
        WHERE operation_timestamp = %s
    """
    result = hook.get_records(sql, parameters=(start_date,))
    quantity = result[0][0] if result else 0
    context['ti'].xcom_push(key='purchase_quantity', value=quantity)
    if quantity > 0:
        return 'send_report'
    return 'skip_report'


def send_report(**context):
    """Write a simple text report when purchases exist."""
    start_date, _ = _data_interval(context)
    quantity = context['ti'].xcom_pull(task_ids='branch_decision', key='purchase_quantity')
    report_lines = [
        f'Report for period starting at {start_date}',
        f'Total purchases: {quantity}',
        f'Generated at {datetime.now(timezone.utc)}',
    ]
    report_content = '\n'.join(report_lines)

    os.makedirs(REPORTS_DIR, exist_ok=True)
    filename = f'report_{start_date.strftime("%Y%m%d_%H%M%S")}.txt'
    filepath = os.path.join(REPORTS_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(report_content)

    log.info('Report saved to %s', filepath)


DAG_START_DATE = datetime(2025, 1, 1)


def alert_on_failure(context):
    """Demo alert: log to task stdout. In prod, use email_on_failure + SMTP."""
    ti = context['task_instance']
    message = (
        f'ALERT task failed: dag_id={ti.dag_id}, task_id={ti.task_id}, '
        f'run_id={context.get("run_id")}, try_number={ti.try_number}'
    )
    print(message)
    log.error(message)


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'start_date': DAG_START_DATE,
    'on_failure_callback': alert_on_failure,
}

with DAG(
    dag_id='ecommerce_marts_update',
    default_args=default_args,
    description='Build ecommerce marts after raw ingest',
    schedule=timedelta(seconds=30),  # short interval for local/demo runs
    catchup=False,
    max_active_runs=1,
    tags=['marts'],
) as dag:
    wait_for_raw = ExternalTaskSensor(
        task_id='wait_for_raw_events_processing',
        external_dag_id='raw_events_processing',
        external_task_id='ingest_raw_events',
        execution_delta=timedelta(seconds=0),
        timeout=180,
        poke_interval=30,
        mode='poke',
    )

    update_task = PythonOperator(
        task_id='update_cart_snapshot',
        python_callable=cart_snapshot,
    )

    purchase_quantity_task = PythonOperator(
        task_id='purchase_quantity_per_hour',
        python_callable=quantity_of_purchases,
    )

    branch = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=branch_decision,
    )

    send_report_task = PythonOperator(
        task_id='send_report',
        python_callable=send_report,
    )

    skip_report = EmptyOperator(task_id='skip_report')

    wait_for_raw >> update_task >> purchase_quantity_task >> branch >> [send_report_task, skip_report]
