# Airflow image: webserver, scheduler, and DAG execution.
# Base image already includes Python and Apache Airflow.
FROM apache/airflow:2.9.3-python3.11

# Extra Python packages (Postgres provider, pytest for optional in-container checks)
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# Pipeline code: DAG definitions and optional plugins
COPY dags/ ${AIRFLOW_HOME}/dags/
COPY plugins/ ${AIRFLOW_HOME}/plugins/
