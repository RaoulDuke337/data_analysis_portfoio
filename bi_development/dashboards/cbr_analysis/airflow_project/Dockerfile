FROM apache/airflow:2.9.1

USER airflow

COPY airflow_project/requirements_airflow.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-3.12.txt"

# Install additional dependencies
COPY cbr_currencies /opt/cbr_currencies

USER root

RUN chown -R airflow: /opt/cbr_currencies

USER airflow

RUN pip install --no-cache-dir /opt/cbr_currencies