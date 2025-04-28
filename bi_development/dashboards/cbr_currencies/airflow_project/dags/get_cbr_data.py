from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pickle
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from dashboards.cbr_currencies.factory import get_data_pipeline  # Фабрика эээ, возвращающая pipeline по имени сервиса





def push_dataframe_to_xcom(ti, key, dataframe):
    """Функция для сериализации DataFrame и передачи через XCom"""
    ti.xcom_push(key=key, value=pickle.dumps(dataframe))

def pull_dataframe_from_xcom(ti, key):
    """Функция для извлечения DataFrame из XCom"""
    return pickle.loads(ti.xcom_pull(key=key))


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 28),
    "retries": 1
}

with DAG(
    dag_id="cbr_data_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    def run_fetch(**kwargs):
        service = kwargs["service"]
        pipeline = get_data_pipeline(service)
        kwargs['ti'].xcom_push(key="fetched_data", value=pipeline.soap_client.fetch_data())

    def run_parse(**kwargs):
        service = kwargs["service"]
        pipeline = get_data_pipeline(service)
        xml_data = kwargs['ti'].xcom_pull(key="fetched_data")
        parsed_data = pipeline.parser.parse(xml_data)
        push_dataframe_to_xcom(kwargs['ti'], "parsed_data", parsed_data)

    def run_transform(**kwargs):
        service = kwargs["service"]
        pipeline = get_data_pipeline(service)
        parsed_data = pull_dataframe_from_xcom(kwargs['ti'], "parsed_data")
        transformed_data = pipeline.transformer.transform(parsed_data)
        push_dataframe_to_xcom(kwargs['ti'], "transformed_data", transformed_data)

    def run_load(**kwargs):
        service = kwargs["service"]
        pipeline = get_data_pipeline(service)
        data = pull_dataframe_from_xcom(kwargs['ti'], "transformed_data")
        pipeline.loader.load(data)

    for service in ["Currencies", "Metals"]:
        fetch = PythonOperator(
            task_id=f"{service}_fetch",
            python_callable=run_fetch,
            op_kwargs={"service": service}
        )
        parse = PythonOperator(
            task_id=f"{service}_parse",
            python_callable=run_parse,
            op_kwargs={"service": service}
        )
        transform = PythonOperator(
            task_id=f"{service}_transform",
            python_callable=run_transform,
            op_kwargs={"service": service}
        )
        load = PythonOperator(
            task_id=f"{service}_load",
            python_callable=run_load,
            op_kwargs={"service": service}
        )

        fetch >> parse >> transform >> load
