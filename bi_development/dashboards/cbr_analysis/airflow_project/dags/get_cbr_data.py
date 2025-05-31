from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from lxml import etree
import pandas as pd
import logging

from cbr_currencies.factory import get_data_pipeline  # Фабрика эээ обнова !, возвращающая pipeline по имени сервиса


log = logging.getLogger(__name__)

def resolve_xcom_value(xcom_value):
    if hasattr(xcom_value, "get_value"):
        return xcom_value.get_value()
    return xcom_value

def push_dataframe_to_xcom(ti, key, dataframe):
    """Сериализует DataFrame в JSON-строку и пушит в XCom"""
    json_data = dataframe.to_json(orient='split')
    ti.xcom_push(key=key, value=json_data)

def pull_dataframe_from_xcom(ti, task_id, key):
    """Пуллит JSON-строку из XCom и превращает обратно в DataFrame"""
    xcom_value = ti.xcom_pull(task_ids=task_id, key=key)
    json_data = resolve_xcom_value(xcom_value)
    dataframe = pd.read_json(json_data, orient='split', convert_dates=False)
    return dataframe


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

    # Создаем pipeline для каждого сервиса один раз
    pipelines = {
        service: get_data_pipeline(service)
        for service in ["metals", "currencies", "reserves", "bonds", "inflation", "avg_key_rate", "deposits"]
    }

    def run_fetch(**kwargs):
        soap_client = kwargs["soap_client"]
        fetched_data = soap_client.fetch_data()
        # Сериализуем данные и передаем их через XCom
        xml_strings = [etree.tostring(elem).decode('utf-8') for elem in fetched_data]
        log.info(f"[XCom PUSH] task_id=%s, key=%s, value=%s", kwargs['ti'].task_id, "fetched_data", "xml_strings")
        kwargs['ti'].xcom_push(key="fetched_data", value=xml_strings)

    def run_parse(**kwargs):
        parser = kwargs["parser"]
        xml_strings = kwargs['ti'].xcom_pull(task_ids=kwargs['fetch_task_id'], key='fetched_data')
        log.info(f"Pulled from XCom: task_id={kwargs['fetch_task_id']}, key=fetched_data, value={xml_strings}")
        # Из строк обратно переводим в элементы XML для корректной работы парсера
        xml_elements = [etree.fromstring(s) for s in xml_strings]
        parsed_data = parser.parse(xml_elements)
        push_dataframe_to_xcom(kwargs['ti'], "parsed_data", parsed_data)
        log.info(f"Pushing to XCom: task_id={kwargs['parse_task_id']}, key=parsed_data, value={parsed_data}")

    def run_transform(**kwargs):
        transformer = kwargs["transformer"]
        parsed_data = pull_dataframe_from_xcom(kwargs['ti'], task_id=kwargs['parse_task_id'], key="parsed_data")
        transformed_data = transformer.transform(parsed_data)
        push_dataframe_to_xcom(kwargs['ti'], "transformed_data", transformed_data)

    def run_load(**kwargs):
        loader = kwargs["loader"]
        data = pull_dataframe_from_xcom(kwargs['ti'], task_id=kwargs['transform_task_id'], key="transformed_data")
        loader.load(data)

    for service, pipeline in pipelines.items():
        group_id = f"{service}_pipeline"
        with TaskGroup(group_id=group_id) as group:
            fetch = PythonOperator(
                task_id=f"{service}_fetch",
                python_callable=run_fetch,
                op_kwargs={
                    "soap_client": pipeline.soap_client,
                    "fetch_task_id": f"{group_id}.{service}_fetch"
                }
            )
            parse = PythonOperator(
                task_id=f"{service}_parse",
                python_callable=run_parse,
                op_kwargs={
                    "parser": pipeline.parser,
                    "fetch_task_id": f"{group_id}.{service}_fetch",
                    "parse_task_id": f"{group_id}.{service}_parse"
                }
            )
            transform = PythonOperator(
                task_id=f"{service}_transform",
                python_callable=run_transform,
                op_kwargs={
                    "transformer": pipeline.transformer,
                    "parse_task_id": f"{group_id}.{service}_parse",
                    "transform_task_id": f"{group_id}.{service}_transform"
                }
            )
            load = PythonOperator(
                task_id=f"{service}_load",
                python_callable=run_load,
                op_kwargs={
                    "loader": pipeline.loader,
                    "transform_task_id": f"{group_id}.{service}_transform",
                    "load_task_id": f"{group_id}.{service}_load"
                }
            )

            fetch >> parse >> transform >> load