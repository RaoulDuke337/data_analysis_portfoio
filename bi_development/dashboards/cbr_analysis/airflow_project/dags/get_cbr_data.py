from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pickle

from cbr_currencies.factory import get_data_pipeline  # Фабрика эээ обнова !, возвращающая pipeline по имени сервиса



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

    # Создаем pipeline для каждого сервиса один раз
    pipelines = {
        service: get_data_pipeline(service)
        for service in ["currencies", "metals"]
    }

    def run_fetch(**kwargs):
        soap_client = kwargs["soap_client"]
        kwargs['ti'].xcom_push(key="fetched_data", value=soap_client.fetch_data())

    def run_parse(**kwargs):
        parser = kwargs["parser"]
        xml_data = kwargs['ti'].xcom_pull(key="fetched_data")
        parsed_data = parser.parse(xml_data)
        push_dataframe_to_xcom(kwargs['ti'], "parsed_data", parsed_data)

    def run_transform(**kwargs):
        transformer = kwargs["transformer"]
        parsed_data = pull_dataframe_from_xcom(kwargs['ti'], "parsed_data")
        transformed_data = transformer.transform(parsed_data)
        push_dataframe_to_xcom(kwargs['ti'], "transformed_data", transformed_data)

    def run_load(**kwargs):
        loader = kwargs["loader"]
        data = pull_dataframe_from_xcom(kwargs['ti'], "transformed_data")
        loader.load(data)

    for service, pipeline in pipelines.items():
        fetch = PythonOperator(
            task_id=f"{service}_fetch",
            python_callable=run_fetch,
            op_kwargs={"soap_client": pipeline.soap_client}
        )
        parse = PythonOperator(
            task_id=f"{service}_parse",
            python_callable=run_parse,
            op_kwargs={"parser": pipeline.parser}
        )
        transform = PythonOperator(
            task_id=f"{service}_transform",
            python_callable=run_transform,
            op_kwargs={"transformer": pipeline.transformer}
        )
        load = PythonOperator(
            task_id=f"{service}_load",
            python_callable=run_load,
            op_kwargs={"loader": pipeline.loader}
        )
         
        fetch >> parse >> transform >> load