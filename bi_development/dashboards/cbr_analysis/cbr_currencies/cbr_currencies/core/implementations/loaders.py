from cbr_currencies.core.interfaces import ILoader, ICredential
from decouple import config
import psycopg2
import pandas as pd
import os

class Credential(ICredential):
    def __init__(self):
        self.conn = None

    def get_postgres_credentials(self):
        """Гибко получает параметры подключения"""
        try:
            from airflow.hooks.base_hook import BaseHook
            is_airflow = True
        except ImportError:
            is_airflow = False

        if is_airflow and os.getenv('AIRFLOW__CORE__EXECUTOR'):
            # Если внутри Airflow → Airflow Connection
            self.conn = BaseHook.get_connection('currencies_db')
            return {
                'host': self.conn.host,
                'port': self.conn.port,
                'database': self.conn.schema,
                'user': self.conn.login,
                'password': self.conn.password,
            }
        else:
            # Если локально → .env
            return {
                'host': config('DB_HOST'),
                'database': config('DB_NAME'),
                'user': config('DB_USER'),
                'password': config('DB_PASSWORD'),
            }



class PostgresLoader(ILoader):
    def __init__(self, context):
        super().__init__(context)
        self.credential = Credential()
        self.credentials = self.credential.get_postgres_credentials()
        self.conn = psycopg2.connect(dbname=self.credentials['database'], user=self.credentials['user'], password=self.credentials['password'], host=self.credentials['host'])
        self.service_query = self.context.get_attr('service_query')
        self.insert_query = self.context.get_attr('insert_query')
        
    def load(self, df: pd.DataFrame):
        print(f"Загрузка {len(df)} записей в БД...")
        cur = self.conn.cursor()
        service_query = self.service_query
        insert_query = self.insert_query + f'({", ".join(["%s" for _ in range(len(df.columns))])})'
        data_to_insert = list(df.itertuples(index=False, name=None))
        try:
            cur.execute(service_query)
            cur.executemany(insert_query, data_to_insert)
            self.conn.commit()
        except Exception as e:
            print(f"Произошла ошибка: {e}")
        finally:
            cur.close()
        self.conn.close()
        print('загрузка в БД прошла успешно')