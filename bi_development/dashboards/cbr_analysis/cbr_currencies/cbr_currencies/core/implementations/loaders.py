from cbr_currencies.core.interfaces import ILoader
from decouple import config
import psycopg2
import pandas as pd

class PostgresLoader(ILoader):
    def __init__(self, context):
        super().__init__(context)
        self.username = config('USERNAME')
        self.password = config('PASSWORD')
        self.db_name = config('DB_NAME')
        self.conn = psycopg2.connect(dbname=self.db_name, user=self.username, password=self.password, host="127.0.0.1")
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