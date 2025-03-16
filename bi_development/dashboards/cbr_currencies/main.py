from abc import ABC, abstractmethod
import json
import pandas as pd
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from zeep import Client, Plugin
from decouple import config
import psycopg2


class CustomHeaderPlugin(Plugin):
    def __init__(self, soap_action):
        super().__init__()
        self.soap_action = soap_action

    def egress(self, envelope, http_headers, operation, binding=None, client=None):
        http_headers['SOAPAction'] = self.soap_action
        return envelope, http_headers
    

class Context:
    """Объект для хранения общих атрибутов между компонентами"""
    def __init__(self, days_before, service_name, params={}, separator = ';', extension = '.csv'):
        self.list_column_name = []
        self.alt_columns = []
        self.separator = separator
        self.extension = extension
        self.soap_action = ''
        self.wsdl = ''
        self.method = ''
        self.service_name = service_name
        self.root_tag = ''
        self.tags = []
        self.dates = ()
        self.currency = ''
        self.currency_code = ''
        self.days_before = days_before
        self.df_indexes = pd.DataFrame(columns=self.list_column_name)
        self.parametrs = params
        self.query_parametrs = {}
        self.username = config('USERNAME')
        self.password = config('PASSWORD')
        self.db_name = config('DB_NAME')
        self.configuration = {}
        self.conn = psycopg2.connect(dbname=self.db_name, user=self.username, password=self.password, host="127.0.0.1")

    def get_attr(self, attr: str):
        with open("./services.json", "r") as file:
            services = json.load(file)
        self.configuration = [service for service in services["services"] if service["name"] == self.service_name]
        return self.configuration[0].get(attr)


# 1. Абстракция клиента SOAP
class ISoapClient(ABC):
    """Базовый интерфейс для SOAP-клиентов"""
    def __init__(self, context: Context):
        self.context = context
        self.wsdl = context.get_attr('wsdl')
        self.soap_action = context.get_attr('soap_action')
        self.method = context.get_attr('method')

    @abstractmethod
    def fetch_data(self) -> any:
        """Общий метод для получения данных"""
        pass

class IDateSoapClient(ISoapClient):
    """Расширенный клиент с датами"""
    def __init__(self, context: Context):
        super().__init__(context)
        self.days_before = context.get_attr("days_before")
        self.dates = None

    def get_request_date(self):
        """Определяем диапазон дат"""
        current_date = datetime.now()
        todate = current_date
        fromdate = todate - timedelta(days=self.days_before)
        self.dates = (fromdate, todate)


# 2. Абстракция парсера XML -> DataFrame
class IParser(ABC):
    @abstractmethod
    def parse(self, xml_data: any) -> pd.DataFrame:
        """Метод для парсинга XML-данных в DataFrame"""
        pass


# 3. Абстракция трансформации данных
class ITransformer(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


# 4. Абстракция загрузки в БД
class ILoader(ABC):
    @abstractmethod
    def load(self, df: pd.DataFrame):
        pass


# 5. Конкретный клиент SOAP
class NoDateSoapClient(ISoapClient):
    def __init__(self, context: Context):
        super().__init__(context)
        self.query_parametrs = context.get_attr('parametrs')

    def fetch_data(self) -> any:
        client = Client(wsdl=self.wsdl, plugins=[CustomHeaderPlugin(self.soap_action)])
        response = getattr(client.service, self.method)(**self.query_parametrs)
        return response

# конкретная реализация SOAP-клиента с датами   
class DateSoapClient(IDateSoapClient):
    def __init__(self, context: Context):
        super().__init__(context)
        self.parameters = context.get_attr('parametrs')
        self.query_parametrs = None

    def fetch_data(self) -> any:
        self.get_request_date()
        self.query_parametrs = {
            param: value for param, value in zip(self.parameters, self.dates)
            }

        client = Client(wsdl=self.wsdl, plugins=[CustomHeaderPlugin(self.soap_action)])
        response = getattr(client.service, self.method)(**self.query_parametrs)
        return response
    
class CurrencySoapClient(IDateSoapClient):
    def __init__(self, context: Context):
        super().__init__(context)
        self.parameters = context.get_attr('parametrs')
        self.query_parametrs = None

    def fetch_data(self) -> any:
        self.get_request_date()
        self.query_parametrs = {
            param: value for param, value in zip(self.parameters, self.dates)
        }
        

        client = Client(wsdl=self.wsdl, plugins=[CustomHeaderPlugin(self.soap_action)])
        response = getattr(client.service, self.method)(**self.query_parametrs)
        return response

# 6. Конкретный парсер
class ExampleParser(IParser):
    def parse(self, xml_data: str) -> pd.DataFrame:
        # Заглушка для парсинга XML
        return pd.DataFrame([{"id": 1, "name": "test"}])


# 7. Конкретный трансформер
class ExampleTransformer(ITransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df["name"] = df["name"].str.upper()
        return df


# 8. Конкретный загрузчик в БД
class ExampleLoader(ILoader):
    def load(self, df: pd.DataFrame):
        print(f"Загрузка {len(df)} записей в БД...")


# 9. Главный обработчик
class DataPipeline:
    def __init__(self, client: ISoapClient, parser: IParser, transformer: ITransformer, loader: ILoader):
        self.client = client
        self.parser = parser
        self.transformer = transformer
        self.loader = loader

    def run(self):
        xml_data = self.client.fetch_data()
        df = self.parser.parse(xml_data)
        df = self.transformer.transform(df)
        self.loader.load(df)


# 🔥 Запуск с разными реализациями
if __name__ == "__main__":
    pipeline = DataPipeline(
        client=ExampleSoapClient(),
        parser=ExampleParser(),
        transformer=ExampleTransformer(),
        loader=ExampleLoader()
    )
    pipeline.run()
