from abc import ABC, abstractmethod
import json
from lxml import etree
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
    def __init__(self, service_name):
        self.username = config('USERNAME')
        self.password = config('PASSWORD')
        self.db_name = config('DB_NAME')
        self.configuration = {}
        self.service_name = service_name
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
    def __init__(self, context: Context):
        self.root_tag = './' + context.get_attr('root_tag')
        self.tags = context.get_attr('tags')
        self.columns = context.get_attr('columns')

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

# конкретная реализаци я SOAP-клиента для сервиса currencies 
class CurrencySoapClient(IDateSoapClient):
    def __init__(self, context: Context):
        super().__init__(context)
        self.parameters = context.get_attr('parametrs')
        self.query_parametrs = None

    def fetch_data(self, currency_code) -> any:
        self.get_request_date()
        self.query_parametrs = {
            param: value for param, value in zip(self.parameters, self.dates)
        }
        self.query_parametrs[self.parameters[2]] = currency_code

        client = Client(wsdl=self.wsdl, plugins=[CustomHeaderPlugin(self.soap_action)])
        response = getattr(client.service, self.method)(**self.query_parametrs)
        return response

class CurrencyFetcher:
    """Класс, который управляет запросами для каждой валюты."""
    
    def __init__(self, soap_client: CurrencySoapClient, csv_path):
        self.soap_client = soap_client
        self.csv_path = csv_path
        self.currencies = self.load_currencies()

    def load_currencies(self):
        """Загружает список валют из CSV."""
        df = pd.read_csv(self.csv_path, sep=';')
        return df['v_code'].tolist()

    def fetch_all(self):
        """Итерация по валютам и запрос данных."""
        results = []
        for currency in self.currencies:
            print(f"Извлечение данных для валюты {currency}...")  
            result = self.soap_client.fetch_data(currency)
            results.append(result)
        return results


class SoapServiceFactory:
    """Фабрика для создания SOAP-сервисов"""
    
    @staticmethod
    def create(context: Context):
        print('Фабрика запущена')
        service_type = context.get_attr('name')  # Определяем тип сервиса
        csv_path = './' + context.get_attr('csv_source')

        if service_type == 'currencies':
            print(f'Источник данных для клиента: {csv_path}, используется CurrencyFetcher')
            return CurrencyFetcher(CurrencySoapClient(context), csv_path)
        else:
            return NoDateSoapClient(context)  # Заглушка для других сервисов

# 6. Конкретный парсер
class MainParser(IParser):
    def __init__(self, context: Context):
        super().__init__(context)

    def parse(self, xml_data: list) -> pd.DataFrame:
        data = []
        for xml_doc in xml_data:
            tag = xml_doc
            row = {
                # извлекаем в root-теге все данные сопоставляя названия столбцов с тегами через dict comp
                column_name: (tag.find(tag_name).text.strip() if tag.find(tag_name) is not None else None)
                for column_name, tag_name in zip(self.columns, self.tags)
            }
            print(row)
            data.append(row)
        print(data)
        df = pd.DataFrame(data)
        df.to_csv('./' + 'currencies' + '.csv', index=False, sep=';')
        return df


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
    """Главный пайплайн для обработки данных"""
    
    def __init__(self, service: SoapServiceFactory, parser: IParser, transformer: ITransformer, loader: ILoader, context: Context):
        self.parser = parser
        self.transformer = transformer
        self.loader = loader
        self.context = context
        self.service = service.create(context)

    def run(self):
        """Запускает процесс обработки данных"""
        if isinstance(self.service, CurrencyFetcher):
            print('На фабрике выбран экземпляр CurrencyFetcher')
            results = self.service.fetch_all()
        else:
            print('На фабрике выбран экземпляр NoDateSoapClient')
            results = self.service.fetch_data()

        # results = self.service.fetch_all() if isinstance(self.service, CurrencyFetcher) else [self.service.fetch_data()]
        
        parsed = self.parser.parse(results)

        # for result in results:
            # transformed = self.transformer.transform(parsed)
            # self.loader.load(transformed)

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# 🔥 Запуск с разными реализациями
if __name__ == "__main__":
    context = Context(service_name="currencies")
    pipeline = DataPipeline(
        context=context,
        service=SoapServiceFactory(),
        parser=MainParser(context),
        transformer=ExampleTransformer(),
        loader=ExampleLoader(),
    )
    pipeline.run()
