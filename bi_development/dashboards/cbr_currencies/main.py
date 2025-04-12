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
import importlib


class CustomHeaderPlugin(Plugin):
    def __init__(self, soap_action):
        super().__init__()
        self.soap_action = soap_action

    def egress(self, envelope, http_headers, operation, binding=None, client=None):
        http_headers['SOAPAction'] = self.soap_action
        return envelope, http_headers
    

class Context:
    """Объект для хранения общих атрибутов между компонентами"""
    def __init__(self, service_name: str, config_path: str, registry_path: str):
        self.service_name = service_name
        
        with open(config_path) as f:
            self.configuration = json.load(f)

        with open(registry_path) as f:
            raw_registry = json.load(f)
            self.registry = {
                service_type: {
                    component: self._import_class(dotted_path)
                    for component, dotted_path in components.items()
                }
                for service_type, components in raw_registry.items()
            }

    def get_attr(self, attr: str):
        self.configuration = [service for service in self.configuration["services"] if service["name"] == self.service_name]
        return self.configuration[0].get(attr)

    def get_component(self, component_name: str):
        """Вернёт нужный класс"""
        service_type = self.configuration.get("name")
        return self.registry[service_type][component_name]

    def _import_class(self, dotted_path: str):
        """Импортирует класс по полному dotted path."""
        module_path, class_name = dotted_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    
    


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
        

# 5. Конкретный клиент SOAP
class NoDateSoapClient(ISoapClient):
    def __init__(self, context: Context):
        super().__init__(context)
        self.query_parametrs = context.get_attr('parametrs')

    def fetch_all(self) -> any:
        client = Client(wsdl=self.wsdl, plugins=[CustomHeaderPlugin(self.soap_action)])
        response = getattr(client.service, self.method)(**self.query_parametrs)
        return [response]

# конкретная реализация SOAP-клиента с датами   
class DateSoapClient(IDateSoapClient):
    def __init__(self, context: Context):
        super().__init__(context)
        self.parameters = context.get_attr('parametrs')
        self.query_parametrs = None

    def fetch_all(self) -> any:
        self.get_request_date()
        self.query_parametrs = {
            param: value for param, value in zip(self.parameters, self.dates)
            }

        client = Client(wsdl=self.wsdl, plugins=[CustomHeaderPlugin(self.soap_action)])
        response = getattr(client.service, self.method)(**self.query_parametrs)
        return [response]

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
    
    def __init__(self, soap_client: CurrencySoapClient):
        self.soap_client = soap_client
        self.csv_path = context.get_attr('csv_source')
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
        service_type = context.get_attr('name') 

        if service_type == 'currencies':
            csv_path = './' + context.get_attr('csv_source')
            print(f'Источник данных для клиента: {csv_path}, используется CurrencyFetcher')
            return CurrencyFetcher(CurrencySoapClient(context), csv_path)
        elif service_type == 'metals':
            return DateSoapClient(context)
        else:
            return NoDateSoapClient(context)


class ServiceFactory:
    """Фабрика для переключения сервисов"""
    def __init__(self, context: Context):
        self.context = context

    def get_soap_client(self):
        return self.context.get_component("soap_client")(self.context)

    def get_parser(self):
        return self.context.get_component("parser")(self.context)

    def get_transformer(self):
        return self.context.get_component("transformer")(self.context)

    def get_loader(self):
        return self.context.get_component("loader")(self.context)

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

# 6. Конкретный парсер
class MainParser(IParser):
    def __init__(self, context: Context):
        super().__init__(context)

    def parse(self, xml_data) -> pd.DataFrame:
        data = []
        service = context.get_attr("name")
        for xml_doc in xml_data:
            for tag in xml_doc.findall(self.root_tag, namespaces={'': ''}):
                row = {
                    # извлекаем в root-теге все данные сопоставляя названия столбцов с тегами через dict comp
                    column_name: (tag.find(tag_name).text.strip() if tag.find(tag_name) is not None else None)
                    for column_name, tag_name in zip(self.columns, self.tags)
                }
                #print(row)
                data.append(row)
        #print(data)
        df = pd.DataFrame(data)
        df.to_csv('./' + service + '.csv', index=False, sep=';')
        return df


# 3. Абстракция трансформации данных
class ITransformer(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

class NoTransformer(ITransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

class CurrencyTransformer(ITransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


# 4. Абстракция загрузки в БД
class ILoader(ABC):
    def __init__(self, context: Context):
        self.context = context

    @abstractmethod
    def load(self, df: pd.DataFrame):
        pass

# 8. Конкретный загрузчик в БД
class PostgresLoader(ILoader):
    def __init__(self, context: Context):
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


# 9. Главный обработчик
class DataPipeline:
    """Главный пайплайн для обработки данных"""
    
    def __init__(self, service: ISoapClient, parser: IParser, transformer: ITransformer, loader: ILoader, context: Context):
        self.parser = parser
        self.transformer = transformer
        self.loader = loader
        self.context = context
        self.service = service

    def run(self):
        """Запускает процесс обработки данных"""
        print('Запуск пайплайна')    
        results = self.service.fetch_all()
        print(results)
        parsed = self.parser.parse(results)
        transformed = self.transformer.transform(parsed)
        self.loader.load(transformed)

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

#  Запуск с разными реализациями
if __name__ == "__main__":
    context = Context(service_name="metals", config_path="./services.json", registry_path="./service_registry.json")
    factory = ServiceFactory(context)
    pipeline = DataPipeline(
        context=context,
        soap_client = factory.get_soap_client(),
        parser = factory.get_parser(),
        transformer = factory.get_transformer(),
        loader=factory.get_loader(),
    )
    pipeline.run()

