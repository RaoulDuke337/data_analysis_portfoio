from zeep import Client, Plugin
import pandas as pd
from core.interfaces import ISoapClient, IDateSoapClient

class CustomHeaderPlugin(Plugin):
    def __init__(self, soap_action):
        super().__init__()
        self.soap_action = soap_action

    def egress(self, envelope, http_headers, operation, binding=None, client=None):
        http_headers['SOAPAction'] = self.soap_action
        return envelope, http_headers

class NoDateSoapClient(ISoapClient):
    def __init__(self, context):
        super().__init__(context)
        self.query_parametrs = context.get_attr('parametrs')

    def fetch_data(self) -> any:
        client = Client(wsdl=self.wsdl, plugins=[CustomHeaderPlugin(self.soap_action)])
        response = getattr(client.service, self.method)(**self.query_parametrs)
        return [response]

# конкретная реализация SOAP-клиента с датами   
class DateSoapClient(IDateSoapClient):
    def __init__(self, context):
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
        return [response]

# конкретная реализаци я SOAP-клиента для сервиса currencies 
class CurrencySoapClient(IDateSoapClient):
    def __init__(self, context):
        super().__init__(context)
        self.parameters = context.get_attr('parametrs')
        self.csv_path = context.get_attr('csv_source')
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
    
    def __init__(self, context):
        self.soap_client = CurrencySoapClient(context)
        self.csv_path = self.soap_client.csv_path
        self.currencies = self.load_currencies()

    def load_currencies(self):
        """Загружает список валют из CSV."""
        df = pd.read_csv(self.csv_path, sep=';')
        return df['v_code'].tolist()

    def fetch_data(self):
        """Итерация по валютам и запрос данных."""
        results = []
        for currency in self.currencies:
            print(f"Извлечение данных для валюты {currency}...")  
            result = self.soap_client.fetch_data(currency)
            results.append(result)
        return results
