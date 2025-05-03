from abc import ABC, abstractmethod
from cbr_currencies.core.context import Context
from datetime import datetime, timedelta

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