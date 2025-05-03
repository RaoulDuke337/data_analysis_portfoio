from abc import ABC, abstractmethod
from cbr_currencies.core.context import Context
import pandas as pd

class IParser(ABC):
    def __init__(self, context: Context):
        self.root_tag = './' + context.get_attr('root_tag')
        self.tags = context.get_attr('tags')
        self.columns = context.get_attr('columns')

    @abstractmethod
    def parse(self, xml_data: any) -> pd.DataFrame:
        """Метод для парсинга XML-данных в DataFrame"""
        pass