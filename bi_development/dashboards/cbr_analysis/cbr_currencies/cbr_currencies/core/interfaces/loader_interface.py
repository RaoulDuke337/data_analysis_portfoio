from abc import ABC, abstractmethod
from cbr_currencies.core.context import Context
import pandas as pd

class ICredential(ABC):
    @abstractmethod
    def get_postgres_credentials(self):
        """Получает параметры подключения в зависимости от среды выполнения"""
        pass


class ILoader(ABC):
    def __init__(self, context: Context):
        self.context = context

    @abstractmethod
    def load(self, df: pd.DataFrame):
        pass
