from abc import ABC, abstractmethod
import pandas as pd

class ITransformer(ABC):
    """Базовый интерфейс для трансформеров"""
    def __init__(self, context):
        self.context = context

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass