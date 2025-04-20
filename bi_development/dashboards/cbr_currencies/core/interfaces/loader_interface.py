from abc import ABC, abstractmethod
from context import Context
import pandas as pd

class ILoader(ABC):
    def __init__(self, context: Context):
        self.context = context

    @abstractmethod
    def load(self, df: pd.DataFrame):
        pass