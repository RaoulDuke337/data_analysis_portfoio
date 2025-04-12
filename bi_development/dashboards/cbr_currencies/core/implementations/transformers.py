import pandas as pd
from core.interfaces import ITransformer

class NoTransformer(ITransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

class CurrencyTransformer(ITransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

class MeltTransformer(ITransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df
    
class DateTransformer(ITransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%Y')
        return df