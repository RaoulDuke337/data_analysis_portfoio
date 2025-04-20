import pandas as pd
from datetime import datetime
from core.interfaces import ITransformer

class NoTransformer(ITransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

class CurrencyTransformer(ITransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

class UnpivotTransformer(ITransformer):
    def __init__(self, context):
        super().__init__(context)
        self.columns = self.context.get_attr('columns')
        self.alt_columns = self.context.get_attr('alt_columns')

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.melt(
            id_vars=[self.columns[0]], value_vars=self.columns[1:],
            var_name=self.alt_columns[0], value_name=self.alt_columns[1]
            )
        return df
    
class DateTransformer(ITransformer):
    def convert_to_datetime(self, date_str):
        return datetime.strptime(f"01.{date_str}", "%d.%m.%Y").strftime("%Y-%m-%d")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df['date'] = df['date'].apply(self.convert_to_datetime)
        return df