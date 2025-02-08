import pandas as pd
from sqlalchemy import create_engine, text


def db_process(self):
    df = self.df
    engine = create_engine(self.connection_string)
    with engine.begin() as connection:
        data_to_insert = [
            (row['Vcode'], row['Vname'], row['VEngname'], row['Vnom']) 
            for _, row in df.iterrows()
        ]
        insert_stmt = text("INSERT INTO currencies (v_code, v_name, v_eng_name, v_nom) VALUES (:v_code, :v_name, :v_eng_name, :v_nom)")
        connection.execute(text("TRUNCATE TABLE currencies;"))
        for row in data_to_insert:
            connection.execute(insert_stmt, {
                'v_code': row[0],
                'v_name': row[1],
                'v_eng_name': row[2],
                'v_nom': row[3]
            })