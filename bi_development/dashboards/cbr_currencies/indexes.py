import json
import pandas as pd
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from zeep import Client, Plugin
from decouple import config
import psycopg2
    

class CustomHeaderPlugin(Plugin):
    def __init__(self, soap_action):
        super().__init__()
        self.soap_action = soap_action

    def egress(self, envelope, http_headers, operation, binding=None, client=None):
        http_headers['SOAPAction'] = self.soap_action
        return envelope, http_headers

class Cbr():
    def __init__(self, days_before, service_name, params={}, separator = ';', extension = '.csv'):
        self.list_column_name = []
        self.alt_columns = []
        self.separator = separator
        self.extension = extension
        self.soap_action = ''
        self.wsdl = ''
        self.method = ''
        self.service_name = service_name
        self.root_tag = ''
        self.tags = []
        self.dates = ()
        self.currency = ''
        self.currency_code = ''
        self.days_before = days_before
        self.df_indexes = pd.DataFrame(columns=self.list_column_name)
        self.parametrs = params
        self.query_parametrs = {}
        self.username = config('USERNAME')
        self.password = config('PASSWORD')
        self.db_name = config('DB_NAME')
        self.configuration = {}
        self.conn = psycopg2.connect(dbname=self.db_name, user=self.username, password=self.password, host="127.0.0.1")

    def read_config(self):
        with open("./services.json", "r") as file:
            services = json.load(file)
        self.configuration = [service for service in services["services"] if service["name"] == self.service_name]
        self.method = self.configuration[0].get('method')
        self.wsdl = self.configuration[0].get('wsdl')
        self.soap_action = self.configuration[0].get('soap_action')
        self.list_column_name = self.configuration[0].get('columns')
        self.alt_columns = self.configuration[0].get('alt_columns')
        self.root_tag = './/' + self.configuration[0].get('root_tag')
        self.tags = self.configuration[0].get('tags')
        self.parametrs = self.configuration[0].get('parametrs')

    def get_request_date(self):
        current_date = datetime.now()
        todate = current_date
        fromdate = todate - timedelta(days=self.days_before)
        self.dates = (fromdate, todate)

    def convert_to_datetime(self, date_str):
        return datetime.strptime(f"01.{date_str}", "%d.%m.%Y").strftime("%Y-%m-%d")

    def get_request(self):
        self.get_request_date()
        client = Client(wsdl=self.wsdl, plugins=[CustomHeaderPlugin(self.soap_action)])                
        response = getattr(client.service, self.method)(**self.query_parametrs)
        return response
    
    def parsing(self, response):
        data = []
        for tag in response.findall(self.root_tag, namespaces={'': ''}):
            row = {
                # извекаем в root-теге все данные сопоставля названия столбцов с тегами через dict comp
                column_name: (tag.find(tag_name).text.strip() if tag.find(tag_name) is not None else None)
                for column_name, tag_name in zip(self.list_column_name, self.tags)
            }
            data.append(row)
        df = pd.DataFrame(data)
        return df
    
    def universal_parsing(self):
        self.get_request_date()
        self.query_parametrs = {
            param: value for param, value in zip(self.parametrs, self.dates)
        }
        response = self.get_request()
        data = []
        for tag in response.findall(self.root_tag, namespaces={'': ''}):
            row = {
                # извекаем в root-теге все данные сопоставля названия столбцов с тегами через dict comp
                column_name: (tag.find(tag_name).text.strip() if tag.find(tag_name) is not None else None)
                for column_name, tag_name in zip(self.list_column_name, self.tags)
            }
            data.append(row)
        df = pd.DataFrame(data)
        df.to_csv('./' + self.service_name + '.csv', index=False, sep=';')
        self.df_indexes = df
    
    def db_process(self, service_query='', insert_query=''):
        df = self.df_indexes
        cur = self.conn.cursor()
        service_query = service_query
        insert_query = insert_query
        data_to_insert = list(df.itertuples(index=False))
        try:
            cur.execute(service_query)
            cur.executemany(insert_query, data_to_insert)
            self.conn.commit()
        except Exception as e:
            print(f"Произошла ошибка: {e}")
        finally:
            cur.close()
        self.conn.close()

class Currencies(Cbr):   
    def parsing(self):
        response = self.get_request()
        df = super().parsing(response)
        df['name'] = self.currency
        df['v_code'] = self.currency_code
        return df
    
    def parsing_cycle(self):
        act_df = self.df_indexes.copy()
        enum_df = pd.read_csv('./' + 'enum_currencies' + '.csv', sep = ';')
        for idx, row in enum_df.iterrows():
            self.currency = row['v_name']
            self.currency_code = row['v_code']
            print(self.currency)
            self.get_request_date()
            self.query_parametrs = {
                param: value for param, value in zip(self.parametrs, self.dates)
                }
            self.query_parametrs[self.parametrs[2]] = self.currency_code
            act_df = act_df._append(self.parsing())
            self.df_indexes = act_df.copy()
        self.df_indexes.to_csv('./' + self.service_name + '.csv', index = False, sep = ';')

class EnumCurrencies(Cbr):
   def parsing(self):
        self.query_parametrs = self.parametrs
        self.dates = self.get_request_date()
        response = self.get_request()
        df = super().parsing(response)
        df.to_csv('./' + self.service_name + '.csv', index=False, sep=';')
        self.df_indexes = df

class ReservesAndBonds(Cbr):
    def parsing(self):
        self.universal_parsing()
        df = self.df_indexes
        df = df.melt(id_vars=[self.list_column_name[0]], value_vars=self.list_column_name[1:], var_name=self.alt_columns[0], value_name=self.alt_columns[1])
        df.to_csv('./' + self.service_name + '.csv', index=False, sep=';')
        self.df_indexes = df

class Inflation(Cbr):
    def processing(self):
        df = self.df_indexes
        df['date'] = df['date'].apply(self.convert_to_datetime)
        df.to_csv('./' + self.service_name + '.csv', index=False, sep=';')
        self.df_indexes = df


script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# service_query = 'TRUNCATE TABLE currencies;'
# insert_query = 'INSERT INTO currencies (v_code, v_name, v_eng_name, v_nom) VALUES (%s, %s, %s, %s)'

# indexes = EnumCurrencies(3, service_name='enum_currencies')
# indexes.read_config()
# indexes.parsing()
# indexes.db_process(service_query, insert_query)

# print('Загрузка справочника успешно')

# service_query = "TRUNCATE TABLE currencies_stage;"
# insert_query = "INSERT INTO currencies_stage (date, value, unit, name, v_code) VALUES (%s, %s, %s, %s, %s)"

# indexes = Currencies(10, service_name='currencies')
# indexes.read_config()
# indexes.parsing_cycle()
# # indexes.processing()
# indexes.db_process(service_query, insert_query)

# print('Загрузка фактов валют успешно')

service_query = 'TRUNCATE TABLE metals_stage;'
insert_query = 'INSERT INTO metals_stage (date, code_met, price) VALUES (%s, %s, %s)'

indexes = Cbr(10, service_name='metals')
indexes.read_config()
indexes.universal_parsing()
indexes.db_process(service_query, insert_query)

print('Загрузка металлов прошла успешно')


# service_query = 'TRUNCATE TABLE reserves_stage;'
# insert_query = 'INSERT INTO reserves_stage (date, measure, value) VALUES (%s, %s, %s)'

# indexes = Reserves(60, service_name='reserves')
# indexes.read_config()
# indexes.parsing()
# indexes.db_process(service_query, insert_query)

# service_query = 'TRUNCATE TABLE gov_bonds_stage;'
# insert_query = 'INSERT INTO gov_bonds_stage (date, measure, value) VALUES (%s, %s, %s)'

# indexes = Bonds(60, service_name='bonds')
# indexes.read_config()
# indexes.parsing()
# indexes.db_process(service_query, insert_query)


# service_query = 'TRUNCATE TABLE inflation_stage;'
# insert_query = 'INSERT INTO inflation_stage (date, key_rate, inf_fact, inf_goal) VALUES (%s, %s, %s, %s)'


# indexes = Inflation(300, service_name='inflation')
# indexes.read_config()
# indexes.parsing()
# indexes.processing()
# indexes.db_process(service_query, insert_query)


# service_query = 'TRUNCATE TABLE av_key_rate_stage;'
# insert_query = 'INSERT INTO av_key_rate_stage (date, fact) VALUES (%s, %s)'

# indexes = AvKeyRate(90, service_name='avg_key_rate')
# indexes.read_config()
# indexes.parsing()
# indexes.db_process(service_query, insert_query)


# service_query = 'TRUNCATE TABLE deposits_stage;'
# insert_query = 'INSERT INTO deposits_stage (date, total, one_week, overnight) VALUES (%s, %s, %s, %s)'


# indexes = Deposits(30, service_name='deposits')
# indexes.read_config()
# indexes.parsing()
# indexes.db_process(service_query, insert_query)