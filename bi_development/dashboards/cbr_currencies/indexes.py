import pandas as pd
import os
import soap_requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from zeep import Client, Plugin
from sqlalchemy import create_engine, text
    

class CustomHeaderPlugin(Plugin):
    def __init__(self, soap_action):
        super().__init__()
        self.soap_action = soap_action

    def egress(self, envelope, http_headers, operation, binding=None, client=None):
        http_headers['SOAPAction'] = self.soap_action
        return envelope, http_headers

class Cbr():
    def __init__(self, list_column_name, link_folder, link_file, days_before, soap_action, wsdl, method, params, separator = ';', extension = '.csv'):
        self.list_column_name = list_column_name
        self.link_address = link_folder + link_file + '.txt'
        self.open_file = open(self.link_address, encoding='utf-8')
        self.readline = ''
        self.len_rl = 0
        self.link_list = []
        self.separator = separator
        self.extension = extension
        self.soap_action = soap_action
        self.wsdl = wsdl
        self.method = method
        self.dates = ()
        self.currency = ''
        self.days_before = days_before
        self.df_indexes = pd.DataFrame(columns=self.list_column_name)
        self.parametrs = params
        self.username = 'python_writer'
        self.password = 'python3'
        self.dbname = 'currencies'
        self.connection_string = f'postgresql+psycopg2://{self.username}:{self.password}@localhost/{self.dbname}'

    def read_line(self):
        self.readline = self.open_file.readline()
        self.len_rl = len(self.readline)    
        self.link_list = self.readline.strip().split(self.separator)

    def close_file(self):
        self.open_file.close()

    def get_request_date(self):
        current_date = datetime.now()
        todate = current_date
        fromdate = todate - timedelta(days=self.days_before)
        self.dates = (fromdate, todate)

    def get_request(self):
        self.get_request_date()
        client = Client(wsdl=self.wsdl, plugins=[CustomHeaderPlugin(self.soap_action)])                
        response = getattr(client.service, self.method)(**self.parametrs)
        return response

class Currencies(Cbr):   
    def xml_parsing(self):
        response = self.get_request()
        data = []
        for tag in response.findall('.//ValuteCursDynamic', namespaces={'': ''}):
            row = {'date': tag.find('CursDate').text,
                   'name': self.currency,
                   'value': tag.find('Vcurs').text,
                   'unit': tag.find('Vnom').text
                   }
            data.append(row)
        df = pd.DataFrame(data)
        return df
        
    def parsing_cycle(self):
        act_df = self.df_indexes.copy()
        self.read_line()
        while self.len_rl > 0:
            print(self.link_list[2])
            self.currency = self.link_list[2]
            act_df = act_df._append(self.xml_parsing())
            self.df_indexes = act_df.copy()
            self.read_line()
        self.close_file()
    
    def processing(self):
        df_indexes = self.df_indexes
        df_indexes['date'] = pd.to_datetime(df_indexes['date'])
        df_indexes['date'] = df_indexes['date'].dt.strftime('%d.%m.%Y')
        df_indexes = df_indexes[['date', 'value', 'name', 'unit']]
        self.df_indexes = df_indexes
        self.df_indexes.to_csv(link_folder + 'proc_' + type_measures + '.csv', index = False, sep = ';')

class EnumCurrencies(Cbr):
    def parsing(self):
        response = self.get_request()
        data = []
        for tag in response.findall('.//EnumValutes', namespaces={'': ''}):
            row = {
                'Vcode': tag.find('Vcode').text.strip(),
                'Vname': tag.find('Vname').text.strip(),
                'VEngname': tag.find('VEngname').text.strip(),
                'Vnom': tag.find('Vnom').text.strip()
            }
            data.append(row)
        df = pd.DataFrame(data)
        df.to_csv(link_folder + type_measures + '.csv', index = False, sep = ';')
    
    def db_process(self):
        df = pd.read_csv('bi_development/dashboards/cbr_currencies/enum.csv', encoding='utf-8', sep=';')
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

class CbrZCYC(Cbr):
# Метод parsing() для получения данных с cbr.ru использует SOAP-запрос к веб-сервису ЦБ (функция create_request() из модуля soap_requests)
# В идеале, в случае, если будут использоваться много различных запросов к api, нужно делать хранилище шаблонов и брать параметры парсинга из переменных
    def parsing(self):
        response = ET.fromstring(soap_requests.create_request().content)
        data = []
        for tag in response.findall('.//zcyc_params/ZCYC', namespaces={'': ''}):
            row = {
                self.list_column_name[0]: tag.find('D0').text,
                'year_1': tag.find('v_1_0').text,
                'year_5': tag.find('v_2_0').text,
                'year_10': tag.find('v_10_0').text
                }
            data.append(row)
        df = pd.DataFrame(data)
        return df
    
    def processing(self):
        df = self.parsing()
        df_indexes = df.melt(id_vars=[self.list_column_name[0]], value_vars=['year_1', 'year_5', 'year_10'], var_name=self.list_column_name[1], value_name=self.list_column_name[2])
        df_indexes['date'] = pd.to_datetime(df_indexes['date'])
        df_indexes['date'] = df_indexes['date'].dt.strftime('%d.%m.%Y')
        df_indexes['name'] = df_indexes['name'].replace({
            'year_1':'Доходность облигации Россия годовые, RUB',
            'year_5':'Доходность облигации Россия 2-летние, RUB',
            'year_10':'Доходность облигации Россия 10-летние, RUB'
        })
        df_indexes = df_indexes[['date', 'value', 'name']]
        self.df_indexes = df_indexes
        self.df_indexes['unit'] = ''
        self.df_indexes.to_csv(link_folder + 'proc_' + type_measures + '.csv', index = False, sep = ';')



link_folder = os.getcwd() + '/bi_development/dashboards/cbr_currencies/'
link_file = 'resourses'
# type_measures = 'cbr_currencies'
list_column_name = ['date', 'name', 'value', 'unit']

# indexes = Cbr(list_column_name, link_folder, link_file, days_before=30)
# indexes.parsing_cycle()
# indexes.processing()

soap_action = 'http://web.cbr.ru/EnumValutesXML'
wsdl = 'https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL'
method = 'EnumValutesXML'
params = {'Seld': 0}
type_measures = 'enum'
indexes = EnumCurrencies(list_column_name, link_folder, link_file, 3, soap_action, wsdl, method, params)

indexes.parsing()
indexes.db_process()


