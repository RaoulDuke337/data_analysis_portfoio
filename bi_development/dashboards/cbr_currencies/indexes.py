import pandas as pd
import os
import soap_requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from zeep import Client, Plugin
    

class CustomHeaderPlugin(Plugin):
    def __init__(self, soap_action):
        super().__init__()
        self.soap_action = soap_action

    def egress(self, envelope, http_headers, operation, binding=None, client=None):
        http_headers['SOAPAction'] = self.soap_action
        return envelope, http_headers

class Cbr():
    def __init__(self, list_column_name, link_folder, link_file, days_before, separator = ';', extension = '.csv'):
        self.list_column_name = list_column_name
        self.link_address = link_folder + link_file + '.txt'
        self.open_file = open(self.link_address, encoding='utf-8')
        self.readline = ''
        self.len_rl = 0
        self.link_list = []
        self.separator = separator
        self.extension = extension
        self.soap_action = '"http://web.cbr.ru/GetCursDynamicXML"'
        self.wsdl = 'https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL'
        self.method = 'GetCursDynamicXML'
        self.dates = ()
        self.currency = ''
        self.days_before = days_before
        self.df_indexes = pd.DataFrame(columns=self.list_column_name)
        self.parametrs = {}

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



link_folder = os.getcwd() + '\\'
link_file = 'currencies_cbr'
type_measures = 'cbr_currencies'
list_column_name = ['date', 'name', 'value', 'unit']
class_name = ''

indexes = CbrCurrencies(list_column_name, link_folder, link_file, days_before=30)
indexes.parsing_cycle()
indexes.processing()

# СОБИРАЕМ СПИСОК ФАЙЛОВ
from os import walk
mypath = link_folder
f = []
for (dirpath, dirnames, filenames) in walk(mypath):
    f.extend(filenames)
    break


# ЗАГРУЗКА В БД
driver='Driver=SQL Server;'
server = 'Server=whsqlp02;'
db = 'Database=СлужбаРазвитияПродаж;'
auth='Trusted_Connection=yes;'

db = DB(driver, server, db, auth)


with db.cnxn:
    with db.cur:
        for fn in f:
            if fn[0:8] == 'proc_cbr':
                print(fn)
                table = pd.read_csv(fn, sep = ';')
                table.rename(columns={table.columns[0]: 'dates', table.columns[1]: 'fact', table.columns[2]: 'name'}, inplace=True)
                table['y'] = table['dates'].str[-4:]
                table['m'] = table['dates'].str[3:5]
                table['d'] = table['dates'].str[0:2]
                table['date'] = table['y'] + table['m'] + table['d']
                table['date'] = pd.to_datetime(table['date'], format='%Y%m%d')
                table.unit = table.unit.fillna('')
                
                for idx, row in table.iterrows():
                    db.insert(row['date'], row['fact'], row['name'], row['unit'])
        db.del_duplicates()
db.cnxn.close()