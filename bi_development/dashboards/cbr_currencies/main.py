import soap_requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import re
import json

class Parsing:
    def __init__(self, list_column_name, link_folder, link_file, class_name, separator = ';', extension = '.csv'):
        self.list_column_name = list_column_name
        self.link_address = link_folder + link_file + '.txt'
        self.page_status = 0
        self.class_name = class_name
        self.open_file = open(self.link_address, encoding='utf-8')
        self.readline = ''
        self.len_rl = 0
        self.link_list = []
        self.site_data = pd.DataFrame(columns=self.list_column_name[0:2])
        self.separator = separator
        self.extension = extension
        self.name_value = 'value'
        self.name_unit = 'unit'
        self.df_indexes = pd.DataFrame(columns=self.list_column_name)

    def read_line(self):
        self.readline = self.open_file.readline()
        self.len_rl = len(self.readline)    
        self.link_list = self.readline.strip().split(self.separator)

    def close_file(self):
        self.open_file.close()
    
    def soup(self):
        url = self.link_list[0]
        page = soap_requests.get(url, verify=False)
        i = 0
        if page.status_code == 200:
            print('STATUS', page.status_code)
        else:
            while page.status_code != 200 and i < 5:
                i += 1
                page = soap_requests.get(url, verify=False)
                print (page.status_code, 'попытка №', i)
                time.sleep(20)
        soup = BeautifulSoup(page.text, 'lxml')
        self.page_status = page.status_code
        name_measure = self.link_list[1]
        self.write_to = os.getcwd() + '/' + str(name_measure.split(' ')[0]) + '.html'
        with open(self.write_to, "w", encoding='utf-8') as f:
            f.write(page.text)
        return soup

    def parsing(self):
        our_table = self.soup().find('table', class_=self.class_name)
        headers = []
        for i in our_table.find_all('th'):
            title = i.text.strip()
            headers.append(title)
        mydata = pd.DataFrame(columns=headers)
        for j in our_table.find_all('tr')[1:]:
            row_data = j.find_all('td')
            row = [i.text for i in row_data]
            length = len(mydata)
            mydata.loc[length] = row
        mydata[self.list_column_name[2]] = self.link_list[1]
        self.site_data = mydata[self.list_column_name]        
        return self.site_data
    
    def parsing_cycle(self):
        act_df = self.df_indexes.copy()
        self.read_line()
        while self.len_rl > 0:
            print(self.link_list[1])
            act_df = act_df._append(self.parsing())
            act_df.to_csv(link_folder + type_measures + self.extension, index = False, sep = self.separator)
            self.df_indexes = act_df.copy()
            # time.sleep(20)
            self.read_line()
        self.close_file()
    
    def read_result(self):
        self.df_indexes = pd.read_csv(link_folder + type_measures + self.extension, sep = self.separator)
        self.df_indexes = self.df_indexes.reset_index(drop=True)
    
    def processing(self):
        name_value = self.name_value
        name_unit = self.name_unit
        self.read_result()
        df_indexes = self.df_indexes
        df_indexes[name_unit] = ''
        df_indexes[name_value] = df_indexes[self.list_column_name[1]].str.strip()
        df_indexes = df_indexes.loc[df_indexes[name_value] != '']
        find_symbols = '%B'

        for idx, row in df_indexes.iterrows():
            df_indexes.loc[df_indexes.index == idx, name_value] = row[name_value].replace('.','').replace(',','.')
            df_indexes.loc[df_indexes.index == idx, self.list_column_name[0]] = row[self.list_column_name[0]][0:10]
            rs = row[self.list_column_name[1]][-1]
            if rs in find_symbols:
                df_indexes.loc[df_indexes.index == idx, name_unit] = rs
                value_correct = df_indexes.loc[df_indexes.index == idx, name_value].values[0][0:-1]
                df_indexes.loc[df_indexes.index == idx, name_value] = value_correct
        self.df_indexes = df_indexes[[self.list_column_name[0], name_value, self.list_column_name[2], name_unit]]
        self.df_indexes.to_csv(link_folder + 'proc_' + type_measures + '.csv', index = False, sep = ';')

class Info(Parsing):
    def processing(self):
        self.read_result()
        self.df_indexes['unit'] = ''
        self.df_indexes.to_csv(link_folder + 'proc_' + type_measures + '.csv', index = False, sep = ';')

    def parsing(self):
        for tag in self.soup().find_all(string=re.compile( "datasets")):
            dct = tag.replace('var data = ', '').strip().split(self.separator)[0]
        m = re.search('data.+],',dct)
        data_string = m.group(0)
        data_list = data_string.replace('data:', '')
        data_list = data_list.replace('],', ']')
        data_list = data_list.replace('[{', '{')
        data_list = data_list.replace('}]', '}')
        data_list = data_list.replace('x', '"x"')
        data_list = data_list.replace('y', '"y"')
        data_list = data_list.replace("'", '"')
        json_string = '{'+ '"data"' + ': [' + data_list + ']}'
        data_dict = json.loads(json_string)
        list_dicts = data_dict["data"]
        df_grain = pd.DataFrame(list_dicts)
        df_grain[self.list_column_name[2]] = self.link_list[1]
        df_grain = df_grain.rename(columns = {'x': list_column_name[0], 'y': list_column_name[1]})
        self.site_data = df_grain[self.list_column_name]        
        return self.site_data

class Decision(Parsing):
    def parsing(self):
        our_table = self.soup().find('div', class_ = class_name)
        self.site_data = pd.DataFrame(columns=list_column_name)
        row_split = []
        for tag in our_table.find_all('span'):
            row = tag.text.strip()
            if list_column_name[0] in row:
                val = row.replace(list_column_name[0],'')
                row_split.append(val)
            elif list_column_name[1] in row:
                val = row.replace(list_column_name[1],'')
                row_split.append(val)
        row_split.append(self.link_list[1])
        self.site_data.loc[0] = row_split
        return self.site_data

class DB:
    def __init__(self, driver, server, db, auth):
        self.driver = driver
        self.server = server
        self.db = db
        self.auth = auth
        self.str_connect = driver+server+db+auth
        self.cnxn = pyodbc.connect(self.str_connect)
        self.cur = self.cnxn.cursor()

    def select(self):
        self.cur.execute("""
        SELECT * FROM macroeconomy.stage_indexes;
        """)
        print(self.cur.fetchone())

    def insert(self, date, value, name, unit):
        self.cur.execute("""
        insert into macroeconomy.stage_indexes
        values (?, ?, ?, ?);
        """
        , (date, value, name, unit))
    
    def del_duplicates(self):
        self.cur.execute("""
        with act_rows as (
        select date_measure, name_measure, MAX(id) id
        from macroeconomy.stage_indexes with(nolock)
        group by date_measure, name_measure
        )

        delete
        from macroeconomy.stage_indexes
        where id not in (select id
        from act_rows)
        """)

# ЗАГРУЗКА ИНДЕКСОВ
type_measures = 'indexes'
list_column_name = ['Дата выпуска', 'Факт.', 'Показатель']
link_folder = os.getcwd() + '/'
link_file = 'link_indexes'
class_name = 'genTbl openTbl ecHistoryTbl'

indexes = Parsing(list_column_name, link_folder, link_file, class_name)
indexes.parsing_cycle()
indexes.processing()

# ЗАГРУЗКА КУРСОВ
type_measures = 'investcom'
list_column_name = ['Дата', 'Цена', 'Показатель']
link_file = 'link_investcom'
class_name = 'freeze-column-w-1 w-full overflow-x-auto text-xs leading-4'

indexes = Parsing(list_column_name, link_folder, link_file, class_name)
indexes.parsing_cycle()
indexes.processing()

# ЗАГРУЗКА ЗЕРНА
type_measures = 'grains'
list_column_name = ['Дата', 'Цена', 'Показатель']
link_file = 'link_grain'

indexes = Info(list_column_name, link_folder, link_file, class_name)
indexes.parsing_cycle()
indexes.processing()

# ЗАГРУЗКА PticaInfo
type_measures = 'meats'
list_column_name = ['Дата', 'Цена', 'Показатель']
link_file = 'link_birds'

indexes = Info(list_column_name, link_folder, link_file, class_name)
indexes.parsing_cycle()
indexes.processing()

# ЗАГРУЗКА KorovaInfo
type_measures = 'pork'
list_column_name = ['Дата', 'Цена', 'Показатель']
link_file = 'link_cow'

indexes = Info(list_column_name, link_folder, link_file, class_name)
indexes.parsing_cycle()
indexes.processing()

# ЗАГРУЗКА РЕШЕНИЙ
type_measures = 'decisions'
list_column_name = ['Последний выпуск', 'Факт.', 'Показатель']
link_file = 'link_decisions'
class_name = 'releaseInfo bold'

indexes = Decision(list_column_name, link_folder, link_file, class_name)
indexes.parsing_cycle()
indexes.processing()

#СОБИРАЕМ СПИСОК ФАЙЛОВ
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

from datetime import datetime

with db.cnxn:
    with db.cur:
        for fn in f:
            if fn[0:5] == 'proc_':
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



import pandas as pd 
import xml.etree.ElementTree as ET

root = ET.fromstring(response.content)
data = []
namespace = {
    'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
    'cbr': 'http://web.cbr.ru/',
    'diffgr': 'urn:schemas-microsoft-com:xml-diffgram-v1'
}

for zcyc in root.findall('.//zcyc_params/ZCYC', namespaces={'': ''}):
    row = {
        'date': zcyc.find('D0').text,
        'year_1': zcyc.find('v_1_0').text,
        'year_5': zcyc.find('v_2_0').text,
        'year_10': zcyc.find('v_10_0').text
    }
    data.append(row)
    
df = pd.DataFrame(data)
print(df)