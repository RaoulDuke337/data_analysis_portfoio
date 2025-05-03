from cbr_currencies.core.interfaces import IParser
import pandas as pd

class MainParser(IParser):
    def __init__(self, context):
        super().__init__(context)
        self.context = context

    def parse(self, xml_data) -> pd.DataFrame:
        data = []
        service = self.context.get_attr("name")
        for xml_doc in xml_data:
            for tag in xml_doc.findall(self.root_tag, namespaces={'': ''}):
                row = {
                    # извлекаем в root-теге все данные сопоставляя названия столбцов с тегами через dict comp
                    column_name: (tag.find(tag_name).text.strip() if tag.find(tag_name) is not None else None)
                    for column_name, tag_name in zip(self.columns, self.tags)
                }
                #print(row)
                data.append(row)
        df = pd.DataFrame(data)
        df.to_csv('./' + service + '.csv', index=False, sep=';')
        return df