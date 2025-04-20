import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'core'))

from core.context import Context, Services
from core.interfaces import ISoapClient, IParser, ITransformer, ILoader


class ServiceFactory:
    """Фабрика для переключения реализаций объектов"""
    def __init__(self, context: Context):
        self.context = context

    def get_soap_client(self):
        return self.context.get_component("soap_client")(self.context)

    def get_parser(self):
        return self.context.get_component("parser")(self.context)

    def get_transformer(self):
        return self.context.get_component("transformer")(self.context)

    def get_loader(self):
        return self.context.get_component("loader")(self.context)
       

class DataPipeline:
    """Главный пайплайн для обработки данных"""
    
    def __init__(self, soap_client: ISoapClient, parser: IParser, transformer: ITransformer, loader: ILoader, context: Context):
        self.parser = parser
        self.transformer = transformer
        self.loader = loader
        self.context = context
        self.soap_client = soap_client

    def run(self):
        """Запускает процесс обработки данных"""
        print('Запуск пайплайна')    
        results = self.soap_client.fetch_data()
        print('Данные получены')
        parsed = self.parser.parse(results)
        print('Парсинг завершен')
        transformed = self.transformer.transform(parsed)
        print('Трансформация завершена')
        self.loader.load(transformed)


script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

if __name__ == "__main__":
    services = Services(config_path="./services.json")
    service_list = services.get_services()
    for service in service_list:
        context = Context(target_service=service, config_path="./services.json", registry_path="./service_registry.json")
        factory = ServiceFactory(context)
        pipeline = DataPipeline(
            context=context,
            soap_client = factory.get_soap_client(),
            parser = factory.get_parser(),
            transformer = factory.get_transformer(),
            loader=factory.get_loader(),
        )
        pipeline.run()
