import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'core'))

from core.context import Context
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

def get_data_pipeline(service_name: str) -> DataPipeline:
    """Создает экземпляр DataPipeline для указанного сервиса"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    context = Context(target_service=service_name, config_path="./services.json", registry_path="./service_registry.json")
    factory = ServiceFactory(context)
    return DataPipeline(
        context=context,
        soap_client=factory.get_soap_client(),
        parser=factory.get_parser(),
        transformer=factory.get_transformer(),
        loader=factory.get_loader(),
    )
