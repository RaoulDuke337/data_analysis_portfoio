import importlib
import json

class Context:
    """Объект для хранения общих атрибутов между компонентами"""
    def __init__(self, target_service: str, config_path: str, registry_path: str):
        self.target_service = target_service
        self.general_config = self.read_config(config_path)
        self.configuration = [
            service for service in self.general_config["services"] if service["name"] == target_service
            ]
        self.registry = self.read_registry(registry_path)
        
    def read_config(self, config_path: str):
        """Загружает конфигурацию из JSON файла."""
        with open(config_path) as f:
            return json.load(f)
        
    def read_registry(self, registry_path: str):
        """Загружает реестр классов из JSON файла."""
        with open(registry_path) as f:
            return json.load(f)

    def get_attr(self, attr: str):
        return self.configuration[0].get(attr)

    def _import_class(self, dotted_path: str):
        """Импортирует класс по полному dotted path."""
        module_path, class_name = dotted_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)

    def get_component(self, component_name: str):
        """Вернёт нужный класс"""
        service_type = self.configuration[0].get("name")
        component_dotted_path = self.registry[service_type].get(component_name)
        if component_dotted_path:
            return self._import_class(component_dotted_path)
        else:
            raise KeyError(f"Компонент {component_name} не найден для сервиса {service_type}")
        return self.registry[service_type][component_name]

