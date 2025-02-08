import json 
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

with open("./services.json", "r") as file:
    services = json.load(file)

currencies_services = [service for service in services["services"] if service["name"] == "Currencies"]

