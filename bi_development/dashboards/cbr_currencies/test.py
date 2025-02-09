import json 
import os
from datetime import datetime, timedelta

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

with open("./services.json", "r") as file:
    services = json.load(file)

currencies_services = [service for service in services["services"] if service["name"] == "currencies"]

print(currencies_services[0].get('wsdl'))



def get_request_date():
    current_date = datetime.now()
    todate = current_date
    fromdate = todate - timedelta(days=3)
    dates = (fromdate, todate)
    return dates

parametrs = ["FromDate", "ToDate","ValutaCode"]
dates = get_request_date()

query_params = {
    param: value for param, value in zip(parametrs, dates)
    }

print(type(query_params.get('FromDate')))