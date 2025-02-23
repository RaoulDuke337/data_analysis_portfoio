from tkinter import W
from lxml import etree
from zeep import Client, Plugin
from datetime import datetime, timedelta


class CustomHeaderPlugin(Plugin):
    def __init__(self, soap_actions):
        super().__init__()
        self.soap_actions = soap_actions
        self.index = 0

    def ingress(self, envelope, http_headers, operation):
        return envelope, http_headers

    def egress(self, envelope, http_headers, operation, binding=None, client=None):
        if self.index < len(self.soap_actions):
            http_headers['SOAPAction'] = self.soap_actions[self.index]
            self.index += 1
        return envelope, http_headers

def get_request_date():
    current_date = datetime.now()
    todate = current_date
    fromdate = todate - timedelta(days=30)
    dates = (fromdate, todate)
    return dates

dates = get_request_date()

soap_actions = ['"http://web.cbr.ru/zcyc_paramsXML"']
wsdl = 'https://www.cbr.ru/secinfo/secinfo.asmx?WSDL'
parameters = {
    "OnDate": dates[0],
    "ToDate": dates[1]
}

print(parameters)

client = Client(wsdl=wsdl, plugins=[CustomHeaderPlugin(soap_actions)])
method = 'zcyc_paramsXML'

response = getattr(client.service, method)(**parameters)
    

with open("./zcyc_params.xml", "w") as f:
    f.write(etree.tostring(response, pretty_print=True, encoding='unicode'))

