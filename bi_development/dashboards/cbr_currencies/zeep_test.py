import pprint
from tkinter import W
import requests
from lxml import etree
from zeep import Client, Plugin


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



soap_actions = ['"http://web.cbr.ru/EnumValutesXML"']
wsdl = 'https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL'
parameters = {
    'Seld': 0
}

client = Client(wsdl=wsdl, plugins=[CustomHeaderPlugin(soap_actions)])
methods = ['EnumValutesXML']

for method_name, parameters, soap_action in zip(methods, parameters, soap_actions):
    response = getattr(client.service, method_name)(parameters)
    

with open("./EnumValutesXML.xml", "w") as f:
    f.write(etree.tostring(response, pretty_print=True, encoding='unicode'))

