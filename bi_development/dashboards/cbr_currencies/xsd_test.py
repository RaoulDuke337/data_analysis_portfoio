from jinja2 import Environment, FileSystemLoader
import requests
import zeep

env = Environment(loader=FileSystemLoader('.'))
template = env.get_template('request_template.xml')

data = {
  'method_name': 'EnumValutesXML',
  'params': {
    'Seld': 0
  },
  'options':
  {
    'Content-Type': 'text/xml; charset=utf-8',
    'SOAPAction': 'http://web.cbr.ru/zcyc_params'
  }
}

url = 'https://www.cbr.ru/secinfo/secinfo.asmx'
envelope = template.render(data=data)
print(envelope)

def send_request(envelope):
    client = zeep.Client(url)
    response = client.service.SomeSOAPMethod(envelope)
    return response