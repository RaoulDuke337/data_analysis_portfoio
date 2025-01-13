import requests
from datetime import datetime

todate = datetime(2024, 11, 1, 0, 0, 0)
fromdate = datetime(2024, 6, 17, 0, 0, 0)

fromdate_formatted = fromdate.strftime("%Y-%m-%d")
todate_formatted = todate.strftime("%Y-%m-%d")

url = 'https://www.cbr.ru/secinfo/secinfo.asmx'
SOAPEnvelop = f"""
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <zcyc_params xmlns="http://web.cbr.ru/">
      <FromDate>{fromdate_formatted}</FromDate>
      <ToDate>{todate_formatted}</ToDate>
    </zcyc_params>
  </soap:Body>
</soap:Envelope> """

options = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": "http://web.cbr.ru/zcyc_params"
          }

response = requests.post(url, data = SOAPEnvelop, headers = options, verify=False)
print(response)