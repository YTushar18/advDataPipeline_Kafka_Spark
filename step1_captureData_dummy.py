import requests
import config


url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo'
r = requests.get(url)
data = r.json()

print(data)

import json
with open('step1_TempDatadump/data.json', 'w') as f:
    json.dump(data, f)