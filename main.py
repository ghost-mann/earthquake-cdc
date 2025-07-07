import pandas as pd
import requests
from datetime import datetime, timedelta


end_time = datetime.now()


url = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={NOW-1min ISO8601}&endtime={NOW ISO8601}'

response = requests.get(url)

print(end_time)
print("Status code:", response.status_code)
print("Response content:", response.text)

# earthquake_data = response.json()

# print(earthquake_data)