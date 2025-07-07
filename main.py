import pandas as pd
import requests
from datetime import datetime, timedelta

# get current time
end_time = datetime.now()

# get time one minute ago
start_time = end_time - timedelta(minutes=1)

# format time stamps in ISO-8601
start_iso = start_time.isoformat()
end_iso = end_time.isoformat()


url = f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_iso}&endtime={end_iso}'

response = requests.get(url)

print(end_time)
print("Status code:", response.status_code)
print("Response content:", response.text)

# earthquake_data = response.json()

# print(earthquake_data)