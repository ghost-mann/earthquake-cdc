import pandas as pd
import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

engine = create_engine("mariadb+mariadbconnector://root:password@127.0.0.1:3306/earthquake")

spark = SparkSession.builder \
    .appName("EarthquakeData") \
    .getOrCreate()

# get current time
end_time = datetime.now()

# get time one minute ago
start_time = end_time - timedelta(minutes=1)

# format time stamps in ISO-8601
start_iso = start_time.isoformat()
end_iso = end_time.isoformat()


url = f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_iso}&endtime={end_iso}'
response = requests.get(url)

if response.status_code == 200:
    earthquake_data = response.json()
    features = earthquake_data.get("features",[])

    records = []
    for f in features:
        props = f.get("properties",{})
        geom = f.get("geometry",{})
        coords = geom.get("coordinates",[None, None, None])

        # dictionary for one earthquake event
        records.append({
            "time": datetime.utcfromtimestamp(props.get("time", 0) / 1000.0),
            "place": props.get("place"),
            "magnitude": props.get("mag"),
            "depth": coords[2],
            "longitude": coords[0],
            "latitude": coords[1],
            "type": props.get("type"),
        })
        
        # pyspark schema for df
        schema = StructType([
                StructField("time", TimestampType(), True),
                StructField("place", StringType(), True),
                StructField("magnitude", DoubleType(), True),
                StructField("depth", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("type", StringType(), True),
        ])
        
    df = spark.createDataFrame(records, schema=schema)
    df.show()
        
    
else:
    print("Failed to fetch data!")
