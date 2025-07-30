import requests
import time
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

# Use the same engine as your original script
mariadb_engine = create_engine("mariadb+mariadbconnector://root:password@127.0.0.1:3306/earthquake")
Session = sessionmaker(bind=mariadb_engine)

# API endpoint (USGS real-time earthquake feed)
# Fetches earthquakes from the last hour
API_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"

def create_table_if_not_exists():
    """Creates the MariaDB table if it doesn't exist."""
    inspector = inspect(mariadb_engine)
    if not inspector.has_table("earthquake_data"):
        print("Table 'earthquake_data' not found. Creating it...")
        with mariadb_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE earthquake_data (
                    eq_id VARCHAR(100) PRIMARY KEY,
                    eq_time DATETIME(3),
                    place VARCHAR(500),
                    magnitude DOUBLE PRECISION,
                    depth DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    latitude DOUBLE PRECISION
                )
            """))
            conn.commit()
        print("Table created.")

def fetch_and_ingest():
    """Fetches data from the API and ingests it into MariaDB."""
    print("Fetching new earthquake data...")
    try:
        response = requests.get(API_URL)
        response.raise_for_status() # Raise an exception for bad status codes
        data = response.json()
        
        new_records = 0
        with Session() as session:
            for feature in data.get("features", []):
                props = feature.get("properties", {})
                coords = feature.get("geometry", {}).get("coordinates", [None, None, None])

                # Prepare the record
                record = {
                    "eq_id": feature.get("id"),
                    "eq_time": props.get("time") / 1000 if props.get("time") else None, # Convert ms to s for timestamp
                    "place": props.get("place"),
                    "magnitude": props.get("mag"),
                    "depth": coords[2],
                    "longitude": coords[0],
                    "latitude": coords[1],
                }

                # Construct an UPSERT/INSERT IGNORE statement
                # Using INSERT IGNORE to prevent crashing on duplicates
                insert_stmt = text("""
                    INSERT INTO earthquake_data (eq_id, eq_time, place, magnitude, depth, longitude, latitude)
                    VALUES (:eq_id, FROM_UNIXTIME(:eq_time), :place, :magnitude, :depth, :longitude, :latitude)
                    ON DUPLICATE KEY UPDATE
                        magnitude = VALUES(magnitude),
                        place = VALUES(place),
                        eq_time = VALUES(eq_time); 
                """)
                
                # Execute statement
                session.execute(insert_stmt, record)

            session.commit()
            print(f"Ingested/Updated {len(data.get('features', []))} records into MariaDB.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
    except Exception as e:
        print(f"An error occurred during ingestion: {e}")


if __name__ == "__main__":
    create_table_if_not_exists()
    # Run in a loop to simulate a real-time stream
    while True:
        fetch_and_ingest()
        print("Waiting for 60 seconds before next fetch...")
        time.sleep(60)
