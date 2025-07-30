from sqlalchemy import create_engine, text
from pyspark.sql import SparkSession

# Database connections
mariadb_engine = create_engine("mariadb+mariadbconnector://root:password@127.0.0.1:3306/earthquake")
postgres_engine = create_engine("postgresql://postgres:root@localhost:5432/earthquake")

# Create Spark
spark = SparkSession.builder \
    .appName("EarthquakePipeline") \
    .config("spark.jars", "db drivers/mariadb-java-client-3.3.1.jar,db drivers/postgresql-42.7.7.jar") \
    .getOrCreate()

print("Starting pipeline...")

# Read from MariaDB - your data is clean, just read it
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mariadb://127.0.0.1:3306/earthquake") \
    .option("dbtable", "earthquake_data") \
    .option("user", "root") \
    .option("password", "password") \
    .option("driver", "org.mariadb.jdbc.Driver") \
    .load()

print(f"Loaded {df.count()} records")

# Create PostgreSQL table
with postgres_engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS earthquake_data"))
    conn.execute(text("""
        CREATE TABLE earthquake_data (
            eq_id VARCHAR(100) PRIMARY KEY,
            eq_time TIMESTAMP,
            place VARCHAR(500),
            magnitude DOUBLE PRECISION,
            depth DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION
        )
    """))
    conn.commit()

# Write to PostgreSQL
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/earthquake") \
    .option("dbtable", "earthquake_data") \
    .option("user", "postgres") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("Done!")

# Verify
with postgres_engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM earthquake_data"))
    print(f"PostgreSQL has {result.scalar()} records")

spark.stop()