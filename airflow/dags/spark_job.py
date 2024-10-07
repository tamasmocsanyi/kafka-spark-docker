import json
import time
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, FloatType

# Define the schema of the data
schema = StructType([
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True),
])

# Function to create the PostgreSQL table if it does not exist
def create_postgresql_table():
    # Connect to your PostgreSQL database
    conn = None
    cursor = None  # Initialize cursor to None

    try:
        conn = psycopg2.connect(
            dbname='airflow',
            user='airflow',
            password='airflow',
            host='localhost',  
            port='5432'
        )
        cursor = conn.cursor()
        
        # Create table query
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS gps_coordinates (
            id SERIAL PRIMARY KEY,
            longitude FLOAT NOT NULL,
            latitude FLOAT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        '''
        # Execute the table creation query
        cursor.execute(create_table_query)
        conn.commit()
        print("Table created successfully")

    except Exception as e:
        print(f"Error creating table: {e}")

    finally:
        # Ensure cursor and connection are closed if they were created
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Kafka Spark Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.4")  \
    .getOrCreate()

# Create PostgreSQL table
create_postgresql_table()

# Read stream from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "coordinates") \
    .option("startingOffsets", "latest") \
    .load()

spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.streaming.checkpointLocation", "C:/Users/tommox/spark_checkpoints")

# Convert value from Kafka (JSON) to dataframe
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write to PostgreSQL
def write_to_postgresql(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/airflow") \
        .option("dbtable", "gps_coordinates") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Apply the write to PostgreSQL in streaming mode
query = df_parsed.writeStream \
    .foreachBatch(write_to_postgresql) \
    .start()

query.awaitTermination()