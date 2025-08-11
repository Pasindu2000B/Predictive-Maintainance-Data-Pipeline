from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, FloatType, StringType
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import os

# Define schema for incoming JSON
schema = StructType([
    StructField("mqtt_topic", StringType()),
    StructField("payload", StructType([
        StructField("temperature", FloatType())
    ]))
])  

# Initialize Spark session
spark = SparkSession.builder.appName("TemperatureStream").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read stream from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "iot-sensor-data") \
    .load()


string_df=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") #key : empty and value : {"mqtt_topic":"sensors/room1","payload":{"temperature":25.6}} 

json_df = string_df.select(
    col("key"),
    from_json(col("value"), schema).alias("data")
)

flattened_df = json_df.select(
    col("data.mqtt_topic").alias("topic"),
    col("data.payload.temperature").alias("temperature")
)

def write_to_influxdb(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    records = batch_df.collect()

    client = InfluxDBClient(
        url="http://influxdb:8086",
        token="-pi9FAAHD7gjxwQ5DfT1PVop0K3KBVUvoXZFQlJSMP_WMH2icU645gKKYu6P8I8wpg4Flm6WO4KKziaEr4iKYg==",
        org="Ruhuna_Eng"
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)

    for row in records:
        point = (
            Point("temperature_data")
            .tag("topic", row["topic"]) 
            .field("temperature", row["temperature"])
    
        )
        write_api.write(bucket="Sensor", org="Ruhuna_Eng", record=point)

    client.close()


flattened_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_influxdb) \
    .start() \
    .awaitTermination()


