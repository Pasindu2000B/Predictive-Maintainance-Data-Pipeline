from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, FloatType, StringType, BooleanType
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import os

# -------- Schema to match ESP32 JSON --------
schema = StructType([
    StructField("mqtt_topic", StringType()),
    StructField("payload", StructType([
        StructField("dhtA", StructType([
            StructField("ok", BooleanType()),
            StructField("t", FloatType()),
            StructField("h", FloatType())
        ])),
        StructField("dhtB", StructType([
            StructField("ok", BooleanType()),
            StructField("t", FloatType()),
            StructField("h", FloatType())
        ])),
        StructField("acc", StructType([
            StructField("x", FloatType()),
            StructField("y", FloatType()),
            StructField("z", FloatType())
        ]))
    ]))
])

# -------- Initialize Spark --------
spark = SparkSession.builder.appName("ESP32SensorStream").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# -------- Read from Kafka --------
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "iot-sensor-data") \
    .load()

# Convert Kafka value to string
string_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Parse JSON into structured fields
json_df = string_df.select(
    col("key"),
    from_json(col("value"), schema).alias("data")
)

# Flatten nested fields
flattened_df = json_df.select(
    col("data.mqtt_topic").alias("topic"),
    col("data.payload.dhtA.t").alias("tempA"),
    col("data.payload.dhtA.h").alias("humA"),
    col("data.payload.dhtB.t").alias("tempB"),
    col("data.payload.dhtB.h").alias("humB"),
    col("data.payload.acc.x").alias("accX"),
    col("data.payload.acc.y").alias("accY"),
    col("data.payload.acc.z").alias("accZ")
)

# -------- Write each micro-batch to InfluxDB --------
def write_to_influxdb(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    records = batch_df.collect()

    client = InfluxDBClient(
        url="http://influxdb:8086",
        token="GO7pQ79-Vo-k6uwpQrMmJmITzLRHxyrFbFDrnRbz8PgZbLHKe5hpwNZCWi6Z_zolPRjn7jUQ6irQk-BPe3LK9Q==",
        org="Ruhuna_Eng"
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)

    for row in records:
        point = (
            Point("sensor_data")
            .tag("topic", row["topic"])
            .field("tempA", row["tempA"])
            .field("humA", row["humA"])
            .field("tempB", row["tempB"])
            .field("humB", row["humB"])
            .field("accX", row["accX"])
            .field("accY", row["accY"])
            .field("accZ", row["accZ"])
        )
        write_api.write(bucket="New_Sensor", org="Ruhuna_Eng", record=point)

    client.close()

# -------- Start streaming query --------
flattened_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_influxdb) \
    .start() \
    .awaitTermination()
