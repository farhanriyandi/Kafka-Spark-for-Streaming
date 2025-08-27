# flatten_to_kafka.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StringType, StructField, StructType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import col, to_timestamp, from_unixtime
from pyspark.sql.functions import to_json, struct

def create_spark_session():
    return (
        SparkSession
        .builder
        .appName("Writing to Multiple Sinks")
        .config("spark.streaming.stopGracefullyOnShutdown", True)
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.2.20')
        .config("spark.sql.shuffle.partitions", 8)
        .master("local[*]")
        .getOrCreate()
    )

def get_kafka_df(spark):
    return (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "weather-data")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

def get_json_schema():
    return StructType([
        StructField("device_id", StringType()),
        StructField("location", StructType([
            StructField("country", StringType()),
            StructField("city", StringType())
        ])),
        StructField("metrics", StructType([
            StructField("temperature", DoubleType()),
            StructField("humidity", IntegerType())
        ])),
        StructField("timestamp", StringType())
    ])

def parse_to_streaming_df(kafka_df, json_schema):
    kafka_json_df = kafka_df.withColumn("value", expr("cast(value as string)"))
    return kafka_json_df.withColumn("values_json", from_json(col("value"), json_schema)).selectExpr("values_json.*")

def flatten_streaming_df(streaming_df):
    return (
        streaming_df
        .withColumn("device_id", col("device_id"))
        .withColumn("country", col("location.country"))
        .withColumn("city", col("location.city"))
        .withColumn("temperature", col("metrics.temperature"))
        .withColumn("humidity", col("metrics.humidity"))
        .withColumn("timestamp", to_timestamp(col("timestamp")))  
        .drop("location", "metrics")
    )

def flatten_and_write_to_kafka():
    spark = create_spark_session()
    kafka_df = get_kafka_df(spark)
    json_schema = get_json_schema()
    streaming_df = parse_to_streaming_df(kafka_df, json_schema)
    flattened_df = flatten_streaming_df(streaming_df)
    
    (
        flattened_df
        .select(
            to_json(
                struct(
                    "device_id",
                    "country",
                    "city",
                    "temperature",
                    "humidity",
                    "timestamp"
                )
            ).alias("value")
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "weather-data-flattened")
        .option("checkpointLocation", "checkpoint_flattened_kafka")
        .start()
        .awaitTermination()
    )

if __name__ == "__main__":
    flatten_and_write_to_kafka()