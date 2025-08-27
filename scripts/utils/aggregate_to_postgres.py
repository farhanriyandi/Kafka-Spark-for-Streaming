# aggregate_to_postgres_debug.py

from flatten_to_kafka import create_spark_session, parse_to_streaming_df
from pyspark.sql.functions import col, avg, window
from pyspark.sql.types import DecimalType, StructType, StructField, StringType, DoubleType, IntegerType

def get_kafka_df(spark):
    return (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "weather-data-flattened")  # Read from flattened topic
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

def get_json_schema():
    return StructType([
        StructField("device_id", StringType()),
        StructField("country", StringType()),
        StructField("city", StringType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", IntegerType()),
        StructField("timestamp", StringType())
    ])

def weather_data_output(df, batch_id):
    print("==== BATCH ID:", batch_id, "====")
    df.show(truncate=False)

    if df.count() == 0:
        print("⚠️ Batch kosong, tidak ada data dikirim ke Postgres.")
        return

    # Write to JDBC Postgres
    (
        df.write
        .mode("append")
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", "jdbc:postgresql://localhost:7432/my-postgres")  
        .option("dbtable", "weather_data")
        .option("user", "my-postgres")
        .option("password", "my-postgres")
        .save()
    )
    print("✅ Data berhasil dikirim ke Postgres")

def aggregate_and_write_to_postgres():
    spark = create_spark_session()
    kafka_df = get_kafka_df(spark)
    json_schema = get_json_schema()
    streaming_df = parse_to_streaming_df(kafka_df, json_schema)

    # DEBUG: cek schema awal
    print("=== STREAMING DF SCHEMA ===")
    streaming_df.printSchema()

    # Jangan cast timestamp lagi (biarkan sesuai dari flatten_to_kafka.py)
    flattened_df = streaming_df

    # DEBUG
    debug_query = (
        flattened_df
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )

    # Aggregasi TANPA watermark dulu (biar pasti ada data masuk)
    agg_df = (
        flattened_df
        .groupBy("device_id", "country", "city", window("timestamp", "1 minute"))
        .agg(
            avg("temperature").cast(DecimalType(5, 2)).alias("avg_temp"),
            avg("humidity").cast(DecimalType(5, 2)).alias("avg_humidity")
        )
        .select(
            col("device_id"),
            col("country"),
            col("city"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_temp"),
            col("avg_humidity")
        )
    )

    (
        agg_df
        .writeStream
        .foreachBatch(weather_data_output)
        .outputMode("update")  # penting untuk agregasi
        .trigger(processingTime='10 seconds')
        .option("checkpointLocation", "checkpoint_dir_kafka_debug")
        .start()
        .awaitTermination()
    )

if __name__ == "__main__":
    aggregate_and_write_to_postgres()
