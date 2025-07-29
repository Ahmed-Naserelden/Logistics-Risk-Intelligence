import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Set logging levels
logging.getLogger('py4j').setLevel(logging.ERROR)

# Start Spark session
spark = SparkSession.builder.appName("KafkaStreaming").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Use environment variable for password
password = os.getenv("SNOWFLAKE_PASSWORD")
sfUser = os.getenv("SNOWFLAKE_USER")
sfURL = os.getenv("SNOWFLAKE_URL")

# Define schema for Kafka messages grok
schema = StructType([
    StructField("before", StructType([
        StructField("unid", StringType(), True),
        StructField("source_id", StringType(), True), 
        StructField("source_catalog", StringType(), True),
        StructField("lastupdate", LongType(), True),
        StructField("time", LongType(), True),
        StructField("flynn_region", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("depth", DoubleType(), True),
        StructField("evtype", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("mag", DoubleType(), True),
        StructField("magtype", StringType(), True),
        StructField("action", StringType(), True),
        StructField("received_at", LongType(), True)
    ]), True),
    StructField("after", StructType([
        StructField("unid", StringType(), True),
        StructField("source_id", StringType(), True),
        StructField("source_catalog", StringType(), True),
        StructField("lastupdate", LongType(), True),
        StructField("time", LongType(), True),
        StructField("flynn_region", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("depth", DoubleType(), True),
        StructField("evtype", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("mag", DoubleType(), True),
        StructField("magtype", StringType(), True),
        StructField("action", StringType(), True),
        StructField("received_at", LongType(), True),
        StructField("current_snowflake_pk", LongType(), True),
        StructField("last_snowflake_pk", LongType(), True)
    ]), True),
    StructField("source", StructType([
        StructField("version", StringType(), True),
        StructField("connector", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("snapshot", StringType(), True),
        StructField("db", StringType(), True),
        StructField("sequence", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True),
        StructField("txId", LongType(), True),
        StructField("lsn", LongType(), True),
        StructField("xmin", StringType(), True)
    ]), True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("transaction", StringType(), True)
])

# Topic name
topic_name = 'maritime_logistics_server.public.seismic_events'

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka messages key, value 
messages = df.selectExpr("CAST(value AS STRING) as message") \
                .select(from_json(col("message"), schema).alias("data")) \
                .select("data.*")

# Flatten the DataFrame
flattened_df = messages.select(
    col("after.current_snowflake_pk").alias("pk"),
    col("after.unid").alias("unid"),
    col("after.source_id").alias("source_id"),
    col("after.source_catalog").alias("source_catalog"),
    col("after.lastupdate").alias("lastupdate"),
    col("after.time").alias("time"),
    col("after.flynn_region").alias("flynn_region"),
    col("after.lat").alias("lat"),
    col("after.lon").alias("lon"),
    col("after.depth").alias("depth"),
    col("after.evtype").alias("evtype"),
    col("after.auth").alias("auth"),
    col("after.mag").alias("mag"),
    col("after.magtype").alias("magtype"),
    col("after.action").alias("action"),
    col("after.received_at").alias("received_at"),
    col("source.db").alias("source_db"),
    col("source.table").alias("source_table"),
    col("op").alias("operation"),
    col("ts_ms").alias("kafka_ts_ms"),
    col("after.last_snowflake_pk").alias("bpk"),
)

# Batch writer to Snowflake
def write_to_snowflake(batch_df, batch_id):
    try:

        # Create a temp directory if it doesn't exist
        temp_dir = "file:///tmp/snowflake_temp"
        os.makedirs("/tmp/snowflake_temp", exist_ok=True)

        output_path = f"/tmp/snowflake_temp/batch_{batch_id}.parquet"
        batch_df.write.parquet(output_path)


        sfOptions = {
            "sfURL": sfURL,
            "sfUser": sfUser,
            "sfPassword": password,
            "sfRole": "ACCOUNTADMIN",
            "sfWarehouse": "COMPUTE_WH", 
            "sfDatabase": "SEISMIC_DB",
            "sfSchema": "BRONZE",
            "dbtable": "EARTHQUAKE_EVENTS4",
            "sfStage": "INTERNAL_STAGE",                                 # Snowflake internal stage
            "sfFileFormat": "parquet",                                   # Use Parquet for staging
            
            "tempDir": temp_dir,                                         # Temporary directory for staging files
            "keep_local_files": "true",                                  # Keep local files after upload
            "parallel": "4",                                             # Reduce parallelism if needed
            "purge": "false",                                            # Don't purge files immediately
            "autopushdown": "off"                                        # Try disabling pushdown

            # "connection": "direct"                                     # Try to bypass staging requirement
            # "parallelism": "4",                                        # Reduce parallelism if needed
            # "usestagingtable": "off",                                  # Disables external staging requirement
            # "autopushdown": "on",                                      # Enables query pushdown to Snowflake
            # "sfCompress": "on"                                         # Enable compression
        }

        batch_df.write \
            .format("snowflake") \
            .options(**sfOptions) \
            .mode("append") \
            .save()
        
        print(f"Batch {batch_id} successfully written to Snowflake")
    except Exception as e:
        print(f"Error writing batch {batch_id} to Snowflake: {str(e)}")

# Start streaming query
    # .option("checkpointLocation", "/tmp/checkpoint") \
console_query = flattened_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

snowfalke_query = flattened_df.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

console_query.awaitTermination()
snowfalke_query.awaitTermination()



# To run this script, use the following commands:
# 1. `docker exec -u 0 -it itvdelab bash` (This will give you a bash shell in the container with root).
#     run this command:  `cp /usr/bin/python3 /usr/bin/python`
#     then exit
#
# 2. `docker exec -it itvdelab bash` (This will give you a bash shell in the container).
#     /opt/spark3/sbin/start-history-server.sh

# 3. now you can run the spark-submit command:
# /opt/spark3/bin/spark-submit --master local[2] --conf spark.ui.port=18181 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,net.snowflake:snowflake-jdbc:3.15.1,net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.1 spark/snowflake/streaming_2_snowflake.py
