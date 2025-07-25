from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, when, lit, current_timestamp, coalesce, regexp_replace, length, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import regexp_replace, trim, concat_ws, to_date, least, greatest, current_date, udf



# Initialize Spark session with legacy time parser
spark = SparkSession.builder \
    .appName("SeismicBronzeToSilverTransformation") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.eventLog.logBlockUpdates.enabled", True)\
    .enableHiveSupport() \
    .getOrCreate()


seismic_schema = StructType([
    StructField("PK", StringType(), True),
    StructField("UNID", StringType(), True),
    StructField("SOURCE_ID", StringType(), True),
    StructField("SOURCE_CATALOG", StringType(), True),
    StructField("LASTUPDATE_UTC", TimestampType(), True),
    StructField("EVENT_TIME_UTC", TimestampType(), True),
    StructField("FLYNN_REGION", StringType(), True),
    StructField("LAT", DoubleType(), True),
    StructField("LON", DoubleType(), True),
    StructField("DEPTH", DoubleType(), True),
    StructField("EVTYPE", StringType(), True),
    StructField("AUTH", StringType(), True),
    StructField("MAG", DoubleType(), True),
    StructField("MAGTYPE", StringType(), True),
    StructField("ACTION", StringType(), True),
    StructField("RECEIVED_AT_UTC", TimestampType(), True),
    StructField("SOURCE_DB", StringType(), True),
    StructField("SOURCE_TABLE", StringType(), True),
    StructField("OPERATION", StringType(), True),
    StructField("LOAD_At", DateType(), True),
    StructField("BPK", StringType(), True),
])

vessels_path = "hdfs://localhost:9000/home/itversity/bronze/seismic"


df = spark.read.option("header", "true").schema(seismic_schema).csv(vessels_path)
df.show(5, truncate=False)


spark.sql("CREATE DATABASE IF NOT EXISTS silver")

spark.sql("use silver")
df.write \
  .mode("append") \
  .partitionBy("LOAD_AT") \
  .saveAsTable("seismic")


# to run this script to load seismic which on bronze in this path
# /opt/spark3/bin/spark-submit --master local[2] --conf spark.ui.port=18181 /home/itversity/spark/silver/seismic_to_silver.py

