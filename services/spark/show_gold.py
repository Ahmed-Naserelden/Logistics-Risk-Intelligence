import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read Silver") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Read vessels DataFrame from Silver layer
vessels_silver_path = "hdfs://localhost:9000/home/itversity/gold/vessels_ports_enriched"
vessels_df = spark.read.parquet(vessels_silver_path)

# Display vessels DataFrame
print("Vessels Silver Layer DataFrame:")
vessels_df.show(5, truncate=False)

spark.stop()
