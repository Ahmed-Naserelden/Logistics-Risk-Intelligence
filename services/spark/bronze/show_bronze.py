
from pyspark.sql import SparkSession

# Create or reuse SparkSession
spark = SparkSession.builder \
    .appName("Read Bronze") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.ui.port", "18180") \
    .getOrCreate()

# Read CSV from HDFS with header
df = spark.read.option("header", "true").csv("hdfs://localhost:9000/bronze/vessels")

# Print schema and show first 5 rows
df.printSchema()
df.show(5, truncate=False)

# Optional: Stop Spark session when done
spark.stop()

