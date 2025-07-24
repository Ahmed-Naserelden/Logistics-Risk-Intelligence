from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read Silver") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Read vessels DataFrame from Silver layer
vessels_silver_path = "hdfs://localhost:9000/home/itversity/silver/vessels"
vessels_df = spark.read.parquet(vessels_silver_path)

# Display vessels DataFrame
print("Vessels Silver Layer DataFrame:")
vessels_df.show(5, truncate=False)

# Read ports DataFrame from Silver layer
ports_silver_path = "hdfs://localhost:9000/home/itversity/silver/ports"
ports_df = spark.read.parquet(ports_silver_path)

# Display ports DataFrame
print("Ports Silver Layer DataFrame:")
ports_df.show(5, truncate=False)

spark.stop()