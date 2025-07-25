from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, when, round,
    concat_ws, hour, current_date
)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GoldLayerTransformation") \
    .getOrCreate()

# Read data from silver layer
df_vessels = spark.read.parquet("hdfs://localhost:9000/home/itversity/silver/vessels")
df_ports = spark.read.parquet("hdfs://localhost:9000/home/itversity/silver/ports")

# === Prepare vessels DataFrame ===
vessels_ready = df_vessels \
    .withColumn("dest_lat", round(col("destination_port_lat"), 1)) \
    .withColumn("dest_lon", round(col("destination_port_lon"), 1)) \
    .withColumn("dest_country", trim(lower(col("destination_port_country"))))

# === Prepare ports DataFrame, renaming conflicting columns ===
ports_ready = df_ports \
    .withColumn("port_lat", round(col("Latitude"), 1)) \
    .withColumn("port_lon", round(col("Longitude"), 1)) \
    .withColumn("port_country", trim(lower(col("Country_Code")))) \
    .withColumnRenamed("ingestion_date", "port_ingestion_date") \
    .withColumnRenamed("processing_date", "port_processing_date")

# === Join vessels with ports ===
df_joined = vessels_ready.join(
    ports_ready,
    (vessels_ready["dest_lat"] == ports_ready["port_lat"]) &
    (vessels_ready["dest_lon"] == ports_ready["port_lon"]),
    how="left"
)

# === Add KPI & enrichment columns ===
df_gold = df_joined \
    .withColumn("is_old_vessel", when(col("year_built") < 2000, True).otherwise(False)) \
    .withColumn("vessel_size_type", 
        when(col("gross_tonnage") > 80000, "Large")
        .when(col("gross_tonnage") > 30000, "Medium")
        .otherwise("Small")
    ) \
    .withColumn("delay_category", 
        when(col("delay_in_arrival") > 48, "Delayed")
        .when(col("delay_in_arrival") > 0, "OnTime")
        .otherwise("Unknown")
    ) \
    .withColumn("port_depth_type",
        when(col("Channel_Depth_m") >= 15, "Deep")
        .when(col("Channel_Depth_m") >= 8, "Medium")
        .otherwise("Shallow")
    ) \
    .withColumn("is_high_deadweight", when(col("deadweight") > 100000, True).otherwise(False)) \
    .withColumn("turnaround_time_hours", 
        (col("departure_date").cast("long") - col("arrival_date").cast("long")) / 3600
    ) \
    .withColumn("route_key", concat_ws(" → ", col("last_port_name"), col("destination_port_name"))) \
    .withColumn("arrival_hour", hour(col("arrival_date"))) \
    .withColumn("is_peak_hour", 
        when((col("arrival_hour").between(8,10)) | (col("arrival_hour").between(16,18)), True).otherwise(False)
    ) \
    .withColumn("is_delayed", when(col("delay_in_arrival") > 0, True).otherwise(False)) \
    .withColumn("gold_created_at", current_date())

# === Save Gold data to HDFS (partitioned by creation date) ===
# .partitionBy("gold_created_at") \
df_gold.write \
    .mode("append") \
    .parquet("hdfs://localhost:9000/home/itversity/gold/vessels_ports_enriched")

print("Gold layer written to HDFS.")

# Stop Spark session
spark.stop()
