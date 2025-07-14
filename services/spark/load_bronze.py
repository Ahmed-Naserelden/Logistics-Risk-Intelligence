# =================================================================
# This Script ingests CSV files from a local directory into HDFS as Parquet files.
# It cleans the column names, adds an ingestion date, and maintains the directory structure.
# =================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import re
import os

# Create a Spark session
spark = SparkSession.builder \
    .appName("IngestToHDFS") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# Define source and destination paths
local_base = "/tmp/shared_data"  # for os operations
source_base = "file:///" + local_base
subdirs = [d for d in os.listdir(local_base) if os.path.isdir(os.path.join(local_base, d))]
hdfs_base = "hdfs://localhost:9000/bronze/"

# Function to clean column names
def clean_column_names(df):
    """
    Cleans column names in a PySpark DataFrame by replacing invalid characters with underscores,
    converting to lowercase, and handling duplicates.

    Args:
        df (DataFrame): Input PySpark DataFrame with potentially invalid column names.

    Returns:
        DataFrame: DataFrame with cleaned column names.

    Example:
        Input columns: ["World Port Index Number", "Region Name", "Port Code"]
        Output columns: ["world_port_index_number", "region_name", "port_code"]
    """
    for col_name in df.columns:
        # Replace invalid characters with underscore and convert to lowercase
        cleaned_name = re.sub(r'[^a-zA-Z0-9]', '_', col_name).lower()
        if col_name != cleaned_name:
            df = df.withColumnRenamed(col_name, cleaned_name)
    return df

# Process each subdirectory independently
for subdir in subdirs:
    subdir_path = os.path.join(local_base, subdir)
    hdfs_subdir = os.path.join(hdfs_base, subdir)
    
    # Read CSV files from the subdirectory
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"file://{subdir_path}/*.csv")

    # Apply column name cleaning
    df = clean_column_names(df)

    # Add ingestion date column with current timestamp
    df = df.withColumn("ingestion_date", current_timestamp())

    # Extract year, month, day from filename if needed (optional, adjust regex if necessary)
    # df = df.withColumn("year", regexp_extract(input_file_name(), r"/(\d{4})-\d{2}-\d{2}", 1))
    # df = df.withColumn("month", regexp_extract(input_file_name(), r"/\d{4}-(\d{2})-\d{2}", 1))
    # df = df.withColumn("day", regexp_extract(input_file_name(), r"/\d{4}-\d{2}-(\d{2})", 1))

    # Write to HDFS as Parquet, preserving directory structure
    df.write \
        .mode("overwrite") \
        .parquet(hdfs_subdir)  # Use hdfs_subdir to maintain subdirectory structure

# Stop the Spark session
spark.stop()