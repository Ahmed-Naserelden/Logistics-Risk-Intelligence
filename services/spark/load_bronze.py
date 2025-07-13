# This script ingests CSV files from a shared directory into HDFS as Parquet files.

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import re
# Create a Spark session
spark = SparkSession.builder \
    .appName("IngestToHDFS") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# Define source and destination paths
source_base = "file:///tmp/shared_data/"
hdfs_base = "hdfs://localhost:9000/bronze/"

# Recursively read all CSV files and infer schema
df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv(source_base + "**/*.csv", recursiveFileLookup=True)

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

# Apply column name cleaning
df = clean_column_names(df)

# Add ingestion date column with current timestamp
df = df.withColumn("ingestion_date", current_timestamp())

# Extract year, month, day from filename if needed (optional, adjust regex if necessary)
# df = df.withColumn("year", regexp_extract(input_file_name(), r"/(\d{4})-\d{2}-\d{2}", 1))
# df = df.withColumn("month", regexp_extract(input_file_name(), r"/\d{4}-(\d{2})-\d{2}", 1))
# df = df.withColumn("day", regexp_extract(input_file_name(), r"/\d{4}-\d{2}-(\d{2})", 1))

# Wrtie to HDFS as Parquet, preserving directory structure and partitioning
df.write\
    .mode("overwrite") \
    .parquet(hdfs_base)

# Stop the Spark session
spark.stop()
