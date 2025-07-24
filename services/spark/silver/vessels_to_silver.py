from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, when, lit, current_timestamp, coalesce, regexp_replace, length, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import regexp_replace, trim, concat_ws, lit, to_date, least, greatest, when, current_date, udf


# Initialize Spark session with legacy time parser
spark = SparkSession.builder \
    .appName("VesselsBronzeToSilverTransformation") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.eventLog.logBlockUpdates.enabled", True)\
    .enableHiveSupport() \
    .getOrCreate()


vessel_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("url", StringType(), True),
    StructField("type", StringType(), True),
    StructField("year_built", IntegerType(), True),
    StructField("gross_tonnage", IntegerType(), True),
    StructField("deadweight", IntegerType(), True),
    StructField("length_m", DoubleType(), True),
    StructField("beam_m", DoubleType(), True),
    StructField("detail_link", StringType(), True),
    StructField("departure_date", StringType(), True),
    StructField("last_port_country", StringType(), True),
    StructField("last_port_name", StringType(), True),
    StructField("arrival_date", StringType(), True),
    StructField("destination_port_country", StringType(), True),
    StructField("destination_port_name", StringType(), True),
    StructField("destination_port_lat", StringType(), True),
    StructField("destination_port_lon", StringType(), True),
    StructField("reported_status", StringType(), True),
    StructField("report_date", DateType(), True),
    StructField("ingestion_date", DateType(), True)
])

vessels_path = "hdfs://localhost:9000/home/itversity/bronze/vessels"



df = spark.read.option("header", "true").schema(vessel_schema).csv(vessels_path)
cond = (df["last_port_name"].isNotNull() & df["destination_port_name"].isNotNull())
df = df.filter(cond)


cond = (df["destination_port_lon"].isNotNull()) & (df["destination_port_lon"] != "-") 
non_null_count = df.filter(cond).count()


cond = (df["destination_port_lon"].isNotNull()) & (df["destination_port_lon"] != "-")
df.select("destination_port_lon", "destination_port_lon").filter(cond).show(5)


def clean_directional_column(df: DataFrame, col_name: str) -> DataFrame:
    """
    Clean a directional coordinate column and convert it to a signed double.
    Overwrites the original column.

    Args:
        df: Input Spark DataFrame
        col_name: Column to clean (e.g., 'destination_port_lon' or 'destination_port_lat')

    Returns:
        DataFrame with overwritten column as double
    """
    
    c = col(col_name)
    cleaned = when(
        c.endswith("N") | c.endswith("E"),
        regexp_replace(c, "[NE]", "").cast("double")
    ).when(
        c.endswith("S") | c.endswith("W"),
        -1 * regexp_replace(c, "[SW]", "").cast("double")
    ).otherwise(None)

    return df.withColumn(col_name, cleaned)

df = clean_directional_column(df, "destination_port_lat")
df = clean_directional_column(df, "destination_port_lon")

# df.select("destination_port_lon", "destination_port_lon").filter(cond).show(50)


def clean_and_convert_date_column(
    df: DataFrame,
    column_name: str,
    year: str = "2025"
) -> DataFrame:
    """
    Clean a date string column (e.g., 'Jul 13, 17:30 UTC') and convert it to a date (yyyy-MM-dd).
    Appends the given year if missing.

    Args:
        df: Input Spark DataFrame
        column_name: Name of the date string column (e.g., 'departure_date')
        year: Year to append for parsing (default = '2025')

    Returns:
        DataFrame with the same column name converted to date (DateType)
    """
    # Clean: remove "UTC" and trim
    cleaned_col = trim(regexp_replace(column_name, "UTC", ""))

    # Rebuild full string: "Jul 13, 17:30" => "Jul 13 2025"
    full_date_str = concat_ws(" ", cleaned_col.substr(1, 6), lit(year))

    # Convert to DateType: "MMM d yyyy"
    df = df.withColumn(column_name, to_date(full_date_str, "MMM d yyyy"))

    return df

df = clean_and_convert_date_column(df, "departure_date", year="2025")
df = clean_and_convert_date_column(df, "arrival_date", "2025")




def fill_missing_dates_with_max(df):
    """
    Fill null values in departure_date and arrival_date columns
    with the maximum of departure_date, arrival_date, and report_date.
    """
    G = df.replace('-', None, subset=["departure_date", "arrival_date", "ingestion_date", "report_date"])
    G = G.withColumn(
        "ingestion_date",
         when(
            col("ingestion_date").isNull(),
            current_date()
        ).otherwise(col("ingestion_date"))
    ).withColumn(
        "report_date",
         when(
            col("report_date").isNull(),
            current_date()
        ).otherwise(col("report_date"))
    )
    
    df_filled = G.withColumn(
        "departure_date",
        when(
            col("departure_date").isNull(),
            least(col("departure_date"), col("arrival_date"), col("report_date"), col("ingestion_date"))
        ).otherwise(col("departure_date"))
    ).withColumn(
        "arrival_date",
        when(
            col("arrival_date").isNull(),
            greatest(col("departure_date"), col("arrival_date"), col("report_date"), col("ingestion_date"))
        ).otherwise(col("arrival_date"))
    )

    return df_filled

df = fill_missing_dates_with_max(df)

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

spark.sql("use silver")
df.write \
  .mode("append") \
  .partitionBy("ingestion_date") \
  .saveAsTable("vessels")


print("Data written to silver.vessels table successfully.")



# to run this script to load vessels which on bronze in this path
# /opt/spark3/bin/spark-submit --master local[2] --conf spark.ui.port=18181 /home/itversity/spark/silver/vessels_to_silver.py
# 
