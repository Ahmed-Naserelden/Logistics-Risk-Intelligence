from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, when, lit, current_timestamp, coalesce, regexp_replace, length, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import regexp_replace, trim, concat_ws, to_date, least, greatest, current_date, udf


# Initialize Spark session with legacy time parser
spark = SparkSession.builder \
    .appName("PORTBronzeToSilverTransformation") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.eventLog.logBlockUpdates.enabled", True)\
    .enableHiveSupport() \
    .getOrCreate()


# Ports schema (COMPLETE DEFINITION)
ports_schema = StructType([
    StructField("OID_", IntegerType(), True),
    StructField("World_Port_Index_Number", IntegerType(), True),
    StructField("Region_Name", StringType(), True),
    StructField("Main_Port_Name", StringType(), True),
    StructField("Alternate_Port_Name", StringType(), True),
    StructField("UN_LOCODE", StringType(), True),
    StructField("Country_Code", StringType(), True),
    StructField("World_Water_Body", StringType(), True),
    StructField("IHO_S130_Sea_Area", StringType(), True),
    StructField("Sailing_Direction_or_Publication", StringType(), True),
    StructField("Publication_Link", StringType(), True),
    StructField("Standard_Nautical_Chart", IntegerType(), True),
    StructField("IHO_S57_Electronic_Navigational_Chart", StringType(), True),
    StructField("IHO_S101_Electronic_Navigational_Chart", StringType(), True),
    StructField("Digital_Nautical_Chart", StringType(), True),
    StructField("Tidal_Range_m", DoubleType(), True),
    StructField("Entrance_Width_m", DoubleType(), True),
    StructField("Channel_Depth_m", DoubleType(), True),
    StructField("Anchorage_Depth_m", DoubleType(), True),
    StructField("Cargo_Pier_Depth_m", DoubleType(), True),
    StructField("Oil_Terminal_Depth_m", DoubleType(), True),
    StructField("Liquified_Natural_Gas_Terminal_Depth_m", DoubleType(), True),
    StructField("Maximum_Vessel_Length_m", DoubleType(), True),
    StructField("Maximum_Vessel_Beam_m", DoubleType(), True),
    StructField("Maximum_Vessel_Draft_m", DoubleType(), True),
    StructField("Offshore_Maximum_Vessel_Length_m", DoubleType(), True),
    StructField("Offshore_Maximum_Vessel_Beam_m", DoubleType(), True),
    StructField("Offshore_Maximum_Vessel_Draft_m", DoubleType(), True),
    StructField("Harbor_Size", StringType(), True),
    StructField("Harbor_Type", StringType(), True),
    StructField("Harbor_Use", StringType(), True),
    StructField("Shelter_Afforded", StringType(), True),
    StructField("Entrance_Restriction_Tide", StringType(), True),
    StructField("Entrance_Restriction_Heavy_Swell", StringType(), True),
    StructField("Entrance_Restriction_Ice", StringType(), True),
    StructField("Entrance_Restriction_Other", StringType(), True),
    StructField("Overhead_Limits", StringType(), True),
    StructField("Underkeel_Clearance_Management_System", StringType(), True),
    StructField("Good_Holding_Ground", StringType(), True),
    StructField("Turning_Area", StringType(), True),
    StructField("Port_Security", StringType(), True),
    StructField("Estimated_Time_of_Arrival_Message", StringType(), True),
    StructField("Quarantine_Pratique", StringType(), True),
    StructField("Quarantine_Sanitation", StringType(), True),
    StructField("Quarantine_Other", StringType(), True),
    StructField("Traffic_Separation_Scheme", StringType(), True),
    StructField("Vessel_Traffic_Service", StringType(), True),
    StructField("First_Port_of_Entry", StringType(), True),
    StructField("US_Representative", StringType(), True),
    StructField("Pilotage_Compulsory", StringType(), True),
    StructField("Pilotage_Available", StringType(), True),
    StructField("Pilotage_Local_Assistance", StringType(), True),
    StructField("Pilotage_Advisable", StringType(), True),
    StructField("Tugs_Salvage", StringType(), True),
    StructField("Tugs_Assistance", StringType(), True),
    StructField("Communications_Telephone", StringType(), True),
    StructField("Communications_Telefax", StringType(), True),
    StructField("Communications_Radio", StringType(), True),
    StructField("Communications_Radiotelephone", StringType(), True),
    StructField("Communications_Airport", StringType(), True),
    StructField("Communications_Rail", StringType(), True),
    StructField("Search_and_Rescue", StringType(), True),
    StructField("NAVAREA", StringType(), True),
    StructField("Facilities_Wharves", StringType(), True),
    StructField("Facilities_Anchorage", StringType(), True),
    StructField("Facilities_Dangerous_Cargo_Anchorage", StringType(), True),
    StructField("Facilities_Med_Mooring", StringType(), True),
    StructField("Facilities_Beach_Mooring", StringType(), True),
    StructField("Facilities_Ice_Mooring", StringType(), True),
    StructField("Facilities_Ro_Ro", StringType(), True),
    StructField("Facilities_Solid_Bulk", StringType(), True),
    StructField("Facilities_Liquid_Bulk", StringType(), True),
    StructField("Facilities_Container", StringType(), True),
    StructField("Facilities_Breakbulk", StringType(), True),
    StructField("Facilities_Oil_Terminal", StringType(), True),
    StructField("Facilities_LNG_Terminal", StringType(), True),
    StructField("Facilities_Other", StringType(), True),
    StructField("Medical_Facilities", StringType(), True),
    StructField("Garbage_Disposal", StringType(), True),
    StructField("Chemical_Holding_Tank_Disposal", StringType(), True),
    StructField("Degaussing", StringType(), True),
    StructField("Dirty_Ballast_Disposal", StringType(), True),
    StructField("Cranes_Fixed", StringType(), True),
    StructField("Cranes_Mobile", StringType(), True),
    StructField("Cranes_Floating", StringType(), True),
    StructField("Cranes_Container", StringType(), True),
    StructField("Lifts_100_Tons", StringType(), True),
    StructField("Lifts_50_100_Tons", StringType(), True),
    StructField("Lifts_25_49_Tons", StringType(), True),
    StructField("Lifts_0_24_Tons", StringType(), True),
    StructField("Services_Longshoremen", StringType(), True),
    StructField("Services_Electricity", StringType(), True),
    StructField("Services_Steam", StringType(), True),
    StructField("Services_Navigation_Equipment", StringType(), True),
    StructField("Services_Electrical_Repair", StringType(), True),
    StructField("Services_Ice_Breaking", StringType(), True),
    StructField("Services_Diving", StringType(), True),
    StructField("Supplies_Provisions", StringType(), True),
    StructField("Supplies_Potable_Water", StringType(), True),
    StructField("Supplies_Fuel_Oil", StringType(), True),
    StructField("Supplies_Diesel_Oil", StringType(), True),
    StructField("Supplies_Aviation_Fuel", StringType(), True),
    StructField("Supplies_Deck", StringType(), True),
    StructField("Supplies_Engine", StringType(), True),
    StructField("Repairs", StringType(), True),
    StructField("Dry_Dock", StringType(), True),
    StructField("Railway", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("ingestion_date", TimestampType(), True)
])



ports_path = "hdfs://localhost:9000/home/itversity/bronze/ports"

df = spark.read.option("header", "true").schema(ports_schema).csv(ports_path)


def clean_column_name(col_name):
    """Clean column names by replacing special characters with underscores"""
    return (
        col_name.replace(" ", "_")
        .replace("-", "_")
        .replace("(", "")
        .replace(")", "")
        .replace(",", "")
        .replace("/", "_")
        .replace("___", "_")  # Replace triple underscores
        .replace("__", "_")   # Replace double underscores
    )


def transform_ports(df):
    """Transform ports data from Bronze to Silver"""
    # Clean all column names first
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, clean_column_name(col_name))
    
    # Define columns we want to keep (after renaming)
    ports_columns = [
        "OID_", "World_Port_Index_Number", "Region_Name", "Main_Port_Name", 
        "Country_Code", "World_Water_Body", "Sailing_Direction_or_Publication", 
        "Publication_Link", "Standard_Nautical_Chart", "Tidal_Range_m", 
        "Channel_Depth_m", "Anchorage_Depth_m", "Cargo_Pier_Depth_m", 
        "Oil_Terminal_Depth_m", "Liquified_Natural_Gas_Terminal_Depth_m",
        "Maximum_Vessel_Length_m", "Maximum_Vessel_Beam_m", "Maximum_Vessel_Draft_m",
        "Offshore_Maximum_Vessel_Length_m", "Offshore_Maximum_Vessel_Beam_m",
        "Offshore_Maximum_Vessel_Draft_m", "Harbor_Size", "Harbor_Type", "Harbor_Use",
        "Shelter_Afforded", "Entrance_Restriction_Tide", "Entrance_Restriction_Heavy_Swell",
        "Entrance_Restriction_Ice", "Entrance_Restriction_Other", "Pilotage_Compulsory",
        "Pilotage_Available", "Communications_Telephone", "Latitude", "Longitude",
        "ingestion_date"
    ]
    
    # Only keep columns that exist in the dataframe
    existing_columns = [c for c in ports_columns if c in df.columns]
    
    return df.select(*existing_columns) \
        .withColumn("Tidal_Range_m", coalesce(col("Tidal_Range_m").cast(DoubleType()), lit(0.0))) \
        .withColumn("Channel_Depth_m", coalesce(col("Channel_Depth_m").cast(DoubleType()), lit(0.0))) \
        .withColumn("Anchorage_Depth_m", coalesce(col("Anchorage_Depth_m").cast(DoubleType()), lit(0.0))) \
        .withColumn("Cargo_Pier_Depth_m", coalesce(col("Cargo_Pier_Depth_m").cast(DoubleType()), lit(0.0))) \
        .withColumn("Oil_Terminal_Depth_m", coalesce(col("Oil_Terminal_Depth_m").cast(DoubleType()), lit(0.0))) \
        .withColumn("Liquified_Natural_Gas_Terminal_Depth_m", 
                   coalesce(col("Liquified_Natural_Gas_Terminal_Depth_m").cast(DoubleType()), lit(0.0))) \
        .withColumn("Maximum_Vessel_Length_m", coalesce(col("Maximum_Vessel_Length_m").cast(DoubleType()), lit(0.0))) \
        .withColumn("Maximum_Vessel_Beam_m", coalesce(col("Maximum_Vessel_Beam_m").cast(DoubleType()), lit(0.0))) \
        .withColumn("Maximum_Vessel_Draft_m", coalesce(col("Maximum_Vessel_Draft_m").cast(DoubleType()), lit(0.0))) \
        .withColumn("Offshore_Maximum_Vessel_Length_m", 
                   coalesce(col("Offshore_Maximum_Vessel_Length_m").cast(DoubleType()), lit(0.0))) \
        .withColumn("Offshore_Maximum_Vessel_Beam_m", 
                   coalesce(col("Offshore_Maximum_Vessel_Beam_m").cast(DoubleType()), lit(0.0))) \
        .withColumn("Offshore_Maximum_Vessel_Draft_m", 
                   coalesce(col("Offshore_Maximum_Vessel_Draft_m").cast(DoubleType()), lit(0.0))) \
        .withColumn("Latitude", col("Latitude").cast(DoubleType())) \
        .withColumn("Longitude", col("Longitude").cast(DoubleType())) \
        .withColumn("ingestion_date", to_date(col("ingestion_date"))) \
        .withColumn("processing_date", current_timestamp())

df = transform_ports(df)


spark.sql("CREATE DATABASE IF NOT EXISTS silver")

spark.sql("use silver")
df.write \
  .mode("append") \
  .partitionBy("ingestion_date") \
  .saveAsTable("ports")