from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, unix_timestamp, when, 
    lit, current_timestamp, coalesce, regexp_replace, length
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, DoubleType, TimestampType
)
import os
from datetime import datetime

# Initialize Spark session with legacy time parser
spark = SparkSession.builder \
    .appName("BronzeToSilverTransformation") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# =============================================
# SCHEMA DEFINITIONS
# =============================================

# Vessels schema
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
    StructField("report_date", StringType(), True),
    StructField("ingestion_date", TimestampType(), True)
])

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

# =============================================
# HELPER FUNCTIONS
# =============================================

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

def parse_custom_date(date_col):
    """Handle multiple date formats in the source data using PySpark column operations"""
    return when(
        # Check for null or empty strings
        (col(date_col).isNull()) | (col(date_col).cast("string").isin("", "-", "null")),
        lit(None).cast(TimestampType())
    ).when(
        # Format: "Jul 6, 21:11 UTC (21 hours ago)"
        (col(date_col).contains("UTC") & col(date_col).contains("ago")),
        to_timestamp(regexp_replace(col(date_col), r"\s*UTC.*", ""), "MMM d, HH:mm")
    ).when(
        # Format: "Jul 5, 02:00"
        (col(date_col).contains(",") & col(date_col).contains(":")),
        to_timestamp(col(date_col), "MMM d, HH:mm")
    ).when(
        # Format: "2025-07-07"
        (col(date_col).contains("-") & (length(col(date_col)) == 10)),
        to_timestamp(col(date_col), "yyyy-MM-dd")
    ).when(
        # Format: "2025-07-19 01:01:21"
        (col(date_col).contains("-") & col(date_col).contains(":")),
        to_timestamp(col(date_col), "yyyy-MM-dd HH:mm:ss")
    ).otherwise(lit(None).cast(TimestampType()))

# =============================================
# DATA TRANSFORMATION LOGIC
# =============================================

def transform_vessels(df):
    """Transform vessels data from Bronze to Silver"""
    return df \
        .filter((col("name").isNotNull()) & (col("report_date").isNotNull())) \
        .withColumn("gross_tonnage", coalesce(col("gross_tonnage").cast(IntegerType()), lit(0))) \
        .withColumn("deadweight", coalesce(col("deadweight").cast(IntegerType()), lit(0))) \
        .withColumn("length_m", coalesce(col("length_m").cast(DoubleType()), lit(0.0))) \
        .withColumn("beam_m", coalesce(col("beam_m").cast(DoubleType()), lit(0.0))) \
        .withColumn("departure_date", parse_custom_date("departure_date")) \
        .withColumn("arrival_date", parse_custom_date("arrival_date")) \
        .withColumn("report_date", parse_custom_date("report_date")) \
        .withColumn("delay_in_arrival", 
            when(
                (col("arrival_date").isNotNull()) & (col("departure_date").isNotNull()), 
                (unix_timestamp(col("arrival_date")) - unix_timestamp(col("departure_date"))) / 3600
            ).otherwise(lit(None).cast(DoubleType()))
        ) \
        .withColumn("processing_date", current_timestamp()) \
        .withColumn("destination_port_lat", 
            when(
                col("destination_port_lat").isNotNull(), 
                regexp_replace(col("destination_port_lat"), r"[NS]", "").cast(DoubleType())
            ).otherwise(lit(None).cast(DoubleType()))
        ) \
        .withColumn("destination_port_lon", 
            when(
                col("destination_port_lon").isNotNull(), 
                regexp_replace(col("destination_port_lon"), r"[EW]", "").cast(DoubleType())
            ).otherwise(lit(None).cast(DoubleType()))
        ) \
        .select(
            "id", "name", "url", "type", "year_built", "gross_tonnage", "deadweight", 
            "length_m", "beam_m", "detail_link", "departure_date", "last_port_country", 
            "last_port_name", "arrival_date", "destination_port_country", "destination_port_name", 
            "destination_port_lat", "destination_port_lon", "reported_status", "report_date", 
            "ingestion_date", "delay_in_arrival", "processing_date"
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
        .withColumn("ingestion_date", to_timestamp(col("ingestion_date"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("processing_date", current_timestamp())

# =============================================
# MAIN PROCESSING LOGIC
# =============================================

def process_directory(bronze_dir, dataset_type):
    """Process all files in a bronze directory"""
    files = spark.sparkContext.wholeTextFiles(bronze_dir + "/*.csv").keys().collect()
    
    for file_path in files:
        try:
            print(f"\nProcessing {file_path}...")
            
            # Read data with appropriate method
            if dataset_type == "vessels":
                df = spark.read.option("header", "true").schema(vessel_schema).csv(file_path)
                silver_df = transform_vessels(df)
                partition_col = "report_date"
            else:
                df = spark.read.option("header", "true").schema(ports_schema).csv(file_path)
                silver_df = transform_ports(df)
                partition_col = "ingestion_date"
            
            # Determine output path
            silver_base = "hdfs://localhost:9000/home/itversity/silver/"
            silver_dir = "vessels" if dataset_type == "vessels" else "ports"
            relative_path = os.path.relpath(os.path.dirname(file_path), bronze_dir)
            output_path = os.path.join(silver_base, silver_dir, relative_path) if relative_path != "." else os.path.join(silver_base, silver_dir)
            
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path.replace("hdfs://localhost:9000/", "/")), exist_ok=True)
            
            # Write to Silver layer
            silver_df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            print(f"Successfully processed {file_path} → {output_path}")
            
        except Exception as e:
            print(f"ERROR processing {file_path}: {str(e)}")
            continue

# =============================================
# EXECUTION
# =============================================

if __name__ == "__main__":
    # Process both datasets
    process_directory("hdfs://localhost:9000/bronze/vessels", "vessels")
    process_directory("hdfs://localhost:9000/bronze/ports", "ports")
    
    # Completion message
    completion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\nSilver layer transformation completed at {completion_time}")
    
    # Stop Spark
    spark.stop()