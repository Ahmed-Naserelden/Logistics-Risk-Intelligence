

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


-- CREATE EXTERNAL TABLE IF NOT EXISTS gold_vessel_port (
--     id INT,
--     name STRING,
--     url STRING,
--     type STRING,
--     year_built INT,
--     gross_tonnage INT,
--     deadweight INT,
--     length_m DOUBLE,
--     beam_m DOUBLE,
--     detail_link STRING,
--     departure_date TIMESTAMP,
--     last_port_country STRING,
--     last_port_name STRING,
--     arrival_date TIMESTAMP,
--     destination_port_country STRING,
--     destination_port_name STRING,
--     destination_port_lat DOUBLE,
--     destination_port_lon DOUBLE,
--     reported_status STRING,
--     report_date TIMESTAMP,
--     ingestion_date TIMESTAMP,
--     delay_in_arrival DOUBLE,
--     processing_date TIMESTAMP,
--     dest_lat DOUBLE,
--     dest_lon DOUBLE,
--     dest_country STRING,
--     OID_ STRING,
--     World_Port_Index_Number STRING,
--     Region_Name STRING,
--     Main_Port_Name STRING,
--     Country_Code STRING,
--     World_Water_Body STRING,
--     Sailing_Direction_or_Publication STRING,
--     Publication_Link STRING,
--     Standard_Nautical_Chart STRING,
--     Tidal_Range_m DOUBLE,
--     Channel_Depth_m DOUBLE,
--     Anchorage_Depth_m DOUBLE,
--     Cargo_Pier_Depth_m DOUBLE,
--     Oil_Terminal_Depth_m DOUBLE,
--     Liquified_Natural_Gas_Terminal_Depth_m DOUBLE,
--     Maximum_Vessel_Length_m DOUBLE,
--     Maximum_Vessel_Beam_m DOUBLE,
--     Maximum_Vessel_Draft_m DOUBLE,
--     Offshore_Maximum_Vessel_Length_m DOUBLE,
--     Offshore_Maximum_Vessel_Beam_m DOUBLE,
--     Offshore_Maximum_Vessel_Draft_m DOUBLE,
--     Harbor_Size STRING,
--     Harbor_Type STRING,
--     Harbor_Use STRING,
--     Shelter_Afforded STRING,
--     Entrance_Restriction_Tide STRING,
--     Entrance_Restriction_Heavy_Swell STRING,
--     Entrance_Restriction_Ice STRING,
--     Entrance_Restriction_Other STRING,
--     Pilotage_Compulsory STRING,
--     Pilotage_Available STRING,
--     Communications_Telephone STRING,
--     Latitude DOUBLE,
--     Longitude DOUBLE,
--     port_ingestion_date STRING,
--     port_processing_date STRING,
--     port_lat DOUBLE,
--     port_lon DOUBLE,
--     port_country STRING,
--     is_old_vessel BOOLEAN,
--     vessel_size_type STRING,
--     delay_category STRING,
--     port_depth_type STRING,
--     is_high_deadweight BOOLEAN,
--     turnaround_time_hours DOUBLE,
--     route_key STRING,
--     arrival_hour INT,
--     is_peak_hour BOOLEAN,
--     is_delayed BOOLEAN,
--     gold_created_at STRING
-- )

-- LOCATION 'hdfs://localhost:9000/home/itversity/gold/vessels_ports_enriched';



CREATE DATABASE IF NOT EXISTS silver;


show databases;
use silver;

-- DROP TABLE IF EXISTS silver.vessels;

CREATE EXTERNAL TABLE IF NOT EXISTS silver.vessels (
    id INT,
    name STRING,
    url STRING,
    type STRING,
    year_built INT,
    gross_tonnage INT,
    deadweight INT,
    length_m DOUBLE,
    beam_m DOUBLE,
    detail_link STRING,
    departure_date TIMESTAMP,
    last_port_country STRING,
    last_port_name STRING,
    arrival_date TIMESTAMP,
    destination_port_country STRING,
    destination_port_name STRING,
    destination_port_lat STRING,
    destination_port_lon STRING,
    reported_status STRING,
    report_date TIMESTAMP,
    ingestion_date TIMESTAMP,
    delay_in_arrival DOUBLE,
    processing_date TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/home/itversity/silver/vessels';






SELECT * 
FROM bronze.vessels
WHERE destination_port_lon IS NOT NULL AND destination_port_lon != '-';


SELECT * 
FROM silver.vessels
WHERE destination_port_lon IS NOT NULL AND destination_port_lon != '-';


SELECT * 
FROM silver.vessels
WHERE id in (0); --  NULL AND destination_port_lon != '-';




























CREATE EXTERNAL TABLE IF NOT EXISTS silver.ports (
    OID_ INT,
    World_Port_Index_Number INT,
    Region_Name STRING,
    Main_Port_Name STRING,
    Country_Code STRING,
    World_Water_Body STRING,
    Sailing_Direction_or_Publication STRING,
    Publication_Link STRING,
    Standard_Nautical_Chart INT,
    Tidal_Range_m DOUBLE,
    Channel_Depth_m DOUBLE,
    Anchorage_Depth_m DOUBLE,
    Cargo_Pier_Depth_m DOUBLE,
    Oil_Terminal_Depth_m DOUBLE,
    Liquified_Natural_Gas_Terminal_Depth_m DOUBLE,
    Maximum_Vessel_Length_m DOUBLE,
    Maximum_Vessel_Beam_m DOUBLE,
    Maximum_Vessel_Draft_m DOUBLE,
    Offshore_Maximum_Vessel_Length_m DOUBLE,
    Offshore_Maximum_Vessel_Beam_m DOUBLE,
    Offshore_Maximum_Vessel_Draft_m DOUBLE,
    Harbor_Size STRING,
    Harbor_Type STRING,
    Harbor_Use STRING,
    Shelter_Afforded STRING,
    Entrance_Restriction_Tide STRING,
    Entrance_Restriction_Heavy_Swell STRING,
    Entrance_Restriction_Ice STRING,
    Entrance_Restriction_Other STRING,
    Pilotage_Compulsory STRING,
    Pilotage_Available STRING,
    Communications_Telephone STRING,
    Latitude DOUBLE,
    Longitude DOUBLE,
    ingestion_date TIMESTAMP,
    processing_date TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/home/itversity/silver/ports';

