
CREATE DATABASE IF NOT EXISTS bronze;

USE bronze;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


CREATE EXTERNAL TABLE IF NOT EXISTS bronze.vessels (
    id INT,
    name STRING,
    url STRING,
    type STRING,
    year_built INT,
    gross_tonnage INT,
    deadweight INT,
    `length(m)` DOUBLE,
    `beam(m)` DOUBLE,
    detail_link STRING,
    departure_date STRING,
    last_port_country STRING,
    last_port_name STRING,
    arrival_date STRING,
    destination_port_country STRING,
    destination_port_name STRING,
    destination_port_lat STRING,
    destination_port_lon STRING,
    reported_status STRING,
    report_date STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/bronze/vessels'
TBLPROPERTIES ("skip.header.line.count" = "1");
