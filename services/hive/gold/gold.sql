-- Active: 1753308548962@@localhost@10000@bronze


CREATE DATABASE IF NOT EXISTS gold;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

DROP TABLE IF EXISTS gold.vessel_count ;

CREATE TABLE gold.vessel_count  
STORED AS PARQUET
AS
SELECT COUNT(DISTINCT name) AS distinct_vessel_count
FROM silver.vessels;

SELECT * FROM gold.vessel_count;


-- ---------------------------------------------


