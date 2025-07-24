
CREATE DATABASE gold;

DROP Table  gold.port_statistics; 

CREATE TABLE IF NOT EXISTS gold.port_statistics
STORED AS PARQUET
AS
SELECT
    destination_port_country,
    destination_port_name,
    COUNT(*) AS total_arrivals,
    COUNT(DISTINCT id) AS unique_vessels,
    AVG(delay_in_arrival) AS avg_delay_days,
    MAX(arrival_date) AS last_arrival,
    MIN(arrival_date) AS first_arrival
FROM silver.vessels
WHERE destination_port_name IS NOT NULL
GROUP BY destination_port_country, destination_port_name;


SELECT * FROM gold.port_statistics;

SELECT
    destination_port_country,
    destination_port_name,
    COUNT(*) AS total_arrivals,
    COUNT(DISTINCT id) AS unique_vessels,
    AVG(delay_in_arrival) AS avg_delay_days,
    MAX(arrival_date) AS last_arrival,
    MIN(arrival_date) AS first_arrival
FROM silver.vessels
WHERE destination_port_name IS NOT NULL
GROUP BY destination_port_country, destination_port_name;



CREATE OR REPLACE VIEW gold.vessel_count AS
SELECT COUNT(DISTINCT name) AS distinct_vessel_count
FROM silver.vessels;


SELECT * FROM gold.vessel_count;

SELECT * FROM silver.vessels
WHERE destination_port_country IS NOT NULL
  AND destination_port_name IS NOT NULL
LIMIT 10;


SELECT *
FROM gold.port_statistics;


