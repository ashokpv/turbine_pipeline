USE CATALOG collibri_catalog;
USE SCHEMA bronze_turbine;
CREATE TABLE IF NOT EXISTS cleaned_data (
    timestamp TIMESTAMP,
    turbine_id INT,
    wind_speed DOUBLE,
    wind_direction DOUBLE,
    power_output DOUBLE
) USING DELTA;
