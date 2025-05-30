USE CATALOG collibri_catalog;
USE SCHEMA silver_turbine;

CREATE TABLE IF NOT EXISTS cleaned_data (
    timestamp TIMESTAMP,
    turbine_id INT,
    wind_speed DOUBLE,
    wind_direction DOUBLE,
    power_output DOUBLE,
    date DATE
) USING DELTA;
