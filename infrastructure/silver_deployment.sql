USE CATALOG collibri_catalog;
USE SCHEMA silver_turbine;

CREATE TABLE IF NOT EXISTS turbine_cleansed (
    timestamp TIMESTAMP,
    turbine_id INT,
    power_output DOUBLE,
    date DATE,
    meta_slot_timestamp TIMESTAMP
) USING DELTA;
