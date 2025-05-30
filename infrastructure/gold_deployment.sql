USE CATALOG collibri_catalog;
USE SCHEMA gold_turbine;

CREATE TABLE IF NOT EXISTS summary_stats (
    turbine_id INT,
    date DATE,
    min_power DOUBLE,
    max_power DOUBLE,
    avg_power DOUBLE
) USING DELTA;

CREATE TABLE IF NOT EXISTS anomalies (
    timestamp TIMESTAMP,
    turbine_id INT,
    power_output DOUBLE,
    mean_power DOUBLE,
    std_power DOUBLE,
    z_score DOUBLE
) USING DELTA;
