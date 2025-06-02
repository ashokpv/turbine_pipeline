from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from datetime import datetime
# Import custom modules
from spark_session import spark
from utils import (
    get_latest_max_date,
    write_delta_table,
    mark_missing_entries
)
def clean_data(df):
    """
    Clean the given DataFrame by filtering out rows with invalid data.

    Filters out rows with negative power output, and drops any rows with null
    values in the turbine_id, power_output, or timestamp columns. Also limits
    the DataFrame to only include rows with dates later than the latest date
    written to the silver turbine cleansed table.

    Args:
        df (DataFrame): DataFrame containing the data to clean.

    Returns:
        DataFrame: DataFrame with the cleaned data.
    """
    max_date = get_latest_max_date("silver_turbine", "turbine_cleansed", "date")
    if max_date:
        latest_df = df.filter(F.col("date") > max_date)
    else:
        latest_df = df
    latest_df = latest_df.filter(F.col("power_output") >= 0)
    latest_df = latest_df.dropna(subset=["turbine_id", "power_output", "timestamp"])
    latest_df = latest_df.select("timestamp", "turbine_id", "power_output", "date", "meta_slot_timestamp")
    return latest_df

def calculate_daily_summary(df):
    """
    Calculates summary statistics for each turbine on each calendar day.

    Args:
        df (DataFrame): DataFrame containing the data to summarize.

    Returns:
        DataFrame: DataFrame with the summary statistics, containing the columns:
            - turbine_id: turbine ID
            - date: date
            - min_power: minimum power output for that turbine on that day
            - max_power: maximum power output for that turbine on that day
            - avg_power: average power output for that turbine on that day
    """
    return df.groupBy("turbine_id", "date").agg(
        F.min("power_output").alias("min_power"),
        F.max("power_output").alias("max_power"),
        F.avg("power_output").alias("avg_power")
    )

def detect_anomalies(df):

    """
    Detects anomalies in the given DataFrame.

    Uses the Z-score method to detect power output readings that are more than 2
    standard deviations away from the mean power output for that turbine on that day.

    Args:
        df (DataFrame): DataFrame containing the data to detect anomalies in.

    Returns:
        DataFrame: DataFrame with the original columns and three additional columns:
            - mean_power: mean power output for that turbine on that day
            - std_power: standard deviation of power output for that turbine on that day
            - z_score: Z-score of the power output reading, calculated as
                (power_output - mean_power) / std_power
    """
    stats = df.groupBy("turbine_id", "date").agg(
        F.mean("power_output").alias("mean_power"),
        F.stddev("power_output").alias("std_power")
    )
    df_stats = df.join(stats, on=["turbine_id", "date"], how="left")
    # Filter out rows where std_power is 0 or null
    df_stats = df_stats.filter(F.col("std_power").isNotNull() & (F.col("std_power") > 0))
    df_stats = df_stats.withColumn(
        "z_score",
        (F.col("power_output") - F.col("mean_power")) / F.col("std_power")
    )
    df_stats = df_stats.filter(F.abs(F.col("z_score")) > 2).select(
        "timestamp", "turbine_id", "power_output", "mean_power", "std_power", "z_score"
    ).orderBy("turbine_id", "timestamp")
    return df_stats

def run_pipeline(input_path):

    """
    Runs the pipeline, given an input path for the CSV files.

    Reads the CSV files, cleans the data, calculates daily summary statistics,
    and detects anomalies. Writes all the intermediate results to Delta tables
    in the Unity Catalog.

    Args:
        input_path (str): Path to the CSV files, can be a local path, a DBFS path,
            or an S3 path.
    """
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
    df = df.withColumn("date", F.to_date("timestamp"))
    df = df.withColumn("meta_slot_timestamp", F.current_timestamp())

    cleaned_df = clean_data(df)
    write_delta_table(cleaned_df, 'silver_turbine', 'turbine_cleansed')

    # Summary stats
    summary_df = calculate_daily_summary(cleaned_df)
    write_delta_table(summary_df, 'gold_turbine', 'summary_stats')

    # Anomalies
    anomalies_df = detect_anomalies(cleaned_df)
    write_delta_table(anomalies_df, 'gold_turbine', 'anomalies')