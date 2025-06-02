from pyspark.sql import SparkSession, functions as F, Window
from datetime import datetime
from spark_session import spark
from utils import get_latest_max_date, write_delta_table, mark_missing_entries

def clean_data(df):
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
    # Calendar day summary
    return df.groupBy("turbine_id", "date").agg(
        F.min("power_output").alias("min_power"),
        F.max("power_output").alias("max_power"),
        F.avg("power_output").alias("avg_power")
    )

def detect_anomalies(df):

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