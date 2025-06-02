from pyspark.sql import functions as F
from spark_session import spark
from utils import get_latest_max_date, write_delta_table

class TurbinePipeline:
    def __init__(self, input_path):
        self.input_path = input_path
        self.df = None
        self.cleaned_df = None

    def read_data(self):
        """
        Read in the data from the input path. The dataframe is stored in the
        instance variable self.df.

        The dataframe is transformed to add three columns:

        - timestamp: the timestamp column converted to a timestamp type
        - date: the date part of the timestamp column
        - meta_slot_timestamp: the current timestamp

        :return: None
        """
        self.df = spark.read.option("header", True).option("inferSchema", True).csv(self.input_path)
        self.df = self.df.withColumn("timestamp", F.to_timestamp("timestamp"))
        self.df = self.df.withColumn("date", F.to_date("timestamp"))
        self.df = self.df.withColumn("meta_slot_timestamp", F.current_timestamp())

    def clean_data(self):
        """
        Clean the data by:

        - Only considering dates greater than the latest date in the
          silver_turbine.turbine_cleansed table.
        - Removing rows with negative power output values.
        - Removing rows with missing values in turbine_id, power_output, or timestamp.
        - Selecting only the columns we care about: timestamp, turbine_id, power_output, date, meta_slot_timestamp.

        Returns the cleaned DataFrame.
        """
        max_date = get_latest_max_date("silver_turbine", "turbine_cleansed", "date")
        df = self.df
        if max_date:
            df = df.filter(F.col("date") > max_date)
        df = df.filter(F.col("power_output") >= 0)
        df = df.dropna(subset=["turbine_id", "power_output", "timestamp"])
        df = df.select("timestamp", "turbine_id", "power_output", "date", "meta_slot_timestamp")
        self.cleaned_df = df
        return df

    def calculate_daily_summary(self):
        """
        Calculate daily summary statistics for each turbine.

        Returns a DataFrame with the columns:
        - turbine_id
        - date
        - min_power
        - max_power
        - avg_power
        """
        return self.cleaned_df.groupBy("turbine_id", "date").agg(
            F.min("power_output").alias("min_power"),
            F.max("power_output").alias("max_power"),
            F.avg("power_output").alias("avg_power")
        )

    def detect_anomalies(self):
        """
        Detect anomalies in the cleaned data by calculating the Z-score for each record,
        and flagging those with an absolute Z-score greater than 2.

        Returns a DataFrame with the columns:
        - timestamp
        - turbine_id
        - power_output
        - mean_power
        - std_power
        - z_score

        The DataFrame is sorted by turbine_id and timestamp.
        """
        stats = self.cleaned_df.groupBy("turbine_id", "date").agg(
            F.mean("power_output").alias("mean_power"),
            F.stddev("power_output").alias("std_power")
        )
        df_stats = self.cleaned_df.join(stats, on=["turbine_id", "date"], how="left")
        df_stats = df_stats.filter(F.col("std_power").isNotNull() & (F.col("std_power") > 0))
        df_stats = df_stats.withColumn(
            "z_score",
            (F.col("power_output") - F.col("mean_power")) / F.col("std_power")
        )
        return df_stats.filter(F.abs(F.col("z_score")) > 2).select(
            "timestamp", "turbine_id", "power_output", "mean_power", "std_power", "z_score"
        ).orderBy("turbine_id", "timestamp")

    def write_outputs(self):
    """
    Writes the cleaned data, daily summary statistics, and detected anomalies to Delta tables.

    - The cleaned data is written to the 'silver_turbine.turbine_cleansed' table.
    - The daily summary statistics are calculated and written to the 'gold_turbine.summary_stats' table.
    - The detected anomalies are calculated and written to the 'gold_turbine.anomalies' table.

    :return: None
    """

        write_delta_table(self.cleaned_df, 'silver_turbine', 'turbine_cleansed')
        summary_df = self.calculate_daily_summary()
        write_delta_table(summary_df, 'gold_turbine', 'summary_stats')
        anomalies_df = self.detect_anomalies()
        write_delta_table(anomalies_df, 'gold_turbine', 'anomalies')

    def run(self):
        """
        Runs the pipeline.

        This method reads the input data, cleans it, and writes the cleaned data, daily summary statistics, and detected anomalies to Delta tables.

        :return: None
        """
        self.read_data()
        self.clean_data()
        self.write_outputs()
