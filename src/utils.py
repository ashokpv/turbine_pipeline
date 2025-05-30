

def get_latest_max_date(schema_name, table_name, date_column):
    """
    Reads the latest max date from a Unity Catalog cleansed table.

    Args:
        table_name (str): Name of the Unity Catalog table.
        date_column (str): Name of the date column to read.

    Returns:
        str: Latest max date as a string in the format 'YYYY-MM-DD'.
    """
    full_table_name = f"{schema_name}.{table_name}"
    table_df = spark.read.format("delta").table(full_table_name)
    max_date = table_df.agg(F.max(date_column)).first()[0]
    return max_date


def mark_missing_entries(df):
    # Count records per turbine per date
    counts = df.groupBy("turbine_id", "date").agg(F.count("*").alias("record_count"))

    # Show those with missing or extra records (not equal to 24)
    missing_df = counts.filter(counts.record_count != 24).show()
    return missing_df

    
def write_delta_table(df, schema_name, table_name, merge_keys=None, mode="append"):
    """
    Writes a DataFrame to a Unity Catalog Delta table.

    Args:
        df (DataFrame): DataFrame to write to the table.
        schema_name (str): Name of the Unity Catalog schema.
        table_name (str): Name of the Unity Catalog table.
        merge_keys (List[str], optional): Columns to merge on when mode is "replace". Defaults to None.
        mode (str, optional): Write mode. One of "append", "replace". Defaults to "append".
    """
    full_table_name = f"{schema_name}.{table_name}"
    if mode == "append":
        df.write.format("delta").mode("append").saveAsTable(full_table_name)
    elif mode == "replace":
        if not merge_keys:
            raise ValueError("Mode 'replace' requires merge_keys to be specified.")
        # Register the DataFrame as a temp view
        df.createOrReplaceTempView("source_temp")
        # Build the merge SQL
        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        merge_sql = f"""
        MERGE INTO {full_table_name} AS target
        USING source_temp AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(merge_sql)
    else:
        raise ValueError(f"Invalid mode: {mode}")
