import pytest
from pyspark.sql import SparkSession, Row, functions as F
from datetime import datetime, date
from databricks.connect import DatabricksSession

# Import functions from your pipeline module
from src.pipeline import clean_data, calculate_daily_summary, detect_anomalies


@pytest.fixture(scope="session")
def spark():
    spark = DatabricksSession.builder.getOrCreate()
    return spark

def test_clean_data_filters_valid_rows(spark, monkeypatch):
    # Mock get_latest_max_date to return a specific date
    monkeypatch.setattr("src.utils.get_latest_max_date", lambda schema, table, col: date(2025, 5, 1))

    # Create test data
    data = [
        Row(timestamp="2025-05-02 00:00:00", turbine_id=1, power_output=2.5, date=date(2025, 5, 2), meta_slot_timestamp=datetime.now()),
        Row(timestamp="2025-05-01 00:00:00", turbine_id=1, power_output=2.5, date=date(2025, 5, 1), meta_slot_timestamp=datetime.now()),
        Row(timestamp="2025-05-02 02:00:00", turbine_id=1, power_output=-1.0, date=date(2025, 5, 2), meta_slot_timestamp=datetime.now()),
        
    ]
    df = spark.createDataFrame(data)
    result = clean_data(df)
    rows = result.collect()
    # Only the first row should remain
    assert result.count() == 2
    assert rows[0].turbine_id == 1
    assert rows[0].power_output == 2.5
    assert rows[0].date == date(2025, 5, 2)

def test_calculate_daily_summary_aggregation(spark):
    data = [
        Row(turbine_id=1, date=date(2025, 5, 2), power_output=2.5),
        Row(turbine_id=1, date=date(2025, 5, 2), power_output=3.5),
        Row(turbine_id=2, date=date(2025, 5, 2), power_output=4.0),
    ]
    df = spark.createDataFrame(data)
    summary = calculate_daily_summary(df).orderBy("turbine_id")
    rows = summary.collect()
    # There should be two rows, one for each turbine_id
    assert len(rows) == 2
    assert rows[0].turbine_id == 1
    assert rows[0].min_power == 2.5
    assert rows[0].max_power == 3.5
    assert rows[1].turbine_id == 2
    assert rows[1].min_power == 4.0
    assert rows[1].max_power == 4.0

def test_detect_anomalies_flags_outliers(spark):
    data = [
        Row(turbine_id=1, date=date(2025, 5, 2), timestamp="2025-05-02 00:00:00", power_output=2.5),
        Row(turbine_id=1, date=date(2025, 5, 2), timestamp="2025-05-02 01:00:00", power_output=2.6),
        Row(turbine_id=1, date=date(2025, 5, 2), timestamp="2025-05-02 02:00:00", power_output=100.0),
        Row(turbine_id=1, date=date(2025, 5, 2), timestamp="2025-05-02 03:00:00", power_output=2.4),
        Row(turbine_id=1, date=date(2025, 5, 2), timestamp="2025-05-02 04:00:00", power_output=2.2),
        Row(turbine_id=1, date=date(2025, 5, 2), timestamp="2025-05-02 05:00:00", power_output=2.5)
        
    ]
    df = spark.createDataFrame(data)
    anomalies = detect_anomalies(df)
    rows = anomalies.collect()
    # Only the outlier (100.0) should be detected
    assert anomalies.count() == 1
    assert rows[0].power_output == 100.0
    assert abs(rows[0].z_score) > 2