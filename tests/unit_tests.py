import pytest
from pyspark.sql import SparkSession, Row, functions as F
from datetime import datetime

# Import functions from your pipeline module
from src.pipeline import clean_data, calculate_daily_summary, detect_anomalies

# Import your functions here
# from your_module import clean_data, calculate_daily_summary, detect_anomalies, mark_missing_entries

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def test_clean_data(spark, monkeypatch):
    # Mock get_latest_max_date to return a specific date
    monkeypatch.setattr("src.utils.get_latest_max_date", lambda schema, table, col: datetime(2025, 5, 1).date())

    # Sample data
    data = [
        Row(timestamp="2025-05-02 00:00:00", turbine_id=1, power_output=2.5, date=datetime(2025, 5, 2).date(), meta_slot_timestamp=datetime.now()),
        Row(timestamp="2025-05-01 00:00:00", turbine_id=1, power_output=2.5, date=datetime(2025, 5, 1).date(), meta_slot_timestamp=datetime.now()),
        Row(timestamp="2025-05-02 01:00:00", turbine_id=None, power_output=2.7, date=datetime(2025, 5, 2).date(), meta_slot_timestamp=datetime.now()),
        Row(timestamp="2025-05-02 02:00:00", turbine_id=1, power_output=-1.0, date=datetime(2025, 5, 2).date(), meta_slot_timestamp=datetime.now()),
    ]
    df = spark.createDataFrame(data)
    result = clean_data(df)
    # Should only include row with date > 2025-05-01, non-negative power_output, and non-null turbine_id
    assert result.count() == 1
    row = result.collect()[0]
    assert row.turbine_id == 1
    assert row.power_output == 2.5

def test_calculate_daily_summary(spark):
    data = [
        Row(turbine_id=1, date=datetime(2025, 5, 2).date(), power_output=2.5),
        Row(turbine_id=1, date=datetime(2025, 5, 2).date(), power_output=3.5),
        Row(turbine_id=2, date=datetime(2025, 5, 2).date(), power_output=4.0),
    ]
    df = spark.createDataFrame(data)
    summary = calculate_daily_summary(df).orderBy("turbine_id")
    rows = summary.collect()
    assert len(rows) == 2
    assert rows[0].turbine_id == 1
    assert rows[0].min_power == 2.5
    assert rows[0].max_power == 3.5
    assert abs(rows[0].avg_power - 3.0) < 1e-6

def test_detect_anomalies(spark):
    data = [
    
        Row(turbine_id=1, date=datetime(2025, 5, 2).date(), timestamp="2025-05-02 00:00:00", power_output=2.5),
        Row(turbine_id=1, date=datetime(2025, 5, 2).date(), timestamp="2025-05-02 01:00:00", power_output=2.6),
        Row(turbine_id=1, date=datetime(2025, 5, 2).date(), timestamp="2025-05-02 02:00:00", power_output=100.0),
    ]
    df = spark.createDataFrame(data)
    anomalies = detect_anomalies(df)
    # Only the outlier should be detected
    assert anomalies.count() == 1
    row = anomalies.collect()[0]
    assert row.power_output == 100.0
