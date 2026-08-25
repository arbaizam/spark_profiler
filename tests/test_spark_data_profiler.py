import os

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from spark_data_profiler import ProfilerConfig, profile_dataframe


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("spark-data-profiler-tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"A local Java/Spark runtime is not available: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_bronze_inference_and_quality_metrics(spark):
    rows = [
        ("001", "10.50", "2026-08-25", "yes", " A "),
        ("002", "20.00", "2026-08-26", "no", "A"),
        ("003", "bad", None, "yes", "B"),
        (None, "", "NULL", None, "B"),
    ]
    frame = spark.createDataFrame(
        rows, ["order_id", "amount", "event_date", "active", "category"]
    )

    result = profile_dataframe(
        frame,
        ProfilerConfig(inference_threshold=0.65, top_values_max_cardinality=100),
    )
    profile = {
        row["column_name"]: row.asDict(recursive=True)
        for row in result.profile_df.collect()
    }

    assert profile["order_id"]["inferred_type"] == "integer"
    assert profile["order_id"]["suggested_type"] == "string"
    assert profile["order_id"]["semantic_type"] == "identifier"
    assert profile["order_id"]["leading_zero_count"] == 3

    assert profile["amount"]["suggested_type"] == "decimal(4,2)"
    assert profile["amount"]["inferred_invalid_count"] == 1
    assert profile["amount"]["blank_count"] == 1
    assert profile["amount"]["min_value"] == "10.50"
    assert profile["amount"]["max_value"] == "20.00"
    assert profile["amount"]["invalid_examples"] == ["bad"]

    assert profile["event_date"]["suggested_type"] == "date"
    assert profile["event_date"]["null_like_count"] == 1
    assert profile["active"]["suggested_type"] == "boolean"
    assert profile["active"]["mode_value"] == "true"
    assert profile["category"]["padded_count"] == 1
    assert profile["category"]["mode_value"] in {"A", "B"}
    assert profile["category"]["unique_values"] == ["A", "B"]
    assert profile["category"]["unique_values_complete"] is True
    assert profile["amount"]["unique_values"] == ["10.50", "20.00", "bad"]
    assert profile["amount"]["unique_values_complete"] is True

    assert "TRY_CAST" in result.silver_expressions["amount"]
    assert "TRY_TO_TIMESTAMP" in result.silver_expressions["event_date"]
    assert result.suggested_schema["order_id"].dataType.simpleString() == "string"


def test_empty_frame_produces_stable_profile(spark):
    frame = spark.createDataFrame([], "value string")
    result = profile_dataframe(frame, ProfilerConfig(calculate_top_values=False))
    row = result.profile_df.first()

    assert row["row_count"] == 0
    assert row["missing_count"] == 0
    assert row["parse_success_rate"] is None
    assert "empty_dataset" in row["quality_flags"]
