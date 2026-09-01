import os
import math

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession, functions as F, types as T

from spark_data_profiler import ProfilerConfig, _profile_schema, profile_dataframe


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("spark-data-profiler-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.sql.ansi.enabled",
                os.environ.get("SPARK_ANSI_ENABLED", "true"),
            )
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"A local Java/Spark runtime is not available: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_bronze_inference_and_quality_metrics(spark):
    rows = [
        ("001", "10.50", "2026-08-25", "yes", " A ", "0.04"),
        ("002", "20.00", "2026-08-26", "no", "A", "4.0"),
        ("003", "bad", None, "yes", "B", "0.05"),
        (None, "", "NULL", None, "B", None),
    ]
    frame = spark.createDataFrame(
        rows,
        [
            "order_id",
            "amount",
            "event_date",
            "active",
            "category",
            "conversion_rate",
        ],
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

    percentage = profile["conversion_rate"]
    assert percentage["observed_decimal_scales"] == [1, 2]
    assert percentage["min_observed_decimal_scale"] == 1
    assert percentage["max_observed_decimal_scale"] == 2
    assert percentage["max_observed_decimal_precision"] == 2
    assert percentage["fractional_percentage_scale_count"] == 2
    assert percentage["whole_percentage_scale_count"] == 1
    assert percentage["potential_percentage_type"] is True
    assert percentage["mixed_percentage_scale_candidate"] is True
    assert percentage["percentage_scale_risk"] == "high"
    assert "high_risk_mixed_percentage_scales" in percentage["quality_flags"]

    assert "silver_expression" not in result.profile_df.columns
    assert "quarantine_predicate" not in result.profile_df.columns
    assert result.suggested_schema["order_id"].dataType.simpleString() == "string"


def test_empty_frame_produces_stable_profile(spark):
    frame = spark.createDataFrame([], "value string")
    result = profile_dataframe(frame, ProfilerConfig(calculate_top_values=False))
    row = result.profile_df.first()

    assert row["row_count"] == 0
    assert row["missing_count"] == 0
    assert row["parse_success_rate"] is None
    assert "empty_dataset" in row["quality_flags"]


def test_profiles_malformed_values_under_ansi_mode(spark):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        values = [(str(value),) for value in range(99)] + [("bad",)]
        frame = spark.createDataFrame(values, ["number"])
        result = profile_dataframe(
            frame,
            ProfilerConfig(
                inference_threshold=0.98,
                calculate_top_values=False,
                collect_unique_string_values=False,
            ),
        )
        row = result.profile_df.first()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)

    assert row["inferred_type"] == "integer"
    assert row["inferred_invalid_count"] == 1
    assert row["invalid_examples"] == ["bad"]


def test_special_column_names_are_profiled_literally(spark):
    schema = T.StructType(
        [
            T.StructField(
                "first",
                T.StructType([T.StructField("name", T.StringType(), True)]),
                True,
            ),
            T.StructField("first.name", T.StringType(), True),
            T.StructField("we`ird", T.StringType(), True),
        ]
    )
    frame = spark.createDataFrame([(("nested",), "9", "7")], schema)
    result = profile_dataframe(
        frame,
        ProfilerConfig(
            calculate_top_values=False,
            collect_unique_string_values=False,
        ),
    )
    profile = {row["column_name"]: row for row in result.profile_df.collect()}

    assert profile["first.name"]["min_value"] == "9"
    assert profile["first.name"]["suggested_type"] == "integer"
    assert profile["we`ird"]["min_value"] == "7"
    assert profile["first"]["min_value"] is None


def test_duplicate_column_names_raise_clear_error(spark):
    frame = spark.createDataFrame([(1, 2)], ["left", "right"]).selectExpr(
        "left as duplicate", "right as duplicate"
    )
    with pytest.raises(ValueError, match="duplicate column name"):
        profile_dataframe(frame)


def test_percentage_diagnostics_use_corroborating_evidence(spark):
    frame = spark.createDataFrame(
        [
            ("0.99", "0.04", "0.04", "0.5"),
            ("25.00", "4%", "4.0", "50"),
            ("3.50", None, "bad", "150"),
            ("120.00", None, None, None),
        ],
        ["unit_price", "conversion_rate", "dirty_rate", "outside_rate"],
    )
    result = profile_dataframe(
        frame,
        ProfilerConfig(
            inference_threshold=0.60,
            calculate_top_values=False,
            collect_unique_string_values=False,
        ),
    )
    profile = {
        row["column_name"]: row.asDict(recursive=True)
        for row in result.profile_df.collect()
    }

    ordinary = profile["unit_price"]
    assert ordinary["numeric_range_spans_unit"] is True
    assert ordinary["mixed_percentage_scale_candidate"] is False
    assert ordinary["percentage_scale_risk"] is None

    explicit = profile["conversion_rate"]
    assert explicit["percentage_evidence_rate"] == 1.0
    assert explicit["mixed_percentage_scale_candidate"] is True
    assert explicit["percentage_scale_risk"] == "high"
    assert "mixed_percent_symbol_and_numeric_values" in explicit["quality_flags"]

    dirty = profile["dirty_rate"]
    assert dirty["mixed_percentage_scale_candidate"] is True
    assert dirty["percentage_scale_risk"] == "high"

    outside = profile["outside_rate"]
    assert outside["outside_percentage_range_count"] == 1
    assert outside["mixed_percentage_scale_candidate"] is False
    assert outside["percentage_scale_risk"] is None
    assert "percentage_values_outside_expected_range" in outside["quality_flags"]


def test_typed_nan_and_infinity_are_not_missing(spark):
    frame = spark.createDataFrame(
        [(float("nan"),), (float("inf"),), (1.0,), (2.0,)],
        T.StructType([T.StructField("measure", T.DoubleType(), True)]),
    )
    row = profile_dataframe(
        frame,
        ProfilerConfig(
            calculate_top_values=False,
            collect_unique_string_values=False,
        ),
    ).profile_df.first()

    assert row["missing_count"] == 0
    assert row["non_missing_count"] == 4
    assert row["nan_count"] == 1
    assert row["positive_infinity_count"] == 1
    assert row["non_finite_count"] == 2
    assert row["mean"] is None
    assert "non_finite_values_present" in row["quality_flags"]


def test_ambiguous_boolean_and_yyyymmdd_values_are_flagged(spark):
    frame = spark.createDataFrame(
        [("0", "20230115"), ("1", "20230116"), ("1", "20230117")],
        ["quantity", "batch_num"],
    )
    profile = {
        row["column_name"]: row.asDict(recursive=True)
        for row in profile_dataframe(
            frame,
            ProfilerConfig(
                calculate_top_values=False,
                collect_unique_string_values=False,
            ),
        ).profile_df.collect()
    }

    assert profile["quantity"]["inferred_type"] == "boolean"
    assert "boolean_integer_ambiguous" in profile["quantity"]["quality_flags"]
    assert profile["batch_num"]["suggested_type"] == "integer"
    assert "ambiguous_yyyymmdd_or_integer" in profile["batch_num"]["quality_flags"]


def test_wide_decimal_statistics_do_not_overflow_under_ansi(spark):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        large = "99999999999999999999999999999999999.99"
        frame = spark.createDataFrame(
            [("1.50",), ("1.50",), ("1.50",), (large,)], ["amount"]
        )
        row = profile_dataframe(
            frame,
            ProfilerConfig(
                calculate_top_values=False,
                collect_unique_string_values=False,
            ),
        ).profile_df.first()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)

    assert row["suggested_type"] == "decimal(37,2)"
    assert row["mean"] is not None and math.isfinite(row["mean"])


def test_timestamp_ntz_and_timestamp_statistics_are_stable(spark):
    previous = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    try:
        frame = spark.sql(
            "select timestamp_ntz'2024-01-01 10:00:00.123456' as ts_ntz, "
            "timestamp'2024-01-01 10:00:00.123456' as ts"
        )
        profile = {
            row["column_name"]: row.asDict(recursive=True)
            for row in profile_dataframe(
                frame,
                ProfilerConfig(
                    calculate_top_values=False,
                    collect_unique_string_values=False,
                ),
            ).profile_df.collect()
        }
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)

    assert profile["ts_ntz"]["source_type"] == "timestamp_ntz"
    assert profile["ts_ntz"]["min_value"] == profile["ts_ntz"]["median_value"]
    assert profile["ts"]["min_value"] == profile["ts"]["median_value"]
    assert profile["ts"]["min_value"].endswith("Z")


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"top_values_max_cardinality": -1}, "top_values_max_cardinality"),
        ({"categorical_max_distinct": -1}, "categorical_max_distinct"),
        ({"approx_distinct_rsd": 1e-7}, "approx_distinct_rsd"),
        ({"percentage_min_group_count": 0}, "percentage_min_group_count"),
    ],
)
def test_invalid_config_values_fail_early(kwargs, message):
    with pytest.raises(ValueError, match=message):
        ProfilerConfig(**kwargs)


def test_invalid_datetime_pattern_fails_before_spark_actions(spark):
    frame = spark.createDataFrame([("2024-01-01",)], ["event_date"])
    with pytest.raises(ValueError, match="Invalid Java datetime pattern"):
        profile_dataframe(frame, ProfilerConfig(date_formats=("not-a-pattern",)))


def test_source_dataframe_and_profile_schema_contract_are_stable(spark):
    frame = spark.createDataFrame([("1",), (None,)], ["value"])
    schema_before = frame.schema.json()
    cached_before = frame.is_cached
    count_before = frame.count()

    result = profile_dataframe(
        frame,
        ProfilerConfig(
            calculate_top_values=False,
            collect_unique_string_values=False,
        ),
    )

    assert frame.schema.json() == schema_before
    assert frame.is_cached == cached_before
    assert frame.count() == count_before
    assert result.profile_df.columns == _profile_schema().fieldNames()


def test_clean_wide_profile_uses_batched_actions(spark):
    frame = spark.range(100).select(
        *[
            (F.col("id") + F.lit(index)).cast("string").alias(f"c{index}")
            for index in range(24)
        ]
    )
    group = "spark-data-profiler-batched-action-test"
    spark.sparkContext.setJobGroup(group, group)
    try:
        profile_dataframe(
            frame,
            ProfilerConfig(
                aggregation_batch_size=8,
                calculate_top_values=False,
                collect_unique_string_values=False,
                cache_input=False,
            ),
        )
    finally:
        spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)

    jobs = spark.sparkContext.statusTracker().getJobIdsForGroup(group)
    # Spark may materialize two jobs for an aggregate action. This bound still
    # guards against returning to detail and invalid-example scans per column.
    assert len(jobs) <= 16
