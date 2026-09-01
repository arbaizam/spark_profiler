"""Scalable, bronze-to-silver profiling for PySpark DataFrames.

The public entry point is :func:`profile_dataframe`.  It returns a
``ProfileResult`` containing a Spark DataFrame with one row per input column
and a suggested ``StructType``.

The profiler never collects source rows to the driver.  It only collects
aggregate results, bounded value domains, and small invalid-value samples.
Column work is grouped into configurable batches to keep both Spark job count
and generated query plans bounded.
"""

from __future__ import annotations

import datetime as dt
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

try:
    from pyspark import StorageLevel
    from pyspark.sql import Column, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window
except ImportError as exc:  # pragma: no cover - exercised only without PySpark
    raise ImportError(
        "spark_data_profiler requires pyspark. Install it with `pip install pyspark`."
    ) from exc


_INTEGER_RE = r"^[+-]?[0-9]+$"
_DECIMAL_RE = r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$"
_DOUBLE_RE = (
    r"^[+-]?(?:(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)"
    r"(?:[eE][+-]?[0-9]+)?)$"
)

_TRUE_VALUES = ("true", "t", "yes", "y", "1")
_FALSE_VALUES = ("false", "f", "no", "n", "0")
_BOOLEAN_VALUES = _TRUE_VALUES + _FALSE_VALUES

_DEFAULT_DATE_FORMATS = (
    "yyyy-MM-dd",
    "M/d/yyyy",
    "yyyyMMdd",
)
_DEFAULT_TIMESTAMP_FORMATS = (
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ssXXX",
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
)

_FORMAT_REGEXES = {
    "yyyy-MM-dd": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
    "M/d/yyyy": r"^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$",
    "MM/dd/yyyy": r"^[0-9]{2}/[0-9]{2}/[0-9]{4}$",
    "yyyyMMdd": r"^[0-9]{8}$",
    "yyyy-MM-dd HH:mm:ss": (r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$"),
    "yyyy-MM-dd HH:mm:ss.SSS": (
        r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}"
        r"\.[0-9]{1,9}$"
    ),
    "yyyy-MM-dd'T'HH:mm:ss": (
        r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$"
    ),
    "yyyy-MM-dd'T'HH:mm:ss.SSS": (
        r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
        r"\.[0-9]{1,9}$"
    ),
    "yyyy-MM-dd'T'HH:mm:ssXXX": (
        r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
        r"(?:Z|[+-][0-9]{2}:[0-9]{2})$"
    ),
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX": (
        r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
        r"\.[0-9]{1,9}(?:Z|[+-][0-9]{2}:[0-9]{2})$"
    ),
}

_IDENTIFIER_NAME_RE = re.compile(
    r"(?:^|_)(?:id|uuid|guid|sku|zip|zipcode|postal|postal_code|phone|ssn|"
    r"account|account_number|customer_number|order_number|invoice_number|code)"
    r"(?:$|_)",
    re.IGNORECASE,
)

_PERCENTAGE_NAME_RE = re.compile(
    r"(?:^|_)(?:pct(?:change)?|percent(?:age)?|rate|ratio|share|margin|yield|"
    r"utilization|utilisation|ctr|apr|roi|irr|roas|cpc)(?:$|_)",
    re.IGNORECASE,
)
_NON_PERCENTAGE_AMOUNT_NAME_RE = re.compile(
    r"(?:^|_)(?:amount|dollars?|price|cost|revenue|sales|value)(?:$|_)",
    re.IGNORECASE,
)

_MIN_APPROX_DISTINCT_RSD = 0.000017


@dataclass(frozen=True)
class ProfilerConfig:
    """Controls inference, cost, and the generated recommendations."""

    inference_threshold: float = 0.98
    null_like_values: Tuple[str, ...] = (
        "null",
        "none",
        "n/a",
        "na",
        "nan",
        "missing",
    )
    case_sensitive_nulls: bool = False
    trim_strings: bool = True
    date_formats: Tuple[str, ...] = _DEFAULT_DATE_FORMATS
    timestamp_formats: Tuple[str, ...] = _DEFAULT_TIMESTAMP_FORMATS
    approx_distinct_rsd: float = 0.05
    exact_distinct: bool = False
    percentile_accuracy: int = 10_000
    calculate_top_values: bool = True
    top_n: int = 5
    top_values_max_cardinality: Optional[int] = 10_000
    collect_unique_string_values: bool = True
    unique_values_max_cardinality: Optional[int] = 200
    invalid_sample_size: int = 5
    aggregation_batch_size: int = 8
    high_missing_rate: float = 0.20
    categorical_max_distinct: int = 50
    preserve_identifier_strings: bool = True
    percentage_detection_min_numeric_rate: float = 0.80
    percentage_min_group_count: int = 1
    cache_input: bool = True

    def __post_init__(self) -> None:
        if not 0.0 < self.inference_threshold <= 1.0:
            raise ValueError("inference_threshold must be in (0, 1]")
        if not _MIN_APPROX_DISTINCT_RSD <= self.approx_distinct_rsd < 1.0:
            raise ValueError(
                "approx_distinct_rsd must be at least 0.000017 and less than 1"
            )
        if self.percentile_accuracy < 1:
            raise ValueError("percentile_accuracy must be positive")
        if self.top_n < 1:
            raise ValueError("top_n must be positive")
        if (
            self.top_values_max_cardinality is not None
            and self.top_values_max_cardinality < 1
        ):
            raise ValueError("top_values_max_cardinality must be positive or None")
        if (
            self.unique_values_max_cardinality is not None
            and self.unique_values_max_cardinality < 1
        ):
            raise ValueError("unique_values_max_cardinality must be positive or None")
        if self.invalid_sample_size < 0:
            raise ValueError("invalid_sample_size cannot be negative")
        if self.aggregation_batch_size < 1:
            raise ValueError("aggregation_batch_size must be positive")
        if self.categorical_max_distinct < 1:
            raise ValueError("categorical_max_distinct must be positive")
        if not 0.0 <= self.high_missing_rate <= 1.0:
            raise ValueError("high_missing_rate must be in [0, 1]")
        if not 0.0 <= self.percentage_detection_min_numeric_rate <= 1.0:
            raise ValueError("percentage_detection_min_numeric_rate must be in [0, 1]")
        if self.percentage_min_group_count < 1:
            raise ValueError("percentage_min_group_count must be positive")
        for setting_name, formats in (
            ("date_formats", self.date_formats),
            ("timestamp_formats", self.timestamp_formats),
        ):
            if isinstance(formats, (str, bytes)):
                raise ValueError(f"{setting_name} must be a sequence of format strings")
            try:
                invalid_format = any(
                    not isinstance(fmt, str) or not fmt.strip() for fmt in formats
                )
            except TypeError as exc:
                raise ValueError(
                    f"{setting_name} must be a sequence of format strings"
                ) from exc
            if invalid_format:
                raise ValueError(
                    f"{setting_name} must contain only non-empty format strings"
                )


@dataclass
class ProfileResult:
    """Artifacts produced by a profiling run."""

    profile_df: DataFrame
    suggested_schema: T.StructType


@dataclass
class _ColumnSpec:
    inferred_type: str
    suggested_type: str
    best_candidate_type: Optional[str]
    best_candidate_rate: Optional[float]
    inference_confidence: Optional[float]
    semantic_type: str
    inferred_valid_count: int
    observed_formats: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


_ProfileContext = Tuple[int, T.StructField, Mapping[str, Any], _ColumnSpec]


class DataFrameProfiler:
    """Profile a Spark DataFrame for bronze-to-silver promotion decisions."""

    def __init__(self, config: Optional[ProfilerConfig] = None) -> None:
        self.config = config or ProfilerConfig()

    def _validate_datetime_formats(self, df: DataFrame) -> None:
        """Fail early on invalid Java datetime patterns when a JVM is available."""

        jvm = getattr(df.sparkSession, "_jvm", None)
        if jvm is None:  # Spark Connect does not expose the embedded JVM.
            return
        for setting_name, formats in (
            ("date_formats", self.config.date_formats),
            ("timestamp_formats", self.config.timestamp_formats),
        ):
            for fmt in formats:
                try:
                    jvm.java.time.format.DateTimeFormatter.ofPattern(fmt)
                except Exception as exc:
                    raise ValueError(
                        f"Invalid Java datetime pattern in {setting_name}: {fmt!r}"
                    ) from exc

    def profile(self, df: DataFrame) -> ProfileResult:
        """Profile ``df`` and return aggregate metrics and a schema recommendation."""

        if not isinstance(df, DataFrame):
            raise TypeError("df must be a pyspark.sql.DataFrame")
        version_match = re.match(r"^(\d+)\.(\d+)", df.sparkSession.version)
        if version_match and tuple(map(int, version_match.groups())) < (3, 5):
            raise RuntimeError("spark_data_profiler requires Apache Spark 3.5 or newer")

        duplicate_names = sorted(
            name for name, count in Counter(df.columns).items() if count > 1
        )
        if duplicate_names:
            joined = ", ".join(repr(name) for name in duplicate_names)
            raise ValueError(
                "DataFrame columns must be unique before profiling; duplicate "
                f"column name(s): {joined}. Alias duplicate columns first."
            )

        self._validate_datetime_formats(df)

        persisted_here = False
        work_df = df
        if self.config.cache_input and not df.is_cached:
            work_df = df.persist(StorageLevel.MEMORY_AND_DISK)
            persisted_here = True

        try:
            row_count = work_df.count()
            base_stats = self._collect_base_stats(work_df)
            rows: List[Dict[str, Any]] = []
            schema_fields: List[T.StructField] = []
            contexts = [
                (
                    ordinal,
                    source_field,
                    base_stats[source_field.name],
                    self._infer_column(
                        source_field, base_stats[source_field.name], row_count
                    ),
                )
                for ordinal, source_field in enumerate(work_df.schema.fields)
            ]
            details = self._collect_details(work_df, contexts)
            top_value_results = self._collect_top_values_batched(work_df, contexts)
            unique_value_results = self._collect_unique_string_values_batched(
                work_df, contexts
            )
            invalid_example_results = self._collect_invalid_examples_batched(
                work_df, contexts
            )

            for ordinal, source_field, base, spec in contexts:
                detail = details[ordinal]
                top_values, top_skipped = top_value_results[ordinal]
                (
                    unique_values,
                    unique_values_complete,
                    unique_values_skipped,
                ) = unique_value_results[ordinal]
                invalid_examples = invalid_example_results[ordinal]
                flags = self._quality_flags(
                    source_field,
                    base,
                    spec,
                    row_count,
                    top_skipped,
                    unique_values_skipped,
                )
                rows.append(
                    self._profile_row(
                        source_field,
                        ordinal,
                        base,
                        spec,
                        detail,
                        top_values,
                        unique_values,
                        unique_values_complete,
                        invalid_examples,
                        flags,
                        row_count,
                    )
                )
                target_type = self._spark_type(source_field, spec.suggested_type)
                inferred_invalid = max(
                    0, int(base["non_missing_count"]) - spec.inferred_valid_count
                )
                nullable = bool(
                    source_field.nullable
                    or base["missing_count"] > 0
                    or inferred_invalid > 0
                )
                schema_fields.append(
                    T.StructField(source_field.name, target_type, nullable=nullable)
                )

            spark = work_df.sparkSession
            profile_df = spark.createDataFrame(rows, schema=_profile_schema())
            return ProfileResult(
                profile_df=profile_df,
                suggested_schema=T.StructType(schema_fields),
            )
        finally:
            if persisted_here:
                work_df.unpersist()

    def _collect_base_stats(self, df: DataFrame) -> Dict[str, Dict[str, Any]]:
        results: Dict[str, Dict[str, Any]] = {}
        fields = df.schema.fields
        batch_size = self.config.aggregation_batch_size

        for start in range(0, len(fields), batch_size):
            batch = fields[start : start + batch_size]
            expressions: List[Column] = []
            aliases: Dict[str, Tuple[str, str]] = {}

            for offset, source_field in enumerate(batch):
                key = f"c{start + offset}"
                metrics = self._base_metric_expressions(source_field)
                for metric_name, expression in metrics.items():
                    alias = f"{key}__{metric_name}"
                    expressions.append(expression.alias(alias))
                    aliases[alias] = (source_field.name, metric_name)

            if not expressions:
                continue
            aggregated = df.agg(*expressions).first().asDict()
            for alias, value in aggregated.items():
                column_name, metric_name = aliases[alias]
                results.setdefault(column_name, {})[metric_name] = value

        for source_field in fields:
            values = results[source_field.name]
            for metric in (
                "null_count",
                "blank_count",
                "null_like_count",
                "missing_count",
                "non_missing_count",
                "padded_count",
                "distinct_count",
                "leading_zero_count",
                "boolean_count",
                "integer_count",
                "decimal_count",
                "double_count",
                "date_count",
                "timestamp_count",
                "fractional_percentage_scale_count",
                "whole_percentage_scale_count",
                "outside_percentage_range_count",
                "percentage_symbol_count",
                "nan_count",
                "positive_infinity_count",
                "negative_infinity_count",
                "non_finite_count",
                "max_integer_digits",
                "max_decimal_integer_digits",
            ):
                values[metric] = _as_int(values.get(metric))
            values["observed_decimal_scales"] = sorted(
                int(value) for value in (values.get("observed_decimal_scales") or [])
            )
            for index, _ in enumerate(self.config.date_formats):
                metric = f"date_format_{index}_count"
                values[metric] = _as_int(values.get(metric))
            for index, _ in enumerate(self.config.timestamp_formats):
                metric = f"timestamp_format_{index}_count"
                values[metric] = _as_int(values.get(metric))
        return results

    def _base_metric_expressions(
        self, source_field: T.StructField
    ) -> Dict[str, Column]:
        raw, text, clean, missing, blank, null_like = self._text_components(
            source_field
        )
        valid_text = F.when(~missing, clean)
        integer_match = valid_text.rlike(_INTEGER_RE)
        decimal_match = valid_text.rlike(_DECIMAL_RE)
        double_match = valid_text.rlike(_DOUBLE_RE)
        lowered = F.lower(valid_text)

        signless = F.regexp_replace(valid_text, r"^[+-]", "")
        integer_digits_text = F.regexp_replace(signless, r"^0+", "")
        integer_digits = F.when(F.length(integer_digits_text) == 0, F.lit(1)).otherwise(
            F.length(integer_digits_text)
        )
        integer_part = F.regexp_extract(signless, r"^([0-9]*)", 1)
        integer_part_no_zeros = F.regexp_replace(integer_part, r"^0+", "")
        fractional_part = F.regexp_extract(signless, r"\.([0-9]+)$", 1)
        decimal_integer_digits = F.when(
            F.length(integer_part_no_zeros) == 0,
            F.when(F.length(fractional_part) > 0, F.lit(0)).otherwise(F.lit(1)),
        ).otherwise(F.length(integer_part_no_zeros))
        decimal_scale = F.length(fractional_part)
        decimal_precision = F.greatest(F.lit(1), decimal_integer_digits + decimal_scale)
        numeric_value = F.when(double_match, valid_text.cast("double"))
        numeric_magnitude = F.abs(numeric_value)
        percentage_symbol_match = valid_text.rlike(
            r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)\s*%$"
        )
        floating_source = isinstance(source_field.dataType, (T.FloatType, T.DoubleType))
        if floating_source:
            nan_match = F.isnan(raw)
            positive_infinity_match = raw == F.lit(float("inf"))
            negative_infinity_match = raw == F.lit(float("-inf"))
        else:
            nan_match = F.lit(False)
            positive_infinity_match = F.lit(False)
            negative_infinity_match = F.lit(False)

        date_parsers = [
            self._date_parser(valid_text, fmt) for fmt in self.config.date_formats
        ]
        timestamp_parsers = [
            self._timestamp_parser(valid_text, fmt)
            for fmt in self.config.timestamp_formats
        ]
        parsed_date = _coalesce_or_null(date_parsers, T.DateType())
        parsed_timestamp = _coalesce_or_null(timestamp_parsers, T.TimestampType())

        if self.config.exact_distinct:
            distinct = F.countDistinct(valid_text)
        else:
            distinct = F.approx_count_distinct(
                valid_text, rsd=self.config.approx_distinct_rsd
            )
        padded_match = (
            raw.isNotNull() & (text != F.trim(text))
            if isinstance(source_field.dataType, T.StringType)
            else F.lit(False)
        )

        metrics: Dict[str, Column] = {
            "null_count": _count_when(raw.isNull()),
            "blank_count": _count_when(blank),
            "null_like_count": _count_when(null_like),
            "missing_count": _count_when(missing),
            "non_missing_count": _count_when(~missing),
            "padded_count": _count_when(padded_match),
            "distinct_count": distinct,
            "min_length": F.min(F.length(valid_text)),
            "max_length": F.max(F.length(valid_text)),
            "avg_length": F.avg(F.length(valid_text)),
            "leading_zero_count": _count_when(
                integer_match & signless.rlike(r"^0[0-9]+$")
            ),
            "boolean_count": _count_when(lowered.isin(*_BOOLEAN_VALUES)),
            "integer_count": _count_when(integer_match),
            "decimal_count": _count_when(decimal_match),
            "double_count": _count_when(double_match),
            "date_count": _count_when(parsed_date.isNotNull()),
            "timestamp_count": _count_when(parsed_timestamp.isNotNull()),
            "fractional_percentage_scale_count": _count_when(
                (numeric_magnitude > 0) & (numeric_magnitude < 1)
            ),
            "whole_percentage_scale_count": _count_when(
                (numeric_magnitude > 1) & (numeric_magnitude <= 100)
            ),
            "outside_percentage_range_count": _count_when(numeric_magnitude > 100),
            "percentage_symbol_count": _count_when(percentage_symbol_match),
            "nan_count": _count_when(nan_match),
            "positive_infinity_count": _count_when(positive_infinity_match),
            "negative_infinity_count": _count_when(negative_infinity_match),
            "non_finite_count": _count_when(
                nan_match | positive_infinity_match | negative_infinity_match
            ),
            "max_integer_digits": F.max(F.when(integer_match, integer_digits)),
            "max_decimal_integer_digits": F.max(
                F.when(decimal_match, decimal_integer_digits)
            ),
            "min_decimal_scale": F.min(F.when(decimal_match, decimal_scale)),
            "max_decimal_scale": F.max(F.when(decimal_match, decimal_scale)),
            "max_decimal_precision": F.max(F.when(decimal_match, decimal_precision)),
            "observed_decimal_scales": F.sort_array(
                F.collect_set(F.when(decimal_match, decimal_scale))
            ),
        }
        for index, parser in enumerate(date_parsers):
            metrics[f"date_format_{index}_count"] = _count_when(parser.isNotNull())
        for index, parser in enumerate(timestamp_parsers):
            metrics[f"timestamp_format_{index}_count"] = _count_when(parser.isNotNull())
        return metrics

    def _infer_column(
        self,
        source_field: T.StructField,
        base: Mapping[str, Any],
        row_count: int,
    ) -> _ColumnSpec:
        non_missing = int(base["non_missing_count"])
        distinct = int(base["distinct_count"])
        uniqueness = _uniqueness_ratio(distinct, non_missing)

        if not isinstance(source_field.dataType, T.StringType):
            source_type = source_field.dataType.simpleString()
            return _ColumnSpec(
                inferred_type=source_type,
                suggested_type=source_type,
                best_candidate_type=source_type,
                best_candidate_rate=1.0 if non_missing else None,
                inference_confidence=1.0 if non_missing else None,
                semantic_type=self._semantic_type(
                    source_field, source_type, base, uniqueness
                ),
                inferred_valid_count=non_missing,
            )

        if non_missing == 0:
            return _ColumnSpec(
                inferred_type="string",
                suggested_type="string",
                best_candidate_type=None,
                best_candidate_rate=None,
                inference_confidence=None,
                semantic_type="empty",
                inferred_valid_count=0,
                notes=["No non-missing values were available for inference."],
            )

        candidate_counts = {
            "boolean": int(base["boolean_count"]),
            "timestamp": int(base["timestamp_count"]),
            "date": int(base["date_count"]),
            "integer": int(base["integer_count"]),
            "decimal": int(base["decimal_count"]),
            "double": int(base["double_count"]),
        }
        candidate_rates = {
            name: count / non_missing for name, count in candidate_counts.items()
        }
        # Tie order intentionally favors the most specific lossless type.
        priority = ("boolean", "timestamp", "date", "integer", "decimal", "double")
        best_candidate = max(
            priority,
            key=lambda name: (candidate_rates[name], -priority.index(name)),
        )
        best_rate = candidate_rates[best_candidate]
        yyyymmdd_ambiguity = self._yyyymmdd_integer_ambiguity(base, non_missing)
        if best_candidate == "date" and yyyymmdd_ambiguity:
            best_candidate = "integer"
            best_rate = candidate_rates["integer"]

        accepted = (
            best_candidate if best_rate >= self.config.inference_threshold else None
        )
        notes: List[str] = []
        observed_formats: List[str] = []
        if yyyymmdd_ambiguity:
            notes.append(
                "Values match both yyyyMMdd dates and 8-digit integers; integer "
                "is preferred until date semantics are confirmed."
            )

        if accepted is None:
            inferred = "string"
            suggested = "string"
            valid_count = non_missing
            confidence = 1.0
        elif accepted == "integer":
            digits = int(base["max_integer_digits"])
            if digits <= 9:
                inferred = suggested = "integer"
            elif digits <= 18:
                inferred = suggested = "bigint"
            elif digits <= 38:
                inferred = suggested = f"decimal({digits},0)"
            else:
                inferred = "integer (>38 digits)"
                suggested = "string"
                notes.append(
                    "Observed integer precision exceeds Spark DecimalType(38); preserve as string."
                )
            valid_count = candidate_counts["integer"]
            confidence = candidate_rates["integer"]
        elif accepted == "decimal":
            integer_digits = max(0, int(base["max_decimal_integer_digits"]))
            scale = int(base["max_decimal_scale"])
            precision = max(1, integer_digits + scale)
            if precision <= 38:
                inferred = suggested = f"decimal({precision},{scale})"
            else:
                inferred = f"decimal ({precision} digits observed)"
                suggested = "string"
                notes.append(
                    "Observed decimal precision exceeds Spark's 38-digit limit; preserve as string."
                )
            valid_count = candidate_counts["decimal"]
            confidence = candidate_rates["decimal"]
        else:
            inferred = suggested = accepted
            valid_count = candidate_counts[accepted]
            confidence = candidate_rates[accepted]
            if accepted == "date":
                observed_formats = self._observed_formats(base, is_timestamp=False)
            elif accepted == "timestamp":
                observed_formats = self._observed_formats(base, is_timestamp=True)

        semantic = self._semantic_type(source_field, inferred, base, uniqueness)
        if (
            inferred == "boolean"
            and candidate_counts["boolean"] == non_missing
            and candidate_counts["integer"] == non_missing
        ):
            notes.append(
                "The observed 0/1 domain is valid as both boolean and integer."
            )
        if (
            self.config.preserve_identifier_strings
            and semantic == "identifier"
            and inferred != "string"
        ):
            suggested = "string"
            notes.append(
                "Numeric-looking identifier is preserved as string to avoid losing formatting."
            )

        return _ColumnSpec(
            inferred_type=inferred,
            suggested_type=suggested,
            best_candidate_type=best_candidate if best_rate > 0 else None,
            best_candidate_rate=best_rate if best_rate > 0 else None,
            inference_confidence=confidence,
            semantic_type=semantic,
            inferred_valid_count=valid_count,
            observed_formats=observed_formats,
            notes=notes,
        )

    def _yyyymmdd_integer_ambiguity(
        self, base: Mapping[str, Any], non_missing: int
    ) -> bool:
        if not non_missing:
            return False
        matching_formats = {
            fmt
            for index, fmt in enumerate(self.config.date_formats)
            if int(base[f"date_format_{index}_count"]) > 0
        }
        return bool(
            matching_formats == {"yyyyMMdd"}
            and int(base["date_count"]) == non_missing
            and int(base["integer_count"]) == non_missing
        )

    def _semantic_type(
        self,
        source_field: T.StructField,
        inferred_type: str,
        base: Mapping[str, Any],
        uniqueness: Optional[float],
    ) -> str:
        if _IDENTIFIER_NAME_RE.search(source_field.name):
            if base["leading_zero_count"] > 0 or (uniqueness or 0.0) >= 0.80:
                return "identifier"
        if inferred_type == "boolean":
            return "boolean"
        if inferred_type in ("date", "timestamp", "timestamp_ntz"):
            return "date/time"
        if _is_numeric_type(inferred_type):
            return "numeric"
        distinct = int(base["distinct_count"])
        non_missing = int(base["non_missing_count"])
        if distinct and distinct <= self.config.categorical_max_distinct:
            if non_missing == 0 or distinct / non_missing <= 0.20:
                return "categorical"
        if (base.get("avg_length") or 0.0) >= 50.0:
            return "free_text"
        return "string"

    def _observed_formats(
        self, base: Mapping[str, Any], is_timestamp: bool
    ) -> List[str]:
        prefix = "timestamp" if is_timestamp else "date"
        formats = (
            self.config.timestamp_formats if is_timestamp else self.config.date_formats
        )
        counts = [
            (fmt, int(base[f"{prefix}_format_{index}_count"]))
            for index, fmt in enumerate(formats)
        ]
        return [
            fmt for fmt, count in sorted(counts, key=lambda item: -item[1]) if count
        ]

    def _collect_details(
        self, df: DataFrame, contexts: Sequence[_ProfileContext]
    ) -> Dict[int, Dict[str, Any]]:
        """Collect detailed aggregates in bounded multi-column Spark jobs."""

        results: Dict[int, Dict[str, Any]] = {}
        batch_size = self.config.aggregation_batch_size
        for start in range(0, len(contexts), batch_size):
            batch = contexts[start : start + batch_size]
            expressions: List[Column] = []
            aliases: Dict[str, Tuple[int, str]] = {}
            targets: Dict[int, str] = {}
            for ordinal, source_field, _, spec in batch:
                targets[ordinal] = spec.suggested_type
                for metric_name, expression in self._detail_metric_expressions(
                    source_field, spec
                ).items():
                    alias = f"d{ordinal}__{metric_name}"
                    expressions.append(expression.alias(alias))
                    aliases[alias] = (ordinal, metric_name)

            if not expressions:
                continue
            aggregated = df.agg(*expressions).first().asDict()
            raw_results: Dict[int, Dict[str, Any]] = defaultdict(dict)
            for alias, value in aggregated.items():
                ordinal, metric_name = aliases[alias]
                raw_results[ordinal][metric_name] = value
            for ordinal, values in raw_results.items():
                results[ordinal] = self._normalize_detail(values, targets[ordinal])
        return results

    def _detail_metric_expressions(
        self, source_field: T.StructField, spec: _ColumnSpec
    ) -> Dict[str, Column]:
        parsed = self._parsed_expr(source_field, spec.suggested_type, spec)
        target = spec.suggested_type
        metrics: Dict[str, Column] = {}

        numeric = _is_numeric_type(target)
        boolean = target == "boolean"
        date_type = target == "date"
        timestamp_type = target in ("timestamp", "timestamp_ntz")
        orderable = numeric or date_type or timestamp_type or target == "string"
        timestamp_micros = (
            self._timestamp_micros(parsed, target) if timestamp_type else None
        )

        if boolean:
            metrics["min_value"] = F.min(parsed.cast("integer"))
            metrics["max_value"] = F.max(parsed.cast("integer"))
        elif timestamp_type:
            metrics["min_value"] = F.min(timestamp_micros)
            metrics["max_value"] = F.max(timestamp_micros)
        elif orderable:
            metrics["min_value"] = F.min(parsed)
            metrics["max_value"] = F.max(parsed)
        else:
            metrics["min_value"] = _aggregate_literal(None, "string")
            metrics["max_value"] = _aggregate_literal(None, "string")

        quantile_source: Optional[Column] = None
        if numeric:
            numeric_double = parsed.cast("double")
            quantile_source = numeric_double
            metrics["mean"] = F.avg(numeric_double)
            metrics["stddev"] = F.stddev_samp(numeric_double)
            metrics["negative_count"] = _count_when(parsed < 0)
            metrics["zero_count"] = _count_when(parsed == 0)
        elif boolean:
            numeric_boolean = parsed.cast("integer")
            quantile_source = numeric_boolean
            metrics["mean"] = F.avg(numeric_boolean)
            metrics["stddev"] = F.stddev_samp(numeric_boolean)
            metrics["negative_count"] = _aggregate_literal(0, "long")
            metrics["zero_count"] = _count_when(~parsed)
        elif date_type:
            quantile_source = F.datediff(parsed, F.lit("1970-01-01"))
            metrics.update(self._empty_numeric_metrics())
        elif timestamp_type:
            quantile_source = timestamp_micros
            metrics.update(self._empty_numeric_metrics())
        else:
            metrics.update(self._empty_numeric_metrics())

        if quantile_source is not None:
            metrics["quantiles"] = F.percentile_approx(
                quantile_source,
                [0.25, 0.5, 0.75],
                self.config.percentile_accuracy,
            )
        else:
            metrics["quantiles"] = _aggregate_literal(None, T.ArrayType(T.DoubleType()))

        if boolean:
            metrics["true_count"] = _count_when(parsed)
            metrics["false_count"] = _count_when(~parsed)
        else:
            metrics["true_count"] = _aggregate_literal(0, "long")
            metrics["false_count"] = _aggregate_literal(0, "long")
        return metrics

    def _normalize_detail(
        self, raw_values: Mapping[str, Any], target: str
    ) -> Dict[str, Any]:
        values = dict(raw_values)
        quantiles = values.pop("quantiles", None)
        boolean = target == "boolean"
        timestamp_type = target in ("timestamp", "timestamp_ntz")
        if boolean:
            values["min_value"] = _boolean_string(values.get("min_value"))
            values["max_value"] = _boolean_string(values.get("max_value"))
        elif timestamp_type:
            values["min_value"] = _format_timestamp_micros(
                values.get("min_value"), target
            )
            values["max_value"] = _format_timestamp_micros(
                values.get("max_value"), target
            )
        else:
            values["min_value"] = _stringify(values.get("min_value"))
            values["max_value"] = _stringify(values.get("max_value"))
        values["mean"] = _as_float(values.get("mean"))
        values["stddev"] = _as_float(values.get("stddev"))
        for metric in ("negative_count", "zero_count", "true_count", "false_count"):
            values[metric] = _as_int(values.get(metric))

        converted = self._convert_quantiles(quantiles, target)
        values["q1_value"], values["median_value"], values["q3_value"] = converted
        return values

    @staticmethod
    def _empty_numeric_metrics() -> Dict[str, Column]:
        return {
            "mean": _aggregate_literal(None, "double"),
            "stddev": _aggregate_literal(None, "double"),
            "negative_count": _aggregate_literal(0, "long"),
            "zero_count": _aggregate_literal(0, "long"),
        }

    @staticmethod
    def _timestamp_micros(value: Column, target: str) -> Column:
        if target == "timestamp_ntz":
            days = F.datediff(value.cast("date"), F.lit("1970-01-01")).cast("long")
            seconds = (
                F.hour(value).cast("long") * F.lit(3_600)
                + F.minute(value).cast("long") * F.lit(60)
                + F.second(value).cast("long")
            )
            fractional_micros = F.date_format(value, "SSSSSS").cast("long")
            return (
                days * F.lit(86_400_000_000)
                + seconds * F.lit(1_000_000)
                + fractional_micros
            ).cast("long")
        return F.unix_micros(value)

    def _convert_quantiles(
        self, quantiles: Optional[Sequence[Any]], target: str
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        if not quantiles or len(quantiles) != 3:
            return None, None, None
        if target == "date":
            epoch = dt.date(1970, 1, 1)
            return tuple(  # type: ignore[return-value]
                (epoch + dt.timedelta(days=int(value))).isoformat()
                if value is not None
                else None
                for value in quantiles
            )
        if target in ("timestamp", "timestamp_ntz"):
            return tuple(  # type: ignore[return-value]
                _format_timestamp_micros(value, target) for value in quantiles
            )
        if target == "boolean":
            return tuple(  # type: ignore[return-value]
                "true"
                if value is not None and float(value) >= 1.0
                else "false"
                if value is not None
                else None
                for value in quantiles
            )
        return tuple(_stringify(value) for value in quantiles)  # type: ignore[return-value]

    def _collect_top_values_batched(
        self, df: DataFrame, contexts: Sequence[_ProfileContext]
    ) -> Dict[int, Tuple[List[Tuple[str, int, Optional[float]]], bool]]:
        results: Dict[int, Tuple[List[Tuple[str, int, Optional[float]]], bool]] = {
            ordinal: ([], True) for ordinal, _, _, _ in contexts
        }
        if not self.config.calculate_top_values:
            return results

        threshold = self.config.top_values_max_cardinality
        eligible = [
            context
            for context in contexts
            if threshold is None or int(context[2]["distinct_count"]) <= threshold
        ]
        for ordinal, _, _, _ in eligible:
            results[ordinal] = ([], False)

        batch_size = self.config.aggregation_batch_size
        for start in range(0, len(eligible), batch_size):
            batch = eligible[start : start + batch_size]
            entries = [
                F.struct(
                    F.lit(ordinal).cast("integer").alias("ordinal"),
                    self._parsed_expr(source_field, spec.suggested_type, spec)
                    .cast("string")
                    .alias("value"),
                )
                for ordinal, source_field, _, spec in batch
            ]
            if not entries:
                continue
            long_values = (
                df.select(F.explode(F.array(*entries)).alias("profile_value"))
                .select("profile_value.*")
                .where(F.col("value").isNotNull())
            )
            counts = long_values.groupBy("ordinal", "value").count()
            rank_window = Window.partitionBy("ordinal").orderBy(
                F.desc("count"), F.asc("value")
            )
            rows = (
                counts.withColumn("rank", F.row_number().over(rank_window))
                .where(F.col("rank") <= self.config.top_n)
                .orderBy("ordinal", "rank")
                .collect()
            )
            grouped: Dict[int, List[Tuple[str, int, Optional[float]]]] = defaultdict(
                list
            )
            denominators = {
                ordinal: int(base["non_missing_count"]) for ordinal, _, base, _ in batch
            }
            for row in rows:
                ordinal = int(row["ordinal"])
                count = int(row["count"])
                grouped[ordinal].append(
                    (str(row["value"]), count, _ratio(count, denominators[ordinal]))
                )
            for ordinal, _, _, _ in batch:
                results[ordinal] = (grouped[ordinal], False)
        return results

    def _collect_unique_string_values_batched(
        self, df: DataFrame, contexts: Sequence[_ProfileContext]
    ) -> Dict[int, Tuple[List[str], Optional[bool], bool]]:
        """Collect normalized string domains in bounded multi-column jobs."""

        results: Dict[int, Tuple[List[str], Optional[bool], bool]] = {
            ordinal: ([], None, False) for ordinal, _, _, _ in contexts
        }
        string_contexts = [
            context
            for context in contexts
            if isinstance(context[1].dataType, T.StringType)
        ]
        if not self.config.collect_unique_string_values:
            for ordinal, _, _, _ in string_contexts:
                results[ordinal] = ([], False, True)
            return results

        limit = self.config.unique_values_max_cardinality
        eligible: List[_ProfileContext] = []
        for context in string_contexts:
            ordinal, _, base, _ = context
            if limit is not None and int(base["distinct_count"]) > limit:
                results[ordinal] = ([], False, True)
            else:
                eligible.append(context)
                results[ordinal] = ([], True, False)

        batch_size = self.config.aggregation_batch_size
        for start in range(0, len(eligible), batch_size):
            batch = eligible[start : start + batch_size]
            entries = [
                F.struct(
                    F.lit(ordinal).cast("integer").alias("ordinal"),
                    self._parsed_expr(source_field, "string", spec)
                    .cast("string")
                    .alias("value"),
                )
                for ordinal, source_field, _, spec in batch
            ]
            if not entries:
                continue
            domain = (
                df.select(F.explode(F.array(*entries)).alias("profile_value"))
                .select("profile_value.*")
                .where(F.col("value").isNotNull())
                .distinct()
            )
            rank_window = Window.partitionBy("ordinal").orderBy(F.asc("value"))
            ranked = domain.withColumn("rank", F.row_number().over(rank_window))
            if limit is not None:
                ranked = ranked.where(F.col("rank") <= limit + 1)
            rows = ranked.orderBy("ordinal", "rank").collect()
            grouped: Dict[int, List[str]] = defaultdict(list)
            for row in rows:
                grouped[int(row["ordinal"])].append(str(row["value"]))
            for ordinal, _, _, _ in batch:
                values = grouped[ordinal]
                if limit is not None and len(values) > limit:
                    results[ordinal] = ([], False, True)
                else:
                    results[ordinal] = (values, True, False)
        return results

    def _collect_invalid_examples_batched(
        self, df: DataFrame, contexts: Sequence[_ProfileContext]
    ) -> Dict[int, List[str]]:
        results: Dict[int, List[str]] = {ordinal: [] for ordinal, _, _, _ in contexts}
        if self.config.invalid_sample_size == 0:
            return results

        eligible = [
            context
            for context in contexts
            if isinstance(context[1].dataType, T.StringType)
            and context[3].inferred_type != "string"
            and int(context[2]["non_missing_count"]) > context[3].inferred_valid_count
        ]
        batch_size = self.config.aggregation_batch_size
        for start in range(0, len(eligible), batch_size):
            batch = eligible[start : start + batch_size]
            entries = [
                F.struct(
                    F.lit(ordinal).cast("integer").alias("ordinal"),
                    self._invalid_value_expression(source_field, spec).alias("value"),
                )
                for ordinal, source_field, _, spec in batch
            ]
            if not entries:
                continue
            invalid_values = (
                df.select(F.explode(F.array(*entries)).alias("invalid_value"))
                .select("invalid_value.*")
                .where(F.col("value").isNotNull())
                .distinct()
            )
            rank_window = Window.partitionBy("ordinal").orderBy(F.asc("value"))
            rows = (
                invalid_values.withColumn("rank", F.row_number().over(rank_window))
                .where(F.col("rank") <= self.config.invalid_sample_size)
                .orderBy("ordinal", "rank")
                .collect()
            )
            for row in rows:
                results[int(row["ordinal"])].append(str(row["value"]))
        return results

    def _invalid_value_expression(
        self, source_field: T.StructField, spec: _ColumnSpec
    ) -> Column:
        raw, _, clean, missing, _, _ = self._text_components(source_field)
        if spec.inferred_type.startswith("integer (>38"):
            valid = clean.rlike(_INTEGER_RE)
        elif spec.inferred_type.startswith("decimal ("):
            valid = clean.rlike(_DECIMAL_RE)
        else:
            valid = self._parsed_expr(
                source_field, spec.inferred_type, spec
            ).isNotNull()
        return F.when((~missing) & (~valid), raw.cast("string"))

    def _profile_row(
        self,
        source_field: T.StructField,
        ordinal: int,
        base: Mapping[str, Any],
        spec: _ColumnSpec,
        detail: Mapping[str, Any],
        top_values: Sequence[Tuple[str, int, Optional[float]]],
        unique_values: Sequence[str],
        unique_values_complete: Optional[bool],
        invalid_examples: Sequence[str],
        flags: Sequence[str],
        row_count: int,
    ) -> Dict[str, Any]:
        non_missing = int(base["non_missing_count"])
        missing = int(base["missing_count"])
        invalid = max(0, non_missing - spec.inferred_valid_count)
        distinct = int(base["distinct_count"])
        mode = top_values[0] if top_values else (None, 0, None)
        percentage = self._percentage_diagnostics(source_field, base)
        return {
            "column_name": source_field.name,
            "ordinal": ordinal,
            "source_type": source_field.dataType.simpleString(),
            "source_nullable": source_field.nullable,
            "inferred_type": spec.inferred_type,
            "suggested_type": spec.suggested_type,
            "semantic_type": spec.semantic_type,
            "best_candidate_type": spec.best_candidate_type,
            "best_candidate_rate": spec.best_candidate_rate,
            "inference_confidence": spec.inference_confidence,
            "observed_formats": spec.observed_formats,
            "row_count": row_count,
            "null_count": int(base["null_count"]),
            "blank_count": int(base["blank_count"]),
            "null_like_count": int(base["null_like_count"]),
            "missing_count": missing,
            "missing_rate": _ratio(missing, row_count),
            "non_missing_count": non_missing,
            "inferred_valid_count": spec.inferred_valid_count,
            "inferred_invalid_count": invalid,
            "parse_success_rate": _ratio(spec.inferred_valid_count, non_missing),
            "distinct_count": distinct,
            "distinct_is_approximate": not self.config.exact_distinct,
            "uniqueness_ratio": _uniqueness_ratio(distinct, non_missing),
            "min_value": detail.get("min_value"),
            "max_value": detail.get("max_value"),
            "q1_value": detail.get("q1_value"),
            "median_value": detail.get("median_value"),
            "q3_value": detail.get("q3_value"),
            "mean": detail.get("mean"),
            "stddev": detail.get("stddev"),
            "min_length": _optional_int(base.get("min_length")),
            "max_length": _optional_int(base.get("max_length")),
            "avg_length": _as_float(base.get("avg_length")),
            "padded_count": int(base["padded_count"]),
            "leading_zero_count": int(base["leading_zero_count"]),
            "nan_count": int(base["nan_count"]),
            "positive_infinity_count": int(base["positive_infinity_count"]),
            "negative_infinity_count": int(base["negative_infinity_count"]),
            "non_finite_count": int(base["non_finite_count"]),
            "observed_decimal_scales": list(base["observed_decimal_scales"]),
            "min_observed_decimal_scale": _optional_int(base.get("min_decimal_scale")),
            "max_observed_decimal_scale": _optional_int(base.get("max_decimal_scale")),
            "max_observed_decimal_precision": _optional_int(
                base.get("max_decimal_precision")
            ),
            "negative_count": int(detail.get("negative_count", 0)),
            "zero_count": int(detail.get("zero_count", 0)),
            "true_count": int(detail.get("true_count", 0)),
            "false_count": int(detail.get("false_count", 0)),
            "mode_value": mode[0],
            "mode_count": mode[1],
            "mode_rate": mode[2],
            "top_values": list(top_values),
            "unique_values": list(unique_values),
            "unique_values_complete": unique_values_complete,
            "invalid_examples": list(invalid_examples),
            "boolean_parse_rate": _ratio(int(base["boolean_count"]), non_missing),
            "integer_parse_rate": _ratio(int(base["integer_count"]), non_missing),
            "decimal_parse_rate": _ratio(int(base["decimal_count"]), non_missing),
            "double_parse_rate": _ratio(int(base["double_count"]), non_missing),
            "date_parse_rate": _ratio(int(base["date_count"]), non_missing),
            "timestamp_parse_rate": _ratio(int(base["timestamp_count"]), non_missing),
            "percentage_name_hint": percentage["name_hint"],
            "potential_percentage_type": percentage["potential_percentage"],
            "percentage_evidence_rate": percentage["evidence_rate"],
            "percentage_symbol_count": int(base["percentage_symbol_count"]),
            "fractional_percentage_scale_count": int(
                base["fractional_percentage_scale_count"]
            ),
            "whole_percentage_scale_count": int(base["whole_percentage_scale_count"]),
            "outside_percentage_range_count": int(
                base["outside_percentage_range_count"]
            ),
            "numeric_range_spans_unit": percentage["range_spans_unit"],
            "mixed_percentage_scale_candidate": percentage["mixed_scale"],
            "percentage_scale_risk": percentage["risk"],
            "quality_flags": list(flags),
            "notes": list(spec.notes),
        }

    def _percentage_diagnostics(
        self,
        source_field: T.StructField,
        base: Mapping[str, Any],
    ) -> Dict[str, Any]:
        non_missing = int(base["non_missing_count"])
        numeric_count = int(base["double_count"])
        symbol_count = int(base["percentage_symbol_count"])
        evidence_rate = _ratio(numeric_count + symbol_count, non_missing) or 0.0
        normalized_name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", source_field.name)
        name_hint = bool(
            _PERCENTAGE_NAME_RE.search(normalized_name)
            and not _NON_PERCENTAGE_AMOUNT_NAME_RE.search(normalized_name)
        )
        potential_percentage = bool(name_hint or symbol_count > 0)
        fractional_count = int(base["fractional_percentage_scale_count"])
        whole_count = int(base["whole_percentage_scale_count"])
        outside_count = int(base["outside_percentage_range_count"])
        has_both_groups = bool(
            fractional_count >= self.config.percentage_min_group_count
            and whole_count >= self.config.percentage_min_group_count
        )
        range_spans_unit = bool(
            evidence_rate >= self.config.percentage_detection_min_numeric_rate
            and has_both_groups
        )
        explicit_symbol_mix = bool(symbol_count > 0 and numeric_count > 0)
        mixed_scale = bool(
            explicit_symbol_mix
            or (name_hint and has_both_groups and outside_count == 0)
        )
        if explicit_symbol_mix:
            risk: Optional[str] = "high"
        elif mixed_scale:
            risk = "high"
        else:
            risk = None
        return {
            "name_hint": name_hint,
            "potential_percentage": potential_percentage,
            "evidence_rate": evidence_rate if non_missing else None,
            "range_spans_unit": range_spans_unit,
            "explicit_symbol_mix": explicit_symbol_mix,
            "mixed_scale": mixed_scale,
            "risk": risk,
        }

    def _quality_flags(
        self,
        source_field: T.StructField,
        base: Mapping[str, Any],
        spec: _ColumnSpec,
        row_count: int,
        top_skipped: bool,
        unique_values_skipped: bool,
    ) -> List[str]:
        flags: List[str] = []
        missing = int(base["missing_count"])
        non_missing = int(base["non_missing_count"])
        invalid = max(0, non_missing - spec.inferred_valid_count)
        distinct = int(base["distinct_count"])
        uniqueness = _uniqueness_ratio(distinct, non_missing)
        percentage = self._percentage_diagnostics(source_field, base)

        if row_count == 0:
            flags.append("empty_dataset")
        elif non_missing == 0:
            flags.append("all_values_missing")
        if row_count and missing / row_count >= self.config.high_missing_rate:
            flags.append("high_missing_rate")
        if base["padded_count"]:
            flags.append("leading_or_trailing_whitespace")
        if invalid:
            flags.append("parse_failures_present")
        if spec.inferred_type == "string" and (spec.best_candidate_rate or 0.0) >= 0.50:
            flags.append("mixed_type_values")
        if non_missing and distinct == 1:
            flags.append("constant_column")
        if non_missing and (uniqueness or 0.0) >= 0.98 and missing == 0:
            flags.append("possible_key_verify_with_exact_distinct")
        if base["leading_zero_count"]:
            flags.append("leading_zero_numeric_strings")
        if base["non_finite_count"]:
            flags.append("non_finite_values_present")
        if (
            spec.inferred_type == "boolean"
            and int(base["boolean_count"]) == non_missing
            and int(base["integer_count"]) == non_missing
            and non_missing > 0
        ):
            flags.append("boolean_integer_ambiguous")
        if self._yyyymmdd_integer_ambiguity(base, non_missing):
            flags.append("ambiguous_yyyymmdd_or_integer")
        if len(base["observed_decimal_scales"]) > 1:
            flags.append("mixed_decimal_scales")
        if percentage["potential_percentage"]:
            flags.append("potential_percentage_column")
        if percentage["range_spans_unit"]:
            flags.append("numeric_range_spans_unit")
        if percentage["mixed_scale"]:
            flags.append("possible_mixed_percentage_scales")
        if percentage["risk"] == "high":
            flags.append("high_risk_mixed_percentage_scales")
        if percentage["explicit_symbol_mix"]:
            flags.append("mixed_percent_symbol_and_numeric_values")
        if (
            percentage["potential_percentage"]
            and base["outside_percentage_range_count"]
        ):
            flags.append("percentage_values_outside_expected_range")
        if spec.semantic_type == "identifier" and spec.suggested_type == "string":
            flags.append("identifier_preserved_as_string")
        if len(spec.observed_formats) > 1:
            flags.append("mixed_date_or_timestamp_formats")
        if top_skipped:
            flags.append("mode_and_top_values_skipped")
        if unique_values_skipped:
            flags.append("unique_string_values_skipped")
        if not self.config.exact_distinct:
            flags.append("distinct_count_is_approximate")
        return flags

    def _text_components(
        self, source_field: T.StructField
    ) -> Tuple[Column, Column, Column, Column, Column, Column]:
        raw = _source_column(source_field.name)
        text = raw.cast("string")
        if not isinstance(source_field.dataType, T.StringType):
            false = F.lit(False)
            return raw, text, text, raw.isNull(), false, false

        clean = F.trim(text) if self.config.trim_strings else text
        blank = raw.isNotNull() & (F.length(F.trim(text)) == 0)

        null_values = tuple(
            value for value in self.config.null_like_values if value != ""
        )
        if self.config.case_sensitive_nulls:
            null_probe = clean
            candidates = null_values
        else:
            null_probe = F.lower(clean)
            candidates = tuple(value.lower() for value in null_values)
        null_like = (
            raw.isNotNull() & (~blank) & null_probe.isin(*candidates)
            if candidates
            else F.lit(False)
        )
        missing = raw.isNull() | blank | null_like
        return raw, text, clean, missing, blank, null_like

    def _parsed_expr(
        self, source_field: T.StructField, target: str, spec: _ColumnSpec
    ) -> Column:
        raw, _, clean, missing, _, _ = self._text_components(source_field)
        if not isinstance(source_field.dataType, T.StringType):
            return F.when(~missing, raw)

        valid_text = F.when(~missing, clean)
        if (
            target == "string"
            or target.startswith("integer (>")
            or target.startswith("decimal (")
        ):
            return valid_text
        if target == "boolean":
            lowered = F.lower(valid_text)
            return (
                F.when(lowered.isin(*_TRUE_VALUES), F.lit(True))
                .when(lowered.isin(*_FALSE_VALUES), F.lit(False))
                .otherwise(F.lit(None).cast("boolean"))
            )
        if target == "date":
            formats = spec.observed_formats or list(self.config.date_formats)
            return _coalesce_or_null(
                [self._date_parser(valid_text, fmt) for fmt in formats], T.DateType()
            )
        if target in ("timestamp", "timestamp_ntz"):
            formats = spec.observed_formats or list(self.config.timestamp_formats)
            return _coalesce_or_null(
                [self._timestamp_parser(valid_text, fmt) for fmt in formats],
                T.TimestampType(),
            )
        if target in ("integer", "int", "bigint", "long"):
            return F.when(valid_text.rlike(_INTEGER_RE), valid_text.cast(target))
        decimal_target = re.fullmatch(r"decimal\(([0-9]+),([0-9]+)\)", target)
        if decimal_target:
            pattern = _INTEGER_RE if int(decimal_target.group(2)) == 0 else _DECIMAL_RE
            return F.when(valid_text.rlike(pattern), valid_text.cast(target))
        if target in ("double", "float"):
            return F.when(valid_text.rlike(_DOUBLE_RE), valid_text.cast(target))
        return valid_text.cast(target)

    @staticmethod
    def _date_parser(value: Column, fmt: str) -> Column:
        parsed = F.try_to_timestamp(value, F.lit(fmt)).cast("date")
        regex = _FORMAT_REGEXES.get(fmt)
        return F.when(value.rlike(regex), parsed) if regex else parsed

    @staticmethod
    def _timestamp_parser(value: Column, fmt: str) -> Column:
        parsed = F.try_to_timestamp(value, F.lit(fmt))
        regex = _FORMAT_REGEXES.get(fmt)
        return F.when(value.rlike(regex), parsed) if regex else parsed

    @staticmethod
    def _spark_type(source_field: T.StructField, target: str) -> T.DataType:
        simple: Dict[str, T.DataType] = {
            "string": T.StringType(),
            "boolean": T.BooleanType(),
            "byte": T.ByteType(),
            "short": T.ShortType(),
            "integer": T.IntegerType(),
            "int": T.IntegerType(),
            "bigint": T.LongType(),
            "long": T.LongType(),
            "float": T.FloatType(),
            "double": T.DoubleType(),
            "date": T.DateType(),
            "timestamp": T.TimestampType(),
        }
        if target == "timestamp_ntz" and hasattr(T, "TimestampNTZType"):
            return T.TimestampNTZType()
        if target in simple:
            return simple[target]
        decimal_match = re.fullmatch(r"decimal\(([0-9]+),([0-9]+)\)", target)
        if decimal_match:
            return T.DecimalType(
                int(decimal_match.group(1)), int(decimal_match.group(2))
            )
        if target == source_field.dataType.simpleString():
            return source_field.dataType
        return T.StringType()


def profile_dataframe(
    df: DataFrame, config: Optional[ProfilerConfig] = None
) -> ProfileResult:
    """Convenience wrapper around :class:`DataFrameProfiler`.

    Example::

        result = profile_dataframe(bronze_df)
        result.profile_df.show(truncate=False)
        print(result.suggested_schema.simpleString())
    """

    return DataFrameProfiler(config).profile(df)


def _profile_schema() -> T.StructType:
    top_value_type = T.StructType(
        [
            T.StructField("value", T.StringType(), False),
            T.StructField("count", T.LongType(), False),
            T.StructField("rate", T.DoubleType(), True),
        ]
    )
    fields: List[Tuple[str, T.DataType, bool]] = [
        ("column_name", T.StringType(), False),
        ("ordinal", T.IntegerType(), False),
        ("source_type", T.StringType(), False),
        ("source_nullable", T.BooleanType(), False),
        ("inferred_type", T.StringType(), False),
        ("suggested_type", T.StringType(), False),
        ("semantic_type", T.StringType(), False),
        ("best_candidate_type", T.StringType(), True),
        ("best_candidate_rate", T.DoubleType(), True),
        ("inference_confidence", T.DoubleType(), True),
        ("observed_formats", T.ArrayType(T.StringType(), False), False),
        ("row_count", T.LongType(), False),
        ("null_count", T.LongType(), False),
        ("blank_count", T.LongType(), False),
        ("null_like_count", T.LongType(), False),
        ("missing_count", T.LongType(), False),
        ("missing_rate", T.DoubleType(), True),
        ("non_missing_count", T.LongType(), False),
        ("inferred_valid_count", T.LongType(), False),
        ("inferred_invalid_count", T.LongType(), False),
        ("parse_success_rate", T.DoubleType(), True),
        ("distinct_count", T.LongType(), False),
        ("distinct_is_approximate", T.BooleanType(), False),
        ("uniqueness_ratio", T.DoubleType(), True),
        ("min_value", T.StringType(), True),
        ("max_value", T.StringType(), True),
        ("q1_value", T.StringType(), True),
        ("median_value", T.StringType(), True),
        ("q3_value", T.StringType(), True),
        ("mean", T.DoubleType(), True),
        ("stddev", T.DoubleType(), True),
        ("min_length", T.IntegerType(), True),
        ("max_length", T.IntegerType(), True),
        ("avg_length", T.DoubleType(), True),
        ("padded_count", T.LongType(), False),
        ("leading_zero_count", T.LongType(), False),
        ("nan_count", T.LongType(), False),
        ("positive_infinity_count", T.LongType(), False),
        ("negative_infinity_count", T.LongType(), False),
        ("non_finite_count", T.LongType(), False),
        ("observed_decimal_scales", T.ArrayType(T.IntegerType(), False), False),
        ("min_observed_decimal_scale", T.IntegerType(), True),
        ("max_observed_decimal_scale", T.IntegerType(), True),
        ("max_observed_decimal_precision", T.IntegerType(), True),
        ("negative_count", T.LongType(), False),
        ("zero_count", T.LongType(), False),
        ("true_count", T.LongType(), False),
        ("false_count", T.LongType(), False),
        ("mode_value", T.StringType(), True),
        ("mode_count", T.LongType(), False),
        ("mode_rate", T.DoubleType(), True),
        ("top_values", T.ArrayType(top_value_type, False), False),
        ("unique_values", T.ArrayType(T.StringType(), False), False),
        ("unique_values_complete", T.BooleanType(), True),
        ("invalid_examples", T.ArrayType(T.StringType(), False), False),
        ("boolean_parse_rate", T.DoubleType(), True),
        ("integer_parse_rate", T.DoubleType(), True),
        ("decimal_parse_rate", T.DoubleType(), True),
        ("double_parse_rate", T.DoubleType(), True),
        ("date_parse_rate", T.DoubleType(), True),
        ("timestamp_parse_rate", T.DoubleType(), True),
        ("percentage_name_hint", T.BooleanType(), False),
        ("potential_percentage_type", T.BooleanType(), False),
        ("percentage_evidence_rate", T.DoubleType(), True),
        ("percentage_symbol_count", T.LongType(), False),
        ("fractional_percentage_scale_count", T.LongType(), False),
        ("whole_percentage_scale_count", T.LongType(), False),
        ("outside_percentage_range_count", T.LongType(), False),
        ("numeric_range_spans_unit", T.BooleanType(), False),
        ("mixed_percentage_scale_candidate", T.BooleanType(), False),
        ("percentage_scale_risk", T.StringType(), True),
        ("quality_flags", T.ArrayType(T.StringType(), False), False),
        ("notes", T.ArrayType(T.StringType(), False), False),
    ]
    return T.StructType(
        [
            T.StructField(name, data_type, nullable)
            for name, data_type, nullable in fields
        ]
    )


def _count_when(predicate: Column) -> Column:
    return F.sum(F.when(predicate, F.lit(1)).otherwise(F.lit(0))).cast("long")


def _aggregate_literal(value: Any, data_type: Any) -> Column:
    return F.first(F.lit(value).cast(data_type), ignorenulls=False)


def _coalesce_or_null(expressions: Sequence[Column], data_type: T.DataType) -> Column:
    if expressions:
        return F.coalesce(*expressions)
    return F.lit(None).cast(data_type)


def _quote_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _source_column(name: str) -> Column:
    return F.col(_quote_identifier(name))


def _ratio(numerator: int, denominator: int) -> Optional[float]:
    return numerator / denominator if denominator else None


def _uniqueness_ratio(distinct: int, non_missing: int) -> Optional[float]:
    ratio = _ratio(distinct, non_missing)
    return min(1.0, ratio) if ratio is not None else None


def _as_int(value: Any) -> int:
    return int(value) if value is not None else 0


def _optional_int(value: Any) -> Optional[int]:
    return int(value) if value is not None else None


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _stringify(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, float):
        if not math.isfinite(value):
            return str(value)
        return format(value, ".15g")
    return str(value)


def _format_timestamp_micros(value: Any, target: str) -> Optional[str]:
    if value is None:
        return None
    microseconds = int(value)
    if target == "timestamp_ntz":
        epoch = dt.datetime(1970, 1, 1)
        return (epoch + dt.timedelta(microseconds=microseconds)).isoformat()
    timestamp = dt.datetime.fromtimestamp(microseconds / 1_000_000, tz=dt.timezone.utc)
    return timestamp.isoformat().replace("+00:00", "Z")


def _boolean_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    return "true" if int(value) else "false"


def _is_numeric_type(type_name: str) -> bool:
    return bool(
        type_name
        in {
            "byte",
            "tinyint",
            "short",
            "smallint",
            "integer",
            "int",
            "bigint",
            "long",
            "float",
            "double",
        }
        or type_name.startswith("decimal(")
        or type_name.startswith("integer (>")
        or type_name.startswith("decimal (")
    )


__all__ = [
    "DataFrameProfiler",
    "ProfileResult",
    "ProfilerConfig",
    "profile_dataframe",
]
