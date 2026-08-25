"""Scalable, bronze-to-silver profiling for PySpark DataFrames.

The public entry point is :func:`profile_dataframe`.  It returns a
``ProfileResult`` containing a Spark DataFrame with one row per input column,
a suggested ``StructType``, and SQL expressions that can be used in a silver
select.

The profiler never collects source rows to the driver.  It only collects
aggregate results, the configured number of top values, and a small sample of
invalid values.  Exact top values/modes still require a shuffle per profiled
column and can be disabled through ``ProfilerConfig``.
"""

from __future__ import annotations

import datetime as dt
import math
import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

try:
    from pyspark import StorageLevel
    from pyspark.sql import Column, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
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
    "yyyy-MM-dd HH:mm:ss": (
        r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$"
    ),
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
    aggregation_batch_size: int = 40
    high_missing_rate: float = 0.20
    categorical_max_distinct: int = 50
    preserve_identifier_strings: bool = True
    cache_input: bool = True
    safe_cast_sql_function: str = "TRY_CAST"

    def __post_init__(self) -> None:
        if not 0.0 < self.inference_threshold <= 1.0:
            raise ValueError("inference_threshold must be in (0, 1]")
        if not 0.0 < self.approx_distinct_rsd < 1.0:
            raise ValueError("approx_distinct_rsd must be in (0, 1)")
        if self.percentile_accuracy < 1:
            raise ValueError("percentile_accuracy must be positive")
        if self.top_n < 1:
            raise ValueError("top_n must be positive")
        if (
            self.unique_values_max_cardinality is not None
            and self.unique_values_max_cardinality < 1
        ):
            raise ValueError("unique_values_max_cardinality must be positive or None")
        if self.invalid_sample_size < 0:
            raise ValueError("invalid_sample_size cannot be negative")
        if self.aggregation_batch_size < 1:
            raise ValueError("aggregation_batch_size must be positive")
        if not 0.0 <= self.high_missing_rate <= 1.0:
            raise ValueError("high_missing_rate must be in [0, 1]")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.safe_cast_sql_function):
            raise ValueError("safe_cast_sql_function must be a SQL function name")


@dataclass
class ProfileResult:
    """Artifacts produced by a profiling run."""

    profile_df: DataFrame
    suggested_schema: T.StructType
    silver_expressions: Mapping[str, str]
    quarantine_predicates: Mapping[str, str]

    def silver_select_sql(self, source: str) -> str:
        """Return a complete SELECT that applies every suggested conversion."""

        projections = [
            f"  {expression} AS {_quote_identifier(name)}"
            for name, expression in self.silver_expressions.items()
        ]
        return "SELECT\n" + ",\n".join(projections) + f"\nFROM {source}"

    def schema_ddl(self) -> str:
        """Return the suggested schema as a comma-separated DDL fragment."""

        return ",\n".join(
            f"  {_quote_identifier(field.name)} {field.dataType.simpleString().upper()}"
            for field in self.suggested_schema.fields
        )


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


class DataFrameProfiler:
    """Profile a Spark DataFrame for bronze-to-silver promotion decisions."""

    def __init__(self, config: Optional[ProfilerConfig] = None) -> None:
        self.config = config or ProfilerConfig()

    def profile(self, df: DataFrame) -> ProfileResult:
        """Profile ``df`` and return aggregate metrics and silver artifacts."""

        if not isinstance(df, DataFrame):
            raise TypeError("df must be a pyspark.sql.DataFrame")

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
            expressions: Dict[str, str] = {}
            quarantine: Dict[str, str] = {}

            for ordinal, source_field in enumerate(work_df.schema.fields):
                base = base_stats[source_field.name]
                spec = self._infer_column(source_field, base, row_count)
                detail = self._collect_detail(work_df, source_field, spec)
                top_values, top_skipped = self._collect_top_values(
                    work_df, source_field, spec, base
                )
                (
                    unique_values,
                    unique_values_complete,
                    unique_values_skipped,
                ) = self._collect_unique_string_values(
                    work_df, source_field, spec, base
                )
                invalid_examples = self._collect_invalid_examples(
                    work_df, source_field, spec
                )
                silver_expression = self._silver_expression(source_field, spec)
                invalid_predicate = (
                    f"{self._clean_sql(source_field.name)} IS NOT NULL AND "
                    f"({silver_expression}) IS NULL"
                    if spec.suggested_type != "string"
                    else "FALSE"
                )
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
                        silver_expression,
                        invalid_predicate,
                        flags,
                        row_count,
                    )
                )
                expressions[source_field.name] = silver_expression
                quarantine[source_field.name] = invalid_predicate
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
                silver_expressions=expressions,
                quarantine_predicates=quarantine,
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
                "max_integer_digits",
                "max_decimal_integer_digits",
                "max_decimal_scale",
            ):
                values[metric] = _as_int(values.get(metric))
            for index, _ in enumerate(self.config.date_formats):
                metric = f"date_format_{index}_count"
                values[metric] = _as_int(values.get(metric))
            for index, _ in enumerate(self.config.timestamp_formats):
                metric = f"timestamp_format_{index}_count"
                values[metric] = _as_int(values.get(metric))
        return results

    def _base_metric_expressions(self, source_field: T.StructField) -> Dict[str, Column]:
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
        integer_digits = F.when(
            F.length(integer_digits_text) == 0, F.lit(1)
        ).otherwise(F.length(integer_digits_text))
        integer_part = F.regexp_extract(signless, r"^([0-9]*)", 1)
        integer_part_no_zeros = F.regexp_replace(integer_part, r"^0+", "")
        fractional_part = F.regexp_extract(signless, r"\.([0-9]+)$", 1)
        decimal_integer_digits = F.when(
            F.length(integer_part_no_zeros) == 0,
            F.when(F.length(fractional_part) > 0, F.lit(0)).otherwise(F.lit(1)),
        ).otherwise(F.length(integer_part_no_zeros))
        decimal_scale = F.length(fractional_part)

        date_parsers = [self._date_parser(valid_text, fmt) for fmt in self.config.date_formats]
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

        metrics: Dict[str, Column] = {
            "null_count": _count_when(raw.isNull()),
            "blank_count": _count_when(blank),
            "null_like_count": _count_when(null_like),
            "missing_count": _count_when(missing),
            "non_missing_count": _count_when(~missing),
            "padded_count": _count_when(
                raw.isNotNull() & (text != F.trim(text))
            ),
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
            "max_integer_digits": F.max(F.when(integer_match, integer_digits)),
            "max_decimal_integer_digits": F.max(
                F.when(decimal_match, decimal_integer_digits)
            ),
            "max_decimal_scale": F.max(F.when(decimal_match, decimal_scale)),
        }
        for index, parser in enumerate(date_parsers):
            metrics[f"date_format_{index}_count"] = _count_when(parser.isNotNull())
        for index, parser in enumerate(timestamp_parsers):
            metrics[f"timestamp_format_{index}_count"] = _count_when(
                parser.isNotNull()
            )
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

        accepted = (
            best_candidate
            if best_rate >= self.config.inference_threshold
            else None
        )
        notes: List[str] = []
        observed_formats: List[str] = []

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
        return [fmt for fmt, count in sorted(counts, key=lambda item: -item[1]) if count]

    def _collect_detail(
        self, df: DataFrame, source_field: T.StructField, spec: _ColumnSpec
    ) -> Dict[str, Any]:
        parsed = self._parsed_expr(source_field, spec.suggested_type, spec)
        target = spec.suggested_type
        metrics: List[Column] = []

        numeric = _is_numeric_type(target)
        boolean = target == "boolean"
        date_type = target == "date"
        timestamp_type = target in ("timestamp", "timestamp_ntz")
        orderable = numeric or date_type or timestamp_type or target == "string"

        if boolean:
            metrics.extend(
                [
                    F.min(parsed.cast("integer")).alias("min_value"),
                    F.max(parsed.cast("integer")).alias("max_value"),
                ]
            )
        elif orderable:
            metrics.extend(
                [F.min(parsed).alias("min_value"), F.max(parsed).alias("max_value")]
            )
        else:
            metrics.extend(
                [
                    F.lit(None).cast("string").alias("min_value"),
                    F.lit(None).cast("string").alias("max_value"),
                ]
            )

        quantile_source: Optional[Column] = None
        if numeric:
            quantile_source = parsed
            metrics.extend(
                [
                    F.avg(parsed).alias("mean"),
                    F.stddev_samp(parsed).alias("stddev"),
                    _count_when(parsed < 0).alias("negative_count"),
                    _count_when(parsed == 0).alias("zero_count"),
                ]
            )
        elif boolean:
            numeric_boolean = parsed.cast("integer")
            quantile_source = numeric_boolean
            metrics.extend(
                [
                    F.avg(numeric_boolean).alias("mean"),
                    F.stddev_samp(numeric_boolean).alias("stddev"),
                    F.lit(0).cast("long").alias("negative_count"),
                    _count_when(~parsed).alias("zero_count"),
                ]
            )
        elif date_type:
            quantile_source = F.datediff(parsed, F.lit("1970-01-01"))
            metrics.extend(self._empty_numeric_metrics())
        elif timestamp_type:
            quantile_source = parsed.cast("double")
            metrics.extend(self._empty_numeric_metrics())
        else:
            metrics.extend(self._empty_numeric_metrics())

        if quantile_source is not None:
            metrics.append(
                F.percentile_approx(
                    quantile_source,
                    [0.25, 0.5, 0.75],
                    self.config.percentile_accuracy,
                ).alias("quantiles")
            )
        else:
            metrics.append(
                F.lit(None).cast(T.ArrayType(T.DoubleType())).alias("quantiles")
            )

        if boolean:
            metrics.extend(
                [
                    _count_when(parsed).alias("true_count"),
                    _count_when(~parsed).alias("false_count"),
                ]
            )
        else:
            metrics.extend(
                [
                    F.lit(0).cast("long").alias("true_count"),
                    F.lit(0).cast("long").alias("false_count"),
                ]
            )

        values = df.agg(*metrics).first().asDict()
        quantiles = values.pop("quantiles", None)
        if boolean:
            values["min_value"] = _boolean_string(values.get("min_value"))
            values["max_value"] = _boolean_string(values.get("max_value"))
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
    def _empty_numeric_metrics() -> List[Column]:
        return [
            F.lit(None).cast("double").alias("mean"),
            F.lit(None).cast("double").alias("stddev"),
            F.lit(0).cast("long").alias("negative_count"),
            F.lit(0).cast("long").alias("zero_count"),
        ]

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
                dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
                if value is not None
                else None
                for value in quantiles
            )
        if target == "boolean":
            return tuple(  # type: ignore[return-value]
                "true" if value is not None and float(value) >= 1.0 else "false"
                if value is not None
                else None
                for value in quantiles
            )
        return tuple(_stringify(value) for value in quantiles)  # type: ignore[return-value]

    def _collect_top_values(
        self,
        df: DataFrame,
        source_field: T.StructField,
        spec: _ColumnSpec,
        base: Mapping[str, Any],
    ) -> Tuple[List[Tuple[str, int, Optional[float]]], bool]:
        if not self.config.calculate_top_values:
            return [], True
        threshold = self.config.top_values_max_cardinality
        if threshold is not None and int(base["distinct_count"]) > threshold:
            return [], True

        parsed = self._parsed_expr(source_field, spec.suggested_type, spec)
        value = parsed.cast("string").alias("_profile_value")
        counts = (
            df.select(value)
            .where(F.col("_profile_value").isNotNull())
            .groupBy("_profile_value")
            .count()
            .orderBy(F.desc("count"), F.asc("_profile_value"))
            .limit(self.config.top_n)
            .collect()
        )
        denominator = int(base["non_missing_count"])
        return [
            (
                str(row["_profile_value"]),
                int(row["count"]),
                _ratio(int(row["count"]), denominator),
            )
            for row in counts
        ], False

    def _collect_unique_string_values(
        self,
        df: DataFrame,
        source_field: T.StructField,
        spec: _ColumnSpec,
        base: Mapping[str, Any],
    ) -> Tuple[List[str], Optional[bool], bool]:
        """Collect the complete normalized domain for bounded string columns."""

        if not isinstance(source_field.dataType, T.StringType):
            return [], None, False
        if not self.config.collect_unique_string_values:
            return [], False, True

        limit = self.config.unique_values_max_cardinality
        # This inexpensive approximate precheck avoids a distinct shuffle for obvious
        # IDs/free text. A near-boundary approximate overcount can conservatively skip.
        if limit is not None and int(base["distinct_count"]) > limit:
            return [], False, True

        parsed = self._parsed_expr(source_field, "string", spec)
        value = parsed.cast("string").alias("_profile_unique_value")
        domain = (
            df.select(value)
            .where(F.col("_profile_unique_value").isNotNull())
            .distinct()
            .orderBy("_profile_unique_value")
        )
        rows = (
            domain.limit(limit + 1).collect()
            if limit is not None
            else domain.collect()
        )
        if limit is not None and len(rows) > limit:
            return [], False, True
        return [str(row["_profile_unique_value"]) for row in rows], True, False

    def _collect_invalid_examples(
        self, df: DataFrame, source_field: T.StructField, spec: _ColumnSpec
    ) -> List[str]:
        missing = self._text_components(source_field)[3]
        if (
            self.config.invalid_sample_size == 0
            or spec.inferred_type == "string"
            or not isinstance(source_field.dataType, T.StringType)
        ):
            return []
        parsed = self._parsed_expr(source_field, spec.inferred_type, spec)
        raw_text = F.col(source_field.name).cast("string")
        rows = (
            df.where((~missing) & parsed.isNull())
            .select(raw_text.alias("value"))
            .distinct()
            .orderBy("value")
            .limit(self.config.invalid_sample_size)
            .collect()
        )
        return [str(row["value"]) for row in rows]

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
        silver_expression: str,
        invalid_predicate: str,
        flags: Sequence[str],
        row_count: int,
    ) -> Dict[str, Any]:
        non_missing = int(base["non_missing_count"])
        missing = int(base["missing_count"])
        invalid = max(0, non_missing - spec.inferred_valid_count)
        distinct = int(base["distinct_count"])
        mode = top_values[0] if top_values else (None, 0, None)
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
            "timestamp_parse_rate": _ratio(
                int(base["timestamp_count"]), non_missing
            ),
            "silver_expression": silver_expression,
            "quarantine_predicate": invalid_predicate,
            "quality_flags": list(flags),
            "notes": list(spec.notes),
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
        raw = F.col(source_field.name)
        text = raw.cast("string")
        clean = F.trim(text) if self.config.trim_strings else text
        blank = raw.isNotNull() & (F.length(F.trim(text)) == 0)

        null_values = tuple(value for value in self.config.null_like_values if value != "")
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
        if target == "string" or target.startswith("integer (>") or target.startswith(
            "decimal ("
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
        return valid_text.cast(target)

    @staticmethod
    def _date_parser(value: Column, fmt: str) -> Column:
        if hasattr(F, "try_to_timestamp"):
            parsed = F.try_to_timestamp(value, F.lit(fmt)).cast("date")
        else:  # Spark < 3.5
            parsed = F.to_date(value, fmt)
        regex = _FORMAT_REGEXES.get(fmt)
        return F.when(value.rlike(regex), parsed) if regex else parsed

    @staticmethod
    def _timestamp_parser(value: Column, fmt: str) -> Column:
        if hasattr(F, "try_to_timestamp"):
            parsed = F.try_to_timestamp(value, F.lit(fmt))
        else:  # Spark < 3.5
            parsed = F.to_timestamp(value, fmt)
        regex = _FORMAT_REGEXES.get(fmt)
        return F.when(value.rlike(regex), parsed) if regex else parsed

    def _silver_expression(
        self, source_field: T.StructField, spec: _ColumnSpec
    ) -> str:
        if not isinstance(source_field.dataType, T.StringType):
            return _quote_identifier(source_field.name)
        clean = self._clean_sql(source_field.name)
        target = spec.suggested_type
        if target == "string":
            return clean
        if target == "boolean":
            true_values = ", ".join(_sql_literal(value) for value in _TRUE_VALUES)
            false_values = ", ".join(_sql_literal(value) for value in _FALSE_VALUES)
            return (
                f"CASE WHEN LOWER({clean}) IN ({true_values}) THEN TRUE "
                f"WHEN LOWER({clean}) IN ({false_values}) THEN FALSE ELSE NULL END"
            )
        if target == "date":
            formats = spec.observed_formats or list(self.config.date_formats)
            conversions = [
                f"CAST(TRY_TO_TIMESTAMP({clean}, {_sql_literal(fmt)}) AS DATE)"
                for fmt in formats
            ]
            return "COALESCE(" + ", ".join(conversions) + ")"
        if target in ("timestamp", "timestamp_ntz"):
            formats = spec.observed_formats or list(self.config.timestamp_formats)
            conversions = [
                f"TRY_TO_TIMESTAMP({clean}, {_sql_literal(fmt)})" for fmt in formats
            ]
            return "COALESCE(" + ", ".join(conversions) + ")"
        return f"{self.config.safe_cast_sql_function}({clean} AS {target.upper()})"

    def _clean_sql(self, column_name: str) -> str:
        raw = f"CAST({_quote_identifier(column_name)} AS STRING)"
        clean = f"TRIM({raw})" if self.config.trim_strings else raw
        blank_check = f"TRIM({raw}) = ''"
        values = tuple(value for value in self.config.null_like_values if value != "")
        if values:
            if self.config.case_sensitive_nulls:
                probe = clean
                candidates = values
            else:
                probe = f"LOWER({clean})"
                candidates = tuple(value.lower() for value in values)
            values_sql = ", ".join(_sql_literal(value) for value in candidates)
            missing = f"{raw} IS NULL OR {blank_check} OR {probe} IN ({values_sql})"
        else:
            missing = f"{raw} IS NULL OR {blank_check}"
        return f"CASE WHEN {missing} THEN NULL ELSE {clean} END"

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
        print(result.silver_select_sql("bronze.customer"))
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
        ("silver_expression", T.StringType(), False),
        ("quarantine_predicate", T.StringType(), False),
        ("quality_flags", T.ArrayType(T.StringType(), False), False),
        ("notes", T.ArrayType(T.StringType(), False), False),
    ]
    return T.StructType(
        [T.StructField(name, data_type, nullable) for name, data_type, nullable in fields]
    )


def _count_when(predicate: Column) -> Column:
    return F.sum(F.when(predicate, F.lit(1)).otherwise(F.lit(0))).cast("long")


def _coalesce_or_null(expressions: Sequence[Column], data_type: T.DataType) -> Column:
    if expressions:
        return F.coalesce(*expressions)
    return F.lit(None).cast(data_type)


def _quote_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


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
