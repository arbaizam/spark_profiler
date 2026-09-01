# Spark bronze-to-silver data profiler

`spark_data_profiler.py` profiles a PySpark DataFrame and returns one aggregate
row per source column. It is designed for raw bronze tables where most or all
columns arrived as strings and the next job must choose safe silver types.
It is metadata-only: it does not generate SQL or mutate the source DataFrame.
Apache Spark 3.5 or newer is required. Both ANSI and non-ANSI sessions are
supported.

## What it reports

- Likely and suggested Spark types, with a confidence score
- Separate counts for real nulls, blanks, and configurable null-like strings
- Parse success/failure counts and a few invalid examples
- Min, max, quartiles, median, mean, standard deviation, zero, and negative counts
- Exact mode and top values (configurable because these require shuffles)
- A complete sorted `unique_values` list for source string columns whose
  domain fits under a configurable safety cap
- Approximate or exact distinct count, uniqueness ratio, and possible-key flags
- String length and leading/trailing whitespace diagnostics
- Separate NaN and positive/negative infinity counts for typed floating columns
- Boolean, integer, decimal, double, date, and timestamp candidate parse rates
- Observed decimal precision and every scale found in the source values
- Potential percentage detection and mixed-unit warnings for columns containing
  both fractional forms such as `0.04` and whole-percent forms such as `4.0`
- Observed date/timestamp formats, including mixed-format warnings
- Leading-zero and identifier detection so values such as `00123` are not
  accidentally promoted to numbers
- A suggested `StructType` for downstream schema planning

Min/max for a suggested string column are lexical. Quartiles and median are
reported only when they are meaningful (numeric, boolean, date, or timestamp).
Timestamp values are rendered in UTC with a `Z` suffix; `timestamp_ntz` values
remain timezone-free wall-clock values.

## Quick start

```python
from spark_data_profiler import ProfilerConfig, profile_dataframe

config = ProfilerConfig(
    inference_threshold=0.98,  # at least 98% of non-missing values must parse
    top_n=5,
    exact_distinct=False,
)

result = profile_dataframe(bronze_df, config)

# One row per source column
result.profile_df.select(
    "column_name",
    "inferred_type",
    "suggested_type",
    "parse_success_rate",
    "null_count",
    "blank_count",
    "min_value",
    "max_value",
    "median_value",
    "mode_value",
    "observed_decimal_scales",
    "mixed_percentage_scale_candidate",
    "percentage_scale_risk",
    "quality_flags",
).show(truncate=False)

print(result.suggested_schema.simpleString())
```

## Return values

`profile_dataframe` returns a `ProfileResult` with these attributes:

| Attribute | Type | Definition |
|---|---|---|
| `profile_df` | PySpark `DataFrame` | One row per source column containing all profiling metadata described below. |
| `suggested_schema` | PySpark `StructType` | A schema assembled from each column's `suggested_type`. This is a recommendation only and does not alter the source DataFrame. |

All rates range from 0 to 1. Missing values are excluded from type-parse rate
denominators. Value statistics use the suggested parsed type; consequently,
min/max for a suggested string are lexical. Values are represented as strings
for min/max/quantile fields so one stable profile schema can hold numbers,
dates, timestamps, booleans, and strings.

| `profile_df` column | Spark type | Definition |
|---|---|---|
| `column_name` | `string` | Source column name. |
| `ordinal` | `int` | Zero-based position of the column in the source schema. |
| `source_type` | `string` | Source Spark data type in `simpleString()` form. |
| `source_nullable` | `boolean` | Nullable flag from the source `StructField`; this describes the schema rather than observed nulls. |
| `inferred_type` | `string` | Most specific type whose parse rate reaches `inference_threshold`. Falls back to `string`; may remain numeric when identifier safeguards make the suggestion a string. |
| `suggested_type` | `string` | Recommended Spark type after precision limits and identifier-preservation safeguards are applied. |
| `semantic_type` | `string` | Broad classification: `empty`, `identifier`, `boolean`, `date/time`, `numeric`, `categorical`, `free_text`, or `string`. |
| `best_candidate_type` | `string` | Highest-scoring candidate among boolean, timestamp, date, integer, decimal, and double, even when it does not reach the inference threshold. For an already typed source it is the source type. Null when there is no evidence. |
| `best_candidate_rate` | `double` | Fraction of non-missing values matching `best_candidate_type`; null when there are no non-missing values or no candidate matches. |
| `inference_confidence` | `double` | Parse rate supporting `inferred_type`. A string fallback reports 1 because every non-missing value is valid as a string; use `best_candidate_rate` to assess borderline typed candidates. |
| `observed_formats` | `array<string>` | Matching date or timestamp patterns, ordered by match count. Empty for other inferred types. |
| `row_count` | `bigint` | Total source DataFrame row count, repeated for each column. |
| `null_count` | `bigint` | Count of actual Spark nulls. |
| `blank_count` | `bigint` | Count of non-null strings that are empty after trimming. |
| `null_like_count` | `bigint` | Count of non-null, non-blank source strings matching configured null sentinels such as `null`, `n/a`, or `missing`. Null-sentinel normalization is not applied to already typed columns. |
| `missing_count` | `bigint` | Combined actual-null, blank, and null-like count; the component categories are mutually exclusive. |
| `missing_rate` | `double` | `missing_count / row_count`; null for an empty DataFrame. |
| `non_missing_count` | `bigint` | Values remaining after actual nulls, blanks, and null-like sentinels are excluded. |
| `inferred_valid_count` | `bigint` | Non-missing values valid for `inferred_type`. For inferred strings, every non-missing value is valid. |
| `inferred_invalid_count` | `bigint` | `non_missing_count - inferred_valid_count`. |
| `parse_success_rate` | `double` | `inferred_valid_count / non_missing_count`; null when the column has no non-missing values. |
| `distinct_count` | `bigint` | Distinct normalized non-missing values. Approximate by default and exact when `exact_distinct=True`. |
| `distinct_is_approximate` | `boolean` | Whether `distinct_count` was calculated with `approx_count_distinct`. |
| `uniqueness_ratio` | `double` | `distinct_count / non_missing_count`, capped at 1 to account for approximate-count overestimation; null with no non-missing values. |
| `min_value` | `string` | Minimum suggested-type value. String minima are lexical. Timestamps use UTC with `Z`; `timestamp_ntz` values are timezone-free. Null when unsupported or no valid values exist. |
| `max_value` | `string` | Maximum suggested-type value, with the same rendering convention as `min_value`. String maxima are lexical. Null when unsupported or no valid values exist. |
| `q1_value` | `string` | Approximate 25th percentile for numeric, boolean, date, timestamp, or `timestamp_ntz` suggestions. Null for strings and unsupported types. |
| `median_value` | `string` | Approximate 50th percentile, rendered with the same type and timezone convention as min/max. |
| `q3_value` | `string` | Approximate 75th percentile, rendered with the same type and timezone convention as min/max. |
| `mean` | `double` | Arithmetic mean for numeric suggestions, calculated in double precision to avoid Spark decimal-average overflow. For booleans, the true fraction after encoding false as 0 and true as 1. Null for a non-finite result. |
| `stddev` | `double` | Sample standard deviation for numeric or boolean suggestions; null when insufficient values exist. |
| `min_length` | `int` | Minimum character length of normalized non-missing values. |
| `max_length` | `int` | Maximum character length of normalized non-missing values. |
| `avg_length` | `double` | Average character length of normalized non-missing values. |
| `padded_count` | `bigint` | Non-null values whose string representation has leading or trailing whitespace. |
| `leading_zero_count` | `bigint` | Integer-looking values with more than one digit whose unsigned representation begins with zero. |
| `nan_count` | `bigint` | Count of IEEE NaN values in an already typed `float` or `double` source column. NaN is not treated as missing. |
| `positive_infinity_count` | `bigint` | Count of positive infinity values in an already typed `float` or `double` source column. |
| `negative_infinity_count` | `bigint` | Count of negative infinity values in an already typed `float` or `double` source column. |
| `non_finite_count` | `bigint` | Combined NaN, positive-infinity, and negative-infinity count for typed floating columns. |
| `observed_decimal_scales` | `array<int>` | Sorted set of scales found in fixed-point source values. Scale is the number of digits appearing after the decimal point, so `4`, `4.0`, and `4.00` have scales 0, 1, and 2. |
| `min_observed_decimal_scale` | `int` | Smallest observed fixed-point scale; null when no fixed-point values were found. |
| `max_observed_decimal_scale` | `int` | Largest observed fixed-point scale; null when no fixed-point values were found. |
| `max_observed_decimal_precision` | `int` | Largest per-value observed fixed-point precision: significant integral digits plus displayed scale, with a minimum precision of 1. This can be smaller than the conservative precision in `suggested_type`. |
| `negative_count` | `bigint` | Valid suggested numeric values below zero. Zero for non-numeric suggestions. |
| `zero_count` | `bigint` | Valid suggested numeric values equal to zero. For booleans, this is also the false count. |
| `true_count` | `bigint` | Valid values parsed as true for a boolean suggestion; otherwise zero. |
| `false_count` | `bigint` | Valid values parsed as false for a boolean suggestion; otherwise zero. |
| `mode_value` | `string` | Most frequent valid suggested-type value. Ties are resolved lexically. Null when top-value calculation is skipped or no valid value exists. |
| `mode_count` | `bigint` | Occurrence count for `mode_value`; zero when unavailable. |
| `mode_rate` | `double` | `mode_count / non_missing_count`; null when the mode is unavailable or there are no non-missing values. |
| `top_values` | `array<struct<value:string,count:bigint,rate:double>>` | Up to `top_n` exact frequency-ranked valid suggested-type values. Each rate uses `non_missing_count` as its denominator. Empty when disabled, over the cardinality cap, or no valid values exist. |
| `unique_values` | `array<string>` | Complete sorted set of normalized non-missing source values for a source string column when it fits under `unique_values_max_cardinality`. Empty for non-string sources or when skipped. |
| `unique_values_complete` | `boolean` | True when `unique_values` is the complete domain; false when collection was disabled or exceeded the cap; null for non-string source columns. |
| `invalid_examples` | `array<string>` | Up to `invalid_sample_size` distinct source strings that failed the inferred non-string parse, sorted lexically. Empty for inferred strings, already typed sources, no failures, or disabled sampling. |
| `boolean_parse_rate` | `double` | Fraction of non-missing values matching accepted boolean tokens: `true`, `t`, `yes`, `y`, `1`, `false`, `f`, `no`, `n`, or `0`; null with no non-missing values. |
| `integer_parse_rate` | `double` | Fraction of non-missing values matching signed integer syntax. |
| `decimal_parse_rate` | `double` | Fraction of non-missing values matching fixed-point decimal syntax, including integer text but excluding exponent notation. |
| `double_parse_rate` | `double` | Fraction of non-missing values matching decimal or scientific-notation numeric syntax. |
| `date_parse_rate` | `double` | Fraction of non-missing values parsed by at least one configured date format. |
| `timestamp_parse_rate` | `double` | Fraction of non-missing values parsed by at least one configured timestamp format. |
| `percentage_name_hint` | `boolean` | Whether the column name contains a percentage-like token such as `pct`, `percent`, `rate`, `ratio`, `margin`, or `yield`. Snake case and camel case are recognized. |
| `potential_percentage_type` | `boolean` | True when the name has a percentage hint or at least one value uses a numeric `%` suffix. It does not prove percentage semantics. |
| `percentage_evidence_rate` | `double` | Fraction of non-missing values that are either ordinary numeric strings or numeric strings ending in `%`; null with no non-missing values. |
| `percentage_symbol_count` | `bigint` | Count of non-missing numeric-looking strings ending in `%`, allowing whitespace immediately before the symbol. |
| `fractional_percentage_scale_count` | `bigint` | Numeric values with absolute magnitude greater than 0 and less than 1—the common fractional representation of percentages. |
| `whole_percentage_scale_count` | `bigint` | Numeric values with absolute magnitude greater than 1 and at most 100—the common whole-percent representation. Exactly 1 is excluded as ambiguous. |
| `outside_percentage_range_count` | `bigint` | Numeric values with absolute magnitude greater than 100. |
| `numeric_range_spans_unit` | `boolean` | True when numeric evidence reaches `percentage_detection_min_numeric_rate` and values occur on both sides of 1. This neutral range observation does not claim percentage semantics. |
| `mixed_percentage_scale_candidate` | `boolean` | True when percentage semantics have corroborating evidence and units appear inconsistent: ordinary numeric and `%`-suffixed values are mixed, or a percentage-named column contains both fractional and whole-percent numeric forms. Values above 100 suppress the name-only candidate. This is a review flag, not proof. |
| `percentage_scale_risk` | `string` | `high` for a mixed-scale candidate and null otherwise. Use `numeric_range_spans_unit` for the lower-confidence unnamed numeric observation. |
| `quality_flags` | `array<string>` | Machine-friendly warning and informational codes. Possible values are defined below. |
| `notes` | `array<string>` | Human-readable explanations for inference decisions such as preserving an identifier or exceeding Spark's decimal precision limit. |

Possible `quality_flags` values:

| Flag | Meaning |
|---|---|
| `empty_dataset` | The DataFrame contains no rows. |
| `all_values_missing` | The column has no non-missing values. |
| `high_missing_rate` | Missing rate reaches the configured `high_missing_rate` threshold. |
| `leading_or_trailing_whitespace` | At least one non-null value has outer whitespace. |
| `parse_failures_present` | At least one non-missing value fails the inferred non-string type. |
| `mixed_type_values` | The column fell back to string while at least half of its values match another candidate type. |
| `constant_column` | Exactly one normalized non-missing value was observed. |
| `possible_key_verify_with_exact_distinct` | Observed uniqueness is at least 98% with no missing values; exact verification is recommended when approximate distinct counting is enabled. |
| `leading_zero_numeric_strings` | At least one integer-looking value contains meaningful leading zeros. |
| `non_finite_values_present` | An already typed floating column contains NaN or positive/negative infinity. |
| `boolean_integer_ambiguous` | Every non-missing value is `0` or `1`, so both boolean and integer interpretations are valid. |
| `ambiguous_yyyymmdd_or_integer` | All values match both the `yyyyMMdd` date pattern and 8-digit integer syntax; integer is conservatively suggested. |
| `mixed_decimal_scales` | More than one displayed fixed-point scale was observed. This is informational and does not by itself imply mixed units. |
| `potential_percentage_column` | The column name or a `%`-suffixed value suggests percentage semantics. |
| `numeric_range_spans_unit` | Numeric values occur below and above 1 at the configured evidence rate. This intentionally avoids asserting percentage semantics. |
| `possible_mixed_percentage_scales` | Corroborating percentage evidence and inconsistent representations were detected. |
| `high_risk_mixed_percentage_scales` | A percentage-named column mixes fractional and whole forms, or ordinary numeric and `%`-suffixed values are mixed. |
| `mixed_percent_symbol_and_numeric_values` | Both `%`-suffixed values and ordinary numeric values occur in the column. |
| `percentage_values_outside_expected_range` | A percentage-like column contains ordinary numeric magnitudes above 100; review the semantics before conversion. |
| `identifier_preserved_as_string` | A numeric-looking identifier was kept as string to preserve formatting. |
| `mixed_date_or_timestamp_formats` | More than one configured date or timestamp format matched. |
| `mode_and_top_values_skipped` | Mode/top-value work was disabled or the approximate distinct count exceeded its cap. |
| `unique_string_values_skipped` | Complete string-domain collection was disabled or exceeded its cap. |
| `distinct_count_is_approximate` | The distinct count uses the default approximate algorithm. |

## Decimal scale and percentage-unit risk

```python
result.profile_df.select(
    "column_name",
    "observed_decimal_scales",
    "max_observed_decimal_precision",
    "percentage_evidence_rate",
    "fractional_percentage_scale_count",
    "whole_percentage_scale_count",
    "outside_percentage_range_count",
    "numeric_range_spans_unit",
    "mixed_percentage_scale_candidate",
    "percentage_scale_risk",
).show(truncate=False)
```

`observed_decimal_scales` records the number of digits after the decimal point
as they appeared in the source strings. For example, `4`, `4.0`, and `4.00`
have observed scales 0, 1, and 2 even though they are numerically equal.

Percentage-unit detection is intentionally heuristic:

- Non-zero absolute values below 1 count as fractional percentage forms.
- Absolute values above 1 and at most 100 count as whole-percentage forms.
- If both forms occur and at least 80% of non-missing values are numeric or
  `%`-suffixed, `numeric_range_spans_unit` records the neutral range signal.
- The stronger `mixed_percentage_scale_candidate` also requires percentage
  evidence: a percentage-like name, or an explicit mixture of ordinary numeric
  and `%`-suffixed values. `0.04` mixed with `4%` is therefore high risk.
- A name-only candidate is suppressed when ordinary numeric magnitudes above
  100 occur. The outside-range count and quality flag remain available for
  review.
- Amount-like names containing tokens such as `price`, `cost`, or `amount`
  suppress percentage name hints, reducing false positives on ordinary values
  that happen to straddle 1.
- Zero and exactly 1 are excluded from the two groups because their intended
  percentage unit is inherently ambiguous.

The threshold is configurable:

```python
config = ProfilerConfig(
    percentage_detection_min_numeric_rate=0.90,
    percentage_min_group_count=2,
)
```

This warning requires business review. Values alone cannot prove that units are
mixed; `numeric_range_spans_unit` is deliberately kept separate from the
percentage-specific candidate.

## Cost controls

Profiling is inherently an action-heavy workload. Base and detail aggregates,
top values, unique domains, and invalid examples are processed in bounded
multi-column batches. This changes the action count from roughly linear per
column to roughly linear per batch. Exact mode/top-value work is skipped by
default above 10,000 approximate distinct values, and complete unique string
domains are skipped above 200 values.

For a very wide or very large table:

```python
config = ProfilerConfig(
    calculate_top_values=False,
    collect_unique_string_values=False,
    exact_distinct=False,
    aggregation_batch_size=8,
    cache_input=True,
)
```

The default batch size of 8 limits wide generated aggregate plans. Larger
batches reduce action count but increase query-planning and code-generation
pressure; measure before increasing it.

If exact cardinality is necessary, set `exact_distinct=True`; this can be much
more expensive than the default approximate count.

Top values and invalid examples contain source data. Disable them when the
profile output should not expose potentially sensitive values. Unique string
domains can expose source data as well:

```python
config = ProfilerConfig(
    calculate_top_values=False,
    collect_unique_string_values=False,
    invalid_sample_size=0,
)
```

To raise the complete-domain limit for string columns—or deliberately remove
the limit—set `unique_values_max_cardinality=1000` or
`unique_values_max_cardinality=None`. Removing the limit can collect a very
large domain to the driver and should be used only when cardinality is known.

When `cache_input=True`, the profiler persists and releases the exact DataFrame
object it receives if that object is not already marked cached. Spark's
`is_cached` flag is per DataFrame object, so callers that already cache an
equivalent upstream plan through a different object should pass
`cache_input=False` to avoid a second cache entry.

## `ProfilerConfig` reference

| Setting | Default | Definition |
|---|---:|---|
| `inference_threshold` | `0.98` | Minimum candidate parse rate required to infer a non-string type. |
| `null_like_values` | `("null", "none", "n/a", "na", "nan", "missing")` | Case-normalized sentinels treated as missing in source string columns. |
| `case_sensitive_nulls` | `False` | Match null-like sentinels with case sensitivity. |
| `trim_strings` | `True` | Trim source strings before inference and value statistics. |
| `date_formats` | built-in date patterns | Java datetime patterns considered for date inference. |
| `timestamp_formats` | built-in timestamp patterns | Java datetime patterns considered for timestamp inference. |
| `approx_distinct_rsd` | `0.05` | Relative standard deviation passed to approximate distinct counting; Spark requires at least `0.000017`. |
| `exact_distinct` | `False` | Use exact `countDistinct` instead of approximate distinct counting. |
| `percentile_accuracy` | `10_000` | Accuracy parameter passed to `percentile_approx`; higher values use more memory. |
| `calculate_top_values` | `True` | Calculate exact mode and frequency-ranked top values. |
| `top_n` | `5` | Maximum number of top values returned per eligible column. |
| `top_values_max_cardinality` | `10_000` | Skip top-value work when approximate distinct count exceeds this cap; `None` removes the cap. |
| `collect_unique_string_values` | `True` | Collect complete normalized domains for eligible source string columns. |
| `unique_values_max_cardinality` | `200` | Safety cap for unique string domains; `None` permits unbounded driver collection. |
| `invalid_sample_size` | `5` | Maximum number of distinct invalid examples returned per inferred typed column; zero disables sampling. |
| `aggregation_batch_size` | `8` | Number of source columns combined in each bounded profiling batch. |
| `high_missing_rate` | `0.20` | Missing-rate threshold for the `high_missing_rate` quality flag. |
| `categorical_max_distinct` | `50` | Maximum distinct count considered by the categorical semantic-type heuristic. |
| `preserve_identifier_strings` | `True` | Recommend string for numeric-looking identifier columns when formatting could be lost. |
| `percentage_detection_min_numeric_rate` | `0.80` | Minimum numeric-or-`%` evidence rate for `numeric_range_spans_unit`. |
| `percentage_min_group_count` | `1` | Minimum observations required in both fractional and whole percentage-scale buckets. Increase this to reduce small-sample alerts. |
| `cache_input` | `True` | Persist the received DataFrame for the profiling run when that object is not already cached. |

## Important inference behavior

- Missing values are excluded from parse-rate denominators.
- Integer widths are conservative: up to 9 significant digits becomes `INT`,
  up to 18 becomes `BIGINT`, and up to 38 becomes `DECIMAL(p,0)`.
- Fixed-point values become the smallest conservative `DECIMAL(p,s)` capable
  of holding the observed integral digits and scale.
- `mixed_decimal_scales` is informational: differing source scales are common
  and do not necessarily mean the values use different units.
- Numeric-looking columns with identifier-like names and high uniqueness or
  leading zeros are recommended as `STRING` even though `inferred_type` still
  records their numeric syntax.
- A pure `0`/`1` domain is inferred as boolean but explicitly flagged as
  boolean/integer ambiguous.
- Values matching only `yyyyMMdd` and 8-digit integer syntax are conservatively
  suggested as integers and flagged for date review.
- Typed NaN and infinity values remain non-missing and receive explicit counts
  and a quality flag; derived non-finite means are returned as null.
- A type is recommended only when its parse rate reaches
  `inference_threshold`; the individual candidate rates remain available for
  borderline or mixed columns.
- Inference is evidence from the profiled data, not a contract for future
  batches. Treat inferred types and percentage warnings as review inputs.

## Customizing source conventions

```python
config = ProfilerConfig(
    null_like_values=("null", "n/a", "not supplied", "-"),
    date_formats=("yyyy-MM-dd", "dd/MM/yyyy"),
    timestamp_formats=("yyyy-MM-dd HH:mm:ss",),
    preserve_identifier_strings=True,
)
```

Custom Java datetime patterns are validated before source-data actions and then
passed to Spark's non-throwing parser. The built-in formats also have structural
guards to reduce unnecessary parsing work.
