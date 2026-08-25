# Spark bronze-to-silver data profiler

`spark_data_profiler.py` profiles a PySpark DataFrame and returns one aggregate
row per source column. It is designed for raw bronze tables where most or all
columns arrived as strings and the next job must choose safe silver types.
It is metadata-only: it does not generate SQL or mutate the source DataFrame.

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
| `null_like_count` | `bigint` | Count of non-null, non-blank values matching configured null sentinels such as `null`, `n/a`, or `missing`. |
| `missing_count` | `bigint` | Combined actual-null, blank, and null-like count; the component categories are mutually exclusive. |
| `missing_rate` | `double` | `missing_count / row_count`; null for an empty DataFrame. |
| `non_missing_count` | `bigint` | Values remaining after actual nulls, blanks, and null-like sentinels are excluded. |
| `inferred_valid_count` | `bigint` | Non-missing values valid for `inferred_type`. For inferred strings, every non-missing value is valid. |
| `inferred_invalid_count` | `bigint` | `non_missing_count - inferred_valid_count`. |
| `parse_success_rate` | `double` | `inferred_valid_count / non_missing_count`; null when the column has no non-missing values. |
| `distinct_count` | `bigint` | Distinct normalized non-missing values. Approximate by default and exact when `exact_distinct=True`. |
| `distinct_is_approximate` | `boolean` | Whether `distinct_count` was calculated with `approx_count_distinct`. |
| `uniqueness_ratio` | `double` | `distinct_count / non_missing_count`, capped at 1 to account for approximate-count overestimation; null with no non-missing values. |
| `min_value` | `string` | Minimum suggested-type value. String minima are lexical. Null when unsupported or no valid values exist. |
| `max_value` | `string` | Maximum suggested-type value. String maxima are lexical. Null when unsupported or no valid values exist. |
| `q1_value` | `string` | Approximate 25th percentile for numeric, boolean, date, or timestamp suggestions. Null for strings and unsupported types. |
| `median_value` | `string` | Approximate 50th percentile for numeric, boolean, date, or timestamp suggestions. |
| `q3_value` | `string` | Approximate 75th percentile for numeric, boolean, date, or timestamp suggestions. |
| `mean` | `double` | Arithmetic mean for numeric suggestions. For booleans, the true fraction after encoding false as 0 and true as 1. |
| `stddev` | `double` | Sample standard deviation for numeric or boolean suggestions; null when insufficient values exist. |
| `min_length` | `int` | Minimum character length of normalized non-missing values. |
| `max_length` | `int` | Maximum character length of normalized non-missing values. |
| `avg_length` | `double` | Average character length of normalized non-missing values. |
| `padded_count` | `bigint` | Non-null values whose string representation has leading or trailing whitespace. |
| `leading_zero_count` | `bigint` | Integer-looking values with more than one digit whose unsigned representation begins with zero. |
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
| `percentage_symbol_count` | `bigint` | Count of non-missing numeric-looking strings ending in `%`, allowing whitespace immediately before the symbol. |
| `fractional_percentage_scale_count` | `bigint` | Numeric values with absolute magnitude greater than 0 and less than 1—the common fractional representation of percentages. |
| `whole_percentage_scale_count` | `bigint` | Numeric values with absolute magnitude greater than 1 and at most 100—the common whole-percent representation. Exactly 1 is excluded as ambiguous. |
| `outside_percentage_range_count` | `bigint` | Numeric values with absolute magnitude greater than 100. |
| `mixed_percentage_scale_candidate` | `boolean` | True when both fractional and whole-percent forms occur and the numeric parse rate reaches `percentage_detection_min_numeric_rate`. This is a review flag, not proof of mixed units. |
| `percentage_scale_risk` | `string` | `high` for a mixed-scale candidate with a percentage name hint or `%` value, `possible` for an unnamed mixed-scale candidate, and null when no mixed-scale candidate is detected. |
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
| `mixed_decimal_scales` | More than one displayed fixed-point scale was observed. This is informational and does not by itself imply mixed units. |
| `potential_percentage_column` | The column name or a `%`-suffixed value suggests percentage semantics. |
| `possible_mixed_percentage_scales` | Both fractional and whole-percent numeric forms were detected. |
| `high_risk_mixed_percentage_scales` | Mixed forms were detected together with a percentage name hint or `%` value. |
| `mixed_percent_symbol_and_numeric_values` | Both `%`-suffixed values and ordinary numeric values occur in the column. |
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
    "fractional_percentage_scale_count",
    "whole_percentage_scale_count",
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
- If both forms occur and at least 80% of non-missing values are numeric,
  `mixed_percentage_scale_candidate` is true.
- The risk is `high` when the column name also resembles a percentage—such as
  `rate`, `pct`, `ratio`, `margin`, or `yield`—and `possible` otherwise.
- Zero and exactly 1 are excluded from the two groups because their intended
  percentage unit is inherently ambiguous.

The threshold is configurable:

```python
config = ProfilerConfig(percentage_detection_min_numeric_rate=0.90)
```

This warning requires business review. A general amount column can legitimately
contain both `0.04` and `4.0`; values alone cannot prove that its units are mixed.

## Cost controls

Profiling is inherently an action-heavy workload. The implementation batches
base aggregates and caches the input for the duration of the run. Exact modes
and top values require one group-by per eligible column. They are skipped by
default above 10,000 approximate distinct values. Complete unique string lists
require a distinct operation and are skipped above 200 values by default.

For a very wide or very large table:

```python
config = ProfilerConfig(
    calculate_top_values=False,
    collect_unique_string_values=False,
    exact_distinct=False,
    aggregation_batch_size=25,
    cache_input=True,
)
```

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

Custom Java datetime patterns are passed to Spark. The built-in formats also
have structural guards to reduce unnecessary parsing work.
