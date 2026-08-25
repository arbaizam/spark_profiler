# Spark bronze-to-silver data profiler

`spark_data_profiler.py` profiles a PySpark DataFrame and returns one aggregate
row per source column. It is designed for raw bronze tables where most or all
columns arrived as strings and the next job must choose safe silver types.

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
- Observed date/timestamp formats, including mixed-format warnings
- Leading-zero and identifier detection so values such as `00123` are not
  accidentally promoted to numbers
- A suggested `StructType`, safe silver SQL expressions, and quarantine
  predicates for values that fail conversion

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
    "quality_flags",
).show(truncate=False)

print(result.suggested_schema.simpleString())
print(result.silver_select_sql("bronze.orders"))
```

The generated conversion SQL uses `TRY_CAST` and `TRY_TO_TIMESTAMP`, available
in Spark 3.5+ and current Databricks runtimes. The Python profiler falls back to
ordinary date/timestamp parsing on older Spark versions; with old Spark and
ANSI mode enabled, malformed datetime inputs may need platform-specific safe
parsing expressions.

## Using failures in a pipeline

Every profile row contains both a `silver_expression` and a
`quarantine_predicate`. The same values are available as Python mappings:

```python
result.silver_expressions["amount"]
# TRY_CAST(... AS DECIMAL(12,2))

result.quarantine_predicates["amount"]
# cleaned source is non-null AND converted value is null
```

This makes the inferred schema a recommendation rather than a silent lossy
cast. A production silver job should route records matching any applicable
quarantine predicate to an error table and monitor the failure rate.

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
- Numeric-looking columns with identifier-like names and high uniqueness or
  leading zeros are recommended as `STRING` even though `inferred_type` still
  records their numeric syntax.
- A type is recommended only when its parse rate reaches
  `inference_threshold`; the individual candidate rates remain available for
  borderline or mixed columns.
- Inference is evidence from the profiled data, not a contract for future
  batches. Keep the generated safe casts and quarantine checks in the pipeline.

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
