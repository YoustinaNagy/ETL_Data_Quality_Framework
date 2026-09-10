"""Source-to-target reconciliation logic.

This is the most important part of the framework from an ETL perspective: it compares two
related datasets using a business key, then classifies the differences into one of three
patterns:

- missing in source
- missing in target
- value mismatch

The reconciliation step is what turns raw CSV data into an actionable quality signal for
data engineers and analysts.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def row_count_match(source_df: DataFrame, target_df: DataFrame) -> bool:
    """Quickly check whether both datasets have the same number of rows.

    This is a coarse validation signal; it helps detect obvious drift but does not explain
    what changed. It is often used as a first-pass sanity check before more precise
    row-by-row comparisons.
    """
    return source_df.count() == target_df.count()


def reconcile(
    source_df: DataFrame,
    target_df: DataFrame,
    key_columns: list[str],
    compare_columns: list[str],
) -> DataFrame:
    """Compare source and target records and classify data mismatches.

    The method does a full outer join on the business key(s), so that all rows from both
    sides are preserved. This allows us to identify:

    1. rows present only in source
    2. rows present only in target
    3. rows in both but with different values for monitored columns

    Args:
        source_df: Reference dataset, typically the extracted source feed.
        target_df: Dataset produced by the ETL target system.
        key_columns: Columns that uniquely identify a record in both datasets.
        compare_columns: Field values to compare for mismatches after a key match.

    Returns:
        A DataFrame containing the key, compared values, and a discrepancy type.
    """
    # Alias both sides to keep the join logic readable and avoid column collisions.
    s = source_df.alias("s")
    t = target_df.alias("t")

    # Full outer join preserves all rows from both sides so missing records can be found.
    join_condition = [F.col(f"s.{k}") == F.col(f"t.{k}") for k in key_columns]
    joined = s.join(t, on=join_condition, how="full_outer")

    # If a source key is null, the record only exists on the target side.
    source_missing = F.col(f"s.{key_columns[0]}").isNull()
    target_missing = F.col(f"t.{key_columns[0]}").isNull()

    # Compare every chosen field and mark a mismatch when the source and target values are
    # not equal, even when null handling is involved.
    mismatch_condition = None
    for column in compare_columns:
        diff = ~F.col(f"s.{column}").eqNullSafe(F.col(f"t.{column}"))
        mismatch_condition = diff if mismatch_condition is None else (mismatch_condition | diff)

    # Priority order matters here: if a row is missing in source or target, that should be
    # reported before a field-level mismatch is considered.
    discrepancy_type = (
        F.when(source_missing, F.lit("MISSING_IN_SOURCE"))
        .when(target_missing, F.lit("MISSING_IN_TARGET"))
        .when(mismatch_condition, F.lit("VALUE_MISMATCH"))
    )

    # Keep the relevant identifying columns and both side-by-side values for human review.
    selected = [
        F.coalesce(F.col(f"s.{k}"), F.col(f"t.{k}")).alias(k)
        for k in key_columns
    ]
    for column in compare_columns:
        selected.extend([
            F.col(f"s.{column}").alias(f"source_{column}"),
            F.col(f"t.{column}").alias(f"target_{column}"),
        ])

    return (
        joined
        .withColumn("discrepancy_type", discrepancy_type)
        .filter(F.col("discrepancy_type").isNotNull())
        .select(*selected, "discrepancy_type")
    )
