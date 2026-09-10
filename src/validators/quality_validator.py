"""Business-rule validation helpers for dataset quality checks.

This module focuses on data quality issues that are common in ETL pipelines: missing
values, duplicate keys, and invalid numeric ranges. These checks do not compare source to
 target; instead, they validate each dataset independently to ensure the data is clean and
 trustworthy before any reconciliation work begins.
"""

from functools import reduce
from operator import or_
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def null_counts(df: DataFrame, columns: list[str]) -> dict[str, int]:
    """Count null values in each requested column.

    This is useful for required-field validation and is often one of the first quality
    checks to run after reading a dataset. A healthy ETL output should not have nulls in
    critical business columns such as IDs, amounts, or statuses.
    """
    expressions = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in columns
    ]
    row = df.select(*expressions).first()
    return row.asDict()


def duplicate_rows(df: DataFrame, key_columns: list[str]) -> DataFrame:
    """Return rows whose business key appears more than once.

    Duplicate detection is one of the strongest indicators of data integrity issues. If an
    order ID or customer ID appears multiple times, downstream reconciliation and reporting
    can become ambiguous or misleading.
    """
    return (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
    )


def invalid_numeric_range(df: DataFrame, column: str, minimum=None, maximum=None) -> DataFrame:
    """Return rows whose numeric value violates the configured range.

    This helper is used for business rules such as "amount must be >= 0" or "score must be
    less than 100". It is intentionally flexible enough to support either lower-bound,
    upper-bound, or both-side range validation.
    """
    conditions = []
    if minimum is not None:
        conditions.append(F.col(column) < F.lit(minimum))
    if maximum is not None:
        conditions.append(F.col(column) > F.lit(maximum))
    if not conditions:
        raise ValueError("At least one of minimum or maximum must be provided")
    return df.filter(reduce(or_, conditions))
