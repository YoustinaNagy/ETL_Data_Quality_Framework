"""Schema-level validation helpers.

These functions ensure that source and target datasets remain structurally aligned before
business logic or reconciliation checks run. If required columns are missing or the
schema drifted between source and target, the framework should fail early instead of
producing misleading data-quality results.
"""

from pyspark.sql import DataFrame


def missing_columns(df: DataFrame, required_columns: list[str]) -> list[str]:
    """Return every required column that is missing from the DataFrame.

    This is a fast structural check used to confirm that all expected fields exist before
    downstream tests evaluate nulls, duplicates, or reconciliation mismatches.
    """
    actual = set(df.columns)
    return [column for column in required_columns if column not in actual]


def schemas_match(source_df: DataFrame, target_df: DataFrame) -> bool:
    """Compare the full schema of two DataFrames field by field.

    This is stricter than simply checking row counts or column names. It ensures that both
    datasets agree on names and types, which matters when comparing source and target
    records for ETL correctness.
    """
    source_schema = [(f.name, f.dataType.simpleString()) for f in source_df.schema.fields]
    target_schema = [(f.name, f.dataType.simpleString()) for f in target_df.schema.fields]
    return source_schema == target_schema
