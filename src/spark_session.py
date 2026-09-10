"""Spark session factory for the ETL quality framework.

This module is the entry point for the local Spark runtime used by every validator and
integration test in the project. It centralizes the session creation so all tests and
data checks use the same configuration, which keeps execution consistent and avoids
repeated boilerplate setup across the codebase.
"""

from pyspark.sql import SparkSession


def build_spark_session(app_name: str = "etl-data-quality-framework") -> SparkSession:
    """Create and return the local Spark session used by this project.

    The framework is designed to run against a small local Spark cluster in single-node
    mode for development and automated validation. Using "local[*]" means Spark will use
    all available local CPU cores for parallel processing, which is ideal for lightweight
    CSV-based ETL checks and test workloads.

    Args:
        app_name: Logical name displayed in the Spark UI and logs.

    Returns:
        A configured SparkSession ready for DataFrame operations.
    """
    # Configure the local session once and reuse it across tests. This avoids creating
    # multiple Spark contexts and keeps the validation workflow stable.
    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
