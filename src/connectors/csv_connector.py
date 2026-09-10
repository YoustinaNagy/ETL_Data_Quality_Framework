"""CSV ingestion layer for the ETL data-quality framework.

The connector is responsible for translating raw CSV files from the repository's data
folder into Spark DataFrames. This layer acts as the boundary between file-based test
inputs and the validation logic, which operates on DataFrames rather than raw files.
"""

from pyspark.sql import DataFrame, SparkSession


class CsvConnector:
    """Read CSV files into Spark DataFrames using a consistent schema strategy.

    In a real ETL project, connectors often abstract source systems such as CSV, JSON,
    Parquet, databases, or cloud storage. This lightweight version keeps the pattern
    minimal while still demonstrating the separation between data acquisition and
    validation rules.
    """

    def __init__(self, spark: SparkSession):
        # Store the active Spark session so the connector can read files using the
        # same execution context as the rest of the framework.
        self.spark = spark

    def read(self, path: str, infer_schema: bool = True) -> DataFrame:
        """Read a CSV file and return it as a Spark DataFrame.

        Args:
            path: File path to the CSV source. This can point to source or target data.
            infer_schema: If True, Spark infers column types from the CSV contents.

        Returns:
            A Spark DataFrame with the first row treated as column headers.
        """
        return (
            self.spark.read
            .option("header", True)
            .option("inferSchema", infer_schema)
            .csv(path)
        )
