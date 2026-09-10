import pytest
import yaml
from src.spark_session import build_spark_session
from src.connectors.csv_connector import CsvConnector


@pytest.fixture(scope="session")
def settings():
    with open("config/settings.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def spark(settings):
    session = build_spark_session(settings["app_name"])
    yield session
    session.stop()


@pytest.fixture(scope="session")
def datasets(spark, settings):
    connector = CsvConnector(spark)
    source = connector.read(settings["source_path"])
    target = connector.read(settings["target_path"])
    return source, target
