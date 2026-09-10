from src.validators.schema_validator import missing_columns, schemas_match


def test_required_columns_exist(datasets, settings):
    source, target = datasets
    assert missing_columns(source, settings["required_columns"]) == []
    assert missing_columns(target, settings["required_columns"]) == []


def test_source_and_target_schema_match(datasets):
    source, target = datasets
    assert schemas_match(source, target)
