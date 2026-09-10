from src.validators.quality_validator import duplicate_rows, invalid_numeric_range, null_counts


def test_order_id_is_unique_in_source(datasets, settings):
    source, _ = datasets
    assert duplicate_rows(source, settings["key_columns"]).count() == 0


def test_required_fields_are_not_null_in_source(datasets, settings):
    source, _ = datasets
    counts = null_counts(source, settings["required_columns"])
    assert all(value == 0 for value in counts.values())


def test_amount_is_non_negative(datasets):
    source, _ = datasets
    assert invalid_numeric_range(source, "amount", minimum=0).count() == 0
