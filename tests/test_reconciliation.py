from src.validators.reconciliation_validator import reconcile


def test_source_target_reconciliation_detects_expected_defects(datasets, settings):
    source, target = datasets
    discrepancies = reconcile(
        source,
        target,
        key_columns=settings["key_columns"],
        compare_columns=["customer_id", "amount", "status"],
    )

    found = {
        (row["order_id"], row["discrepancy_type"])
        for row in discrepancies.collect()
    }

    assert found == {
        (3, "VALUE_MISMATCH"),
        (4, "MISSING_IN_TARGET"),
        (5, "MISSING_IN_SOURCE"),
    }
