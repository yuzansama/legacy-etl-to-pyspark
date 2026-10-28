import datetime as dt
from decimal import Decimal

from coffee_sales.parity import LEGACY_SCHEMA, reconcile


def df(spark, rows):
    return spark.createDataFrame(rows, LEGACY_SCHEMA)


def test_identical_outputs_pass_including_null_keys(spark):
    d = dt.date(2026, 10, 27)
    rows = [(d, "S1", "Arabika Gayo", 2, Decimal("57001.50")), (d, "S1", None, 1, Decimal("36500.40"))]
    report = reconcile(df(spark, rows), df(spark, rows))
    assert report.passed, report.render()


def test_reports_missing_extra_changed_and_duplicates(spark):
    d = dt.date(2026, 10, 27)
    expected = df(spark, [
        (d, "S1", "Arabika Gayo", 2, Decimal("57001.50")),
        (d, "S1", None, 1, Decimal("36500.40")),
        (d, "S2", "House Blend", 1, Decimal("24000.50")),
    ])
    actual = df(spark, [
        (d, "S1", "Arabika Gayo", 2, Decimal("57001.49")),   # changed by one cent
        (d, "S2", "House Blend", 1, Decimal("24000.50")),
        (d, "S2", "House Blend", 1, Decimal("24000.50")),    # duplicated by a re-run
        (d, "S3", "House Blend", 1, Decimal("1.00")),        # extra
    ])                                                       # (d, S1, NULL) is missing
    report = reconcile(actual, expected)

    assert not report.passed
    failed = {c.name for c in report.checks if not c.ok}
    assert failed == {"row count", "sum(tickets)", "sum(gross_sales)", "duplicate keys", "row-level diffs"}
    statuses = sorted(d["status"] for d in report.diffs)
    assert statuses == ["changed", "extra", "missing"]
