"""End-to-end tests: run the real job on the sample landing data and reconcile with legacy output."""

from pathlib import Path

import pytest

from coffee_sales.job import run
from coffee_sales.parity import read_legacy_csv, reconcile

ROOT = Path(__file__).resolve().parents[1]
LANDING = str(ROOT / "data" / "landing")
CATALOG = str(ROOT / "data" / "ref" / "sku_catalog.csv")
LEGACY = ROOT / "legacy" / "expected"
DAYS = ["2026-10-27", "2026-10-28"]


def run_day(spark, tmp_path, day, **kw):
    return run(
        spark,
        run_date=day,
        landing=kw.pop("landing", LANDING),
        catalog_path=CATALOG,
        curated=str(tmp_path / "curated"),
        quarantine=str(tmp_path / "quarantine"),
        **kw,
    )


def test_output_matches_legacy_record_for_record(spark, tmp_path):
    for day in DAYS:
        run_day(spark, tmp_path, day)
    actual = spark.read.parquet(str(tmp_path / "curated" / "sales_by_store"))
    report = reconcile(actual, read_legacy_csv(spark, str(LEGACY / "sales_by_store.csv")))
    assert report.passed, report.render()


def test_quarantine_matches_legacy_rejects(spark, tmp_path):
    for day in DAYS:
        run_day(spark, tmp_path, day)
    rejects = spark.read.parquet(str(tmp_path / "quarantine" / "orders"))
    got = {(str(r.run_date), r.ticket_no, r.reject_reason) for r in rejects.collect()}
    expected = spark.read.option("header", "true").csv(str(LEGACY / "rejects.csv"))
    assert got == {(r.run_date, r.ticket_no, r.reject_reason) for r in expected.collect()}


def test_stage_counts_account_for_every_row(spark, tmp_path):
    stats = run_day(spark, tmp_path, "2026-10-27")
    assert (stats.input_rows, stats.valid_rows, stats.rejected_rows, stats.output_rows) == (12, 9, 3, 8)


def test_rerun_is_idempotent(spark, tmp_path):
    for _ in range(3):  # e.g. two retries after a failure further downstream
        run_day(spark, tmp_path, "2026-10-27")
    run_day(spark, tmp_path, "2026-10-28")
    actual = spark.read.parquet(str(tmp_path / "curated" / "sales_by_store"))
    report = reconcile(actual, read_legacy_csv(spark, str(LEGACY / "sales_by_store.csv")))
    assert report.passed, report.render()


def test_malformed_file_fails_before_writing(spark, tmp_path):
    day_dir = tmp_path / "landing" / "orders" / "2026-10-27"
    day_dir.mkdir(parents=True)
    (day_dir / "orders.dat").write_text(
        "T1001|PWK-01|AR-01|20261027071503|28500.75\n"
        "T1002|PWK-01|BL-01|20261027073210\n"          # one field missing
    )
    with pytest.raises(Exception, match="(?i)malformed"):
        run_day(spark, tmp_path, "2026-10-27", landing=str(tmp_path / "landing"))
    assert not (tmp_path / "curated").exists()


def test_dry_run_writes_nothing(spark, tmp_path):
    stats = run_day(spark, tmp_path, "2026-10-28", dry_run=True)
    assert stats.input_rows == 10
    assert not (tmp_path / "curated").exists() and not (tmp_path / "quarantine").exists()
