"""Unit tests: one per legacy component, each pinning down the behaviour the legacy graph had."""

import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from coffee_sales.job import RAW_SCHEMA, enrich, reformat, rollup, split_valid
from coffee_sales.quality import DataQualityError


def raw_df(spark, rows):
    return spark.createDataFrame(rows, RAW_SCHEMA)


def test_reformat_normalizes_keys_and_casts_types(spark):
    rows = reformat(raw_df(spark, [
        ("T1", "S1", " ar-02 ", "20261027081522", "41250.30"),
        ("T2", "S1", "AR-01", "2026-10-27 10:30", "abc"),
        ("T3", "S1", "AR-01", "20261340000000", None),
    ])).orderBy("ticket_no").collect()

    assert rows[0]["sku"] == "AR-02"
    assert rows[0]["sold_at"] == dt.datetime(2026, 10, 27, 8, 15, 22)
    assert rows[0]["line_amount"] == Decimal("41250.30")
    # Unparseable values become NULL (to be quarantined), never an exception or a guess.
    assert rows[1]["sold_at"] is None and rows[1]["line_amount"] is None
    assert rows[2]["sold_at"] is None  # month 13
    # The original text is kept for the quarantine.
    assert rows[1]["line_amount_raw"] == "abc"


def test_filter_puts_every_row_in_exactly_one_output(spark):
    clean = reformat(raw_df(spark, [
        ("T1", "S1", "AR-01", "20261027071503", "28500.75"),   # valid
        ("T2", "S1", "AR-01", "bad", "28500.75"),              # bad timestamp
        ("T3", "S1", "AR-01", "20261027071503", None),         # missing amount -> NULL condition
        ("T4", "S1", "AR-01", "20261027071503", "-1.00"),      # refund
    ]))
    valid, rejects = split_valid(clean)

    assert [r.ticket_no for r in valid.collect()] == ["T1"]
    reasons = {r.ticket_no: r.reject_reason for r in rejects.collect()}
    assert reasons == {"T2": "bad_sold_at", "T3": "bad_line_amount", "T4": "below_min_amount"}
    assert valid.count() + rejects.count() == clean.count()


def test_naive_filter_loses_rows_with_null_condition(spark):
    """Documents the pitfall split_valid() guards against."""
    clean = reformat(raw_df(spark, [
        ("T1", "S1", "AR-01", "20261027071503", "28500.75"),
        ("T3", "S1", "AR-01", "20261027071503", None),
    ]))
    cond = F.col("sold_at").isNotNull() & (F.col("line_amount") >= 0)
    kept, deselected = clean.filter(cond).count(), clean.filter(~cond).count()
    assert kept + deselected == 1  # T3 is in neither output


def test_left_join_keeps_unknown_skus(spark):
    valid, _ = split_valid(reformat(raw_df(spark, [
        ("T1", "S1", "AR-01", "20261027071503", "28500.75"),
        ("T2", "S1", "LT-99", "20261027101200", "36500.40"),
    ])))
    catalog = spark.createDataFrame([("AR-01", "Arabika Gayo")], "sku string, bean_origin string")
    out = {r.ticket_no: r.bean_origin for r in enrich(valid, catalog).collect()}
    assert out == {"T1": "Arabika Gayo", "T2": None}


def test_duplicate_lookup_key_fails_instead_of_inflating_totals(spark):
    valid, _ = split_valid(reformat(raw_df(spark, [("T1", "S1", "AR-01", "20261027071503", "1.00")])))
    catalog = spark.createDataFrame(
        [("AR-01", "Arabika Gayo"), (" ar-01", "Arabika Gayo (old)")], "sku string, bean_origin string"
    )
    with pytest.raises(DataQualityError, match="duplicate key"):
        enrich(valid, catalog)


def test_rollup_sums_money_exactly(spark):
    # 0.10 + 0.20 is 0.30000000000000004 in binary floating point; decimal keeps it exact.
    valid, _ = split_valid(reformat(raw_df(spark, [
        ("T1", "S1", "AR-01", "20261027071503", "0.10"),
        ("T2", "S1", "AR-01", "20261027091503", "0.20"),
    ])))
    catalog = spark.createDataFrame([("AR-01", "Arabika Gayo")], "sku string, bean_origin string")
    [row] = rollup(enrich(valid, catalog)).collect()
    assert (row.sale_date, row.store_id, row.tickets, row.gross_sales) == (
        dt.date(2026, 10, 27), "S1", 2, Decimal("0.30"),
    )
