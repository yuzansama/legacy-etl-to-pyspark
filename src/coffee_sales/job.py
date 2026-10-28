"""coffee_sales_summary: a legacy ETL graph rebuilt as a PySpark job.

Legacy component                    PySpark function
----------------------------------  ----------------------------------------------
Input File (DML record format)      read_orders: explicit schema, FAILFAST
Reformat (transform rules)          reformat: one column expression per rule
Filter by Expression (+ deselect)   split_valid: null-safe predicate, quarantine
Join with a lookup                  enrich: unique-key check, broadcast left join
Rollup                              rollup: groupBy + exact decimal sums
Output Table (truncate & load)      write_outputs: dynamic partition overwrite

Run it locally:

    python -m coffee_sales.job --run-date 2026-10-27
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from coffee_sales.quality import DataQualityError, assert_unique
from coffee_sales.spark import build_spark

log = logging.getLogger("coffee_sales.job")

ORDER_FIELDS = ["ticket_no", "store_id", "sku", "sold_at", "line_amount"]
# Everything is read as string first: typing happens in reformat(), where a bad value becomes
# NULL and is quarantined with a reason, instead of failing or being silently coerced.
RAW_SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in ORDER_FIELDS])
CATALOG_SCHEMA = T.StructType([
    T.StructField("sku", T.StringType()),
    T.StructField("bean_origin", T.StringType()),
])
MONEY = T.DecimalType(18, 2)
TS_FORMAT = "yyyyMMddHHmmss"
GROUP_KEYS = ["sale_date", "store_id", "bean_origin"]
RAW_COLS = ["sold_at_raw", "line_amount_raw"]


def read_orders(spark: SparkSession, path: str, mode: str = "FAILFAST") -> DataFrame:
    """Input File. A line with the wrong number of fields stops the job (FAILFAST)."""
    return (
        spark.read.option("sep", "|")
        .option("header", "false")
        .option("mode", mode)
        .schema(RAW_SCHEMA)
        .csv(path)
    )


def read_catalog(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", "true").schema(CATALOG_SCHEMA).csv(path)


def reformat(raw: DataFrame) -> DataFrame:
    """Reformat. try_* functions return NULL for unparseable values in both ANSI and non-ANSI mode."""
    return raw.select(
        "ticket_no",
        "store_id",
        F.upper(F.trim("sku")).alias("sku"),
        F.try_to_timestamp("sold_at", F.lit(TS_FORMAT)).alias("sold_at"),
        F.expr("try_cast(line_amount AS DECIMAL(18,2))").alias("line_amount"),
        # Keep the original text so the quarantine shows what was actually received.
        F.col("sold_at").alias("sold_at_raw"),
        F.col("line_amount").alias("line_amount_raw"),
    )


def split_valid(clean: DataFrame, min_amount: Decimal = Decimal("0")) -> tuple[DataFrame, DataFrame]:
    """Filter by Expression: select port -> valid, deselect port -> rejects.

    In SQL a comparison with NULL is NULL, and both filter(cond) and filter(~cond) drop NULL
    rows. coalesce(cond, false) makes sure every row lands in exactly one of the two outputs.
    """
    cond = F.col("sold_at").isNotNull() & (F.col("line_amount") >= F.lit(min_amount))
    is_valid = F.coalesce(cond, F.lit(False))
    reason = (
        F.when(F.col("sold_at").isNull(), "bad_sold_at")
        .when(F.col("line_amount").isNull(), "bad_line_amount")
        .otherwise("below_min_amount")
    )
    valid = clean.filter(is_valid).drop(*RAW_COLS)
    rejects = clean.filter(~is_valid).withColumn("reject_reason", reason)
    return valid, rejects


def enrich(valid: DataFrame, catalog: DataFrame) -> DataFrame:
    """Join with lookup. A left join keeps orders whose SKU is unknown (legacy outer semantics)."""
    dim = catalog.select(F.upper(F.trim("sku")).alias("sku"), "bean_origin")
    # A duplicate lookup key would multiply orders in the join: fail instead of inflating totals.
    assert_unique(dim, ["sku"], name="sku_catalog")
    return valid.join(F.broadcast(dim), on="sku", how="left")


def rollup(enriched: DataFrame) -> DataFrame:
    """Rollup on {sale_date, store_id, bean_origin}. Money stays decimal end to end."""
    return (
        enriched.withColumn("sale_date", F.to_date("sold_at"))
        .groupBy(*GROUP_KEYS)
        .agg(
            F.count(F.lit(1)).alias("tickets"),
            F.sum("line_amount").cast(MONEY).alias("gross_sales"),
        )
        .select(*GROUP_KEYS, "tickets", "gross_sales")
    )


def write_outputs(daily: DataFrame, rejects: DataFrame, curated: str, quarantine: str, run_date: str) -> None:
    """Output Table. Dynamic partition overwrite makes re-runs idempotent: a retry or a
    backfill for one day replaces only that day's partition."""
    daily.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    daily.write.mode("overwrite").partitionBy("sale_date").parquet(f"{curated}/sales_by_store")
    rejects.write.mode("overwrite").parquet(f"{quarantine}/orders/run_date={run_date}")


@dataclass(frozen=True)
class RunStats:
    run_date: str
    input_rows: int
    valid_rows: int
    rejected_rows: int
    output_rows: int


def run(
    spark: SparkSession,
    *,
    run_date: str,
    landing: str = "data/landing",
    catalog_path: str = "data/ref/sku_catalog.csv",
    curated: str = "out/curated",
    quarantine: str = "out/quarantine",
    min_amount: Decimal = Decimal("0"),
    mode: str = "FAILFAST",
    dry_run: bool = False,
) -> RunStats:
    clean = reformat(read_orders(spark, f"{landing}/orders/{run_date}", mode)).cache()
    valid, rejects = split_valid(clean, min_amount)
    daily = rollup(enrich(valid, read_catalog(spark, catalog_path))).cache()

    stats = RunStats(
        run_date=run_date,
        input_rows=clean.count(),        # materializes the cache, so FAILFAST triggers here
        valid_rows=valid.count(),
        rejected_rows=rejects.count(),
        output_rows=daily.count(),
    )
    log.info("stage counts %s", asdict(stats))
    if stats.valid_rows + stats.rejected_rows != stats.input_rows:
        raise DataQualityError(
            f"{stats.input_rows - stats.valid_rows - stats.rejected_rows} row(s) unaccounted for "
            f"(input={stats.input_rows}, valid={stats.valid_rows}, rejected={stats.rejected_rows})"
        )

    if dry_run:
        log.info("dry run: nothing written")
    else:
        write_outputs(daily, rejects, curated, quarantine, run_date)
    clean.unpersist()
    daily.unpersist()
    return stats


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-date", required=True, help="YYYY-MM-DD; reads <landing>/orders/<run-date>/")
    p.add_argument("--landing", default="data/landing")
    p.add_argument("--catalog", default="data/ref/sku_catalog.csv")
    p.add_argument("--curated", default="out/curated")
    p.add_argument("--quarantine", default="out/quarantine")
    p.add_argument("--min-amount", default="0", type=Decimal)
    p.add_argument("--mode", default="FAILFAST", choices=["FAILFAST", "PERMISSIVE"])
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = build_spark("coffee_sales_summary")
    try:
        stats = run(
            spark,
            run_date=args.run_date,
            landing=args.landing,
            catalog_path=args.catalog,
            curated=args.curated,
            quarantine=args.quarantine,
            min_amount=args.min_amount,
            mode=args.mode,
            dry_run=args.dry_run,
        )
    finally:
        spark.stop()
    print(json.dumps(asdict(stats)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
