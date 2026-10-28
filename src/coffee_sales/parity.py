"""Record-level reconciliation of the PySpark output against the legacy output.

    python -m coffee_sales.parity \\
        --actual out/curated/sales_by_store --expected legacy/expected/sales_by_store.csv

Exits with status 1 when anything differs, so it can gate a cut-over in CI or a scheduler.
"""

from __future__ import annotations

import argparse
import functools
import sys
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from coffee_sales.spark import build_spark

KEYS = ["sale_date", "store_id", "bean_origin"]
MEASURES = ["tickets", "gross_sales"]
LEGACY_SCHEMA = T.StructType([
    T.StructField("sale_date", T.DateType()),
    T.StructField("store_id", T.StringType()),
    T.StructField("bean_origin", T.StringType()),
    T.StructField("tickets", T.LongType()),
    T.StructField("gross_sales", T.DecimalType(18, 2)),
])


@dataclass
class Check:
    name: str
    legacy: str
    pyspark: str
    ok: bool


@dataclass
class ParityReport:
    checks: list[Check] = field(default_factory=list)
    diffs: list[dict] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.ok for c in self.checks)

    def render(self) -> str:
        w = max(len(c.name) for c in self.checks)
        lines = [f"{'':2}{'check':<{w}}  {'legacy':>16}  {'pyspark':>16}"]
        lines += [f"{'✓' if c.ok else '✗':2}{c.name:<{w}}  {c.legacy:>16}  {c.pyspark:>16}" for c in self.checks]
        for d in self.diffs:
            lines.append(f"  - {d['status']}: {d['key']}  legacy={d['legacy']}  pyspark={d['pyspark']}")
        lines.append("PARITY: PASS (0 mismatches)" if self.passed else "PARITY: FAIL")
        return "\n".join(lines)


def _null_safe_on(keys: list[str]):
    # bean_origin is NULL for unknown SKUs; a plain == would never match NULL to NULL.
    return functools.reduce(lambda a, b: a & b, [F.col(f"a.{k}").eqNullSafe(F.col(f"e.{k}")) for k in keys])


def reconcile(actual: DataFrame, expected: DataFrame, keys: list[str] = KEYS,
              measures: list[str] = MEASURES, max_diffs: int = 20) -> ParityReport:
    report = ParityReport()

    n_act, n_exp = actual.count(), expected.count()
    report.checks.append(Check("row count", str(n_exp), str(n_act), n_act == n_exp))

    sums_a = actual.agg(*[F.sum(m).alias(m) for m in measures]).first()
    sums_e = expected.agg(*[F.sum(m).alias(m) for m in measures]).first()
    for m in measures:
        report.checks.append(Check(f"sum({m})", str(sums_e[m]), str(sums_a[m]), sums_a[m] == sums_e[m]))

    dup_keys = actual.groupBy(*keys).count().filter("count > 1").count()
    report.checks.append(Check("duplicate keys", "0", str(dup_keys), dup_keys == 0))

    a = actual.select(*keys, *measures, F.lit(True).alias("_in_a")).alias("a")
    e = expected.select(*keys, *measures, F.lit(True).alias("_in_e")).alias("e")
    joined = a.join(e, _null_safe_on(keys), "full_outer")
    same = functools.reduce(lambda x, y: x & y, [F.col(f"a.{m}").eqNullSafe(F.col(f"e.{m}")) for m in measures])
    bad = joined.filter(F.col("_in_a").isNull() | F.col("_in_e").isNull() | ~same)
    n_bad = bad.count()
    report.checks.append(Check("row-level diffs", "0", str(n_bad), n_bad == 0))

    sample = bad.select(
        F.col("_in_a"), F.col("_in_e"),
        *[F.when(F.col("_in_a"), F.col(f"a.{k}")).otherwise(F.col(f"e.{k}")).alias(k) for k in keys],
        *[F.col(f"a.{m}").alias(f"a_{m}") for m in measures],
        *[F.col(f"e.{m}").alias(f"e_{m}") for m in measures],
    ).limit(max_diffs).collect()
    for r in sample:
        report.diffs.append({
            "status": "missing" if not r["_in_a"] else "extra" if not r["_in_e"] else "changed",
            "key": {k: r[k] for k in keys},
            "legacy": {m: r[f"e_{m}"] for m in measures} if r["_in_e"] else None,
            "pyspark": {m: r[f"a_{m}"] for m in measures} if r["_in_a"] else None,
        })
    return report


def read_legacy_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", "true").schema(LEGACY_SCHEMA).csv(path)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--actual", required=True, help="Parquet output of the PySpark job")
    p.add_argument("--expected", required=True, help="Legacy output (CSV with header)")
    args = p.parse_args(argv)

    spark = build_spark("coffee_sales_parity")
    try:
        report = reconcile(spark.read.parquet(args.actual), read_legacy_csv(spark, args.expected))
        print(report.render())
    finally:
        spark.stop()
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
