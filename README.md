# legacy-etl-to-pyspark

[![ci](https://github.com/yuzansama/legacy-etl-to-pyspark/actions/workflows/ci.yml/badge.svg)](https://github.com/yuzansama/legacy-etl-to-pyspark/actions/workflows/ci.yml)

A legacy ETL job rebuilt as a **reliable, tested PySpark pipeline**, with a **record-level parity check**
against the legacy output.

This is how I approach legacy ETL modernization (Ab Initio → PySpark) at work, shown on a small,
fictional job so everything can be public. No client code, names or data.

- Interactive version (in the browser, no Spark): [Pipeline Playground](https://yuzansama.github.io/CV/playground.html)
- About me: [yuzansama.github.io/CV](https://yuzansama.github.io/CV/)

## The job

`coffee_sales_summary` reads a day of coffee-shop orders, cleans them, enriches them with the bean origin
from a SKU catalog and writes daily sales per store and origin.

```
orders.dat ─► Reformat ─► Filter by Expression ─► Join ─► Rollup ─► sales_by_store (partitioned by sale_date)
                               │ deselect               ▲
                               ▼                        │
                          quarantine             sku_catalog (lookup)
```

The legacy graph is described in [`legacy/`](legacy/) (record format and transforms), and its output for
the sample data is in [`legacy/expected/`](legacy/expected/). That expected output was computed by a
separate plain-Python implementation, so the PySpark job is checked against something independent.

## Legacy component → PySpark

| Legacy component | PySpark ([`src/coffee_sales/job.py`](src/coffee_sales/job.py)) | Why it is done this way |
|---|---|---|
| Input File + DML record format | `read_orders`: explicit `StructType`, `mode=FAILFAST` | No schema inference. A line with the wrong number of fields stops the job before anything is written. |
| Reformat | `reformat`: one column expression per rule | `try_to_timestamp` / `try_cast(... AS DECIMAL(18,2))` turn bad values into NULL, the same in ANSI and non-ANSI mode. The raw text is kept for the quarantine. |
| Filter by Expression (select / deselect) | `split_valid` | `coalesce(cond, false)`. In SQL a NULL condition is neither true nor false, so a naive `filter(cond)` / `filter(~cond)` pair loses those rows. Every reject gets a reason. |
| Join with lookup | `enrich`: broadcast **left** join | Unknown SKUs are kept (legacy outer join). The lookup key must be unique, otherwise the job fails instead of multiplying orders. |
| Rollup | `rollup`: `groupBy().agg()` | Money stays `decimal(18,2)` end to end. No floats. |
| Output Table (truncate & load) | `write_outputs`: dynamic partition overwrite | Re-running a day (retry, backfill) replaces only that day's partition. No duplicates. |

`run()` also checks at runtime that `valid + rejected == input`, so a row can never disappear unnoticed.

## Proving parity

[`src/coffee_sales/parity.py`](src/coffee_sales/parity.py) reconciles the PySpark output with the legacy output:

- row count and sum of every measure,
- duplicate keys (what a non-idempotent re-run produces),
- a row-level diff with a full outer join on the keys, **null-safe** (`eqNullSafe`), because
  `bean_origin` is NULL for unknown SKUs and `NULL = NULL` is not true in SQL.

```
$ make demo
...
  check                       legacy           pyspark
✓ row count                       15                15
✓ sum(tickets)                    18                18
✓ sum(gross_sales)         526258.45         526258.45
✓ duplicate keys                   0                 0
✓ row-level diffs                  0                 0
PARITY: PASS (0 mismatches)
```

It exits with status 1 on any difference, so it can gate a cut-over in CI or in a scheduler.

## Tests

`pytest` runs on a local Spark session, and CI runs it on PySpark 3.5 and 4.0
(ANSI mode off and on by default):

- one unit test per legacy component, pinning the behaviour above;
- a test that documents the NULL pitfall (`test_naive_filter_loses_rows_with_null_condition`);
- end-to-end: both sample days → output equals legacy record for record, quarantine equals legacy rejects;
- re-running a day three times still matches legacy (idempotency);
- a malformed file fails before anything is written; `--dry-run` writes nothing.

I checked that the tests catch real mistakes by breaking the job on purpose: removing the null-safe
`coalesce`, using an inner join, appending instead of overwriting, or skipping key normalization
each makes the suite fail.

## Run it

Needs Python 3.10+ and Java 17+.

```bash
python -m venv .venv && . .venv/bin/activate
make install   # pyspark + pytest + ruff
make test
make demo      # runs 2026-10-27 and 2026-10-28, then the parity check
```

One day by hand:

```bash
python -m coffee_sales.job --run-date 2026-10-27 [--dry-run] [--min-amount 0] [--mode FAILFAST|PERMISSIVE]
```

Output: `out/curated/sales_by_store/sale_date=…/` and `out/quarantine/orders/run_date=…/` (Parquet).

## On a cloud Spark platform

Nothing in the job is tied to local files: paths come from arguments, and the session settings the
results depend on (UTC time zone, strict date parsing) are set in
[`spark.py`](src/coffee_sales/spark.py). With any managed Spark (EMR, Dataproc, Databricks, Kubernetes, …):

```bash
pip install build && python -m build --wheel
spark-submit --py-files dist/coffee_sales-0.1.0-py3-none-any.whl \
  src/coffee_sales/job.py \
  --run-date 2026-10-27 \
  --landing   s3a://my-bucket/landing \
  --catalog   s3a://my-bucket/ref/sku_catalog.csv \
  --curated   s3a://my-bucket/curated \
  --quarantine s3a://my-bucket/quarantine
```

The scheduler runs one `--run-date` per day. Because writes are idempotent, a failed day can simply be
re-run.

## Layout

```
src/coffee_sales/job.py       the job, one function per legacy component
src/coffee_sales/parity.py    reconciliation against legacy output (also a CLI)
src/coffee_sales/quality.py   data-quality guards
src/coffee_sales/spark.py     SparkSession with the settings results depend on
legacy/                       legacy record format, transforms and expected output
data/                         sample landing files and the SKU catalog
tests/                        unit and end-to-end tests
```

## License

MIT
