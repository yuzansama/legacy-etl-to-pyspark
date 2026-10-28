"""Data-quality guards that fail loudly instead of letting bad data through."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(RuntimeError):
    """Raised when a pipeline invariant is broken."""


def assert_unique(df: DataFrame, keys: list[str], name: str, sample: int = 5) -> None:
    dupes = df.groupBy(*keys).count().filter(F.col("count") > 1)
    found = dupes.limit(sample).collect()
    if found:
        shown = ", ".join(str({k: r[k] for k in keys}) for r in found)
        raise DataQualityError(f"{name}: duplicate key(s) {keys}: {shown}")
