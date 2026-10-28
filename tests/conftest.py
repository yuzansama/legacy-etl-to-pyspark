import os
import sys

import pytest

from coffee_sales.spark import build_spark

# Python workers (used by createDataFrame on local data) must run the same interpreter as the tests.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    session = build_spark(
        "coffee_sales_tests",
        master="local[2]",
        conf={"spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"},
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
