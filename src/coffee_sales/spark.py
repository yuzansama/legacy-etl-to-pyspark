from __future__ import annotations

from pyspark.sql import SparkSession

# Settings the job's results depend on. They are set here so local runs, tests and the
# cluster behave the same way.
DEFAULT_CONF = {
    # to_date()/to_timestamp() must not depend on the machine's time zone.
    "spark.sql.session.timeZone": "UTC",
    # Use the strict date parser only: an unparseable timestamp becomes NULL (and is quarantined)
    # instead of raising an upgrade exception or being parsed leniently.
    "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    # Cosmetic: no progress bars in logs.
    "spark.ui.showConsoleProgress": "false",
}


def build_spark(app_name: str, master: str | None = None, conf: dict[str, str] | None = None) -> SparkSession:
    """On a cluster, leave `master` unset so spark-submit / the platform decides."""
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    for key, value in {**DEFAULT_CONF, **(conf or {})}.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
