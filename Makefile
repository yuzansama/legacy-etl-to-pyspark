PY ?= python

.PHONY: install test lint demo clean

install:
	$(PY) -m pip install -e ".[dev]"

test:
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check .

# Run both days, then reconcile the result against the legacy output.
demo: clean
	$(PY) -m coffee_sales.job --run-date 2026-10-27
	$(PY) -m coffee_sales.job --run-date 2026-10-28
	$(PY) -m coffee_sales.parity --actual out/curated/sales_by_store --expected legacy/expected/sales_by_store.csv

clean:
	rm -rf out spark-warehouse
