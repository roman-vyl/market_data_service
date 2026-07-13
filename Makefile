.PHONY: install test lint typecheck verify run docker-build docker-run check-python

PYTHON ?= python3

check-python:
	@$(PYTHON) -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else "Python 3.12+ required; current interpreter is %s.%s at %s" % (sys.version_info.major, sys.version_info.minor, sys.executable))'

install: check-python
	$(PYTHON) -m pip install -e '.[dev]'

test: check-python
	$(PYTHON) -m pytest

lint: check-python
	$(PYTHON) -m ruff check .

typecheck: check-python
	$(PYTHON) -m mypy

verify: lint typecheck test

run: check-python
	$(PYTHON) -m market_data_service

docker-build:
	docker compose build

docker-run:
	docker compose up
