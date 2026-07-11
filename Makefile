.PHONY: install test lint typecheck verify run docker-build docker-run

install:
	python3 -m pip install -e '.[dev]'

test:
	python3 -m pytest

lint:
	python3 -m ruff check .

typecheck:
	python3 -m mypy

verify: lint typecheck test

run:
	python3 -m market_data_service

docker-build:
	docker compose build

docker-run:
	docker compose up
