.PHONY: test lint fmt

test: lint
	pytest -q

lint:
	ruff format --check .
	ruff check .

fmt:
	ruff format .

