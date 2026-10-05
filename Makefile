.PHONY: up down logs test lint fmt

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f api outbox-relay consumer webhook-sink

test:
	docker compose --profile test run --rm --build tests

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff format .
	uv run ruff check --fix .
