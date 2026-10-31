# make <target> [ENV=local|stockholm]   (default: local)
ENV ?= local
ENV_FILE := .env.$(ENV)
ifeq ($(wildcard $(ENV_FILE)),)
$(error $(ENV_FILE) not found: cp .env.example $(ENV_FILE) and fill it in)
endif
include $(ENV_FILE)
export
export ENV_FILE
export GIT_SHA := $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)

COMPOSE := docker compose --env-file $(ENV_FILE)
ENGINE := cargo run -q -p polygemy-engine --

.PHONY: db-up up down ps logs migrate check run test test-rust test-python

db-up:            ## start PostgreSQL only and wait until healthy
	$(COMPOSE) up -d --wait postgres

up:               ## build, migrate and start the full stack in Docker
	$(COMPOSE) up -d --build --wait

down:             ## stop containers (data volume is kept)
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=200

migrate: db-up    ## apply migrations from the host
	$(ENGINE) migrate

check: db-up      ## config valid + schema current
	$(ENGINE) check

run: migrate      ## run the engine on the host (Ctrl-C for graceful stop)
	$(ENGINE) run

test: db-up       ## all tests against the disposable polygemy_test database
	$(COMPOSE) exec -T postgres sh -c 'psql -U $$POSTGRES_USER -d $$POSTGRES_DB -tAc "SELECT 1 FROM pg_database WHERE datname = '"'"'polygemy_test'"'"'" | grep -q 1 || createdb -U $$POSTGRES_USER polygemy_test'
	DATABASE_URL=$(TEST_DATABASE_URL) $(ENGINE) migrate
	$(MAKE) test-rust test-python

test-rust:
	cargo test --workspace

test-python:
	cd python && uv run pytest -q
