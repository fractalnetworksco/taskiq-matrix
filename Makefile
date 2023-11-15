SHELL=/bin/bash

TEST=""

.PHONY: synapse setup

py-lint-install:
	pip install black isort

py-lint:
	black device/
	isort --profile=black device/

.synapse:
	docker compose -f ./synapse/docker-compose.yml up synapse -d --force-recreate --build --wait
	touch .synapse

element:
	docker compose -f ./synapse/docker-compose.yml up element -d --force-recreate

taskiq-matrix.dev.env: .synapse
	@echo "Creating synapse environment file"
	python test-config/prepare-test.py

test: taskiq-matrix.dev.env
	. taskiq-matrix.dev.env && pytest -k ${TEST} -s --cov-config=.coveragerc --cov=taskiq_matrix -v --asyncio-mode=auto --cov-report=lcov --cov-report=term tests/

qtest: taskiq-matrix.dev.env
	. taskiq-matrix.dev.env && pytest -k ${TEST} -s --cov-config=.coveragerc --cov=taskiq_matrix --asyncio-mode=auto --cov-report=lcov tests/

test-ci:
	docker compose -f docker-compose.test.yml up synapse --build --force-recreate -d --wait
	docker compose -f docker-compose.test.yml up tester --build --force-recreate --exit-code-from tester
	docker compose -f docker-compose.test.yml down

build:
	docker compose -f docker-compose.test.yml build

dev-install:
	pip install -e .[dev]
