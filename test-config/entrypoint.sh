#!/bin/bash

# expected environment variables:
# ENV - environment name (e.g. test, dev, prod)
# TEST_CONFIG_DIR - path to the test-config directory

set -e

PREPARE_SCRIPT="$TEST_CONFIG_DIR/prepare-test.py"

python3 "$PREPARE_SCRIPT"

# environment file should be created by prepare-test.py
source "$TEST_CONFIG_DIR/taskiq-matrix.$ENV.env"

cd /code

pytest -v --asyncio-mode=auto --cov=/code/homeserver --cov-report=lcov --cov-report=term
