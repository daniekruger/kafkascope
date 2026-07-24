#!/usr/bin/env bash
# Run the test suite in a throwaway container on the bundled stack (docker-compose.yml).
# Integration tests hit the broker at $KAFKA_BROKERCONNECT (kafka:9092) and skip when it
# isn't reachable — bring the stack up first (`docker compose up -d`) to include them.
#
#   ./run-tests.sh                 # whole suite
#   ./run-tests.sh -k security     # pass args straight through to pytest
set -euo pipefail
cd "$(dirname "$0")"

# --no-deps: don't spin up Kafka just to run the (broker-optional) unit tests.
# --user root: the image runs as non-root, but installing dev deps at runtime needs
# write access to site-packages. ./app is mounted so tests run against the working
# tree rather than the code baked into the image.
docker compose run --rm --no-deps --user root \
    -v "$(pwd)/tests:/app/tests" \
    -v "$(pwd)/pytest.ini:/app/pytest.ini" \
    -v "$(pwd)/requirements.txt:/app/requirements.txt" \
    -v "$(pwd)/requirements-dev.txt:/app/requirements-dev.txt" \
    -v "$(pwd)/app:/app/app" \
    -v "$(pwd)/examples:/app/examples" \
    --entrypoint sh kafkascope -c \
    'pip install -q -r requirements-dev.txt && python -m pytest "$@"' _ "${@:-tests/}"
