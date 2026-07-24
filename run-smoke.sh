#!/usr/bin/env bash
# Browser smoke test: drives a real Chromium through the running app's HTML/HTMX/SSE.
#
# Runs inside the official Playwright image (browsers + all system libs preinstalled —
# a bare host is usually missing libatk/libgbm/etc), joined to the bundled stack's
# network so it reaches the app as kafkascope:9000. Start the app first:
# `docker compose up -d`.
#
#   ./run-smoke.sh                 # whole smoke suite
#   ./run-smoke.sh -k golden       # args pass straight through to pytest
set -euo pipefail
cd "$(dirname "$0")"

# Must match the playwright pin in requirements-smoke.txt (pytest-playwright 0.5.2).
IMAGE=mcr.microsoft.com/playwright/python:v1.47.0-jammy

docker run --rm --network kafkascope_default \
    -e KAFKASCOPE_BASE_URL="${KAFKASCOPE_BASE_URL:-http://kafkascope:9000}" \
    -e KAFKASCOPE_CLUSTER="${KAFKASCOPE_CLUSTER:-local}" \
    -v "$(pwd)/tests:/work/tests" \
    -v "$(pwd)/pytest.ini:/work/pytest.ini" \
    -w /work "$IMAGE" \
    sh -c "pip install -q pytest==8.3.4 pytest-playwright==0.5.2 playwright==1.47.0 httpx==0.28.1 && \
           python -m pytest -m smoke ${*:-tests/test_smoke.py}"
