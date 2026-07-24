# Contributing to kafkascope

Thanks for taking the time to help. kafkascope aims to be a small, sharp Kafka
UI: server-rendered, no build step, easy to read end to end. Contributions that
keep it that way are very welcome.

## Ways to contribute

- **Report a bug** — open an issue with steps to reproduce, what you expected, and
  what happened. Include your Kafka version and how kafkascope is configured
  (auth mode, single vs. multi-cluster) where relevant.
- **Suggest a feature** — open an issue describing the problem you're trying to
  solve, not just the solution. Small, focused features fit best.
- **Send a pull request** — for anything non-trivial, please open an issue first
  so we can agree on the approach before you invest the time.

## Development setup

kafkascope runs against a Kafka broker. The quickest way to a working environment is
the self-contained stack (bundled single-node Kafka):

```bash
docker compose up --build       # app on http://localhost:8080
```

To iterate on the code with live reload, run the app directly against a broker — the
bundled Kafka exposes `localhost:29092` for host tools:

```bash
pip install -r requirements.txt
KAFKA_BROKERCONNECT=localhost:29092 uvicorn app.main:app --reload
```

The app is FastAPI + Jinja2 + HTMX with vendored htmx.min.js — there is no
front-end build to run. Templates live in `app/templates/`, styles in
`app/static/app.css`, and each Kafka concern has its own module under
`app/services/`.

## Running the tests

```bash
./run-tests.sh              # unit + integration (integration skips if no broker)
./run-tests.sh -k csrf      # args pass straight through to pytest
./run-smoke.sh              # Playwright browser smoke test (needs the app running)
```

Please add or update tests for any behaviour you change. Unit tests should not
require a broker; integration tests skip automatically when one isn't reachable.

## Pull request checklist

- [ ] Tests pass (`./run-tests.sh`).
- [ ] New behaviour is covered by a test.
- [ ] The change keeps the "no build step, readable end to end" spirit.
- [ ] Docs/README updated if you changed configuration or user-facing behaviour.

## Security

Please do **not** open a public issue for security vulnerabilities. See
[SECURITY.md](SECURITY.md) for how to report them privately.

## License

By contributing, you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE), the same license as the project.
