# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-07-24

First public release.

### Added
- **Topics** — browse topics with lazy message counts, view partitions,
  watermarks, and configuration; create, delete, purge, add partitions, and edit
  config, with typed confirmation on destructive actions.
- **Messages** — search/scan a topic with header and pretty-printed JSON views,
  produce messages, and a **live tail** mode with filtering and pin-to-bottom
  autoscroll.
- **Schema Registry** — decode Avro / JSON Schema / Protobuf messages inline (each
  shown with a schema badge), and produce Avro / JSON Schema **keys and values** from
  the UI, reusing the topic's registered schema or registering a new version. Configure
  with `KAFKASCOPE_SCHEMA_REGISTRY_URL`; the bundled compose ships an optional registry
  (`--profile schema-registry`). Producing Protobuf isn't supported (it needs the
  compiled `.proto`). Example schemas live in [`examples/`](examples/).
- **Consumer groups** — list groups with lag, inspect per-partition offsets and
  lag, and reset or delete offsets (typed confirmation).
- **Multi-cluster** — point one UI at several clusters via `KAFKASCOPE_CLUSTERS`,
  switch with the header picker; each cluster can be marked read-only.
- **Auto-refresh** — an interval toggle on the groups list, group detail, topic
  detail, and topic overview.
- **Light and dark themes** with a no-flash toggle, remembered per browser.
- **Authentication** — `none` (default), HTTP `basic`, or `proxy` (trusted
  identity headers); writes require the admin role.
- **Read-only mode** — global (`KAFKASCOPE_READONLY`) or per-cluster kill switch.
- **Audit log** — every mutation attempt logged as JSON (success, error, denied).
- **CSRF protection** on write requests when authentication is enabled.
- **Security headers** — CSP, `X-Frame-Options`, `X-Content-Type-Options`,
  `Referrer-Policy`.
- **`/healthz`** liveness probe, a **`/version`** endpoint, and a version footer
  linking to it.
- Hardened, non-root, multi-arch (amd64 + arm64) Docker image with a
  `HEALTHCHECK`; a self-contained docker-compose stack (with an optional
  Schema Registry profile).
- Browser smoke test suite (Playwright).

[Unreleased]: https://github.com/daniekruger/kafkascope/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/daniekruger/kafkascope/releases/tag/v1.0.0
