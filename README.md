# kafkascope

[![CI](https://github.com/daniekruger/kafkascope/actions/workflows/ci.yml/badge.svg)](https://github.com/daniekruger/kafkascope/actions/workflows/ci.yml)
[![Docker Pulls](https://img.shields.io/docker/pulls/daniekruger/kafkascope)](https://hub.docker.com/r/daniekruger/kafkascope)
[![Docker Image Version](https://img.shields.io/docker/v/daniekruger/kafkascope?sort=semver)](https://hub.docker.com/r/daniekruger/kafkascope/tags)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

A web UI to inspect and operate Apache Kafka — browse, search and produce messages,
manage consumer groups (lag, offset resets), and administer topics — with live views
(auto-refresh, message tail) and multi-cluster support.

FastAPI + Jinja + HTMX, `confluent-kafka` under the hood. No build step, no JavaScript
bundler — a single container.

![kafkascope — searching messages on a Kafka topic, with headers and pretty-printed JSON](https://raw.githubusercontent.com/daniekruger/kafkascope/main/docs/hero-messages.png)

## Quick start

Self-contained stack — a single-node Kafka plus kafkascope, nothing else needed:

```bash
docker compose up -d
```

Then open <http://localhost:8080>. The bundled Kafka is only there so you can try it
out; point kafkascope at your own broker with `KAFKA_BROKERCONNECT` (see Configuration).

Run the published image directly against your own broker:

```bash
docker run -p 8080:9000 -e KAFKA_BROKERCONNECT=your-broker:9092 daniekruger/kafkascope
```

Every page lives under `/c/<cluster>/`; `/` redirects to the default cluster. With a
single cluster (the default) that's invisible; with several it's how you switch.

### Limitations

- **Protobuf decoding is field-number keyed.** With a Schema Registry configured,
  Avro and JSON Schema payloads decode to their full structure. Protobuf has no field
  names on the wire, so those messages decode to `field_1`, `field_2`, … with the
  real values plus the schema id — pair it with the schema to read the names.

### Developing

For a full environment, use the self-contained stack above (`docker compose up`). To
iterate on the code with live reload, run the app directly against any broker — the
bundled Kafka exposes `localhost:29092` for host tools:

```bash
pip install -r requirements.txt
KAFKA_BROKERCONNECT=localhost:29092 uvicorn app.main:app --reload   # http://localhost:8000
```

## Screenshots

Cluster overview — topics, partitions, and lazily-loaded message counts, with a live
auto-refresh and theme toggle:

![Cluster overview](https://raw.githubusercontent.com/daniekruger/kafkascope/main/docs/overview.png)

Topic administration — non-default configs, add-partitions, and guarded purge / delete
(type the topic name to confirm):

![Topic administration](https://raw.githubusercontent.com/daniekruger/kafkascope/main/docs/topic-admin.png)

Both light and dark themes are built in; the toggle is in the header and the choice is
remembered. The same views in light theme:

![Messages, light theme](https://raw.githubusercontent.com/daniekruger/kafkascope/main/docs/hero-messages-light.png)

![Cluster overview, light theme](https://raw.githubusercontent.com/daniekruger/kafkascope/main/docs/overview-light.png)

![Topic administration, light theme](https://raw.githubusercontent.com/daniekruger/kafkascope/main/docs/topic-admin-light.png)

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BROKERCONNECT` | `kafka:9092` | Broker list (same name kafdrop uses) |
| `KAFKASCOPE_CLUSTER_NAME` | `dev` | Name of the single default cluster (in the header and URL) |
| `KAFKASCOPE_CLUSTERS` | _(unset)_ | JSON array to front several clusters at once — see below |
| `KAFKASCOPE_READONLY` | `false` | Blocks every mutating operation, on every cluster |
| `KAFKASCOPE_SCAN_LIMIT` | `50000` | Max records any single search will scan |
| `KAFKASCOPE_REQUEST_TIMEOUT` | `10.0` | Broker request timeout, seconds |
| `KAFKASCOPE_SCHEMA_REGISTRY_URL` | _(unset)_ | Schema Registry for decoding Avro/Protobuf/JSON — see below |
| `KAFKASCOPE_SCHEMA_REGISTRY_AUTH` | _(unset)_ | `user:password` for a registry behind HTTP Basic |

### Decoding Avro / Protobuf / JSON Schema

Point `KAFKASCOPE_SCHEMA_REGISTRY_URL` at any **Confluent-compatible** registry
(Confluent Schema Registry, or Apicurio in its Confluent-compatible mode) and
messages written in the standard wire format — magic byte, schema id, encoded body —
are decoded inline:

- **Avro** and **JSON Schema** decode to their full JSON structure.
- **Protobuf** decodes best-effort by field number (`field_1`, `field_2`, …), since
  field names aren't on the wire. The schema id is shown on each message.

Each message carries a small badge (`avro #12`, `protobuf #7`) when it was decoded
via the registry. Search filters (value contains, JSON path) run against the decoded
form, so you can search Avro fields directly. Non-encoded messages are unaffected —
plain UTF-8/JSON still renders as before, and no registry call is made for them.

![Avro messages decoded via the schema registry](https://raw.githubusercontent.com/daniekruger/kafkascope/main/docs/schema-registry.png)

The bundled compose ships a registry you can turn on:

```bash
docker compose --profile schema-registry up -d   # registry on localhost:8087
```

In a multi-cluster setup, give each cluster its own `schema_registry_url` (and
optional `schema_registry_auth`) inside its `KAFKASCOPE_CLUSTERS` entry.

#### Producing schema messages

With a registry configured, the Produce form gains **Key encoding** and **Value
encoding** pickers. Choose Avro or JSON Schema for the key, the value, or both, type
JSON, and kafkascope serializes it to the wire format and sends it — so consumers
(and kafkascope's own decoder) read it back as a proper schema message:

- By default it reuses the topic's **already-registered** schema (subjects
  `<topic>-key` and `<topic>-value`) — no change to the registry.
- Tick **Register** to register the schema you pasted, creating a new version (for a
  brand-new topic, or to evolve the schema — the registry enforces its compatibility rules).
- The input is validated against the schema before sending; a mismatch is rejected with
  the offending field, nothing is produced.
- Protobuf is decode-only — producing it generically needs the compiled `.proto`.

Ready-to-use example schemas (Avro and JSON Schema) with matching sample values are in
[`examples/`](examples/) — paste them straight into the Produce form to try it.

![Producing an Avro message against a registered schema](https://raw.githubusercontent.com/daniekruger/kafkascope/main/docs/produce-schema.png)

### Connecting to a secured cluster

A dev broker is PLAINTEXT and needs none of the below. A real cluster is almost
always `SASL_SSL` or mTLS. These map straight onto the librdkafka keys of the same
name, and the app fails fast at startup on an incoherent combination (e.g. a SASL
protocol with no mechanism).

| Variable | Purpose |
| --- | --- |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` (default), `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | `PLAIN`, `SCRAM-SHA-256/512`, `OAUTHBEARER`, `GSSAPI` |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | credentials for PLAIN/SCRAM |
| `KAFKA_SSL_CA_LOCATION` | CA cert path (verify the broker) |
| `KAFKA_SSL_CERTIFICATE_LOCATION` / `KAFKA_SSL_KEY_LOCATION` | client cert + key for mTLS |
| `KAFKA_SSL_KEY_PASSWORD` | client key passphrase, if any |
| `KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM` | set to `none` to skip hostname check (dev certs) |

Typical SASL_SSL block:

```yaml
        environment:
            KAFKA_BROKERCONNECT: broker.example.com:9093
            KAFKA_SECURITY_PROTOCOL: SASL_SSL
            KAFKA_SASL_MECHANISM: SCRAM-SHA-512
            KAFKA_SASL_USERNAME: kafkascope
            KAFKA_SASL_PASSWORD: ${KAFKASCOPE_PASSWORD}
            KAFKA_SSL_CA_LOCATION: /certs/ca.pem
            KAFKASCOPE_READONLY: "true"   # recommended when pointed at prod
```

### Pointing at several clusters

One UI can front dev, staging and prod at once. Set `KAFKASCOPE_CLUSTERS` to a JSON
array; each object needs at least `name` and `brokers`, and may carry the same
security keys as above (dropping the `kafka_ssl_` prefix — `security_protocol`,
`sasl_mechanism`, `ssl_ca_location`, …) plus a per-cluster `readonly` and
`schema_registry_url`. The first
entry is the default landing cluster; a header dropdown switches between them, and
each carries its own colour of read-only.

```yaml
        environment:
            KAFKASCOPE_CLUSTERS: >-
              [{"name":"dev","brokers":"kafka:9092"},
               {"name":"prod","brokers":"prod-broker:9093",
                "security_protocol":"SASL_SSL","sasl_mechanism":"SCRAM-SHA-512",
                "sasl_username":"kafkascope","sasl_password":"…","readonly":true}]
```

`readonly` on a cluster is enforced exactly like the global `KAFKASCOPE_READONLY`
switch — the route dependency rejects the write and the service refuses it — so
`prod` is structurally safe from a mis-click while `dev` stays fully writable.
Cluster names appear in the URL (`/c/<name>/…`), so keep them to letters, digits,
`.`, `_` and `-`. When `KAFKASCOPE_CLUSTERS` is unset, a single cluster named
`KAFKASCOPE_CLUSTER_NAME` is built from the flat `KAFKA_*` vars — nothing to change
for an existing single-broker setup.

## Authentication & audit

Off by default — `AUTH_MODE=none` makes every request an anonymous admin, so as a
dev tool it needs no setup. Turn on auth only when pointing at something real.

| Variable | Purpose |
| --- | --- |
| `KAFKASCOPE_AUTH_MODE` | `none` (default), `basic`, or `proxy` |
| `KAFKASCOPE_USERS` | basic mode: `name:sha256hex:role` entries, space/comma separated |
| `KAFKASCOPE_PROXY_USER_HEADER` | proxy mode identity header (default `X-Forwarded-Email`) |
| `KAFKASCOPE_PROXY_GROUPS_HEADER` | proxy mode groups header (default `X-Forwarded-Groups`) |
| `KAFKASCOPE_ADMIN_GROUPS` | groups granting admin in proxy mode; empty = all authenticated are admin |
| `KAFKASCOPE_AUDIT_LOG` | optional file path; mutations always also log to stdout |

Two roles: **admin** may write, **viewer** is read-only. `KAFKASCOPE_READONLY=true` is a
hard kill switch that demotes everyone to viewer regardless of role — the way to point
at prod and guarantee nobody mutates anything.

- **basic** — HTTP Basic. Passwords are stored as SHA-256 hex digests, never in the
  clear; generate one with
  `python -c 'import hashlib,sys;print(hashlib.sha256(sys.argv[1].encode()).hexdigest())' <pw>`.
  Stateless (no session), so run it behind TLS. Good for a small team.
- **proxy** — delegate real authentication to an auth proxy (oauth2-proxy, Authelia,
  ingress auth) that injects identity and group headers. No secrets in this app; the
  recommended production path.

Every **mutation** (produce, create/delete/purge topic, add partitions, config change,
offset reset, group delete) writes one JSON **audit** line — including *denied*
attempts that failed the typed confirmation:

```
AUDIT {"ts": "...", "user": "alice", "role": "admin", "action": "delete_topic",
       "target": "orders", "outcome": "denied", "ip": "...",
       "detail": {"reason": "confirmation mismatch"}}
```

Reads are not audited (too noisy); the log is about who changed what.

## Testing

```bash
./run-tests.sh                 # whole suite in a throwaway container
./run-tests.sh -k security     # args pass through to pytest
```

Unit tests (filters, JSON-path, header parsing, security-config assembly, and the
read-only / internal-topic / validation **guardrails**) need no broker and run
anywhere — that's the CI target. Integration tests do a real
create→produce→scan→purge→delete round trip against `KAFKA_BROKERCONNECT` and
**skip automatically** when no broker is reachable. They use a throwaway
`pytest-kafkascope-tmp` topic and always clean it up.

### Browser smoke test

Everything above tests Kafka logic server-side. The one thing it can't catch is a
broken template, a mis-wired `hx-post`, or a JavaScript error that kills the scan
stream — bugs that only exist in the rendered page. `./run-smoke.sh` drives a real
Chromium (in the official Playwright image, so no host browser deps) through the whole
path: overview → create topic → produce → search-over-SSE → groups → the typed-delete
guardrail → delete. It needs the **app running** (`docker compose up -d`) and only ever
touches its own `playwright-smoke-tmp` topic, deleting it again over HTTP in a `finally`.

```bash
docker compose up -d              # app must be up
./run-smoke.sh                    # runs against http://kafkascope:9000 on the bundled network
```

Smoke tests carry the `smoke` marker and are excluded from the default `pytest` run
(they need a browser and a live app), so `./run-tests.sh` stays fast and broker-only.

## Layout

```
app/
    main.py            app + error handlers
    config.py          env-driven settings
    templating.py      shared Jinja environment
    kafka_client.py    AdminClient / Consumer construction, watermarks
    services/          Kafka logic, returns plain dataclasses
        cluster.py         brokers, topic list, topic detail
        messages.py        bounded scan, filters, single-message fetch
        produce.py         delivery-confirmed sends
        groups.py          lag, offset resets, group deletion
        admin.py           create, delete, partitions, config, purge
    routers/           HTTP layer; pushes blocking calls to a threadpool
    templates/         Jinja templates
    static/            css + vendored htmx
tests/                 pytest suite (unit + broker-backed integration + browser smoke)
```

`run-tests.sh` runs the unit + integration suite in a throwaway container; `run-smoke.sh`
runs the browser smoke test against a live app in the Playwright image.

`base_config()` in `kafka_client.py` is the single place connection security is
assembled; every AdminClient, Consumer and Producer is built from it, so there is
one place to get SASL/SSL right.

`confluent-kafka` is synchronous, so service calls go through `run_in_threadpool`
rather than being awaited directly.

## Notes and gotchas

- **Never request metadata for a single topic by name.** The broker auto-creates it
  when `auto.create.topics.enable` is on (the default). `get_topic()` and the scan
  planner both fetch full cluster metadata and look the name up locally for this reason.
- **Every search is a forward scan** — Kafka has no index. A scan ends at whichever
  comes first: the result limit, `KAFKASCOPE_SCAN_LIMIT`, the end of the window, or the
  browser disconnecting. Each fetch is capped to the remaining budget, so the budget
  is exact rather than approximate.
- Scans `assign()` partitions directly and never join a consumer group or commit, so
  browsing can't disturb a real consumer's offsets.
- **Live tail** on the messages page assigns at the current end offsets and streams
  new messages as they arrive (the same filters apply), until you stop it or the tab
  closes — a `tail -f` that, like scan, never joins a group or commits. The browser
  keeps only the last 500 messages so a long tail on a busy topic can't grow unbounded.
- **Auto-refresh** is available on the group, groups-list, topic and overview pages via
  an Off/5s/10s/30s/1m dropdown (default Off — no unprompted polling). It swaps only the
  data region, never a form, so a half-typed confirmation or config edit is safe; the
  choice is remembered per page type in `localStorage`. Each poll is one describe/metadata
  call, and the lazy lag/count cells re-fetch only for the rows on screen.
- **Producing waits for the broker's delivery report** before reporting success, and
  uses `acks=all` with idempotence. A UI that says "sent" before the broker has
  acknowledged is actively misleading when you're debugging a missing message.
- **Resend re-serializes.** A JSON payload is prefilled pretty-printed, so resending
  it writes semantically identical but not byte-identical JSON. Binary payloads are
  prefilled base64 and the form says so — resending one as-is writes the base64 text.
- `KAFKASCOPE_READONLY` is enforced twice: the router rejects the request and the
  service raises on its own, so a new route can't bypass the guard by accident.
- **Destructive group operations need the group id typed back.** Resetting the wrong
  group's offsets is silent and has no undo — you find out days later from a lag graph
  or a duplicate-processing bug.
- **Offset resets require an empty group.** Kafka enforces this, but reports it as
  `Unknown member`, which explains nothing. `_require_empty()` checks first and names
  the clients still holding the group. Kafka's own rejection remains the backstop for
  the race where a consumer joins mid-request.
- Lag is `high watermark - committed offset`, and is cross-checked to agree with
  `kafka-consumer-groups.sh --describe`. Partitions with no committed offset show
  `none`/`—` rather than a misleading zero.
- **The groups list shows total lag per group, loaded lazily** (HTMX on reveal, like
  the topic overview's counts). The list itself stays a single cheap describe call;
  only the rows on screen pay the per-group offset/watermark round trips.
- **Delete-topic and purge require the topic name typed back**, and both refuse
  `__`-prefixed internal topics outright — no amount of confirmation will purge
  `__consumer_offsets`.
- **Purge is `delete_records`, not a topic drop.** It moves each partition's low
  watermark up to its high, discarding the data while the topic, partitions and
  config survive. Delete-topic removes the whole thing.
- Partitions can only grow — Kafka has no shrink — and the service rejects a smaller
  count instead of surfacing a cryptic broker error. Growing a topic changes how
  keys hash to partitions.
- Message counts are `high - low` watermarks, so they overstate slightly for
  compacted topics and count tombstones.
- Internal topics (`__`-prefixed) skip the watermark round trips — `__consumer_offsets`
  has 50 partitions and nobody is counting them.
- **The overview loads message counts lazily.** Computing them eagerly was
  O(topics × partitions) sequential watermark calls, which fell over on a large
  cluster. `list_topics()` is now a single metadata round trip; each visible row
  fetches its own count via HTMX on reveal (`/topic/{name}/count`), so total cost is
  O(visible rows). Set `KAFKASCOPE_SHOW_COUNTS=false` to drop the column entirely.
- Authorisation lives in the `require_write` route dependency (principal-aware), with
  the service-layer read-only check as a backstop. Both consult `is_readonly()`, which
  is true when either the current cluster or the whole instance is read-only.
- **The target cluster is resolved once per request from the `/c/<name>` URL** into a
  ContextVar, which propagates into the worker threads `run_in_threadpool` spins up —
  so `admin()`/`consumer()`/`base_config()` reach the right broker without every
  service signature having to carry a cluster argument. An unknown cluster name is a
  404, never a silent fall-through to the wrong broker.

## Status

- [x] **Phase 1** — cluster overview, topic list, topic detail
- [x] **Phase 2** — message browsing + search (bounded scan, SSE-streamed results)
- [x] **Phase 3** — produce, resend, edit-and-resend
- [x] **Phase 4** — consumer groups: lag, offset reset, delete
- [x] **Phase 5** — topic admin: create, delete, configs, partitions, purge
- [x] **Production** — SASL/SSL, auth (none/basic/proxy) + RBAC + audit log, lazy
  overview counts, unit/integration/browser-smoke test suites
- [x] **Multi-cluster** — front several clusters from one UI (`/c/<name>`), header
  picker, per-cluster read-only; groups list shows lazy total lag
- [x] **Live views** — auto-refresh (group, groups list, topic, overview) and a
  live-tail mode on the messages page

## License

Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).

Copyright 2026 Danie Krüger.
