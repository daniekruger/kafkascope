# Example schemas

Ready-to-use schemas and matching sample values for kafkascope's schema-aware
producing (and for testing the Avro / JSON Schema decode path).

| Schema | Sample value | Notes |
| --- | --- | --- |
| [`orders.avsc`](orders.avsc) | [`orders-value.json`](orders-value.json) | Avro record: enum, array of records, a nullable `notes` union |
| [`page-view.schema.json`](page-view.schema.json) | [`page-view-value.json`](page-view-value.json) | JSON Schema object: required fields, an enum, a nested object |

## Use them from the Produce form

1. Start a registry and point kafkascope at it — the bundled compose ships one:
   ```bash
   docker compose --profile schema-registry up -d
   ```
2. Open any topic → **Produce**, and set **Value encoding** to `Avro` or `JSON Schema`.
3. Paste the schema (e.g. `orders.avsc`) into the schema box and tick **Register**
   (only needed the first time — after that the topic's schema loads automatically).
4. Paste the matching sample value (e.g. `orders-value.json`) into **Value** and **Send**.
5. Browse the topic — the message decodes back to its JSON with an `avro #N` badge.

## Use them from the command line

Register a schema against a subject (`<topic>-value`) directly:

```bash
# Avro — the schema string must be JSON-escaped; jq does that with -Rs.
curl -s -X POST http://localhost:8087/subjects/orders-value/versions \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d "$(jq -Rs '{schema: .}' < examples/orders.avsc)"

# JSON Schema — same, but tell the registry the type:
curl -s -X POST http://localhost:8087/subjects/events-value/versions \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d "$(jq -Rs '{schema: ., schemaType: "JSON"}' < examples/page-view.schema.json)"
```

## A note on the Avro value

kafkascope serializes the value you type as a plain JSON object, so a nullable union
field like `notes` (`["null", "string"]`) is written as the bare value —
`"notes": "leave at the front door"` or `"notes": null` — **not** Avro's
`{"string": "..."}` JSON-union encoding.
