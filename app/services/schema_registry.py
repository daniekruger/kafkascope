"""A tiny Confluent-compatible Schema Registry client.

Only the read path is needed: given a schema id (from the 5-byte wire header on a
message), fetch its schema so the payload can be decoded. Schemas are immutable by
id, so results are cached forever; a failed lookup is cached too, to stop a topic
full of non-registry binary from hammering the registry on every scan.

Deliberately built on urllib rather than adding an HTTP dependency — the calls are
trivial GETs. One client is kept per (url, auth), resolved from the current cluster.
"""

import json
import threading
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64encode
from dataclasses import dataclass

_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"

from ..config import settings
from ..kafka_client import current_cluster

# Registry down/unreachable shouldn't stall a scan on the event loop; keep this well
# under the broker request timeout.
_HTTP_TIMEOUT = 5.0


@dataclass(frozen=True)
class RegisteredSchema:
    schema_id: int
    schema_type: str  # "AVRO" | "PROTOBUF" | "JSON"
    schema_str: str


class SchemaRegistryError(Exception):
    pass


class SchemaRegistry:
    def __init__(self, url: str, auth: str = ""):
        self.url = url.rstrip("/")
        self.auth = auth
        self._cache: dict[int, RegisteredSchema | None] = {}
        self._lock = threading.Lock()

    def _auth_header(self, req: urllib.request.Request) -> None:
        req.add_header("Accept", f"{_CONTENT_TYPE}, application/json")
        if self.auth:
            req.add_header("Authorization", "Basic " + b64encode(self.auth.encode()).decode())

    def _get(self, path: str) -> dict:
        req = urllib.request.Request(f"{self.url}{path}")
        self._auth_header(req)
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _post(self, path: str, body: dict) -> dict:
        req = urllib.request.Request(
            f"{self.url}{path}", data=json.dumps(body).encode("utf-8"), method="POST"
        )
        self._auth_header(req)
        req.add_header("Content-Type", _CONTENT_TYPE)
        try:
            with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            # The registry reports rejections (invalid or incompatible schema) as a JSON
            # body with a human message — surface that rather than a bare 4xx/5xx.
            detail = exc.read().decode("utf-8", "replace")
            try:
                detail = json.loads(detail).get("message", detail)
            except ValueError:
                pass
            raise SchemaRegistryError(detail.strip() or f"registry returned {exc.code}") from exc

    def schema_by_id(self, schema_id: int) -> RegisteredSchema | None:
        """Return the schema for an id, or None if the registry doesn't have it.

        A None result (unknown id, registry error) is cached so repeated non-registry
        payloads with the same leading bytes don't re-hit the network each scan.
        """
        with self._lock:
            if schema_id in self._cache:
                return self._cache[schema_id]

        try:
            data = self._get(f"/schemas/ids/{schema_id}")
            schema = RegisteredSchema(
                schema_id=schema_id,
                # The registry omits schemaType for Avro (it predates the others).
                schema_type=(data.get("schemaType") or "AVRO").upper(),
                schema_str=data.get("schema", ""),
            )
        except (urllib.error.URLError, ValueError, OSError):
            schema = None

        with self._lock:
            self._cache[schema_id] = schema
        return schema

    def latest_by_subject(self, subject: str) -> RegisteredSchema | None:
        """The latest registered schema for a subject, or None if the subject is unknown.

        Not cached — unlike lookup-by-id, the latest version can change over time.
        """
        try:
            data = self._get(f"/subjects/{urllib.parse.quote(subject, safe='')}/versions/latest")
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            raise SchemaRegistryError(f"registry returned {exc.code} for {subject}") from exc
        return RegisteredSchema(
            schema_id=data["id"],
            schema_type=(data.get("schemaType") or "AVRO").upper(),
            schema_str=data.get("schema", ""),
        )

    def register(self, subject: str, schema_str: str, schema_type: str) -> int:
        """Register a schema under a subject (creating a new version) and return its id.

        Idempotent on the registry side: re-registering an identical schema returns the
        existing id rather than making a new version.
        """
        body: dict[str, str] = {"schema": schema_str}
        if schema_type.upper() != "AVRO":  # the registry defaults to AVRO when omitted
            body["schemaType"] = schema_type.upper()
        data = self._post(f"/subjects/{urllib.parse.quote(subject, safe='')}/versions", body)
        return int(data["id"])


_clients: dict[tuple[str, str], SchemaRegistry] = {}
_clients_lock = threading.Lock()


def registry_for_cluster() -> SchemaRegistry | None:
    """The registry client for the request's current cluster, or None if unconfigured."""
    cluster = current_cluster()
    url = getattr(cluster, "schema_registry_url", "") or settings.kafkascope_schema_registry_url
    if not url:
        return None
    auth = getattr(cluster, "schema_registry_auth", "") or settings.kafkascope_schema_registry_auth
    key = (url, auth)
    with _clients_lock:
        client = _clients.get(key)
        if client is None:
            client = SchemaRegistry(url, auth)
            _clients[key] = client
    return client
