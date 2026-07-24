import json
import re
from dataclasses import dataclass, fields

from pydantic import PrivateAttr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

VALID_PROTOCOLS = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
VALID_SASL = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER", "GSSAPI"}
# Mechanisms that authenticate with a username/password pair.
USERPASS_SASL = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}

VALID_AUTH_MODES = {"none", "basic", "proxy"}
VALID_ROLES = {"admin", "viewer"}

# Cluster names appear in URLs (/c/<name>/...), so keep them to a safe, path-clean set.
CLUSTER_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def parse_users(raw: str) -> dict[str, tuple[str, str]]:
    """Parse KAFKASCOPE_USERS into {name: (sha256_hex, role)}.

    Entries are `name:sha256hex:role`, separated by spaces or commas. The password
    is stored as its SHA-256 hex digest, never in the clear.
    """
    users: dict[str, tuple[str, str]] = {}
    for entry in raw.replace(",", " ").split():
        parts = entry.split(":")
        if len(parts) != 3:
            raise ValueError(
                f"KAFKASCOPE_USERS entry must be name:sha256hex:role, got {entry!r}"
            )
        name, digest, role = parts[0], parts[1].lower(), parts[2].lower()
        if role not in VALID_ROLES:
            raise ValueError(f"role must be one of {sorted(VALID_ROLES)}, got {role!r}")
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError(f"password for {name!r} must be a 64-char SHA-256 hex digest")
        users[name] = (digest, role)
    return users


@dataclass(frozen=True)
class ClusterConfig:
    """One Kafka cluster this UI can point at: where it is, how to secure the
    connection, and whether writes are allowed against it.

    A dev deployment has exactly one of these, synthesised from the flat KAFKA_*
    env vars. A multi-cluster deployment lists several via KAFKASCOPE_CLUSTERS.
    """

    name: str
    brokers: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = ""
    sasl_username: str = ""
    sasl_password: str = ""
    ssl_ca_location: str = ""
    ssl_certificate_location: str = ""
    ssl_key_location: str = ""
    ssl_key_password: str = ""
    ssl_endpoint_identification_algorithm: str = ""
    # Optional Confluent-compatible Schema Registry for decoding Avro/Protobuf/JSON
    # payloads. `schema_registry_auth` is a "user:password" pair for HTTP Basic.
    schema_registry_url: str = ""
    schema_registry_auth: str = ""
    # Per-cluster kill switch: mark prod read-only and no write can reach it, even
    # for an admin, regardless of the global KAFKASCOPE_READONLY.
    readonly: bool = False


_CLUSTER_FIELDS = {f.name for f in fields(ClusterConfig)}


def _coherent_security(c: ClusterConfig) -> None:
    """Reject a half-configured SASL setup up front rather than on the first call."""
    if c.security_protocol not in VALID_PROTOCOLS:
        raise ValueError(
            f"cluster {c.name!r}: security_protocol must be one of "
            f"{sorted(VALID_PROTOCOLS)}, got {c.security_protocol!r}"
        )
    if c.sasl_mechanism and c.sasl_mechanism not in VALID_SASL:
        raise ValueError(
            f"cluster {c.name!r}: sasl_mechanism must be one of {sorted(VALID_SASL)}, "
            f"got {c.sasl_mechanism!r}"
        )
    if c.security_protocol.startswith("SASL_"):
        if not c.sasl_mechanism:
            raise ValueError(
                f"cluster {c.name!r}: sasl_mechanism is required for a SASL_* protocol"
            )
        if c.sasl_mechanism in USERPASS_SASL and not (c.sasl_username and c.sasl_password):
            raise ValueError(
                f"cluster {c.name!r}: sasl_username and sasl_password are required for "
                f"{c.sasl_mechanism}"
            )


def _cluster_from_dict(data: dict) -> ClusterConfig:
    unknown = set(data) - _CLUSTER_FIELDS
    if unknown:
        raise ValueError(f"unknown cluster keys {sorted(unknown)}; allowed: {sorted(_CLUSTER_FIELDS)}")

    name = str(data.get("name", "")).strip()
    if not CLUSTER_NAME_RE.match(name):
        raise ValueError(f"cluster name {name!r} must match {CLUSTER_NAME_RE.pattern}")
    brokers = str(data.get("brokers", "")).strip()
    if not brokers:
        raise ValueError(f"cluster {name!r} needs a non-empty 'brokers'")

    cluster = ClusterConfig(
        name=name,
        brokers=brokers,
        security_protocol=str(data.get("security_protocol", "PLAINTEXT")).strip().upper(),
        sasl_mechanism=str(data.get("sasl_mechanism", "")).strip().upper(),
        sasl_username=str(data.get("sasl_username", "")),
        sasl_password=str(data.get("sasl_password", "")),
        ssl_ca_location=str(data.get("ssl_ca_location", "")),
        ssl_certificate_location=str(data.get("ssl_certificate_location", "")),
        ssl_key_location=str(data.get("ssl_key_location", "")),
        ssl_key_password=str(data.get("ssl_key_password", "")),
        ssl_endpoint_identification_algorithm=str(
            data.get("ssl_endpoint_identification_algorithm", "")
        ),
        schema_registry_url=str(data.get("schema_registry_url", "")).strip(),
        schema_registry_auth=str(data.get("schema_registry_auth", "")),
        readonly=bool(data.get("readonly", False)),
    )
    _coherent_security(cluster)
    return cluster


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False)

    # Matches kafdrop's env var name so the compose block stays familiar.
    kafka_brokerconnect: str = "kafka:9092"
    kafkascope_readonly: bool = False
    kafkascope_cluster_name: str = "dev"

    # Ceiling on how many records any single scan will read before giving up.
    # Kafka has no index; every search is a scan, so this is the blast radius.
    kafkascope_scan_limit: int = 50_000
    kafkascope_request_timeout: float = 10.0

    # --- Connection security (the single-cluster / default-cluster case) ---
    # A dev broker is PLAINTEXT; a real one is almost always SASL_SSL or mTLS.
    # These map straight onto librdkafka config keys of the same shape. When
    # KAFKASCOPE_CLUSTERS is set these describe nothing — each cluster carries its own.
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str = ""
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    kafka_ssl_ca_location: str = ""
    kafka_ssl_certificate_location: str = ""
    kafka_ssl_key_location: str = ""
    kafka_ssl_key_password: str = ""
    # Set to "none" to skip broker hostname verification (self-signed dev certs).
    kafka_ssl_endpoint_identification_algorithm: str = ""

    # --- Multiple clusters ---
    # Optional JSON array of cluster objects: point one UI at dev/staging/prod. Each
    # object needs at least {"name", "brokers"} and may carry the same security keys
    # as above (without the kafka_ssl_ prefix) plus "readonly". When empty, a single
    # cluster named KAFKASCOPE_CLUSTER_NAME is built from the flat vars above.
    #   [{"name":"dev","brokers":"kafka:9092"},
    #    {"name":"prod","brokers":"p:9093","security_protocol":"SASL_SSL",
    #     "sasl_mechanism":"SCRAM-SHA-512","sasl_username":"ui","sasl_password":"…",
    #     "readonly":true}]
    kafkascope_clusters: str = ""

    # --- Schema Registry (the single-cluster / default-cluster case) ---
    # A Confluent-compatible registry URL used to decode Avro/Protobuf/JSON message
    # payloads written in the standard wire format. Auth is "user:password" for HTTP
    # Basic. When KAFKASCOPE_CLUSTERS is set, each cluster carries its own instead.
    kafkascope_schema_registry_url: str = ""
    kafkascope_schema_registry_auth: str = ""

    # --- Authentication & authorisation ---
    # "none" (default) = no login, everyone is an admin: the zero-friction dev tool.
    # "basic" = HTTP Basic against KAFKASCOPE_USERS. "proxy" = trust an auth proxy's
    # identity headers. Writes require the "admin" role; "viewer" is read-only.
    kafkascope_auth_mode: str = "none"
    kafkascope_users: str = ""
    kafkascope_proxy_user_header: str = "X-Forwarded-Email"
    kafkascope_proxy_groups_header: str = "X-Forwarded-Groups"
    # Comma-separated groups granting admin in proxy mode; empty = all authenticated.
    kafkascope_admin_groups: str = ""

    # --- CSRF ---
    # Secret used to sign CSRF tokens. Leave empty for a per-process random key
    # (fine for a single instance; tokens simply reset on restart). Set an explicit
    # value if you run several replicas behind a load balancer, so a token minted by
    # one instance validates on another.
    kafkascope_secret_key: str = ""

    # --- Audit & overview ---
    # Mutations always log a JSON audit line to stdout; set a path to also append there.
    kafkascope_audit_log: str = ""
    # Load per-topic message counts lazily on the overview (protects large clusters).
    kafkascope_show_counts: bool = True

    _clusters: dict[str, ClusterConfig] = PrivateAttr(default_factory=dict)

    @property
    def admin_groups_set(self) -> set[str]:
        return {g.strip() for g in self.kafkascope_admin_groups.replace(",", " ").split() if g.strip()}

    @property
    def clusters(self) -> dict[str, ClusterConfig]:
        """All configured clusters, keyed by name, insertion-ordered."""
        return self._clusters

    @property
    def default_cluster(self) -> ClusterConfig:
        """The cluster used when a request carries no /c/<name> prefix."""
        return next(iter(self._clusters.values()))

    @property
    def multi_cluster(self) -> bool:
        return len(self._clusters) > 1

    @field_validator("kafka_security_protocol", mode="before")
    @classmethod
    def _normalise_protocol(cls, v: object) -> str:
        text = str(v).strip().upper()
        if text not in VALID_PROTOCOLS:
            raise ValueError(
                f"kafka_security_protocol must be one of {sorted(VALID_PROTOCOLS)}, got {text!r}"
            )
        return text

    @field_validator("kafka_sasl_mechanism", mode="before")
    @classmethod
    def _normalise_mechanism(cls, v: object) -> str:
        text = str(v).strip().upper()
        if text and text not in VALID_SASL:
            raise ValueError(
                f"kafka_sasl_mechanism must be one of {sorted(VALID_SASL)}, got {text!r}"
            )
        return text

    @field_validator("kafkascope_auth_mode", mode="before")
    @classmethod
    def _normalise_auth_mode(cls, v: object) -> str:
        text = str(v).strip().lower()
        if text not in VALID_AUTH_MODES:
            raise ValueError(
                f"kafkascope_auth_mode must be one of {sorted(VALID_AUTH_MODES)}, got {text!r}"
            )
        return text

    @model_validator(mode="after")
    def _check_sasl_coherent(self) -> "Settings":
        # Only meaningful for the flat-var default cluster; multi-cluster entries are
        # each checked in _cluster_from_dict.
        if not self.kafkascope_clusters.strip():
            if self.kafka_security_protocol.startswith("SASL_"):
                if not self.kafka_sasl_mechanism:
                    raise ValueError(
                        "kafka_sasl_mechanism is required when kafka_security_protocol is SASL_*"
                    )
                if self.kafka_sasl_mechanism in USERPASS_SASL and not (
                    self.kafka_sasl_username and self.kafka_sasl_password
                ):
                    raise ValueError(
                        f"kafka_sasl_username and kafka_sasl_password are required for "
                        f"{self.kafka_sasl_mechanism}"
                    )
        return self

    @model_validator(mode="after")
    def _check_auth_coherent(self) -> "Settings":
        # Basic mode with no users would lock everyone out — fail at startup instead.
        if self.kafkascope_auth_mode == "basic":
            if not parse_users(self.kafkascope_users):
                raise ValueError("kafkascope_auth_mode=basic requires KAFKASCOPE_USERS")
        return self

    @model_validator(mode="after")
    def _build_clusters(self) -> "Settings":
        self._clusters = self._resolve_clusters()
        return self

    def _resolve_clusters(self) -> dict[str, ClusterConfig]:
        raw = self.kafkascope_clusters.strip()
        if not raw:
            # Backwards-compatible single cluster from the flat vars — a dev box needs
            # no clusters JSON at all and keeps working exactly as before.
            legacy = ClusterConfig(
                name=self.kafkascope_cluster_name.strip() or "default",
                brokers=self.kafka_brokerconnect,
                security_protocol=self.kafka_security_protocol,
                sasl_mechanism=self.kafka_sasl_mechanism,
                sasl_username=self.kafka_sasl_username,
                sasl_password=self.kafka_sasl_password,
                ssl_ca_location=self.kafka_ssl_ca_location,
                ssl_certificate_location=self.kafka_ssl_certificate_location,
                ssl_key_location=self.kafka_ssl_key_location,
                ssl_key_password=self.kafka_ssl_key_password,
                ssl_endpoint_identification_algorithm=self.kafka_ssl_endpoint_identification_algorithm,
                schema_registry_url=self.kafkascope_schema_registry_url.strip(),
                schema_registry_auth=self.kafkascope_schema_registry_auth,
                readonly=False,
            )
            if not CLUSTER_NAME_RE.match(legacy.name):
                raise ValueError(
                    f"KAFKASCOPE_CLUSTER_NAME {legacy.name!r} must match {CLUSTER_NAME_RE.pattern}"
                )
            return {legacy.name: legacy}

        try:
            entries = json.loads(raw)
        except ValueError as exc:
            raise ValueError(f"KAFKASCOPE_CLUSTERS is not valid JSON: {exc}") from exc
        if not isinstance(entries, list) or not entries:
            raise ValueError("KAFKASCOPE_CLUSTERS must be a non-empty JSON array of objects")

        clusters: dict[str, ClusterConfig] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError(f"each KAFKASCOPE_CLUSTERS entry must be an object, got {entry!r}")
            cluster = _cluster_from_dict(entry)
            if cluster.name in clusters:
                raise ValueError(f"duplicate cluster name {cluster.name!r} in KAFKASCOPE_CLUSTERS")
            clusters[cluster.name] = cluster
        return clusters


settings = Settings()
