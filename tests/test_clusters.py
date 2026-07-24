"""Multi-cluster configuration: parsing KAFKASCOPE_CLUSTERS, the single-cluster
fall-back, and per-cluster read-only enforcement."""

import json

import pytest
from pydantic import ValidationError

from app import kafka_client
from app.config import Settings, settings


def make(**kw) -> Settings:
    # Isolate from any ambient KAFKASCOPE_CLUSTERS on the test container: default to the
    # single-cluster (flat-var) path unless a test provides its own clusters JSON.
    kw.setdefault("kafkascope_clusters", "")
    return Settings(_env_file="", **kw)


# --- the zero-config default: one cluster from the flat vars ---

def test_single_cluster_from_flat_vars():
    s = make(kafka_brokerconnect="b:9092", kafkascope_cluster_name="dev")
    assert list(s.clusters) == ["dev"]
    assert s.default_cluster.brokers == "b:9092"
    assert s.default_cluster.readonly is False
    assert s.multi_cluster is False


# --- KAFKASCOPE_CLUSTERS: several clusters, each with its own security + readonly ---

def _clusters_json():
    return json.dumps(
        [
            {"name": "dev", "brokers": "kafka:9092"},
            {
                "name": "prod",
                "brokers": "p:9093",
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "SCRAM-SHA-512",
                "sasl_username": "ui",
                "sasl_password": "secret",
                "readonly": True,
            },
        ]
    )


def test_multi_cluster_parsing():
    s = make(kafkascope_clusters=_clusters_json())
    assert list(s.clusters) == ["dev", "prod"]
    assert s.multi_cluster is True
    # First entry is the default landing cluster.
    assert s.default_cluster.name == "dev"
    assert s.clusters["prod"].readonly is True
    assert s.clusters["prod"].security_protocol == "SASL_SSL"


def test_prod_security_assembled_into_base_config():
    s = make(kafkascope_clusters=_clusters_json())
    conf = kafka_client.base_config(s.clusters["prod"])
    assert conf["bootstrap.servers"] == "p:9093"
    assert conf["security.protocol"] == "SASL_SSL"
    assert conf["sasl.mechanism"] == "SCRAM-SHA-512"


def test_duplicate_cluster_name_rejected():
    raw = json.dumps([{"name": "dev", "brokers": "a"}, {"name": "dev", "brokers": "b"}])
    with pytest.raises(ValidationError):
        make(kafkascope_clusters=raw)


def test_cluster_missing_brokers_rejected():
    with pytest.raises(ValidationError):
        make(kafkascope_clusters=json.dumps([{"name": "dev"}]))


def test_cluster_bad_name_rejected():
    with pytest.raises(ValidationError):
        make(kafkascope_clusters=json.dumps([{"name": "has space", "brokers": "a"}]))


def test_cluster_unknown_key_rejected():
    with pytest.raises(ValidationError):
        make(kafkascope_clusters=json.dumps([{"name": "dev", "brokers": "a", "bogus": 1}]))


def test_cluster_sasl_incoherent_rejected():
    # SASL_SSL with no mechanism is caught per-cluster, same as the flat-var path.
    raw = json.dumps([{"name": "dev", "brokers": "a", "security_protocol": "SASL_SSL"}])
    with pytest.raises(ValidationError):
        make(kafkascope_clusters=raw)


def test_not_a_list_rejected():
    with pytest.raises(ValidationError):
        make(kafkascope_clusters=json.dumps({"name": "dev", "brokers": "a"}))


# --- per-cluster read-only flows through is_readonly() ---

@pytest.fixture
def restore_cluster():
    yield
    kafka_client.set_current_cluster(settings.default_cluster)


def test_is_readonly_reflects_current_cluster(restore_cluster):
    s = make(kafkascope_clusters=_clusters_json())

    kafka_client.set_current_cluster(s.clusters["dev"])
    assert kafka_client.is_readonly() is False

    kafka_client.set_current_cluster(s.clusters["prod"])
    assert kafka_client.is_readonly() is True
