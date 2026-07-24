"""Connection-security config assembly and validation (the first production slice)."""

import pytest
from pydantic import ValidationError

from app.config import Settings
from app.kafka_client import base_config


def make(**kw) -> Settings:
    # _env_file="" only disables the dotenv file, not os.environ — so pin the vars
    # these tests care about, otherwise an ambient KAFKASCOPE_CLUSTERS (e.g. set on the
    # test container) would take over the default cluster and skip the flat-var checks.
    kw.setdefault("kafka_brokerconnect", "b:9092")
    kw.setdefault("kafkascope_clusters", "")
    return Settings(_env_file="", **kw)


def conf_of(**kw) -> dict:
    # The flat security vars are assembled into the single default ClusterConfig.
    return base_config(make(**kw).default_cluster)


def test_plaintext_is_minimal():
    conf = conf_of()
    assert conf == {"bootstrap.servers": "b:9092", "security.protocol": "PLAINTEXT"}


def test_sasl_ssl_scram_full():
    conf = conf_of(
        kafka_security_protocol="sasl_ssl",
        kafka_sasl_mechanism="scram-sha-512",
        kafka_sasl_username="u",
        kafka_sasl_password="p",
        kafka_ssl_ca_location="/ca.pem",
    )
    assert conf["security.protocol"] == "SASL_SSL"
    assert conf["sasl.mechanism"] == "SCRAM-SHA-512"
    assert conf["sasl.username"] == "u"
    assert conf["sasl.password"] == "p"
    assert conf["ssl.ca.location"] == "/ca.pem"


def test_mtls_ssl_config():
    conf = conf_of(
        kafka_security_protocol="ssl",
        kafka_ssl_certificate_location="/client.pem",
        kafka_ssl_key_location="/client.key",
    )
    assert conf["security.protocol"] == "SSL"
    assert conf["ssl.certificate.location"] == "/client.pem"
    assert conf["ssl.key.location"] == "/client.key"
    assert "sasl.mechanism" not in conf


def test_empty_optional_keys_are_omitted():
    conf = conf_of()
    assert not any(k.startswith("sasl.") or k.startswith("ssl.") for k in conf)


def test_protocol_and_mechanism_normalised_to_upper():
    s = make(
        kafka_security_protocol="sasl_plaintext",
        kafka_sasl_mechanism="plain",
        kafka_sasl_username="u",
        kafka_sasl_password="p",
    )
    assert s.kafka_security_protocol == "SASL_PLAINTEXT"
    assert s.kafka_sasl_mechanism == "PLAIN"


def test_invalid_protocol_rejected():
    with pytest.raises(ValidationError):
        make(kafka_security_protocol="telnet")


def test_invalid_mechanism_rejected():
    with pytest.raises(ValidationError):
        make(kafka_sasl_mechanism="magic")


def test_sasl_without_mechanism_rejected():
    with pytest.raises(ValidationError):
        make(kafka_security_protocol="SASL_SSL")


def test_scram_without_credentials_rejected():
    with pytest.raises(ValidationError):
        make(kafka_security_protocol="SASL_SSL", kafka_sasl_mechanism="SCRAM-SHA-256")


def test_oauthbearer_does_not_require_userpass():
    # OAUTHBEARER authenticates by token, so username/password aren't mandatory.
    s = make(kafka_security_protocol="SASL_SSL", kafka_sasl_mechanism="OAUTHBEARER")
    assert s.kafka_sasl_mechanism == "OAUTHBEARER"
