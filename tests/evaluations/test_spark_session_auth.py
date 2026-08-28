"""Unit tests for Polaris auth-related logic in spark_session_utils.py.

These test pure config-building/parsing logic directly (using a bare
SparkConf where needed) rather than a real SparkSession, so they don't
require network access to resolve Spark packages.
"""
import os

import pytest
from pyspark import SparkConf

from teehr.evaluation.spark_session_utils import (
    _as_bool_str,
    _build_polaris_auth_configs,
    _build_polaris_auth_packages,
    _configure_iceberg_catalogs,
    _is_sensitive_config_key,
    _split_csv_env,
    _update_configs_and_packages,
)
from teehr.models.evaluation_base import RemoteCatalog


@pytest.fixture(autouse=True)
def _clear_polaris_env(monkeypatch):
    """Ensure Polaris/remote-catalog env vars don't leak between tests."""
    for key in (
        "POLARIS_USE_AUTHMANAGER",
        "POLARIS_USE_STS",
        "POLARIS_CLIENT_ID",
        "POLARIS_CLIENT_SECRET",
        "REMOTE_CATALOG_S3_ENDPOINT",
        "REMOTE_CATALOG_S3_PATH_STYLE_ACCESS",
        "REMOTE_WAREHOUSE_IDENTIFIER",
        "REMOTE_CATALOG_TYPE",
        "REMOTE_CATALOG_REST_URI",
    ):
        monkeypatch.delenv(key, raising=False)


def test_as_bool_str_empty_env_var_does_not_flip_to_enabled(monkeypatch):
    """An env var set to "" (not unset) must not silently enable a feature."""
    monkeypatch.setenv("POLARIS_USE_AUTHMANAGER", "")
    result = _as_bool_str(
        os.getenv("POLARIS_USE_AUTHMANAGER", "false"), default="false"
    )
    assert result == "false"


def test_as_bool_str_unset_env_var_uses_os_getenv_default():
    """An unset env var falls back to os.getenv's own default, as before."""
    result = _as_bool_str(os.getenv("POLARIS_USE_AUTHMANAGER", "false"))
    assert result == "false"


def test_update_configs_and_packages_add_jars_is_optional():
    """add_jars must have a default -- it's not always supplied by callers."""
    conf = SparkConf(loadDefaults=False)
    conf.set("spark.jars.packages", "")
    _update_configs_and_packages(
        conf=conf,
        update_configs=None,
        add_packages=["com.example:my-package:1.0.0"],
    )
    packages = conf.get("spark.jars.packages").split(",")
    assert "com.example:my-package:1.0.0" in packages


def test_build_polaris_auth_configs_sts_flag_threaded_through():
    """resolved_use_sts must be an explicit param, not re-derived from env."""
    os.environ["POLARIS_CLIENT_ID"] = "prefect-polaris"
    os.environ["POLARIS_CLIENT_SECRET"] = "test-secret"
    try:
        with_sts = _build_polaris_auth_configs(None, False, True)
        assert (
            with_sts["spark.sql.catalog.iceberg.header.X-Iceberg-Access-Delegation"]
            == "vended-credentials"
        )

        without_sts = _build_polaris_auth_configs(None, False, False)
        assert (
            "spark.sql.catalog.iceberg.header.X-Iceberg-Access-Delegation"
            not in without_sts
        )
    finally:
        os.environ.pop("POLARIS_CLIENT_ID", None)
        os.environ.pop("POLARIS_CLIENT_SECRET", None)


def test_build_polaris_auth_configs_no_auth_path_returns_empty():
    """With no AuthManager, token, or client credentials, nothing is set."""
    configs = _build_polaris_auth_configs(None, False, False)
    assert configs == {}


def test_configure_iceberg_catalogs_s3_endpoint_override_is_opt_in():
    """No REMOTE_CATALOG_S3_PATH_STYLE_ACCESS -> no s3.endpoint override."""
    conf = SparkConf(loadDefaults=False)
    _configure_iceberg_catalogs(
        conf, "local", "sqlite", "/tmp/wh", "iceberg", "rest", "http://polaris:8181"
    )
    assert not conf.contains("spark.sql.catalog.iceberg.s3.endpoint")


def test_configure_iceberg_catalogs_s3_endpoint_override_applies_when_set(monkeypatch):
    """Setting REMOTE_CATALOG_S3_PATH_STYLE_ACCESS=true applies the override."""
    monkeypatch.setenv("REMOTE_CATALOG_S3_PATH_STYLE_ACCESS", "true")
    monkeypatch.setenv("REMOTE_CATALOG_S3_ENDPOINT", "http://custom-s3:9000")
    conf = SparkConf(loadDefaults=False)
    _configure_iceberg_catalogs(
        conf, "local", "sqlite", "/tmp/wh", "iceberg", "rest", "http://polaris:8181"
    )
    assert conf.get("spark.sql.catalog.iceberg.s3.path-style-access") == "true"
    assert conf.get("spark.sql.catalog.iceberg.s3.endpoint") == "http://custom-s3:9000"


def test_update_configs_and_packages_add_packages_when_key_unset():
    """add_packages must not crash when spark.jars.packages was never set."""
    conf = SparkConf(loadDefaults=False)
    _update_configs_and_packages(
        conf=conf,
        update_configs=None,
        add_packages=["com.example:my-package:1.0.0"],
    )
    assert "com.example:my-package:1.0.0" in conf.get("spark.jars.packages").split(",")


def test_update_configs_and_packages_add_jars_dedupes_and_strips_whitespace():
    """add_jars merging must strip/dedupe consistently (previously diverged)."""
    conf = SparkConf(loadDefaults=False)
    _update_configs_and_packages(
        conf=conf,
        update_configs=None,
        add_jars=[" /a.jar ", "/a.jar", "/b.jar"],
    )
    assert conf.get("spark.jars").split(",") == ["/a.jar", "/b.jar"]


def test_update_configs_and_packages_repositories_merge_dedupes():
    """update_configs' spark.jars.repositories merge path dedupes values."""
    conf = SparkConf(loadDefaults=False)
    _update_configs_and_packages(
        conf=conf,
        update_configs={
            "spark.jars.repositories": "https://repo1, https://repo1, https://repo2"
        },
    )
    assert conf.get("spark.jars.repositories").split(",") == [
        "https://repo1",
        "https://repo2",
    ]


def test_split_csv_env_dedupes_strips_and_defaults(monkeypatch):
    """Shared CSV-env-var helper used by all Polaris package/repo lookups."""
    monkeypatch.setenv("SOME_CSV_VAR", "a:1, a:1, b:2")
    assert _split_csv_env("SOME_CSV_VAR") == ["a:1", "b:2"]

    monkeypatch.delenv("SOME_CSV_VAR", raising=False)
    assert _split_csv_env("SOME_CSV_VAR", default="x:1") == ["x:1"]


def test_build_polaris_auth_packages_disabled_returns_empty():
    """AuthManager disabled -> no package coordinates."""
    assert _build_polaris_auth_packages(False) == []


@pytest.mark.parametrize(
    "key",
    [
        "spark.executorEnv.POLARIS_BROKER_SESSION_TOKEN",
        "spark.executorEnv.POLARIS_CLIENT_SECRET",
        "spark.sql.catalog.iceberg.token",
        "spark.sql.catalog.iceberg.credential",
        "spark.sql.catalog.iceberg.rest.auth.oauth2.credential",
        "spark.hadoop.fs.s3a.secret.key",
    ],
)
def test_is_sensitive_config_key_flags_credential_bearing_keys(key):
    """log_session_config's debug dump must redact these, not print them."""
    assert _is_sensitive_config_key(key) is True


@pytest.mark.parametrize(
    "key",
    [
        "spark.sql.catalog.iceberg.rest.auth.teehr.user-id",
        "spark.sql.shuffle.partitions",
        "spark.executorEnv.POLARIS_DEFAULT_REALM",
    ],
)
def test_is_sensitive_config_key_leaves_non_secrets_alone(key):
    assert _is_sensitive_config_key(key) is False


def test_remote_catalog_warehouse_dir_reads_env_live(monkeypatch):
    """RemoteCatalog.warehouse_dir must not be bound at teehr.const import time."""
    monkeypatch.setenv("REMOTE_WAREHOUSE_IDENTIFIER", "live-value")
    assert RemoteCatalog().warehouse_dir == "live-value"

    monkeypatch.setenv("REMOTE_WAREHOUSE_IDENTIFIER", "changed-value")
    assert RemoteCatalog().warehouse_dir == "changed-value"
