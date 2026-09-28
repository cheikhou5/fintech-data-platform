"""Fixtures partagées par les tests."""
import os

import pytest


@pytest.fixture(scope="session")
def spSark():
    """Session Spark pour les tests.

    Dans Databricks, on réutilise la session du cluster ; ailleurs (poste local,
    GitHub Actions), on démarre un Spark local minimal. Le fuseau UTC rend les
    dates des tests indépendantes de la machine.
    """
    from pyspark.sql import SparkSession

    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return SparkSession.builder.getOrCreate()

    session = (
        SparkSession.builder.master("local[1]")
        .appName("fintech-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
