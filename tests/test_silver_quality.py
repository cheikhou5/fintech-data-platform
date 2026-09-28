"""Tests des règles de qualité Silver.

Chaque test construit une ou deux fausses lignes, comme celles que produit le
générateur, et vérifie le motif de rejet obtenu. Si une règle est cassée par
une modification du code, le test correspondant échoue avant la mise en
production.
"""
from datetime import datetime
from decimal import Decimal

import pytest

from pipelines.silver.quality import add_rejection_reasons

INGESTED_AT = datetime(2026, 9, 21, 12, 0, 0)

SCHEMA = (
    "transaction_id string, customer_id string, merchant_id string, "
    "amount string, status string, event_ts string, _ingested_at timestamp"
)

VALID_ROW = {
    "transaction_id": "T-1",
    "customer_id": "C-1",
    "merchant_id": "M-1",
    "amount": "12.50",
    "status": "SUCCESS",
    "event_ts": "2026-09-21 10:00:00",
    "_ingested_at": INGESTED_AT,
}


def evaluate(spark, **overrides):
    """Applique les règles à une ligne valide modifiée ; renvoie la ligne résultat."""
    row = {**VALID_ROW, **overrides}
    values = tuple(row[c.split()[0]] for c in SCHEMA.split(", "))
    df = spark.createDataFrame([values], SCHEMA)
    return add_rejection_reasons(df).collect()[0]


# ---------------------------------------------------------------------------
# Une ligne correcte passe
# ---------------------------------------------------------------------------
def test_valid_row_has_no_reason(spark):
    assert evaluate(spark).rejection_reasons == []


def test_reasons_are_never_null(spark):
    """Régression : array_remove(..., None) rendait la colonne NULL et vidait
    silver_transactions ET la quarantaine sans aucune erreur."""
    for overrides in ({}, {"amount": "-5"}, {"transaction_id": None}):
        assert evaluate(spark, **overrides).rejection_reasons is not None


# ---------------------------------------------------------------------------
# Montant
# ---------------------------------------------------------------------------
def test_comma_amount_is_cleaned_not_rejected(spark):
    row = evaluate(spark, amount="12,50")
    assert row.rejection_reasons == []
    assert row.amount_clean == Decimal("12.50")


@pytest.mark.parametrize("amount", ["-5", "0", None, "abc"])
def test_invalid_amount_is_rejected(spark, amount):
    assert "amount_invalide" in evaluate(spark, amount=amount).rejection_reasons


# ---------------------------------------------------------------------------
# Identifiants et ligne illisible
# ---------------------------------------------------------------------------
def test_missing_customer_is_rejected(spark):
    assert evaluate(spark, customer_id=None).rejection_reasons == ["customer_id_null"]


def test_missing_merchant_is_rejected(spark):
    assert evaluate(spark, merchant_id=None).rejection_reasons == ["merchant_id_null"]


def test_malformed_line_has_a_single_reason(spark):
    """Une ligne JSON tronquée a tous ses champs à NULL : on veut un seul motif
    clair, pas sept motifs qui noient l'information."""
    row = evaluate(
        spark, transaction_id=None, customer_id=None, merchant_id=None,
        amount=None, status=None, event_ts=None,
    )
    assert row.rejection_reasons == ["ligne_json_illisible"]


# ---------------------------------------------------------------------------
# Date
# ---------------------------------------------------------------------------
def test_unreadable_date_is_rejected(spark):
    assert "event_ts_invalide" in evaluate(spark, event_ts="pas une date").rejection_reasons


def test_future_date_is_judged_against_ingestion_time(spark):
    two_days_after_ingestion = "2026-09-23 12:00:00"
    assert "event_ts_futur" in evaluate(spark, event_ts=two_days_after_ingestion).rejection_reasons


def test_small_clock_skew_is_tolerated(spark):
    """Quelques heures d'avance (fuseau horaire, horloge) restent acceptées."""
    three_hours_after_ingestion = "2026-09-21 15:00:00"
    assert evaluate(spark, event_ts=three_hours_after_ingestion).rejection_reasons == []


# ---------------------------------------------------------------------------
# Statut
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("status", ["SUCCESS", "FAILED", "PENDING"])
def test_known_statuses_are_accepted(spark, status):
    assert evaluate(spark, status=status).rejection_reasons == []


@pytest.mark.parametrize("status", ["success", "REFUNDED", None])
def test_unknown_status_is_rejected(spark, status):
    assert "status_invalide" in evaluate(spark, status=status).rejection_reasons


# ---------------------------------------------------------------------------
# Plusieurs défauts à la fois
# ---------------------------------------------------------------------------
def test_all_reasons_are_reported(spark):
    row = evaluate(spark, customer_id=None, amount="-1")
    assert sorted(row.rejection_reasons) == ["amount_invalide", "customer_id_null"]
