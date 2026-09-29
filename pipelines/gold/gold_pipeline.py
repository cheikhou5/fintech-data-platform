"""
Pipeline Gold - Plateforme data fintech

Construit les tables d'analyse à partir de Silver, prêtes pour les dashboards.

Tables produites :
    - gold_daily_kpis           : indicateurs par jour, canal et devise
    - gold_merchant_performance : performance commerciale par marchand
    - gold_customer_360         : fiche client actuelle + comportement d'achat

Choix de modélisation :
    - le chiffre d'affaires ne compte que les transactions SUCCESS ;
    - on agrège toujours par devise : additionner des montants de devises
      différentes donnerait un total sans signification ;
    - aucune fonction non déterministe (current_timestamp, rand...) afin que
      les tables restent rafraîchissables en incrémental (cf. Silver).
"""
import dlt
from pyspark.sql import functions as F


def _transaction_metrics():
    """Agrégats communs aux trois tables Gold."""
    is_success = F.col("status") == "SUCCESS"
    is_failed = F.col("status") == "FAILED"
    return [
        F.count("*").alias("nb_transactions"),
        F.sum(F.when(is_success, 1).otherwise(0)).alias("nb_success"),
        F.sum(F.when(is_failed, 1).otherwise(0)).alias("nb_failed"),
        F.sum(F.when(is_success, F.col("amount"))).alias("revenue"),
        F.avg(F.when(is_success, F.col("amount"))).alias("avg_basket"),
    ]


def _with_failure_rate(df):
    """Taux d'échec = transactions FAILED / total des transactions."""
    return df.withColumn(
        "failure_rate",
        F.round(F.col("nb_failed") / F.col("nb_transactions"), 4),
    )


# --------------------------------------------------------------------------
# KPIs quotidiens
# --------------------------------------------------------------------------
@dlt.table(
    name="gold_daily_kpis",
    comment="Volume, chiffre d'affaires, panier moyen et taux d'échec par jour, canal et devise.",
)
def gold_daily_kpis():
    df = (
        dlt.read("silver_transactions")
        .groupBy(
            F.to_date("event_ts").alias("event_date"),
            "channel",
            "currency",
        )
        .agg(*_transaction_metrics())
    )
    return _with_failure_rate(df)


# --------------------------------------------------------------------------
# Performance par marchand
# --------------------------------------------------------------------------
@dlt.table(
    name="gold_merchant_performance",
    comment="Chiffre d'affaires, volume et taux d'échec par marchand et devise.",
)
def gold_merchant_performance():
    tx = (
        dlt.read("silver_transactions")
        .groupBy("merchant_id", "currency")
        .agg(
            *_transaction_metrics(),
            F.min("event_ts").alias("first_transaction_at"),
            F.max("event_ts").alias("last_transaction_at"),
        )
    )
    merchants = dlt.read("silver_merchants")
    # Jointure à gauche depuis les transactions : aucun chiffre d'affaires
    # n'est perdu, même si un marchand manque dans le référentiel (son nom
    # sera alors null, ce qui signale un trou dans silver_merchants).
    

    return _with_failure_rate(
        tx.join(merchants, "merchant_id", "left")
          # Marchand absent du référentiel (ex. M-UNKNOWN) : on garde les
          # transactions pour ne pas fausser le CA, mais on le signale.
          .withColumn("is_known_merchant", F.col("merchant_name").isNotNull())
          .withColumn("merchant_name",
                      F.coalesce("merchant_name", F.lit("Marchand inconnu")))
    )
# --------------------------------------------------------------------------
# Vue client 360
# --------------------------------------------------------------------------
@dlt.table(
    name="gold_customer_360",
    comment="Version actuelle de chaque client et son comportement d'achat.",
)
def gold_customer_360():
    # En SCD Type 2, la version actuelle d'un client est celle dont
    # __END_AT est null. Les clients supprimés ont un __END_AT renseigné
    # et sont donc exclus automatiquement.
    customers = (
        dlt.read("silver_customers")
        .filter("__END_AT IS NULL")
        .drop("__START_AT", "__END_AT")
    )
    tx = (
        dlt.read("silver_transactions")
        .groupBy("customer_id", "currency")
        .agg(
            *_transaction_metrics(),
            F.min("event_ts").alias("first_transaction_at"),
            F.max("event_ts").alias("last_transaction_at"),
        )
    )
    # Jointure à gauche depuis les clients : un client actif sans aucune
    # transaction apparaît quand même (métriques à null).
    return _with_failure_rate(
        customers.join(tx, "customer_id", "left")
    )
