"""
Règles de qualité de la couche Silver (transactions).

Ces règles sont isolées ici, sans dépendance à `dlt`, pour pouvoir être testées
en dehors de Databricks (tests/test_silver_quality.py). Le pipeline Silver les
applique ; les tests prouvent qu'elles attrapent bien chaque défaut injecté par
le générateur.

Choix de robustesse :
    - try_cast / try_to_timestamp : une valeur illisible devient NULL (donc un
      motif de rejet) au lieu de faire planter tout le pipeline, y compris
      quand le mode ANSI de Spark est activé ;
    - un statut NULL est un statut invalide ;
    - la date « future » est jugée par rapport à l'heure d'ingestion de la
      ligne, pas à l'heure du run : la règle est déterministe.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_STATUSES = ("SUCCESS", "FAILED", "PENDING")
FUTURE_TOLERANCE = "INTERVAL 1 DAY"


def add_rejection_reasons(df: DataFrame) -> DataFrame:
    """Ajoute les colonnes nettoyées et `rejection_reasons`, sans filtrer.

    Une ligne est valide si et seulement si `rejection_reasons` est un tableau
    vide. La colonne n'est jamais NULL.
    """
    df = (
        df.withColumn(
            "amount_clean",
            F.expr("try_cast(regexp_replace(CAST(amount AS STRING), ',', '.') AS DECIMAL(12, 2))"),
        )
        .withColumn("event_ts_clean", F.expr("try_to_timestamp(CAST(event_ts AS STRING))"))
        # Une ligne née d'un JSON tronqué a tous ses champs à null, y compris
        # transaction_id : c'est le signal le plus fiable pour la repérer.
        .withColumn("is_malformed", F.col("transaction_id").isNull())
    )

    readable = ~F.col("is_malformed")
    amount = F.col("amount_clean")
    event_ts = F.col("event_ts_clean")
    status = F.col("status")

    reasons = F.array_compact(
        F.array(
            F.when(F.col("is_malformed"), F.lit("ligne_json_illisible")),
            F.when(readable & F.col("customer_id").isNull(), F.lit("customer_id_null")),
            F.when(readable & F.col("merchant_id").isNull(), F.lit("merchant_id_null")),
            F.when(readable & (amount.isNull() | (amount <= 0)), F.lit("amount_invalide")),
            F.when(readable & event_ts.isNull(), F.lit("event_ts_invalide")),
            F.when(
                readable & (event_ts > F.col("_ingested_at") + F.expr(FUTURE_TOLERANCE)),
                F.lit("event_ts_futur"),
            ),
            F.when(
                readable & (status.isNull() | ~status.isin(*VALID_STATUSES)),
                F.lit("status_invalide"),
            ),
        )
    )
    return df.withColumn("rejection_reasons", reasons)
