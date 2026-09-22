"""
Pipeline Silver - Plateforme data fintech

Transforme les tables Bronze (brutes) en tables Silver (propres, typées, fiables).

Tables produites :
    - silver_transactions            : transactions valides, dédoublonnées, typées
    - silver_transactions_quarantine : transactions rejetées, avec le motif du rejet
    - silver_merchants               : référentiel des marchands, dédoublonné
    - silver_customers               : historique des clients (SCD Type 2, via AUTO CDC)

Règles de qualité, fixées par l'exploration de la source (docs/decisions.md) :
    - dédoublonnage sur transaction_id (199 doublons trouvés en Bronze) ;
    - amount nettoyé (virgule -> point) puis casté en decimal, doit être > 0 ;
    - customer_id et merchant_id ne doivent pas être nuls ;
    - event_ts doit être une date valide, ni nulle ni dans un futur absurde ;
    - status doit appartenir à l'ensemble attendu (SUCCESS, FAILED, PENDING) ;
    - toute ligne qui échoue une de ces règles part en quarantaine avec son motif.

Les clients sont historisés avec AUTO CDC : la colonne seq (et non l'ordre
d'arrivée des fichiers) fait foi pour déterminer la version la plus récente,
ce qui neutralise les événements CDC périmés injectés par le générateur.
"""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

# --------------------------------------------------------------------------
# Transactions : nettoyage, typage, séparation valide / quarantaine
# --------------------------------------------------------------------------
VALID_STATUSES = ("SUCCESS", "FAILED", "PENDING")


@dlt.view
def v_transactions_cleaned():
    """Vue intermédiaire : typage et calcul des motifs de rejet, sans filtrer.

    On garde chaque ligne avec la raison de son éventuel rejet en clair, ce
    qui permet ensuite de router les lignes vers silver_transactions ou vers
    la quarantaine sans dupliquer la logique de nettoyage.
    """
    df = dlt.read("bronze_transactions")

    amount_clean = F.regexp_replace(F.col("amount"), ",", ".").cast(DecimalType(12, 2))
    event_ts_clean = F.to_timestamp("event_ts")

    df = (
        df.withColumn("amount_clean", amount_clean)
          .withColumn("event_ts_clean", event_ts_clean)
          # Une ligne née d'un JSON tronqué a tous ses champs à null, y compris
          # transaction_id : c'est le signal le plus fiable pour la repérer.
          .withColumn("is_malformed", F.col("transaction_id").isNull())
    )

    reasons = F.array_remove(
        F.array(
            F.when(df.is_malformed, F.lit("ligne_json_illisible")),
            F.when(~df.is_malformed & df.customer_id.isNull(), F.lit("customer_id_null")),
            F.when(~df.is_malformed & df.merchant_id.isNull(), F.lit("merchant_id_null")),
            F.when(~df.is_malformed & (df.amount_clean.isNull() | (df.amount_clean <= 0)),
                   F.lit("amount_invalide")),
            F.when(~df.is_malformed & df.event_ts_clean.isNull(), F.lit("event_ts_invalide")),
            F.when(~df.is_malformed & (df.event_ts_clean > F.current_timestamp()),
                   F.lit("event_ts_futur")),
            F.when(~df.is_malformed & ~df.status.isin(*VALID_STATUSES),
                   F.lit("status_invalide")),
        ),
        None,
    )

    return df.withColumn("rejection_reasons", reasons)


@dlt.table(
    name="silver_transactions",
    comment="Transactions valides, dédoublonnées et typées.",
)
def silver_transactions():
    df = dlt.read("v_transactions_cleaned").filter("size(rejection_reasons) = 0")
    # Dédoublonnage : en cas de doublon exact, on garde la ligne ingérée en
    # premier (les doublons injectés par le générateur sont identiques).
    window = F.row_number().over(
        Window.partitionBy("transaction_id").orderBy("_ingested_at")
    )
    return (
        df.withColumn("_rn", window)
          .filter("_rn = 1")
          .select(
              "transaction_id", "customer_id", "merchant_id",
              F.col("amount_clean").alias("amount"),
              "currency", "channel", "status",
              F.col("event_ts_clean").alias("event_ts"),
              "device_os", "_source_file", "_ingested_at",
          )
    )


@dlt.table(
    name="silver_transactions_quarantine",
    comment="Transactions rejetées en Silver, avec le motif du rejet.",
)
def silver_transactions_quarantine():
    df = dlt.read("v_transactions_cleaned")
    return (
        df.filter("size(rejection_reasons) > 0")
          .select(
              "transaction_id", "customer_id", "merchant_id", "amount",
              "status", "event_ts", "rejection_reasons",
              "_source_file", "_ingested_at",
          )
    )


# --------------------------------------------------------------------------
# Marchands : référentiel simple, dédoublonné
# --------------------------------------------------------------------------
@dlt.table(
    name="silver_merchants",
    comment="Référentiel des marchands, dédoublonné sur merchant_id.",
)
def silver_merchants():
    df = dlt.read("bronze_merchants")
    window = F.row_number().over(
        Window.partitionBy("merchant_id").orderBy(F.col("_ingested_at").desc())
    )
    return (
        df.withColumn("_rn", window)
          .filter("_rn = 1")
          .select("merchant_id", "merchant_name", "category", "city", "onboarded_at")
    )


# --------------------------------------------------------------------------
# Clients : historisation SCD Type 2 via AUTO CDC
# --------------------------------------------------------------------------
@dlt.view
def v_customers_cdc():
    return dlt.read_stream("bronze_customers_cdc")


dlt.create_streaming_table(
    name="silver_customers",
    comment="Historique des clients (SCD Type 2), reconstitué à partir du flux CDC.",
)

dlt.apply_changes(
    target="silver_customers",
    source="v_customers_cdc",
    keys=["customer_id"],
    # seq (et non l'ordre d'arrivée des fichiers) détermine la version la
    # plus récente : les événements CDC périmés injectés par le générateur
    # sont ainsi ignorés automatiquement.
    sequence_by=F.col("seq"),
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op", "seq", "changed_at", "_source_file", "_ingested_at"],
    stored_as_scd_type=2,
)
