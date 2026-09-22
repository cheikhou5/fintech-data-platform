"""
Pipeline Bronze - Plateforme data fintech

Ingère les trois flux bruts avec Auto Loader dans des tables Delta, sans aucune
transformation métier : Bronze conserve les données telles qu'elles arrivent.

Principes :
    - Ingestion incrémentale : un fichier déjà lu n'est jamais relu.
    - Rien n'est perdu, y compris les lignes JSON illisibles.
    - L'apparition d'une nouvelle colonne (ex. device_os) est absorbée
      automatiquement, sans casser le pipeline.
    - Deux colonnes techniques ajoutées à chaque table : le fichier source
      et l'heure d'ingestion, utiles pour tracer d'où vient chaque ligne.

Ce fichier est destiné à être exécuté comme pipeline déclaratif Lakeflow
(Delta Live Tables), pas comme un notebook classique : le décorateur @dlt.table
indique au moteur de pipeline comment construire et maintenir chaque table.
"""
import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "fintech")
RAW = f"/Volumes/{CATALOG}/landing/files/raw"


def _autoloader_read(subfolder: str):
    """Flux Auto Loader commun aux trois sources : lecture incrémentale de
    fichiers JSON, avec absorption automatique des nouvelles colonnes et
    conservation des lignes qui ne correspondent pas au schéma attendu.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"/Volumes/{CATALOG}/landing/schemas/{subfolder}")
        .option("cloudFiles.inferColumnTypes", "true")
        # addNewColumns : une colonne inconnue (ex. device_os) est ajoutée au
        # schéma plutôt que de faire échouer le flux.
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # rescuedDataColumn : toute donnée qui ne correspond pas au schéma
        # inféré (y compris une ligne JSON tronquée) est conservée ici au
        # lieu d'être silencieusement perdue.
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{RAW}/{subfolder}")
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name="bronze_transactions",
    comment="Transactions brutes, telles qu'envoyées par le système de paiement.",
)
def bronze_transactions():
    return _autoloader_read("transactions")


@dlt.table(
    name="bronze_customers_cdc",
    comment="Flux CDC brut des changements clients (INSERT/UPDATE/DELETE).",
)
def bronze_customers_cdc():
    return _autoloader_read("customers_cdc")


@dlt.table(
    name="bronze_merchants",
    comment="Référentiel brut des marchands.",
)
def bronze_merchants():
    return _autoloader_read("merchants")
