# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Mise en place et génération des données
# MAGIC
# MAGIC Crée le catalogue, le schéma et le volume d'atterrissage, puis lance le générateur.
# MAGIC À relancer à chaque fois que tu veux simuler l'arrivée de nouveaux fichiers.

# COMMAND ----------

dbutils.widgets.text("catalog", "fintech", "Catalogue")
dbutils.widgets.text("batches", "10", "Nombre de lots")

CATALOG = dbutils.widgets.get("catalog")
BATCHES = int(dbutils.widgets.get("batches"))
LANDING = f"/Volumes/{CATALOG}/landing/files"

# COMMAND ----------

# Si la création du catalogue est refusée, mets "workspace" dans le widget `catalog`.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.files")

# COMMAND ----------

# Dans un dossier Git Databricks, la racine du dépôt est ajoutée au chemin Python :
# l'import ci-dessous fonctionne donc sans rien installer.
from generator.generate_fintech_data import run

run(LANDING, batches=BATCHES)

# COMMAND ----------

display(dbutils.fs.ls(f"{LANDING}/raw/transactions"))
