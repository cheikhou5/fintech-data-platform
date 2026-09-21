# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 - Exploration de la source
# MAGIC
# MAGIC Avant d'ingérer quoi que ce soit, on regarde ce que la source envoie vraiment.
# MAGIC Chaque question ci-dessous deviendra une règle de qualité en couche Silver.
# MAGIC Note tes réponses dans `docs/decisions.md`.

# COMMAND ----------

dbutils.widgets.text("catalog", "fintech", "Catalogue")
RAW = f"/Volumes/{dbutils.widgets.get('catalog')}/landing/files/raw"

# COMMAND ----------

tx = spark.read.json(f"{RAW}/transactions")
tx.printSchema()
display(tx.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Préparation
# MAGIC
# MAGIC Ces trois variables sont utilisées par plusieurs questions plus bas
# MAGIC (5, 6, 10, 11). Cette cellule doit être exécutée avant elles.

# COMMAND ----------

merchants = spark.read.json(f"{RAW}/merchants")
customers = spark.read.json(f"{RAW}/customers_cdc").filter("op = 'INSERT'")
tx_with_file = (
    spark.read.json(f"{RAW}/transactions")
    .selectExpr("*", "_metadata.file_name AS source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Questions à résoudre toi-même
# MAGIC
# MAGIC 1. Combien de `transaction_id` apparaissent plus d'une fois ? Les doublons sont-ils strictement identiques ?
# MAGIC 2. Quel type Spark a-t-il donné à `amount` ? Pourquoi ? Combien de valeurs sont nulles, négatives, ou écrites avec une virgule ?
# MAGIC 3. Que contient la colonne `_corrupt_record` ? Combien de lignes sont concernées ?
# MAGIC 4. Combien de transactions ont un `event_ts` plus vieux de 24 h que les autres du même fichier ? Et dans le futur ?
# MAGIC 5. Combien de `merchant_id` n'existent pas dans le référentiel des marchands ?
# MAGIC 6. Dans quel fichier la colonne `device_os` apparaît-elle pour la première fois ? (indice : `_metadata.file_name`)
# MAGIC 7. Dans `customers_cdc`, trouve un client qui a plusieurs événements. L'ordre des lignes du fichier suit-il toujours `seq` ?

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Doublons de transaction_id

# COMMAND ----------

# Exemple pour la question 1, à toi d'écrire les suivantes.
from pyspark.sql import functions as F

display(
    tx.groupBy("transaction_id")
      .count()
      .filter("count > 1")
      .agg(F.count("*").alias("ids_en_double"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Type et valeurs invalides de amount

# COMMAND ----------

display(tx.filter("amount IS NULL OR amount RLIKE ','"))

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Lignes JSON tronquées

# COMMAND ----------

display(tx.filter("_corrupt_record IS NOT NULL"))

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Dates aberrantes ou dans le futur

# COMMAND ----------

display(tx.filter(F.col("event_ts") > "2099-01-01"))

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Marchands inconnus

# COMMAND ----------

display(
    tx.join(merchants, "merchant_id", "left_anti")
      .select("merchant_id")
      .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Apparition de device_os

# COMMAND ----------

display(
    tx_with_file.filter("device_os IS NOT NULL")
    .select("source_file")
    .distinct()
    .orderBy("source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Ordre CDC selon seq

# COMMAND ----------

cdc = spark.read.json(f"{RAW}/customers_cdc")
display(cdc.orderBy("customer_id", "seq").limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC 8. Montants aberrants (fraude)

# COMMAND ----------

display(
    tx.filter("amount NOT LIKE '%,%'")
      .selectExpr("cast(amount as double) as amount_num")
      .summary("min", "25%", "50%", "75%", "max")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 9. Cohérence des statuts

# COMMAND ----------

display(tx.groupBy("status").count())

# COMMAND ----------

# MAGIC %md
# MAGIC 10. Transactions sans client existant

# COMMAND ----------

display(
    tx.join(customers, "customer_id", "left_anti")
      .select("customer_id")
      .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC 11. Chevauchement des dates entre fichiers

# COMMAND ----------

display(
    tx_with_file.groupBy("source_file")
    .agg(F.min("event_ts").alias("min_ts"), F.max("event_ts").alias("max_ts"))
    .orderBy("source_file")
)
