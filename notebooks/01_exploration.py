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

# Exemple pour la question 1, à toi d'écrire les suivantes.
from pyspark.sql import functions as F

display(
    tx.groupBy("transaction_id")
      .count()
      .filter("count > 1")
      .agg(F.count("*").alias("ids_en_double"))
)

# COMMAND ----------

cdc = spark.read.json(f"{RAW}/customers_cdc")
display(cdc.orderBy("customer_id", "seq").limit(50))

# COMMAND ----------

display(tx.filter("amount IS NULL OR amount RLIKE ','"))

# COMMAND ----------

display(tx.filter("_corrupt_record IS NOT NULL"))
