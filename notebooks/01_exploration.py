# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Exploration de la source
# MAGIC
# MAGIC **Objectif.** Avant d'ingérer quoi que ce soit dans Bronze, on regarde ce que la
# MAGIC source envoie vraiment : doublons, valeurs invalides, schéma qui évolue, ordre des
# MAGIC événements. Chaque question ci-dessous devient une règle de qualité en couche Silver.
# MAGIC
# MAGIC **Méthode.** Une question = une cellule de code = un résultat chiffré noté juste en
# MAGIC dessous. Le résumé final de cette exploration est reporté dans `docs/decisions.md`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Préparation

# COMMAND ----------

dbutils.widgets.text("catalog", "fintech", "Catalogue")
RAW = f"/Volumes/{dbutils.widgets.get('catalog')}/landing/files/raw"

from pyspark.sql import functions as F

tx = spark.read.json(f"{RAW}/transactions")
merchants = spark.read.json(f"{RAW}/merchants")
customers = spark.read.json(f"{RAW}/customers_cdc").filter("op = 'INSERT'")
tx_with_file = (
    spark.read.json(f"{RAW}/transactions")
    .selectExpr("*", "_metadata.file_name AS source_file")
)

tx.printSchema()
display(tx.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Doublons de `transaction_id`
# MAGIC
# MAGIC Combien de `transaction_id` apparaissent plus d'une fois ?

# COMMAND ----------

display(
    tx.groupBy("transaction_id")
      .count()
      .filter("count > 1")
      .agg(F.count("*").alias("ids_en_double"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Constat :** _(à compléter — nombre de doublons trouvés)_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Type et valeurs invalides de `amount`
# MAGIC
# MAGIC Quel type Spark a-t-il donné à `amount` ? Combien de valeurs sont nulles ou
# MAGIC écrites avec une virgule au lieu d'un point ?

# COMMAND ----------

display(tx.filter("amount IS NULL OR amount RLIKE ','"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Constat :** _(à compléter — type détecté, nombre de valeurs invalides)_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lignes JSON tronquées
# MAGIC
# MAGIC Que contient la colonne `_corrupt_record` ? Combien de lignes sont concernées ?

# COMMAND ----------

display(tx.filter("_corrupt_record IS NOT NULL"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Constat :** _(à compléter — nombre de lignes corrompues)_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dates aberrantes ou dans le futur
# MAGIC
# MAGIC Combien de transactions ont un `event_ts` situé dans le futur ?

# COMMAND ----------

display(tx.filter(F.col("event_ts") > "2099-01-01"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Constat :** _(à compléter)_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Marchands inconnus
# MAGIC
# MAGIC Combien de `merchant_id` n'existent pas dans le référentiel des marchands ?

# COMMAND ----------

display(
    tx.join(merchants, "merchant_id", "left_anti")
      .select("merchant_id")
      .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Constat :** _(à compléter)_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apparition de `device_os`
# MAGIC
# MAGIC Dans quel fichier la colonne `device_os` apparaît-elle pour la première fois ?

# COMMAND ----------

display(
    tx_with_file.filter("device_os IS NOT NULL")
    .select("source_file")
    .distinct()
    .orderBy("source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Constat :** _(à compléter — nom du premier fichier concerné)_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Ordre CDC selon `seq`
# MAGIC
# MAGIC Dans `customers_cdc`, l'ordre des lignes suit-il toujours `seq` ?

# COMMAND ----------

cdc = spark.read.json(f"{RAW}/customers_cdc")
display(cdc.orderBy("customer_id", "seq").limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC **Constat :** _(à compléter — exemple d'un client avec plusieurs événements)_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Montants aberrants (fraude)
# MAGIC
# MAGIC Distribution de `amount` : y a-t-il un écart important entre le 75e centile et
# MAGIC le maximum, signe de montants anormaux ?

# COMMAND ----------

display(
    tx.filter("amount NOT LIKE '%,%'")
      .selectExpr("cast(amount as double) as amount_num")
      .summary("min", "25%", "50%", "75%", "max")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Constat :** _(à compléter)_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cohérence des statuts
# MAGIC
# MAGIC Les valeurs de `status` sont-elles toutes conformes (`SUCCESS`, `FAILED`, `PENDING`) ?

# COMMAND ----------

display(tx.groupBy("status").count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Constat :** _(à compléter)_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Transactions sans client existant
# MAGIC
# MAGIC Combien de transactions référencent un `customer_id` qui n'existe pas ?

# COMMAND ----------

display(
    tx.join(customers, "customer_id", "left_anti")
      .select("customer_id")
      .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Constat :** _(à compléter)_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Chevauchement des dates entre fichiers
# MAGIC
# MAGIC Un fichier plus récent contient-il toujours des dates plus récentes ?

# COMMAND ----------

display(
    tx_with_file.groupBy("source_file")
    .agg(F.min("event_ts").alias("min_ts"), F.max("event_ts").alias("max_ts"))
    .orderBy("source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Constat :** _(à compléter)_

# COMMAND ----------

# MAGIC %md
# MAGIC ## Résumé
# MAGIC
# MAGIC Reporter la synthèse de ces 11 constats dans `docs/decisions.md`, entrée
# MAGIC « Exploration de la source (phase 0) ». C'est ce résumé, chiffré, qui fixe les
# MAGIC règles de qualité de la couche Silver et son seuil de réussite.
