# Phase 1 - Bronze

À construire après le cours « Data Ingestion with Lakeflow Connect ».

Objectif : ingérer les trois flux de `raw/` avec Auto Loader dans un pipeline déclaratif Lakeflow,
sans rien transformer ni rien perdre.

Tables attendues : `bronze_transactions`, `bronze_customers_cdc`, `bronze_merchants`.

Exigences :
- ingestion incrémentale : un fichier déjà lu n'est jamais relu ;
- toutes les colonnes métier gardées telles quelles (pas de typage ici) ;
- colonnes techniques ajoutées : fichier source et heure d'ingestion ;
- l'apparition de `device_os` ne doit pas faire perdre de données ;
- les lignes JSON illisibles doivent être conservées quelque part, pas ignorées ;
- le dossier `generator_state/` n'est jamais lu.

Critère de réussite : relancer le générateur puis le pipeline ne traite que les nouveaux fichiers,
et le nombre de lignes Bronze est égal au nombre de lignes des fichiers bruts.
