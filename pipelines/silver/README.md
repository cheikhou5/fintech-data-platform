# Phase 2 - Silver

À construire après le cours « Build Data Pipelines with Lakeflow Spark Declarative Pipelines ».

Tables attendues : `silver_transactions`, `silver_transactions_quarantine`,
`silver_customers` (SCD Type 2), `silver_merchants`.

Exigences :
- typage propre (`amount` en decimal, `event_ts` en timestamp) ;
- dédoublonnage sur `transaction_id` ;
- règles de qualité déclarées avec des expectations, une par anomalie listée dans `docs/data_contract.md` ;
- les lignes rejetées vont en quarantaine avec le motif du rejet ;
- clients historisés avec AUTO CDC, `seq` comme colonne de séquence, `DELETE` géré.

Critères de réussite : zéro doublon, les événements CDC périmés n'écrasent rien,
un client qui a déménagé a deux lignes avec des périodes de validité qui ne se chevauchent pas.
