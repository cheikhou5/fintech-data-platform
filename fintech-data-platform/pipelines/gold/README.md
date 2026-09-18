# Phase 3 - Gold

À construire après le cours « Deploy Workloads with Lakeflow Jobs ».

Tables attendues : `fact_transactions`, `dim_customer`, `dim_merchant`, `dim_date`,
`agg_merchant_daily`, `fraud_alerts`.

Exigences :
- la table de faits pointe vers la version du client valide à la date de la transaction ;
- une transaction en retard est comptée dans son vrai jour (`event_ts`), pas dans son jour d'arrivée ;
- deux règles de fraude au minimum : rafale de paiements et montant anormal ;
- un dashboard AI/BI et une alerte SQL sur `fraud_alerts`.

Critère de réussite : afficher la précision et le rappel des règles de fraude,
calculés à partir de `generator_state/fraud_truth/`.
