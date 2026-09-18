# Contrat de données de la source

Ce document décrit ce que le système source (le générateur) promet d'envoyer, et surtout ce qu'il envoie de travers.
Les fichiers sont au format JSON Lines, déposés par lot dans `<volume>/raw/`.

## transactions

| Champ | Type attendu | Remarque |
|---|---|---|
| `transaction_id` | string (UUID) | clé métier, censée être unique |
| `customer_id` | string | référence `customers_cdc.customer_id` |
| `merchant_id` | string | référence `merchants.merchant_id` |
| `amount` | decimal > 0 | |
| `currency` | string | |
| `channel` | string | APP, USSD, CARD, QR |
| `status` | string | SUCCESS, FAILED, PENDING |
| `event_ts` | timestamp UTC | heure réelle du paiement, pas heure d'arrivée |
| `device_os` | string | **n'existe qu'à partir du lot 5** |

## customers_cdc

Flux de changements : une ligne par modification, pas un état.

| Champ | Type | Remarque |
|---|---|---|
| `op` | string | INSERT, UPDATE, DELETE |
| `seq` | long | numéro de séquence global croissant, **seule source de vérité sur l'ordre** |
| `changed_at` | timestamp UTC | |
| `customer_id` | string | clé métier |
| `full_name`, `phone` | string | données personnelles, à masquer |
| `city`, `kyc_level`, `status` | string | attributs historisés en SCD2 |
| `created_at` | timestamp UTC | |

## merchants

`merchant_id`, `merchant_name`, `category`, `city`, `onboarded_at`. Ajouts uniquement, pas de modification.

## Anomalies connues

| Anomalie | Fréquence par défaut | Traitement prévu |
|---|---|---|
| Transaction en double (ligne identique) | 2 % | dédoublonnage en Silver |
| Transaction en retard de 1 à 3 jours | 3 % | recalcul des agrégats Gold |
| `amount` nul, négatif, ou texte avec virgule | env. 1 % | quarantaine |
| `customer_id` nul | env. 0,3 % | quarantaine |
| `merchant_id` inconnu (`M-UNKNOWN`) | env. 0,3 % | quarantaine |
| `event_ts` dans le futur (2099) | env. 0,3 % | quarantaine |
| Ligne JSON tronquée | 0,2 % | conservée en Bronze, comptée |
| Événement CDC périmé arrivant après le plus récent | 5 % des updates | ignoré grâce à `seq` |
| Nouvelle colonne `device_os` | à partir du lot 5 | évolution de schéma en Bronze |

Les fréquences se règlent dans la classe `Config` de `generator/generate_fintech_data.py`.

## Hors contrat

`generator_state/` contient l'état interne du générateur et la vérité terrain des fraudes injectées (`fraud_truth/`).
Ce dossier ne doit **jamais** être ingéré par le pipeline : il ne sert qu'à évaluer les règles de fraude en phase 3.
