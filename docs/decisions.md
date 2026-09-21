# Journal des décisions

Une entrée par décision technique importante. C'est la partie du dépôt que les recruteurs lisent vraiment :
elle montre que tu sais pourquoi tu fais les choses, pas seulement comment.

Format : contexte, options envisagées, décision, conséquences.

---

## 001 - Des données simulées plutôt qu'un jeu de données public

**Contexte.** Le projet doit démontrer la gestion des problèmes réels d'un pipeline : doublons, retards,
données invalides, changements de schéma, historisation.

**Options.** (a) Un jeu public comme les taxis de New York : réaliste en volume, mais propre et figé.
(b) Une API publique : données vivantes, mais aucune mutation ni CDC, et accès internet sortant limité sur Free Edition.
(c) Un générateur maison avec défauts contrôlés.

**Décision.** Option (c). Chaque anomalie est injectée à un taux connu, et une vérité terrain est écrite à part.

**Conséquences.** On peut prouver par des tests que le pipeline gère chaque cas, et mesurer la qualité de la
détection de fraude. En contrepartie, les volumes sont modestes et les distributions simplifiées.

---

## 002 - (à toi)

**Contexte.** Avant de construire Bronze, exploration manuelle de `raw/transactions` et
`raw/customers_cdc` sur 10 lots générés (environ 10 200 transactions).

**Observations.**
- 199 `transaction_id` apparaissent plus d'une fois (doublons exacts injectés par le générateur).
- Environ 15 lignes ont un `_corrupt_record` non nul : du JSON tronqué en plein milieu d'un champ
  (ex. `"merchant_id": "M-00055", "` coupé net), donc totalement illisible, pas juste un champ manquant.
- La colonne `amount` est lue en `string` par Spark : certaines valeurs sont `null`, d'autres
  écrites avec une virgule française (`"75,98"`) au lieu d'un point.
- Dans `customers_cdc`, le client `C-000030` (Ibrahima Sow) a 3 événements : `INSERT` (seq 30,
  KYC BASIC), puis deux `UPDATE` (seq 536 → STANDARD, seq 619 → PREMIUM). Confirme que `seq`,
  et non l'ordre des fichiers, doit piloter l'historisation SCD2 en Silver.

**Décision.** Ces observations valident les règles de qualité prévues dans
`docs/data_contract.md` : dédoublonnage sur `transaction_id`, conversion virgule→point avant
typage de `amount`, quarantaine pour les lignes avec `_corrupt_record` non nul, et `seq` comme
colonne de séquence pour AUTO CDC.

**Conséquences.** Le seuil de réussite de la couche Silver est maintenant chiffré : après
dédoublonnage, 0 doublon sur `transaction_id` (contre 199 aujourd'hui en Bronze).
