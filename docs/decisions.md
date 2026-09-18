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

**Contexte.**

**Options.**

**Décision.**

**Conséquences.**
