# Transfert DHIS2 X vers Y — cadrage des fonctionnalités

## 1. Objet du document

Ce document formalise les décisions prises pour faire évoluer le transfert de Data Sets entre deux
instances DHIS2. Il couvre en particulier :

- les longues périodes historiques ;
- la maîtrise de la mémoire ;
- la reprise après incident ;
- la synchronisation sécurisée des métadonnées ;
- l’audit des succès et des erreurs ;
- l’exécution en arrière-plan avec systemd ;
- l’exécution de plusieurs Data Sets.

Statut au 17 octobre 2026 : l’architecture décrite ici est implémentée. Les validations unitaires
couvrent notamment le découpage 36 mois/12 tranches, la reprise par checkpoint, le journal par
tranche et le versionnement des Data Elements historiques. La validation d’intégration sur Y reste
une opération explicite : `reconcile-metadata --dry-run`, puis `--apply` après lecture du plan.

## 2. Principes non négociables

1. Une erreur réseau transitoire ne peut pas être garantie inexistante. Le pipeline doit la rendre
   récupérable et traçable.
2. Une longue période ne doit jamais obliger à conserver toutes les valeurs en mémoire.
3. Une tranche terminée avec succès ne doit pas être retraitée après une reprise normale.
4. Les métadonnées doivent être contrôlées avant l’export massif des valeurs.
5. Un objet absent sur Y peut être créé automatiquement si son identité est non ambiguë.
6. Un objet existant et incompatible sur Y ne doit jamais être écrasé sans preuve que l’opération
   est sûre.
7. Aucun import de valeurs ne commence tant que le modèle de métadonnées cible n’est pas compatible.
8. Les identifiants, mots de passe et tokens restent dans `.env` et ne sont jamais écrits dans les
   logs, rapports ou fichiers YAML versionnés.
9. Les opérations de métadonnées concurrentes sur une même cible doivent être sérialisées.
10. L’état final doit être démontrable à partir du journal SQLite et des rapports conservés.

## 3. État implémenté

### 3.1 Fonctions disponibles

Le pipeline sait :

- charger plusieurs profils d’instances depuis `config/instances.yaml` ;
- récupérer les secrets correspondants depuis `.env` ;
- utiliser un PAT ou un couple utilisateur/mot de passe ;
- découvrir un Data Set source et ses Data Elements associés ;
- comparer les unités d’organisation X et Y par UID ou code ;
- planifier et créer explicitement les unités absentes avec toute leur chaîne de parents ;
- créer un Data Set cible manquant avec un paquet minimal de dépendances ;
- réutiliser les dépendances déjà présentes sur Y sans les mettre à jour implicitement ;
- bloquer un transfert lorsque les Data Elements existants sont incompatibles ;
- réparer explicitement un cas limité de CategoryCombo incomplet avec `repair-metadata` ;
- découper les requêtes source par unités d’organisation et périodes DHIS2 ;
- subdiviser automatiquement une requête après un timeout ou une erreur de passerelle ;
- faire un dry-run des valeurs avant l’import réel ;
- afficher des logs horodatés sur la progression ;
- enregistrer le résultat global dans SQLite et produire un rapport JSON ;
- découper l’exécution complète en fenêtres bornées par type de période ;
- lancer chaque fenêtre dans un sous-processus distinct ;
- reprendre sans rejouer les fenêtres réussies ;
- classifier les erreurs permanentes (`2`) et transitoires (`75`) ;
- retenter les erreurs transitoires avec backoff exponentiel ;
- synchroniser les objets absents et rattacher les Data Elements compatibles avant l’export ;
- conserver les anciennes sémantiques de catégories dans des Data Elements historiques versionnés ;
- isoler une collision d’UID historique dans un graphe cloné sans modifier l’objet existant sur Y ;
- sérialiser les jobs, les changements de métadonnées et la concurrence globale ;
- isoler le cache, le journal, les rapports et les artefacts par job.

### 3.2 Limites assumées

Les protections restent volontairement strictes :

- une collision UID/code courante, une différence de type ou une ambiguïté sémantique bloque ;
- seule une collision du graphe de catégories strictement historique peut être isolée par clonage
  lorsque `uid_collision_strategy: clone` est explicitement configuré ;
- une réparation risquée n’est jamais forcée par `CREATE_AND_UPDATE` ;
- un dry-run qui découvre des métadonnées absentes s’arrête avant l’export massif jusqu’au commit ;
- les payloads complets ne sont conservés que si `--keep-payload` est demandé ;
- l’intégration réelle doit commencer sur une tranche réduite et une instance non-production.

## 4. Deux niveaux de découpage implémentés

Le pipeline distingue deux mécanismes différents.

### 4.1 Découpage des requêtes source

`source_period_batch_size` contrôle la taille d’une requête `dataValueSets` adressée à X.

Exemple pour un Data Set mensuel :

```yaml
transfer:
  source_period_batch_size: 1
```

Chaque requête couvre au maximum un mois. Les unités d’organisation sont découpées séparément par
`organisation_units.batch_size`. Une requête qui échoue sur un timeout peut être subdivisée de
nouveau.

Ce mécanisme protège le serveur DHIS2, nginx et Cloudflare, mais ne suffit pas à limiter la mémoire
globale du processus.

### 4.2 Découpage de l’exécution

La configuration suivante limite la quantité de données exportée, validée et importée dans
une même tranche logique :

```yaml
jobs:
  - id: hospital_service
    source_data_set: OeWrFwkFMvf
    target_data_set: OeWrFwkFMvf
    period_type: month
    default_start_period: 2017-01
    default_end_period: 2019-12

    transfer:
      source_period_batch_size: 1

      execution:
        period_batch_size: 3
        process_isolation: true
        resume: true
        stop_on_error: true
```

`period_batch_size` est exprimé dans le type de période du job :

- `3` signifie trois mois pour un job mensuel ;
- `13` signifie treize semaines pour un job hebdomadaire ;
- `31` signifie trente et un jours pour un job journalier ;
- `1` signifie une année pour un job annuel.

Il n’est pas nécessaire d’ajouter un seuil séparé de six ou douze mois. Une taille maximale suffit :

- une fenêtre plus petite produit une seule tranche ;
- une fenêtre plus grande est divisée jusqu’à atteindre la période de fin ;
- la dernière tranche peut être plus petite que les précédentes.

## 5. Isolation du processus et libération de la mémoire

Un simple `del` suivi de `gc.collect()` libère les objets Python, mais ne garantit pas que la mémoire
réservée soit immédiatement rendue à Linux. La cible retenue est donc une isolation par processus.

Le processus parent :

1. résoudre la période complète ;
2. produire la liste ordonnée des tranches ;
3. créer l’exécution globale dans le journal ;
4. démarrer un sous-processus pour la première tranche non terminée ;
5. transmettre ses logs en temps réel ;
6. enregistrer le résultat ;
7. attendre la fin du sous-processus ;
8. passer à la tranche suivante.

Le sous-processus :

1. charger une seule tranche ;
2. exporter et dédupliquer ses valeurs ;
3. valider tous ses chunks sur Y ;
4. importer les chunks si `--apply` est actif ;
5. écrire son rapport ;
6. se terminer.

La fin du sous-processus garantit que le système d’exploitation récupère sa mémoire avant la
tranche suivante.

### Exemple 2017–2019

Avec `period_batch_size: 3`, trente-six mois deviennent douze tranches :

```text
01/12  2017-01 -> 2017-03
02/12  2017-04 -> 2017-06
03/12  2017-07 -> 2017-09
...
12/12  2019-10 -> 2019-12
```

Avec 3 018 unités d’organisation et des lots de 40, chaque mois nécessite initialement 76 requêtes
source. Une tranche trimestrielle représente donc 228 requêtes initiales, avant les éventuelles
subdivisions automatiques.

## 6. Checkpoints et reprise

Chaque tranche possède une empreinte calculée à partir de :

- l’identifiant du scénario ;
- l’identifiant du job ;
- les instances et URL source/cible ;
- le Data Set source et le Data Set cible ;
- les bornes de la tranche ;
- les stratégies de mapping ;
- le hash de la configuration ;
- le mode dry-run ou apply.

Une tranche `success` n’est pas rejouée lors d’une reprise normale. Une tranche `failed` est reprise
selon la politique de retry. Une tranche `running` laissée par un arrêt brutal est déclarée
interrompue avant sa reprise.

La reprise doit être explicite dans les logs :

```text
Exécution globale trouvée: 2017-01 -> 2019-12
7 tranches déjà terminées
Reprise à la tranche 08/12: 2018-10 -> 2018-12
```

L’option `--force` continue d’exister, mais elle ne doit jamais être ajoutée automatiquement dans
un service systemd.

## 7. Taxonomie des erreurs et retry

### 7.1 Erreurs temporaires

Sont considérées comme potentiellement temporaires :

- timeout réseau ;
- `Connection reset by peer` ;
- HTTP 429 ;
- HTTP 502, 503 ou 504 ;
- erreurs Cloudflare 520, 522 ou 524 ;
- indisponibilité DNS momentanée.

Elles peuvent être retentées avec un backoff exponentiel plafonné :

```yaml
transfer:
  retry:
    attempts: 5
    initial_delay_seconds: 5
    max_delay_seconds: 120
```

Le succès d’un `review` prouve que la connexion fonctionne au moment du test. Il ne garantit pas
qu’une erreur réseau transitoire ne réapparaîtra jamais. L’objectif est de reprendre proprement,
pas de promettre l’absence totale d’incident.

### 7.2 Erreurs permanentes

Les erreurs suivantes ne doivent pas être retentées aveuglément :

- authentification 401 ou autorisation 403 ;
- configuration invalide ;
- collision d’UID ou de code hors stratégie de clonage historique explicite ;
- incompatibilité de `valueType`, `domainType` ou CategoryCombo ;
- conflit de métadonnées nécessitant une décision humaine ;
- Data Element cible contenant déjà des valeurs incompatibles.

Le processus doit les journaliser puis s’arrêter sans importer la tranche suivante.

### 7.3 Codes de sortie disponibles

- `0` : succès ;
- `2` : erreur permanente de configuration, permission ou compatibilité ;
- `75` : erreur temporaire épuisant les retries internes.

Cette distinction permettra à systemd de redémarrer uniquement les erreurs temporaires.

## 8. Synchronisation sécurisée des métadonnées

La synchronisation doit être une phase préliminaire, pas une réaction improvisée après un échec
d’import de données.

### 8.1 Configuration disponible

```yaml
target:
  metadata:
    mode: create_if_missing
    identity_strategy: preserve_uid

    synchronization:
      enabled: true
      create_missing_objects: true
      attach_compatible_data_elements: true

      incompatible:
        policy: safe_repair
        require_no_data_values: true
        require_no_other_dataset_memberships: true
        require_same_identity: true

      historical_data_elements:
        enabled: true
        strategy: versioned
        uid_collision_strategy: clone
        name_suffix: " — Historique"
```

### 8.2 Classification obligatoire

| Situation constatée sur Y | Décision |
|---|---|
| Objet totalement absent, UID et code disponibles | Créer depuis X avec ses dépendances |
| Data Element compatible mais non rattaché au Data Set | Rattacher au Data Set |
| Même UID, CategoryCombo cible par défaut, aucune valeur | Réparation sécurisée possible |
| Même code mais UID différent | Bloquer et demander un mapping explicite |
| `valueType` ou `domainType` différent | Bloquer |
| CategoryCombo différent avec valeurs existantes | Bloquer |
| UID d’un graphe strictement historique déjà incompatible sur Y | Cloner tout le graphe sous de nouveaux UID si la stratégie vaut `clone` |
| Data Element utilisé par un autre Data Set | Bloquer ou exiger un plan de migration |
| Identité ou sémantique ambiguë | Bloquer |

### 8.3 Ordre des opérations

Avant tout export massif de valeurs :

1. récupérer le Data Set et les Data Elements de X ;
2. inventorier les objets présents sur Y ;
3. produire un plan minimal de création, rattachement ou réparation ;
4. conserver un snapshot des objets cibles concernés ;
5. valider le paquet avec `importMode=VALIDATE` ;
6. arrêter sur tout conflit non autorisé ;
7. appliquer le paquet uniquement en mode `--apply` ;
8. relire les métadonnées de Y ;
9. refaire la comparaison complète ;
10. commencer les tranches uniquement si la compatibilité finale est démontrée.

### 8.4 Commande dédiée

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-metadata hospital_service --dry-run
```

Puis, après vérification du rapport :

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-metadata hospital_service --apply
```

### 8.5 Synchronisation de la hiérarchie des unités d’organisation

La synchronisation des unités est distincte de celle des Data Elements. Elle est activée de façon
explicite et n’est disponible qu’avec un mapping par UID :

```yaml
target:
  organisation_units:
    discovery: linked_to_dataset
    mapping_strategy: uid
    synchronization:
      enabled: true
      run_before_transfer: true
      create_missing: true
      require_same_name: true
      require_same_code: false
      allow_root_creation: false
      attach_to_target_data_set: true
      preserve_geometry: true
```

Le mode opératoire est :

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-org-units relevant_epidemiological_data --dry-run

PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-org-units relevant_epidemiological_data --apply
```

La commande lit le `path` de chaque unité directement affectée au Data Set X, récupère tous ses
ancêtres et construit le paquet dans l’ordre parent-enfant. Elle crée uniquement les UID absents.
Une unité déjà présente sur Y n’est jamais renommée, déplacée ou écrasée. Le parent reste toujours
bloquant. Le nom et le code sont contrôlés séparément : `require_same_name: true` protège les
libellés, tandis que `require_same_code: false` autorise la convention de codes préfixés de Y. Les
différences autorisées sont conservées dans `warnings`; les conflits réels produisent
`blocked.json`.

Lorsque le Data Set existe déjà sur Y, les affectations d’unités sont complétées par union : aucune
affectation existante n’est supprimée. La création d’une racine reste désactivée par défaut afin de
ne pas introduire une seconde racine DHIS2. Tous les plans, paquets, validations et commits sont
conservés dans `runs/<run-id>/organisation-units/`, puis résumés dans un rapport JSON du job.

Avec `run_before_transfer: true`, cette réconciliation s’exécute automatiquement sous le verrou des
métadonnées avant la première tranche. Le pipeline vérifie ensuite que chaque OU réellement
présente dans les valeurs est affectée au DataSet cible et bloque localement avant un rejet DHIS2
`E8022`.

### 8.6 Quarantaine dynamique d’unités bloquantes

La politique est configurable globalement et héritée par tous les jobs :

```yaml
target:
  organisation_units:
    mapping_strategy: uid
    synchronization:
      enabled: true
      run_before_transfer: true
      conflict_policy: exclude
```

`block` reste la valeur par défaut. Avec `exclude`, chaque précontrôle recherche les différences de
nom ou de code déclarées obligatoires, les parents divergents et les collisions de code. L’UID en
conflit devient la racine d’une quarantaine calculée : toutes les unités directement sélectionnées
dont le `path` contient cet UID sont retirées, puis le plan de synchronisation est recalculé sur les
unités restantes. La même quarantaine est enregistrée dans le cache du job et consommée par chaque
sous-processus d’import.

Une exclusion manuelle et justifiée peut toujours compléter la détection pour un job particulier :

```yaml
jobs:
  - id: hospital_service
    organisation_units:
      exclude:
        - uid: cKCv828onjo
          reason: "Temporary review: source and target parents differ"
```

Une unité en quarantaine n’est ni demandée à X, ni comparée ou créée sur Y, ni ajoutée aux nouvelles
affectations du DataSet, ni envoyée dans un lot de valeurs. Les affectations déjà présentes sur Y ne
sont jamais supprimées. Un UID manuel absent de toute la sélection et de tous ses chemins bloque
l’exécution afin de détecter une faute de configuration.

Les rapports contiennent `organisationUnitConflictPolicy`,
`detectedOrganisationUnitConflicts`, `organisationUnitExclusions`,
`excludedSourceOrganisationUnitCount` et `excludedSourceOrganisationUnits`. Les erreurs
structurelles qui ne permettent pas d’identifier sûrement un sous-arbre restent bloquantes. Le
pipeline bloque aussi si la quarantaine devait retirer 100 % de la sélection source. Une modification
de la quarantaine change l’empreinte d’exécution afin que les fenêtres concernées ne soient pas
ignorées à tort par le mécanisme de reprise. Une exécution terminée avec des exclusions dynamiques
porte le statut explicite `success_with_quarantine`.

La création automatique d’objets absents peut être autorisée par le YAML. Les mises à jour d’objets
existants restent soumises aux critères `safe_repair`. Un objet contenant des valeurs ou utilisé
ailleurs n’est jamais modifié automatiquement.

## 9. Audit et artefacts

### 9.1 Niveaux d’audit

Le journal représente :

- l’exécution globale ;
- chaque tranche de périodes ;
- chaque phase de métadonnées ;
- chaque chunk de validation/import lorsque cela aide à reprendre ou expliquer un échec ;
- chaque tentative réseau importante.

### 9.2 États enregistrés

Exécution globale :

```text
planned -> running -> success
                   -> failed
                   -> partial
                   -> interrupted
```

Tranche :

```text
pending -> exporting -> validating -> importing -> success
            |             |             |
            +-------------+-------------+-> failed
```

### 9.3 Champs minimums par tranche

- `run_id` et `window_id` ;
- scénario et job ;
- source et cible ;
- Data Set ;
- date/période de début et de fin ;
- numéro de tranche et nombre total ;
- mode dry-run/apply ;
- heure de début et de fin ;
- durée ;
- statut ;
- nombre de tentatives ;
- nombre de valeurs source ;
- compteurs created/updated/ignored/deleted ;
- phase de l’échec ;
- type d’erreur et statut HTTP ;
- message d’erreur nettoyé des secrets ;
- hash de configuration ;
- chemins des rapports et artefacts.

### 9.4 Artefacts produits

```text
state/<target>/<scenario>/jobs/<job>/
  journal.sqlite3
  runs/<run-id>/
    run-summary.json
    configuration.json
    metadata/
      plan.json
      validation.json
      commit.json
      created.json
      attached.json
      repaired.json
      blocked.json
      rollback-evidence.json
    windows/
      2017-01__2017-03/
        report.json
        success.json
      2017-04__2017-06/
        report.json
        failure.json
```

Les payloads complets de valeurs restent optionnels afin de ne pas saturer le disque.

### 9.5 Logs systemd attendus

```text
Run démarré | job=hospital_service | windows=12
Synchronisation des métadonnées démarrée
Synchronisation des métadonnées réussie | created=1 | attached=365 | repaired=0
Tranche 01/12 démarrée | start=2017-01 | end=2017-03
Export source terminé | values=...
Dry-run cible réussi | chunks=...
Import cible réussi | created=... | updated=... | ignored=...
Tranche 01/12 terminée | durationSeconds=...
Sous-processus terminé | mémoire de la tranche libérée
```

Les succès doivent être journalisés avec le même niveau de précision que les erreurs.

## 10. Plusieurs Data Sets et systemd

### 10.1 Service template livré

Un service template permettra à chaque Data Set de posséder son exécution :

```text
dhis2-transfer@hospital_service.service
dhis2-transfer@sigl_fosa.service
dhis2-transfer@population_as.service
```

Chaque job doit disposer de son propre journal, de ses rapports et de son verrou.

### 10.2 Verrous nécessaires

1. **Verrou par job** : interdit deux exécutions simultanées du même job.
2. **Verrou métadonnées cible** : sérialise les créations et réparations sur Y.
3. **Limite globale** : limite le nombre de jobs lourds exécutés simultanément.

Le verrou métadonnées est libéré après revalidation. Les phases de valeurs de jobs différents
peuvent ensuite fonctionner en parallèle si la limite globale l’autorise.

### 10.3 Concurrence recommandée

La valeur initiale recommandée est :

```yaml
execution:
  max_parallel_jobs: 1
```

Une valeur de `2` pourra être testée après mesure de la charge sur X, Y, Cloudflare, la mémoire et
la base de données. Une concurrence illimitée est exclue.

## 11. Flux implémenté de bout en bout

```text
systemd démarre un job
        |
        v
vérification X et Y avec retries classifiés
        |
        v
audit et synchronisation verrouillée des métadonnées
        |
        v
revalidation complète de Y
        |
        v
calcul des tranches et lecture des checkpoints
        |
        v
sous-processus tranche N
  export -> dry-run -> import -> rapport
        |
        v
checkpoint success et libération du sous-processus
        |
        +---- tranche suivante
        |
        v
rapport global success/partial/failed
```

## 12. Critères d’acceptation

### 12.1 Découpage et mémoire

- une fenêtre de trente-six mois configurée par tranches de trois mois produit douze tranches ;
- la dernière tranche est correcte lorsque le total n’est pas divisible ;
- une seule tranche est présente en mémoire ;
- chaque tranche s’exécute dans un processus distinct ;
- un échec n’efface pas les checkpoints réussis.

### 12.2 Reprise

- un arrêt après la tranche 7 reprend à la tranche 8 ;
- une tranche réussie n’est pas réimportée sans `--force` ;
- une tranche interrompue est détectée et reclassée ;
- une erreur temporaire respecte le nombre de retries et le backoff ;
- une erreur permanente s’arrête immédiatement.

### 12.3 Métadonnées

- un Data Element absent est créé avec ses dépendances minimales ;
- un Data Element compatible est rattaché sans être écrasé ;
- une collision de code/UID courante bloque l’exécution ;
- une collision historique configurée en `clone` laisse l’objet Y intact, crée un graphe isolé et
  réécrit le Data Element et le CategoryOptionCombo de chaque valeur concernée ;
- une différence de type bloque l’exécution ;
- une réparation est refusée si des valeurs existent ;
- les métadonnées sont relues et revalidées après le commit ;
- aucun export historique ne commence après un échec de métadonnées.

### 12.4 Audit

- chaque succès et chaque erreur possède un enregistrement horodaté ;
- les rapports relient l’exécution globale, la tranche et les chunks ;
- les messages ne contiennent aucun secret ;
- le rapport global indique les tranches réussies, échouées et restantes ;
- les journaux restent cohérents après un arrêt brutal.

### 12.5 Concurrence

- deux processus du même job ne peuvent pas coexister ;
- deux jobs ne peuvent pas modifier simultanément les métadonnées de Y ;
- les journaux de jobs différents ne se bloquent pas mutuellement ;
- la limite globale de concurrence est respectée.

## 13. Phases réalisées

### Phase 1 — Découpage et journal par tranche

- ajouter la configuration `transfer.execution` ;
- calculer les tranches selon le type de période ;
- ajouter les tables d’exécution globale et de tranches ;
- produire les logs et rapports par tranche.

### Phase 2 — Isolation et reprise

- créer l’orchestrateur parent ;
- exécuter chaque tranche dans un sous-processus ;
- ajouter les checkpoints et la reprise ;
- classifier les erreurs et les codes de sortie.

### Phase 3 — Synchronisation des métadonnées

- ajouter le plan de synchronisation ;
- créer les objets absents ;
- rattacher les objets compatibles ;
- intégrer les règles `safe_repair` ;
- relire et revalider Y.

### Phase 4 — systemd multi-jobs

- ajouter le service template ;
- isoler l’état par job ;
- ajouter les verrous job et métadonnées ;
- limiter la concurrence ;
- ajouter les timers nécessaires.

### Phase 5 — Validation opérationnelle

- tester d’abord un Data Set et une tranche sur l’instance cible autorisée ;
- simuler un timeout et vérifier la reprise ;
- simuler un arrêt du processus et vérifier le checkpoint ;
- tester une métadonnée manquante puis une métadonnée incompatible ;
- augmenter la concurrence uniquement après mesure.

## 14. Recommandation opérationnelle actuelle

- exécuter `review`, puis approuver une première exécution `reconcile-org-units --dry-run` ; après
  cela, activer `run_before_transfer: true` pour automatiser le même garde-fou ;
- exécuter ensuite `reconcile-metadata --dry-run`, puis `--apply` après lecture du plan ;
- relancer `transfer run --dry-run` avant le premier import réel ;
- garder des tranches d’un à trois mois pour les Data Sets mensuels volumineux ;
- ne pas utiliser `--force` dans systemd ;
- conserver les rapports et le journal SQLite par job ;
- commencer avec `execution.max_parallel_jobs: 1` ;
- considérer une erreur réseau réussie au retry comme un incident transitoire, pas comme une preuve
  qu’elle ne reviendra jamais.

## 15. Définition du succès architectural

L’évolution sera considérée comme réussie lorsque le pipeline pourra transférer une période de dix
ans sans croissance mémoire liée à la période totale, reprendre après un arrêt à la dernière
tranche non terminée, aligner automatiquement les métadonnées strictement sûres, bloquer les
incompatibilités dangereuses et fournir un audit complet utilisable depuis systemd.
