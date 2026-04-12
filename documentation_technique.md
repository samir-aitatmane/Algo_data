# Documentation Technique — TourisData

Ce document référence l'architecture conceptuelle, les choix techniques, et le flux des données au sein de la plateforme TourisData. 

---

## 1. Dictionnaire & Modèle des Données

Au bout de notre pipeline ETL, nous produisons le fichier cible `hebergements_unifies.csv` (et `.json`). Ce fichier est le pilier de la plateforme et voici son schéma formel :

| Colonne | Type Python | Description |
|---|---|---|
| `id_source` | string / PK | Clé primaire concaténée (ex: `AF_Hotel_xxx` ou `DT_https...`). Elle permet de gérer les dédoublonnages sur plusieurs jeux de données. |
| `source` | enum | D'où vient la donnée : `atout_france`, `datatourisme`, ou `locations_scraping_gouv`. |
| `type` | enum | Catégoriel strict : `hotel`, `camping`, `residence`, `village_vacances`, `auberge`, `location` ou `hebergement` (fallback). |
| `nom` | string | Dénomination commerciale de l'entité. Normalisé via `str.title()`. |
| `adresse`, `code_postal`, `ville` | string | Eléments postaux. |
| `departement`, `region` | string | Agglomérations supérieures. |
| `etoiles` | float | Float nullable (1.0 à 5.0) indiquant le classement officiel. Utilisé par Pandas en `float64` pour éviter les plantages de l'API lors des `round()`. |
| `latitude`, `longitude` | float | **Crucial pour l'affichage Leaflet**. Récupéré de Datatourisme, ou géocodé dynamiquement. |
| `nb_chambres` | float/Int64 | Capacité de l'établissement. |
| `site_web`, `telephone` | string | Pratique pour l'API / Front. |

---

## 2. Extraction, Transformation, Chargement (Pipeline ETL)

Le code est structuré en modules respectant l'isolation :

### 2.1 Extraction (`extract.py`)
Le pipeline utilise la librairie `requests` pour attaquer plusieurs endpoints API de manière asynchrone / synchronisée :
- **Atout France** : Scraping direct d'un Open Data CSV depuis *data.gouv.fr*. Le paramètre `low_memory=False` est passé à Pandas pour de gros volumes.
- **Meublés de tourisme (Airbnb)** : L'extraction s'appuie désormais sur une requête SPARQL ultra-ciblée envoyée à DATAtourisme (focalisée sur les instances `schema.org/Rental` & `tourisme:RentalAccommodation`) offrant ainsi des hébergements locatifs de type "Meublé de Tourisme".
- **API Géo** : Un pont vers https://geo.api.gouv.fr/communes qui rapporte 35k communes françaises avec leurs polygones et points d'ancrage GPS.

### 2.2 Transformation (`transform.py`)
C'est le composant le plus lourd.
- **Nettoyage Encodage** : Les en-têtes CSV d'Atout France ont un encodage non standard (`TYPOLOGIE ÉTABLISSEMENT`). Un nettoyage complet en pure ASCII est exécuté avec `unicodedata.normalize` pour éviter les `KeyError`.
- **Typage Spatial (Geocoding)** : Si l'API Atout France ignore complètement les coordonnées GPS, notre transformation utilise un dictionnaire d'indexation inversé créé à la volée depuis la table des communes `geo_communes`. C'est une **jointure spatiale virtuelle** (`dict.get()`) sur le nom de la ville converti en minuscule, affectant un géo-point estimé au centre-ville. 

### 2.3 Chargement (`load.py`)
En attente de couplage complet côté serveur de production, le loading se comporte de manière double :
1. Stockage statique local : dans `data_lake/` (faisant office d'historisation statique).
2. Base documentaire & DataWarehouse : (Mock ou connexions SQLite / PosgreSQL selon implémentation locale).

---

## 3. Le Backend REST (FastAPI)

Le fichier `app.py` expose les données préparées. 
Il implémente un système de **cache en mémoire local** Python (`_cache` = dictionary python à l'exterieur des routes). Le chargement CSV complet prend environ 50 millisecondes.

### Résolution de Bugs connus dans FastAPI
1. **Les Typages NumPy vs JSONResponse** : Pandas utilise des concepts comme le `numpy.nan` qui est interdit en JSON standard. Le backend gère la conversion (`math.isnan`) transformant des floats inaccessibles en valeurs scalaires `None` (Null) évitant l'erreur "*Out of range float values are not JSON compliant*".

---

## 4. Frontend & Intégration Continue (Tests)
- **Leaflet & MarkerCluster** est privilégié pour l'interface car il supporte nativement un grand nombre (30,000+) de marqueurs DOM HTML sans lenteur, grâce au "clustering" mathématique spatial.
- **Sécurité et validation continue (Pytest)** : `conftest.py` expose le Mock de réponses. Pour chaque type d'évolution du pipeline, un mock `MagicMock` HTTP intercepte les accès aux API de l'INSEE. Le projet garantit (via une CI future) que ni le front ni l'API ne cassent au gré des changements de formats d'État.
