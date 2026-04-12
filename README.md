# 🏖️ TourisData — Plateforme Open Data de l'Hébergement Touristique en France

TourisData est un pipeline complet de données (ETL) et une application web visant à recenser, nettoyer et visualiser les hébergements touristiques (Hôtels, Campings, Résidences, Locations Airbnb/Meublés de tourisme) en France métropolitaine et outre-mer, **en se basant exclusivement sur les données publiques ouvertes**.

## 📊 Sources des Données (.gouv.fr)

Le projet s'alimente directement depuis l'Open Data français :
1. **Atout France** : Registre national des hébergements classés (Hôtels, campings, villages vacances).
2. **DATAtourisme** : Ontologie unifiée du tourisme en France (Locations de vacances, gîtes, chambres d'hôtes).
3. **API Géo / BAN** : Base Adresse Nationale pour le géocodage spatial des communes et la carte de la France.
4. **INSEE** : Données de fréquentation touristique (séries chronologiques mensuelles).
5. **Locations / Meublés de tourisme** : Scraping de déclarations publiques via la base DATAtourisme ciblée sur l'hébergement locatif.

## 🛠️ Stack Technique

- **Extraction / Transformation (ETL)** : Python, Pandas, Requests, nettoyage UTF-8 et Regex.
- **Data Lake / Storage** : Stockage organisé en dossiers structurés (`raw`, `clean`). Intégration prête pour MongoDB (JSON) et PostgreSQL (Data Warehouse).
- **Backend / API** : FastAPI, Pydantic, Python 3 (requêtes haute performance).
- **Frontend** : HTML5, CSS natif, JavaScript Vanilla, Leaflet.js (Cartographie), Chart.js (Graphiques).
- **Qualité / Tests** : Pytest, mocks d'API `unittest.mock`.

---

## 🚀 Installation & Exécution rapide

### 1. Pré-requis
Avoir Python 3.9+ installé.

```bash
git clone https://github.com/votre-user/TourisData.git
cd TourisData
python -m venv env
source env/bin/activate  # Sous Windows : env\Scripts\activate
pip install -r requirements.txt
```

*(Optionnel)* Créez un fichier `.env` à la racine contenant votre clé d'API DATAtourisme :
```env
DATATOURISME_API_KEY=votre_cle_secrete
```

### 2. Lancer le Pipeline ETL (Extraction & Transformation)
La commande suivante va télécharger, parser, nettoyer et géocoder des milliers de lignes depuis l'Open Data français.
```bash
python -m data_pipline.main
```
Les données validées se retrouveront dans le dossier `data_lake/clean/hebergements_unifies.csv`.

### 3. Démarrer le Backend (L'API)
```bash
uvicorn backend_api.app:app --reload --port 8000
```
L'API documentaire sera visible sur : [http://localhost:8000/docs](http://localhost:8000/docs)

### 4. Démarrer le Frontend (Site Web)
Dans un **nouveau terminal**, lancez un serveur local dans le dossier frontend pour éviter les problèmes de sécurité du navigateur (CORS) :
```bash
cd frontend_web
python -m http.server 8080
```
Ouvrez le site web sur : [http://localhost:8080](http://localhost:8080)

---

## 🧪 Lancer la suite de Tests

Le code de production est couvert par plus de 130 tests unitaires et de bout-en-bout (E2E).
```bash
python -m pytest tests/ -v
```

## 🏗 Architecture & Contribution

Consultez le fichier `documentation_technique.md` (situé à la racine) pour comprendre les flux d'architecture, le routage et le design pattern ETL du projet.
