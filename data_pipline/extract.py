"""
extract.py — Pipeline d'extraction des données touristiques
===========================================================
Sources :
  1. DATAtourisme API      → clé dans .env  (DATATOURISME_API_KEY)
  2. Atout France CSV      → open data, URL directe (data.gouv.fr)
  3. INSEE Fréquentation   → open data, API insee.fr (séries temporelles)
  4. API Géo gouv.fr       → open data, API gratuite sans clé
"""

import os
import io
import json
import time
import logging
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# ─── Config ──────────────────────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# Dossier de sortie des données brutes
RAW_DIR = Path(__file__).parent.parent / "data_lake" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ─── 1. DATAtourisme API ─────────────────────────────────────────────────────
#   → Clé à mettre dans .env : DATATOURISME_API_KEY=<votre_clé>
#   → Portail : https://diffuseur.datatourisme.fr/
#   → Donne : hébergements géolocalisés (hôtels, gîtes, campings…) en JSON-LD
DATATOURISME_API_KEY = os.getenv("DATATOURISME_API_KEY")
DATATOURISME_URL = "https://diffuseur.datatourisme.fr/webservice/query"


def extract_datatourisme(limit: int = 500, offset: int = 0) -> pd.DataFrame:
    """
    Interroge l'API DATAtourisme pour récupérer les offres d'hébergement.
    Utilise un filtre sur la classe 'Accommodation' (hébergements touristiques).

    Args:
        limit  : nombre max de résultats par page (max 1000)
        offset : décalage pour la pagination

    Returns:
        DataFrame avec les hébergements extraits, ou DataFrame vide si erreur.
    """
    if not DATATOURISME_API_KEY:
        log.warning("DATATOURISME_API_KEY manquante dans .env — extraction ignorée.")
        return pd.DataFrame()

    headers = {
        "Authorization": f"Bearer {DATATOURISME_API_KEY}",
        "Content-Type": "application/json"
    }

    # Requête SPARQL-like pour filtrer les hébergements
    query = {
        "query": """
            PREFIX schema: <https://schema.org/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?id ?type ?name ?latitude ?longitude ?postalCode ?city
            WHERE {
                ?id a schema:Accommodation ;
                    schema:name ?name ;
                    schema:geo ?geo .
                ?geo schema:latitude  ?latitude ;
                     schema:longitude ?longitude .
                OPTIONAL { ?id schema:address ?addr .
                           ?addr schema:postalCode ?postalCode ;
                                 schema:addressLocality ?city . }
                OPTIONAL { ?id a ?type . }
            }
            LIMIT %(limit)d OFFSET %(offset)d
        """ % {"limit": limit, "offset": offset}
    }

    try:
        log.info(f"[DATAtourisme] Extraction offset={offset}, limit={limit}…")
        resp = requests.post(DATATOURISME_URL, headers=headers, json=query, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # Structure de la réponse : {"results": {"bindings": [...]}}
        bindings = data.get("results", {}).get("bindings", [])
        records = []
        for b in bindings:
            records.append({
                "id":          b.get("id", {}).get("value"),
                "type":        b.get("type", {}).get("value"),
                "name":        b.get("name", {}).get("value"),
                "latitude":    b.get("latitude", {}).get("value"),
                "longitude":   b.get("longitude", {}).get("value"),
                "postal_code": b.get("postalCode", {}).get("value"),
                "city":        b.get("city", {}).get("value"),
            })
        df = pd.DataFrame(records)
        log.info(f"[DATAtourisme] {len(df)} hébergements récupérés.")
        return df

    except requests.RequestException as e:
        log.error(f"[DATAtourisme] Erreur API : {e}")
        return pd.DataFrame()


def extract_datatourisme_all(max_pages: int = 10) -> pd.DataFrame:
    """Extrait toutes les pages disponibles (pagination automatique)."""
    all_dfs = []
    limit = 500
    for page in range(max_pages):
        df = extract_datatourisme(limit=limit, offset=page * limit)
        if df.empty:
            break
        all_dfs.append(df)
        if len(df) < limit:  # dernière page
            break
        time.sleep(0.5)  # politesse envers l'API

    if all_dfs:
        result = pd.concat(all_dfs, ignore_index=True)
        out = RAW_DIR / "datatourisme.csv"
        result.to_csv(out, index=False, encoding="utf-8")
        log.info(f"[DATAtourisme] Sauvegardé → {out} ({len(result)} lignes)")
        return result
    return pd.DataFrame()


# ─── 2. Atout France — Hébergements classés (open data, CSV direct) ──────────
#   → Source  : https://www.data.gouv.fr/fr/datasets/hebergements-touristiques-classes-en-france/
#   → Licence : Licence Ouverte 2.0 (gratuit, sans clé)
#   → Contenu : hôtels, campings, résidences, villages vacances, auberges
#               avec étoiles, adresse, nb chambres/emplacements
#   → Mis à jour quotidiennement (J-1)
ATOUT_FRANCE_CSV_URL = (
    "https://data.classement.atout-france.fr/static/"
    "exportHebergementsClasses/hebergements_classes.csv"
)
# URL alternative via data.gouv.fr (stable même si l'URL upstream change)
ATOUT_FRANCE_DATAGOUV_URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/"
    "3ce290bf-07ec-4d63-b12b-d0496193a535"
)


def extract_atout_france() -> pd.DataFrame:
    """
    Télécharge le CSV des hébergements classés Atout France.
    Essaie d'abord l'URL directe Atout France, puis le miroir data.gouv.fr.

    Returns:
        DataFrame avec tous les hébergements classés en France.
    """
    for url in [ATOUT_FRANCE_CSV_URL, ATOUT_FRANCE_DATAGOUV_URL]:
        try:
            log.info(f"[Atout France] Téléchargement CSV depuis : {url}")
            resp = requests.get(url, timeout=60, stream=True)
            resp.raise_for_status()

            # Détection auto de l'encodage (habituellement utf-8)
            encoding = resp.encoding or "utf-8"
            content = resp.content.decode(encoding, errors="replace")
            df = pd.read_csv(io.StringIO(content), sep=";", low_memory=False)

            log.info(f"[Atout France] {len(df)} lignes, {len(df.columns)} colonnes.")

            out = RAW_DIR / "atout_france_hebergements.csv"
            df.to_csv(out, index=False, encoding="utf-8")
            log.info(f"[Atout France] Sauvegardé → {out}")
            return df

        except requests.RequestException as e:
            log.warning(f"[Atout France] Échec URL {url} : {e}")

    log.error("[Atout France] Impossible de télécharger le fichier.")
    return pd.DataFrame()


# ─── 3. INSEE — Fréquentation des hébergements touristiques ──────────────────
#   → Source  : https://www.data.gouv.fr/fr/datasets/frequentation-des-hebergements-touristiques/
#   → Licence : Licence Ouverte 2.0 (gratuit, sans clé)
#   → Contenu : nuitées, arrivées, durées de séjour — hôtels/campings/autres
#   → Données HVD (High-Value Dataset) désignées par Eurostat
#   → L'API INSEE BDM donne accès aux séries temporelles
INSEE_FREQUENTATION_URL = (
    "https://api.insee.fr/melodi/file/DS_TOUR_FREQ/DS_TOUR_FREQ_CSV_FR"
)
# Codes de séries INSEE pour la fréquentation (nuitées mensuelles)
# Source : https://www.insee.fr/fr/statistiques/serie/010777645
INSEE_SERIES = {
    "nuitees_hotels_france":   "010777645",   # Nuitées hôtels — France entière
    "nuitees_campings_france": "010777693",   # Nuitées campings — France entière
    "nuitees_autres_france":   "010777709",   # Autres hébergements — France entière
}
INSEE_BDM_URL = "https://api.insee.fr/series/BDM/V1/data/SERIES_BDM/{idbank}"


def extract_insee_frequentation_file() -> pd.DataFrame:
    """
    Télécharge le fichier ZIP complet de fréquentation des hébergements
    depuis l'API INSEE (lien direct data.gouv.fr).

    Returns:
        DataFrame avec les données de fréquentation.
    """
    try:
        log.info("[INSEE] Téléchargement fréquentation hébergements (ZIP)…")
        resp = requests.get(INSEE_FREQUENTATION_URL, timeout=60, stream=True)
        resp.raise_for_status()

        # Le fichier est un ZIP contenant un CSV
        import zipfile
        zip_content = io.BytesIO(resp.content)
        dfs = []

        with zipfile.ZipFile(zip_content) as z:
            log.info(f"[INSEE] Fichiers dans le ZIP : {z.namelist()}")
            for name in z.namelist():
                if name.endswith(".csv"):
                    with z.open(name) as f:
                        df = pd.read_csv(f, sep=";", low_memory=False)
                        log.info(f"[INSEE] {name} → {len(df)} lignes")
                        dfs.append(df)

        if dfs:
            result = pd.concat(dfs, ignore_index=True)
            out = RAW_DIR / "insee_frequentation_hebergements.csv"
            result.to_csv(out, index=False, encoding="utf-8")
            log.info(f"[INSEE] Sauvegardé → {out} ({len(result)} lignes)")
            return result

    except Exception as e:
        log.warning(f"[INSEE] Erreur téléchargement ZIP : {e}")
        log.info("[INSEE] Tentative via API BDM (séries individuelles)…")
        return extract_insee_series_bdm()

    return pd.DataFrame()


def extract_insee_series_bdm() -> pd.DataFrame:
    """
    Récupère les séries de nuitées via l'API BDM de l'INSEE.
    Aucune clé d'API requise pour les séries publiques.

    Returns:
        DataFrame avec les séries temporelles de fréquentation.
    """
    all_dfs = []
    headers = {"Accept": "application/json"}

    for label, idbank in INSEE_SERIES.items():
        url = INSEE_BDM_URL.format(idbank=idbank)
        try:
            log.info(f"[INSEE BDM] Série {label} ({idbank})…")
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()

            # Parse XML/JSON de la réponse SDMX
            # L'API BDM retourne du XML SDMX par défaut
            # On préfère le format JSON avec le header Accept
            data = resp.json()
            observations = (
                data.get("dataSets", [{}])[0]
                   .get("series", {})
                   .get("0:0:0:0", {})
                   .get("observations", {})
            )
            structure = data.get("structure", {})
            time_periods = (
                structure.get("dimensions", {})
                         .get("observation", [{}])[0]
                         .get("values", [])
            )

            records = []
            for idx, value in observations.items():
                idx_int = int(idx)
                records.append({
                    "serie":      label,
                    "idbank":     idbank,
                    "periode":    time_periods[idx_int]["id"] if idx_int < len(time_periods) else idx,
                    "valeur":     value[0],
                    "statut":     value[1] if len(value) > 1 else None,
                })
            df = pd.DataFrame(records)
            log.info(f"[INSEE BDM] {label} → {len(df)} observations")
            all_dfs.append(df)
            time.sleep(0.3)

        except Exception as e:
            log.error(f"[INSEE BDM] Erreur série {label} : {e}")

    if all_dfs:
        result = pd.concat(all_dfs, ignore_index=True)
        out = RAW_DIR / "insee_series_nuitees.csv"
        result.to_csv(out, index=False, encoding="utf-8")
        log.info(f"[INSEE BDM] Sauvegardé → {out}")
        return result

    return pd.DataFrame()


# ─── 4. API Géo gouv.fr — Référentiel géographique ───────────────────────────
#   → Source  : https://api.gouv.fr/les-api/api-geo
#   → Doc     : https://geo.api.gouv.fr/decoupage-administratif
#   → Licence : Licence Ouverte 2.0 (gratuit, sans clé, pas de limite)
#   → Contenu : communes, départements, régions avec coordonnées GPS
GEO_API_BASE = "https://geo.api.gouv.fr"
GEO_COMMUNES_URL = f"{GEO_API_BASE}/communes"
GEO_DEPARTEMENTS_URL = f"{GEO_API_BASE}/departements"
GEO_REGIONS_URL = f"{GEO_API_BASE}/regions"


def extract_geo_communes(fields: str = "nom,code,codesPostaux,centre,population,departement,region") -> pd.DataFrame:
    """
    Télécharge toutes les communes françaises avec coordonnées GPS.
    Utilisé pour enrichir les hébergements avec les données géographiques.

    Args:
        fields : champs à récupérer (voir doc https://geo.api.gouv.fr)

    Returns:
        DataFrame avec les communes françaises.
    """
    params = {"fields": fields, "format": "json", "type": "commune-actuelle"}
    try:
        log.info("[API Géo] Extraction des communes françaises…")
        resp = requests.get(GEO_COMMUNES_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        df = pd.json_normalize(data)
        log.info(f"[API Géo] {len(df)} communes récupérées.")

        out = RAW_DIR / "geo_communes.csv"
        df.to_csv(out, index=False, encoding="utf-8")
        log.info(f"[API Géo] Sauvegardé → {out}")
        return df

    except requests.RequestException as e:
        log.error(f"[API Géo] Erreur communes : {e}")
        return pd.DataFrame()


def extract_geo_departements() -> pd.DataFrame:
    """Récupère la liste des 101 départements français avec leurs codes."""
    params = {"fields": "nom,code,codeRegion", "format": "json"}
    try:
        log.info("[API Géo] Extraction des départements…")
        resp = requests.get(GEO_DEPARTEMENTS_URL, params=params, timeout=30)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())
        log.info(f"[API Géo] {len(df)} départements.")

        out = RAW_DIR / "geo_departements.csv"
        df.to_csv(out, index=False, encoding="utf-8")
        return df

    except requests.RequestException as e:
        log.error(f"[API Géo] Erreur départements : {e}")
        return pd.DataFrame()


def extract_geo_regions() -> pd.DataFrame:
    """Récupère la liste des 18 régions françaises."""
    params = {"fields": "nom,code", "format": "json"}
    try:
        log.info("[API Géo] Extraction des régions…")
        resp = requests.get(GEO_REGIONS_URL, params=params, timeout=30)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())
        log.info(f"[API Géo] {len(df)} régions.")

        out = RAW_DIR / "geo_regions.csv"
        df.to_csv(out, index=False, encoding="utf-8")
        return df

    except requests.RequestException as e:
        log.error(f"[API Géo] Erreur régions : {e}")
        return pd.DataFrame()


# ─── Point d'entrée principal ─────────────────────────────────────────────────
def run_all_extractions() -> dict:
    """
    Lance toutes les extractions et retourne un résumé des résultats.

    Returns:
        dict { source_name: (nb_lignes, chemin_fichier) }
    """
    log.info("=" * 60)
    log.info("  PIPELINE D'EXTRACTION — DONNÉES TOURISTIQUES FRANCE")
    log.info("=" * 60)

    results = {}

    # 1. DATAtourisme (nécessite clé .env)
    df_datatourisme = extract_datatourisme_all(max_pages=5)
    results["datatourisme"] = len(df_datatourisme)

    # 2. Atout France CSV (open data)
    df_atout = extract_atout_france()
    results["atout_france"] = len(df_atout)

    # 3. INSEE Fréquentation (open data)
    df_insee = extract_insee_frequentation_file()
    results["insee_frequentation"] = len(df_insee)

    # 4. API Géo (open data, sans clé)
    df_communes = extract_geo_communes()
    results["geo_communes"] = len(df_communes)

    df_depts = extract_geo_departements()
    results["geo_departements"] = len(df_depts)

    df_regions = extract_geo_regions()
    results["geo_regions"] = len(df_regions)

    # 5. Scraping "Meublés de tourisme" (Locations Gouv)
    df_locs = extract_locations_touristiques(limit=2000)
    results["locations_vacances"] = len(df_locs)

    # Résumé
    log.info("\n" + "=" * 60)
    log.info("  RÉSUMÉ DE L'EXTRACTION")
    log.info("=" * 60)
    for source, count in results.items():
        status = "✅" if count > 0 else "❌"
        log.info(f"  {status}  {source:<30} {count:>6} lignes")
    log.info(f"\n  Fichiers sauvegardés dans : {RAW_DIR}")

    return results

# ─── 4. Scraping "Meublés de Tourisme" (Locations) ───────────────────────────
# Source: data.gouv.fr (requête spécifique sur les hébergements locatifs)

def extract_locations_touristiques(limit: int = 2000) -> pd.DataFrame:
    """
    Extrait spécifiquement les locations de vacances (Meublés de tourisme, Airbnb-like)
    via l'endpoint officiel du gouvernement.
    """
    query = {
        "query": f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX schema: <https://schema.org/>
        PREFIX tourisme: <https://www.datatourisme.fr/ontology/core#>

        SELECT ?id ?name ?latitude ?longitude ?postalCode ?city WHERE {{
            ?id a tourisme:RentalAccommodation .
            ?id rdfs:label ?name .
            OPTIONAL {{
                ?id tourisme:isLocatedAt ?place .
                ?place schema:geo ?geo .
                ?geo schema:latitude ?latitude .
                ?geo schema:longitude ?longitude .
                ?place schema:address ?address .
                ?address schema:postalCode ?postalCode .
                ?address schema:addressLocality ?city .
            }}
            FILTER (lang(?name) = 'fr' || lang(?name) = '')
        }}
        LIMIT {limit}
        """
    }

    try:
        log.info(f"[Locations .gouv] Scraping API open data pour {limit} locations...")
        resp = requests.post(DATATOURISME_URL, headers={"apikey": DATATOURISME_API_KEY}, json=query, timeout=30)
        resp.raise_for_status()
        
        bindings = resp.json().get("results", {}).get("bindings", [])
        records = []
        for b in bindings:
            records.append({
                "id":          b.get("id", {}).get("value"),
                "nom":         b.get("name", {}).get("value"),
                "type":        "location",
                "latitude":    b.get("latitude", {}).get("value"),
                "longitude":   b.get("longitude", {}).get("value"),
                "code_postal": b.get("postalCode", {}).get("value"),
                "ville":       b.get("city", {}).get("value"),
            })
        
        df = pd.DataFrame(records)
        df = df.dropna(subset=['latitude', 'longitude'])  # On force la geo pour la carte
        log.info(f"[Locations .gouv] Scraping réussi : {len(df)} locations extraites.")
        
        out = RAW_DIR / "locations_vacances.csv"
        df.to_csv(out, index=False, encoding="utf-8")
        return df

    except Exception as e:
        log.error(f"[Locations .gouv] Erreur de scraping : {e}")
        return pd.DataFrame()

    return results


if __name__ == "__main__":
    run_all_extractions()
