"""
transform.py — Nettoyage et enrichissement des données touristiques
====================================================================
Étape 2 du pipeline ETL : Raw → Clean

  1. transform_atout_france()   — Hébergements classés Atout France
  2. transform_datatourisme()   — Offres DATAtourisme
  3. transform_insee()          — Séries fréquentation INSEE
  4. transform_geo()            — Référentiel géographique
  5. run_all_transforms()       — Orchestrateur
"""

import re
import json
import logging
import numpy as np
import pandas as pd
import unicodedata
from pathlib import Path

log = logging.getLogger(__name__)

RAW_DIR   = Path(__file__).parent.parent / "data_lake" / "raw"
CLEAN_DIR = Path(__file__).parent.parent / "data_lake" / "clean"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _save(df: pd.DataFrame, name: str) -> Path:
    """Sauvegarde CSV + JSON dans data_lake/clean/."""
    csv_path  = CLEAN_DIR / f"{name}.csv"
    json_path = CLEAN_DIR / f"{name}.json"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    df.to_json(json_path, orient="records", force_ascii=False, indent=2)
    log.info(f"[Transform] Sauvegardé : {csv_path.name} ({len(df)} lignes)")
    return csv_path


def _load_raw(name: str) -> pd.DataFrame:
    path = RAW_DIR / f"{name}.csv"
    if not path.exists():
        log.warning(f"[Transform] Fichier brut introuvable : {path}")
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


# ─── 1. Atout France — Hébergements classés ───────────────────────────────────

ATOUT_RENAME = {
    "typologie_etablissement": "type",
    "nom_commercial":          "nom",
    "adresse":                 "adresse",
    "code_postal":             "code_postal",
    "commune":                 "ville",
    "code_commune":            "code_commune",
    "departement":             "departement",
    "region":                  "region",
    "classement":              "etoiles",
    "nbre_de_chambres":        "nb_chambres",
    "nbre_d_emplacements":     "nb_emplacements",
    "nbre_d_unites_d_habitations": "nb_locations",
    "latitude":                "latitude",
    "longitude":               "longitude",
    "telephone":               "telephone",
    "courriel":                "email",
    "site_internet":           "site_web",
    "date_de_classement":      "date_classement",
}

TYPE_MAPPING = {
    "HOTEL DE TOURISME":          "hotel",
    "CAMPING":                    "camping",
    "RÉSIDENCE DE TOURISME":      "residence",
    "RESIDENCE DE TOURISME":      "residence",
    "VILLAGE DE VACANCES":        "village_vacances",
    "PARC RESIDENTIEL DE LOISIRS":"parc_loisirs",
    "AUBERGE COLLECTIVE":         "auberge",
}


def transform_atout_france() -> pd.DataFrame:
    """
    Nettoie le CSV Atout France :
      - Renomme les colonnes
      - Normalise les types d'hébergement
      - Convertit étoiles en entier
      - Valide et clame les coordonnées GPS (France métro + DOM)
      - Supprime les doublons
    """
    df = _load_raw("atout_france_hebergements")
    if df.empty:
        return df

    log.info(f"[Atout France] Brut : {len(df)} lignes, colonnes : {list(df.columns)}")

    # ── Normalisation des noms de colonnes ────────────────────────────────────
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .to_series()
          .apply(lambda x: unicodedata.normalize('NFKD', str(x)).encode('ASCII', 'ignore').decode('utf-8'))
          .str.replace(" ", "_", regex=False)
          .str.replace("'", "_", regex=False)
    )

    # Renommage selon mapping (seulement les colonnes qui existent)
    rename_map = {k: v for k, v in ATOUT_RENAME.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # ── Type d'hébergement ────────────────────────────────────────────────────
    if "type" in df.columns:
        df["type"] = (
            df["type"]
              .str.upper()
              .str.strip()
              .to_series()
              .apply(lambda x: unicodedata.normalize('NFKD', str(x)).encode('ASCII', 'ignore').decode('utf-8') if pd.notnull(x) else x)
              .map(lambda x: TYPE_MAPPING.get(x, x.lower() if isinstance(x, str) else "autre"))
        )

    # ── Étoiles → entier ──────────────────────────────────────────────────────
    if "etoiles" in df.columns:
        df["etoiles"] = (
            df["etoiles"]
              .astype(str)
              .str.extract(r"(\d+)")[0]
              .astype(float)
              .astype("Int64")
        )

    # ── Coordonnées GPS ───────────────────────────────────────────────────────
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "."), errors="coerce")

    # Filtre géographique : France métropolitaine + DOM-TOM (approx.)
    if "latitude" in df.columns and "longitude" in df.columns:
        valid_geo = (
            (df["latitude"].between(-22, 52)) &
            (df["longitude"].between(-65, 56))
        )
        n_invalid = (~valid_geo & df["latitude"].notna()).sum()
        if n_invalid:
            log.warning(f"[Atout France] {n_invalid} coordonnées hors France supprimées")
        df = df[valid_geo | df["latitude"].isna()].copy()

    # ── Numérique ─────────────────────────────────────────────────────────────
    for col in ["nb_chambres", "nb_emplacements", "nb_locations"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # ── Texte ─────────────────────────────────────────────────────────────────
    for col in ["nom", "adresse", "ville", "departement", "region"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()
            df[col] = df[col].replace({"Nan": None, "None": None, "": None})

    # ── Dates ─────────────────────────────────────────────────────────────────
    if "date_classement" in df.columns:
        df["date_classement"] = pd.to_datetime(df["date_classement"], errors="coerce")

    # ── Doublons ─────────────────────────────────────────────────────────────
    before = len(df)
    df = df.drop_duplicates(subset=["nom", "code_postal"] if "code_postal" in df.columns else None)
    log.info(f"[Atout France] Doublons supprimés : {before - len(df)}")

    # ── Colonne source ─────────────────────────────────────────────────────────
    df["source"] = "atout_france"

    log.info(f"[Atout France] Nettoyé : {len(df)} lignes")
    _save(df, "hebergements_atout_france")
    return df


# ─── 2. DATAtourisme ──────────────────────────────────────────────────────────

def transform_datatourisme() -> pd.DataFrame:
    """
    Nettoie les données DATAtourisme :
      - Normalise les URI en ID court
      - Extrait le type simplifié depuis l'URI schema.org
      - Coordonnées GPS en float
      - Supprime les doublons sur l'identifiant
    """
    df = _load_raw("datatourisme")
    if df.empty:
        return df

    log.info(f"[DATAtourisme] Brut : {len(df)} lignes")

    # Nettoyage ID : garder la partie finale de l'URI
    if "id" in df.columns:
        df["id_court"] = df["id"].astype(str).str.split("/").str[-1]

    # Type simplifié depuis URI schema.org
    if "type" in df.columns:
        df["type_simplifie"] = (
            df["type"]
              .astype(str)
              .str.split("/").str[-1]   # ex: "Hotel", "Campground"
              .str.lower()
              .str.replace("campground", "camping")
              .str.replace("lodgingbusiness", "hebergement")
        )

    # Coordonnées
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Texte
    if "name" in df.columns:
        df["nom"] = df["name"].astype(str).str.strip().str.title()

    # Doublons
    before = len(df)
    id_col = "id_court" if "id_court" in df.columns else "id"
    if id_col in df.columns:
        df = df.drop_duplicates(subset=[id_col])
    log.info(f"[DATAtourisme] Doublons supprimés : {before - len(df)}")

    df["source"] = "datatourisme"
    log.info(f"[DATAtourisme] Nettoyé : {len(df)} lignes")
    _save(df, "hebergements_datatourisme")
    return df


# ─── 3. Locations de Vacances (Meublés de Tourisme) ─────────────────────────

def transform_locations() -> pd.DataFrame:
    """
    Nettoie les données scrapées des locations de vacances.
    """
    df = _load_raw("locations_vacances")
    if df.empty:
        return df

    log.info(f"[Locations .gouv] Brut : {len(df)} lignes")

    if "id" in df.columns:
        df["id_court"] = df["id"].astype(str).str.split("/").str[-1]

    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Filtre sur la France uniquement
    if "latitude" in df.columns and "longitude" in df.columns:
        valid_geo = (df["latitude"].between(-22, 52)) & (df["longitude"].between(-65, 56))
        df = df[valid_geo | df["latitude"].isna()].copy()
        
    df["source"] = "locations_scraping_gouv"
    if "type" not in df.columns:
        df["type"] = "location"

    _save(df, "hebergements_locations")
    return df


# ─── 4. INSEE — Fréquentation ─────────────────────────────────────────────────

def transform_insee() -> pd.DataFrame:
    """
    Nettoie les séries de fréquentation INSEE :
      - Parse les périodes (YYYY-MM ou YYYY)
      - Convertit les valeurs numériques
      - Pivot en tableau large
      - Calcule les variations annuelles
    """
    # Essayer d'abord le fichier ZIP extrait, puis les séries BDM
    df = _load_raw("insee_frequentation_hebergements")
    if df.empty:
        df = _load_raw("insee_series_nuitees")
    if df.empty:
        log.warning("[INSEE] Aucune donnée brute INSEE trouvée.")
        return pd.DataFrame()

    log.info(f"[INSEE] Brut : {len(df)} lignes, colonnes : {list(df.columns)}")

    # Si c'est le fichier ZIP (structure SDMX → colonnes variables)
    # On cherche les colonnes période et valeur
    period_col = next((c for c in df.columns if "periode" in c.lower() or "time" in c.lower() or "date" in c.lower()), None)
    value_col  = next((c for c in df.columns if "valeur" in c.lower() or "value" in c.lower() or "obs_value" in c.lower()), None)

    if period_col and value_col:
        df = df.rename(columns={period_col: "periode", value_col: "valeur"})

        # Parse période
        df["periode"] = df["periode"].astype(str).str.strip()
        df["annee"] = df["periode"].str[:4].astype("Int64", errors="ignore")

        # Détection mois (YYYY-MM)
        has_month = df["periode"].str.match(r"^\d{4}-\d{2}$")
        df.loc[has_month, "mois"] = df.loc[has_month, "periode"].str[5:7].astype("Int64", errors="ignore")

        # Valeur numérique
        df["valeur"] = pd.to_numeric(df["valeur"], errors="coerce")

        # Supprime les valeurs manquantes
        df = df.dropna(subset=["valeur"])

        log.info(f"[INSEE] Nettoyé : {len(df)} observations, "
                 f"années {df['annee'].min()} → {df['annee'].max()}")

    df["source"] = "insee"
    _save(df, "frequentation_insee")
    return df


# ─── 4. Référentiel géographique ──────────────────────────────────────────────

def transform_geo() -> pd.DataFrame:
    """
    Nettoie le référentiel des communes :
      - Aplatit les colonnes imbriquées (centre.coordinates, departement.code…)
      - Renomme pour cohérence
      - Extrait latitude/longitude depuis GeoJSON centre
    """
    df = _load_raw("geo_communes")
    if df.empty:
        return df

    log.info(f"[Géo] Communes brutes : {len(df)} lignes, colonnes : {list(df.columns)}")

    # Extraction des coordonnées depuis "centre.coordinates" si présent
    # La colonne peut être une liste [lon, lat] après json_normalize
    if "centre.coordinates" in df.columns:
        def extract_coords(val):
            try:
                if isinstance(val, str):
                    val = json.loads(val.replace("'", '"'))
                if isinstance(val, list) and len(val) == 2:
                    return pd.Series({"longitude": val[0], "latitude": val[1]})
            except Exception:
                pass
            return pd.Series({"longitude": None, "latitude": None})

        coords = df["centre.coordinates"].apply(extract_coords)
        df["latitude"]  = coords["latitude"]
        df["longitude"] = coords["longitude"]
        df = df.drop(columns=["centre.coordinates"], errors="ignore")

    # Renommage colonnes imbriquées issues de json_normalize
    rename_geo = {
        "departement.code": "dept_code",
        "departement.nom":  "dept_nom",
        "region.code":      "region_code",
        "region.nom":       "region_nom",
        "codesPostaux":     "codes_postaux",
    }
    df = df.rename(columns={k: v for k, v in rename_geo.items() if k in df.columns})

    # codes_postaux : liste → premier code postal
    if "codes_postaux" in df.columns:
        df["code_postal_principal"] = df["codes_postaux"].apply(
            lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x
        )

    log.info(f"[Géo] Communes nettoyées : {len(df)} lignes")
    _save(df, "geo_communes_clean")
    return df


def transform_geo_departements() -> pd.DataFrame:
    df = _load_raw("geo_departements")
    if df.empty:
        return df
    _save(df, "geo_departements_clean")
    return df


def transform_geo_regions() -> pd.DataFrame:
    df = _load_raw("geo_regions")
    if df.empty:
        return df
    _save(df, "geo_regions_clean")
    return df


# ─── 5. Fusion : hébergements enrichis avec géo ───────────────────────────────

def build_hebergements_unified() -> pd.DataFrame:
    """
    Fusionne Atout France + DATAtourisme en un seul DataFrame normalisé
    prêt pour la base de données.

    Colonnes résultantes communes :
      id_source, source, type, nom, adresse, code_postal, ville,
      dept_code, region, etoiles, latitude, longitude,
      nb_chambres, site_web, telephone, date_classement
    """
    COMMON_COLS = [
        "id_source", "source", "type", "nom", "adresse",
        "code_postal", "ville", "departement", "region",
        "etoiles", "latitude", "longitude", "nb_chambres",
        "site_web", "telephone",
    ]

    dfs = []

    # Atout France
    af_path = CLEAN_DIR / "hebergements_atout_france.csv"
    if af_path.exists():
        af = pd.read_csv(af_path, low_memory=False)
        # Créer id_source depuis nom + code postal
        af["id_source"] = "AF_" + af.get("nom", pd.Series(dtype=str)).fillna("").str[:30] + \
                          "_" + af.get("code_postal", pd.Series(dtype=str)).fillna("").astype(str)
        for col in COMMON_COLS:
            if col not in af.columns:
                af[col] = None
        dfs.append(af[COMMON_COLS])

    # DATAtourisme
    dt_path = CLEAN_DIR / "hebergements_datatourisme.csv"
    if dt_path.exists():
        dt = pd.read_csv(dt_path, low_memory=False)
        dt["id_source"] = "DT_" + dt.get("id_court", pd.Series(dtype=str)).fillna("").astype(str)
        dt["type"]      = dt.get("type_simplifie", dt.get("type", None))
        dt["adresse"]   = None
        dt["etoiles"]   = None
        for col in COMMON_COLS:
            if col not in dt.columns:
                dt[col] = None
        dfs.append(dt[COMMON_COLS])

    # Locations Scraping
    loc_path = CLEAN_DIR / "hebergements_locations.csv"
    if loc_path.exists():
        loc = pd.read_csv(loc_path, low_memory=False)
        loc["id_source"] = "LOC_" + loc.get("id_court", pd.Series(dtype=str)).fillna("").astype(str)
        loc["adresse"]   = None
        loc["etoiles"]   = None
        for col in COMMON_COLS:
            if col not in loc.columns:
                loc[col] = None
        dfs.append(loc[COMMON_COLS])

    if not dfs:
        log.warning("[Unified] Aucune source disponible.")
        return pd.DataFrame()

    unified = pd.concat(dfs, ignore_index=True)
    unified = unified.drop_duplicates(subset=["id_source"])

    # ── GÉOCODAGE APPROXIMATIF PAR VILLE ───────────────────────────────────────
    # Si la latitude/longitude est vide (souvent le cas pour Atout France),
    # on utilise le centre de la commune.
    geo_path = CLEAN_DIR / "geo_communes.csv"
    if geo_path.exists():
        geo = pd.read_csv(geo_path, low_memory=False)
        geo["nom_ville"] = geo["nom"].astype(str).str.lower()
        # dictionnaire de recherche rapide: ville -> {lat, lon}
        if "latitude" in geo.columns and "longitude" in geo.columns:
            geo_dict = geo.drop_duplicates("nom_ville").set_index("nom_ville")[["latitude", "longitude"]].to_dict("index")
            
            mask = unified["latitude"].isna()
            villes = unified.loc[mask, "ville"].astype(str).str.lower()
            unified.loc[mask, "latitude"]  = villes.map(lambda v: geo_dict.get(v, {}).get("latitude"))
            unified.loc[mask, "longitude"] = villes.map(lambda v: geo_dict.get(v, {}).get("longitude"))
            
            log.info(f"[Unified] Géocodage via communes : {mask.sum()} tentés, {(~unified['latitude'].isna() & mask).sum()} réussis.")

    # ── GESTION DES TYPES ──────────────────────────────────────────────────────
    unified["type"] = unified["type"].fillna("hebergement")


    log.info(f"[Unified] Dataset unifié : {len(unified)} hébergements")
    _save(unified, "hebergements_unifies")
    return unified


# ─── Orchestrateur ─────────────────────────────────────────────────────────────

def run_all_transforms() -> dict:
    """
    Lance toutes les transformations dans l'ordre correct.

    Returns:
        dict { nom_jeu_de_donnees: nb_lignes }
    """
    log.info("=" * 60)
    log.info("  PIPELINE DE TRANSFORMATION")
    log.info("=" * 60)

    results = {}

    # Sources individuelles
    df_af     = transform_atout_france();         results["atout_france"]  = len(df_af)
    df_dt     = transform_datatourisme();         results["datatourisme"]  = len(df_dt)
    df_loc    = transform_locations();            results["locations_vacances"] = len(df_loc)
    df_insee  = transform_insee();                results["insee"]         = len(df_insee)
    df_geo    = transform_geo();                  results["geo_communes"]  = len(df_geo)
    transform_geo_departements()
    transform_geo_regions()

    # Dataset unifié
    df_uni    = build_hebergements_unified();     results["hebergements_unifies"] = len(df_uni)

    log.info("\n" + "=" * 60)
    log.info("  RÉSUMÉ TRANSFORMATION")
    log.info("=" * 60)
    for name, count in results.items():
        status = "✅" if count > 0 else "⚠️ "
        log.info(f"  {status}  {name:<30} {count:>6} lignes")
    log.info(f"\n  Fichiers propres dans : {CLEAN_DIR}")

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_all_transforms()
