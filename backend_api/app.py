"""
app.py — API REST TourisData (FastAPI)
=======================================
Expose les données touristiques via des endpoints REST.

Lancement :
    uvicorn backend_api.app:app --reload --port 8000

Documentation auto : http://localhost:8000/docs
"""

import os
import json
import logging
from pathlib import Path
from typing import Optional, List
from datetime import datetime

import pandas as pd
from fastapi import FastAPI, Query, HTTPException, Path as FPath
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="TourisData API",
    description=(
        "API REST donnant accès aux données touristiques françaises issues de data.gouv.fr. "
        "Sources : Atout France, DATAtourisme, INSEE, API Géo."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ─── Data Source ──────────────────────────────────────────────────────────────
LAKE_DIR  = Path(__file__).parent.parent / "data_lake"
CLEAN_DIR = LAKE_DIR / "clean"

_cache: dict = {}


def _load_df(name: str) -> pd.DataFrame:
    """Charge un CSV depuis data_lake/clean/ avec mise en cache mémoire."""
    if name in _cache:
        return _cache[name]
    path = CLEAN_DIR / f"{name}.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    df = df.where(pd.notnull(df), None)
    _cache[name] = df
    return df


def _clear_cache():
    _cache.clear()


# ─── Modèles Pydantic ─────────────────────────────────────────────────────────
class Hebergement(BaseModel):
    id_source:   Optional[str]
    source:      Optional[str]
    type:        Optional[str]
    nom:         Optional[str]
    adresse:     Optional[str]
    code_postal: Optional[str]
    ville:       Optional[str]
    departement: Optional[str]
    region:      Optional[str]
    etoiles:     Optional[int]
    latitude:    Optional[float]
    longitude:   Optional[float]
    nb_chambres: Optional[int]
    site_web:    Optional[str]
    telephone:   Optional[str]


class PagedResponse(BaseModel):
    total:    int
    page:     int
    per_page: int
    pages:    int
    data:     List[dict]


# ─── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", tags=["Info"])
def root():
    """Point d'entrée — statut de l'API."""
    return {
        "api":     "TourisData",
        "version": "1.0.0",
        "status":  "ok",
        "docs":    "/docs",
        "endpoints": [
            "/hebergements", "/hebergements/{id}",
            "/stats", "/frequentation",
            "/geo/departements", "/geo/regions", "/geo/communes",
        ]
    }


@app.get("/hebergements", response_model=PagedResponse, tags=["Hébergements"])
def list_hebergements(
    page:        int           = Query(1,    ge=1,   description="Numéro de page"),
    per_page:    int           = Query(20,   ge=1, le=100, description="Résultats par page"),
    type:        Optional[str] = Query(None, description="Type : hotel, camping, residence…"),
    etoiles:     Optional[int] = Query(None, ge=1, le=5, description="Nombre d'étoiles"),
    departement: Optional[str] = Query(None, description="Code ou nom de département"),
    region:      Optional[str] = Query(None, description="Nom de région"),
    ville:       Optional[str] = Query(None, description="Nom de ville (recherche partielle)"),
    source:      Optional[str] = Query(None, description="Source : atout_france / datatourisme"),
    lat:         Optional[float] = Query(None, description="Latitude centre (pour géo-recherche)"),
    lon:         Optional[float] = Query(None, description="Longitude centre"),
    radius_km:   Optional[float] = Query(None, ge=0.1, le=500, description="Rayon en km"),
):
    """
    Liste paginée des hébergements touristiques avec filtres multiples.
    Supporte la recherche géographique par rayon si lat/lon/radius_km fournis.
    """
    df = _load_df("hebergements_unifies")
    if df.empty:
        raise HTTPException(503, "Données non disponibles — lancez le pipeline ETL d'abord")

    # Filtres
    if type:
        df = df[df["type"].str.lower().str.contains(type.lower(), na=False)]
    if etoiles is not None:
        df = df[df["etoiles"] == etoiles]
    if departement:
        mask = df["departement"].str.lower().str.contains(departement.lower(), na=False)
        df = df[mask]
    if region:
        df = df[df["region"].str.lower().str.contains(region.lower(), na=False)]
    if ville:
        df = df[df["ville"].str.lower().str.contains(ville.lower(), na=False)]
    if source:
        df = df[df["source"].str.lower() == source.lower()]

    # Filtrage géographique par rayon (formule Haversine simplifiée)
    if lat is not None and lon is not None and radius_km is not None:
        import math
        def haversine(row):
            lat2 = row.get("latitude")
            lon2 = row.get("longitude")
            if lat2 is None or lon2 is None:
                return float("inf")
            R = 6371
            dlat = math.radians(lat2 - lat)
            dlon = math.radians(lon2 - lon)
            a = math.sin(dlat/2)**2 + math.cos(math.radians(lat)) * \
                math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
            return R * 2 * math.asin(math.sqrt(a))
        df["_dist"] = df.apply(haversine, axis=1)
        df = df[df["_dist"] <= radius_km].sort_values("_dist")
        df = df.drop(columns=["_dist"])

    total = len(df)
    pages = max(1, (total + per_page - 1) // per_page)
    start = (page - 1) * per_page
    end   = start + per_page
    data  = df.iloc[start:end].to_dict(orient="records")

    return {"total": total, "page": page, "per_page": per_page, "pages": pages, "data": data}


@app.get("/hebergements/{id_source}", tags=["Hébergements"])
def get_hebergement(id_source: str = FPath(..., description="Identifiant source de l'hébergement")):
    """Détail complet d'un hébergement par son identifiant."""
    df = _load_df("hebergements_unifies")
    if df.empty:
        raise HTTPException(503, "Données non disponibles")
    row = df[df["id_source"] == id_source]
    if row.empty:
        raise HTTPException(404, f"Hébergement '{id_source}' introuvable")
    return row.iloc[0].to_dict()


@app.get("/stats", tags=["Statistiques"])
def get_stats(
    groupby: str = Query("type", description="Dimension : type | region | departement | etoiles")
):
    """
    Statistiques agrégées : nombre d'hébergements par dimension.
    Retourne aussi le total global et le nombre d'étoiles moyen.
    """
    df = _load_df("hebergements_unifies")
    if df.empty:
        raise HTTPException(503, "Données non disponibles")

    valid_dims = ["type", "region", "departement", "etoiles", "source"]
    if groupby not in valid_dims:
        raise HTTPException(400, f"groupby doit être l'un de : {valid_dims}")

    if groupby not in df.columns:
        raise HTTPException(400, f"Colonne '{groupby}' absente des données")

    grouped = (
        df.groupby(groupby, dropna=False)
          .agg(
              nb_hebergements=("id_source", "count"),
              etoiles_moy=("etoiles", "mean"),
              nb_avec_coords=("latitude", lambda x: x.notna().sum()),
          )
          .reset_index()
          .sort_values("nb_hebergements", ascending=False)
    )
    grouped["etoiles_moy"] = pd.to_numeric(grouped["etoiles_moy"], errors="coerce").round(1)

    import math
    def _clean(v):
        if isinstance(v, float) and math.isnan(v): return None
        return v

    repartition = [{k: _clean(v) for k, v in row.items()} for row in grouped.to_dict(orient="records")]

    return {
        "dimension": groupby,
        "total_hebergements": len(df),
        "repartition": repartition,
        "types_disponibles": df["type"].dropna().unique().tolist(),
    }


@app.get("/frequentation", tags=["Fréquentation INSEE"])
def get_frequentation(
    serie:   Optional[str] = Query(None, description="Nom de la série (nuitees_hotels_france…)"),
    annee:   Optional[int] = Query(None, ge=2000, le=2030, description="Année"),
    limit:   int           = Query(100, ge=1, le=1000),
):
    """
    Données de fréquentation des hébergements touristiques (INSEE).
    Nuitées, arrivées, durées de séjour par type et période.
    """
    df = _load_df("frequentation_insee")
    if df.empty:
        raise HTTPException(503, "Données INSEE non disponibles — vérifiez l'extraction")

    if serie and "serie" in df.columns:
        df = df[df["serie"].str.lower().str.contains(serie.lower(), na=False)]
    if annee and "annee" in df.columns:
        df = df[df["annee"] == annee]

    return {
        "total":  len(df),
        "series": df["serie"].unique().tolist() if "serie" in df.columns else [],
        "data":   df.head(limit).to_dict(orient="records"),
    }


@app.get("/geo/departements", tags=["Géographie"])
def get_departements(region_code: Optional[str] = Query(None, description="Filtrer par code région")):
    """Liste des 101 départements français (source : API Géo gouv.fr)."""
    df = _load_df("geo_departements_clean")
    if df.empty:
        raise HTTPException(503, "Données géo non disponibles")
    if region_code and "codeRegion" in df.columns:
        df = df[df["codeRegion"] == region_code]
    return df.to_dict(orient="records")


@app.get("/geo/regions", tags=["Géographie"])
def get_regions():
    """Liste des 18 régions françaises."""
    df = _load_df("geo_regions_clean")
    if df.empty:
        raise HTTPException(503, "Données géo non disponibles")
    return df.to_dict(orient="records")


@app.get("/geo/communes", tags=["Géographie"])
def get_communes(
    dept_code: Optional[str] = Query(None, description="Code département (ex: 75, 13)"),
    nom:       Optional[str] = Query(None, description="Recherche par nom partiel"),
    limit:     int           = Query(50, ge=1, le=500),
):
    """Communes françaises avec coordonnées GPS (source : API Géo gouv.fr)."""
    df = _load_df("geo_communes_clean")
    if df.empty:
        raise HTTPException(503, "Données géo non disponibles")
    if dept_code and "dept_code" in df.columns:
        df = df[df["dept_code"] == dept_code]
    if nom and "nom" in df.columns:
        df = df[df["nom"].str.lower().str.contains(nom.lower(), na=False)]
    return {"total": len(df), "data": df.head(limit).to_dict(orient="records")}


@app.post("/admin/refresh-cache", tags=["Admin"])
def refresh_cache():
    """Vide le cache mémoire pour recharger les données depuis le disque."""
    _clear_cache()
    return {"message": "Cache vidé — les prochaines requêtes rechargeront les données"}

