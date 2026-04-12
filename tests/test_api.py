"""
test_api.py — Tests de l'API REST FastAPI (backend_api/app.py)
==============================================================

On utilise TestClient de FastAPI (basé sur httpx/requests) pour simuler
de vraies requêtes HTTP sans démarrer un vrai serveur.

Les données sont injectées directement via le mécanisme de cache
de l'application (_cache) pour éviter de lire de vrais fichiers.

Structure :
  - TestRootEndpoint          — GET /
  - TestListHebergements      — GET /hebergements (filtres, pagination, géo)
  - TestGetHebergement        — GET /hebergements/{id}
  - TestStats                 — GET /stats
  - TestFrequentation         — GET /frequentation
  - TestGeoEndpoints          — GET /geo/departements, /regions, /communes
  - TestAdminRefreshCache     — POST /admin/refresh-cache
  - TestEdgeCases             — cas limites et erreurs
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient
from backend_api.app import app, _cache, _clear_cache


# ─── Client de test ───────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def clear_app_cache():
    """
    Vide le cache de l'application avant et après chaque test.
    Garanti que les tests sont indépendants.
    autouse=True = appliqué automatiquement à TOUS les tests du fichier.
    """
    _clear_cache()
    yield
    _clear_cache()


@pytest.fixture
def client():
    """Retourne un TestClient FastAPI."""
    return TestClient(app)


@pytest.fixture
def inject_hebergements(sample_hebergements_unified):
    """
    Injecte des hébergements directement dans le cache de l'app.
    Évite de lire des fichiers disque réels.
    """
    _cache["hebergements_unifies"] = sample_hebergements_unified
    yield sample_hebergements_unified
    _clear_cache()


@pytest.fixture
def inject_frequentation(sample_insee_raw):
    """Injecte les données de fréquentation dans le cache."""
    _cache["frequentation_insee"] = sample_insee_raw
    yield sample_insee_raw
    _clear_cache()


@pytest.fixture
def inject_geo(sample_geo_communes_raw):
    """Injecte les données géo dans le cache."""
    depts = pd.DataFrame({
        "code": ["06", "33", "13"],
        "nom":  ["Alpes-Maritimes", "Gironde", "Bouches-du-Rhône"],
        "codeRegion": ["93", "75", "93"],
    })
    regions = pd.DataFrame({
        "code": ["93", "75"],
        "nom":  ["Provence-Alpes-Côte d'Azur", "Nouvelle-Aquitaine"],
    })
    _cache["geo_communes_clean"]     = sample_geo_communes_raw
    _cache["geo_departements_clean"] = depts
    _cache["geo_regions_clean"]      = regions
    yield


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 1 — GET /
# ═══════════════════════════════════════════════════════════════════════════════

class TestRootEndpoint:
    """Tests du point d'entrée de l'API."""

    def test_status_200(self, client):
        """GET / doit retourner HTTP 200."""
        response = client.get("/")
        assert response.status_code == 200

    def test_contient_info_api(self, client):
        """La réponse doit contenir le nom, la version et le statut."""
        data = client.get("/").json()
        assert data["api"]     == "TourisData"
        assert data["version"] == "1.0.0"
        assert data["status"]  == "ok"

    def test_contient_liste_endpoints(self, client):
        """La réponse doit lister les endpoints disponibles."""
        data = client.get("/").json()
        assert "endpoints" in data
        assert "/hebergements" in data["endpoints"]
        assert "/stats"        in data["endpoints"]


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 2 — GET /hebergements
# ═══════════════════════════════════════════════════════════════════════════════

class TestListHebergements:
    """Tests de la liste paginée des hébergements."""

    def test_503_si_pas_de_donnees(self, client):
        """Sans données dans le cache, doit retourner 503."""
        response = client.get("/hebergements")
        assert response.status_code == 503

    def test_200_avec_donnees(self, client, inject_hebergements):
        """Avec données injectées, doit retourner 200."""
        response = client.get("/hebergements")
        assert response.status_code == 200

    def test_structure_reponse(self, client, inject_hebergements):
        """La réponse doit avoir : total, page, per_page, pages, data."""
        data = client.get("/hebergements").json()
        assert "total"    in data
        assert "page"     in data
        assert "per_page" in data
        assert "pages"    in data
        assert "data"     in data
        assert isinstance(data["data"], list)

    def test_total_correct(self, client, inject_hebergements, sample_hebergements_unified):
        """total doit correspondre au nombre de lignes injectées."""
        data = client.get("/hebergements").json()
        assert data["total"] == len(sample_hebergements_unified)

    def test_filtre_par_type(self, client, inject_hebergements):
        """Filtre type=hotel ne doit retourner que les hôtels."""
        data = client.get("/hebergements?type=hotel").json()
        for item in data["data"]:
            assert item["type"] == "hotel", f"Attendu 'hotel', obtenu '{item['type']}'"

    def test_filtre_par_etoiles(self, client, inject_hebergements):
        """Filtre etoiles=4 ne doit retourner que les 4 étoiles."""
        data = client.get("/hebergements?etoiles=4").json()
        for item in data["data"]:
            assert item["etoiles"] == 4

    def test_filtre_par_ville(self, client, inject_hebergements):
        """Filtre ville=Nice doit ne retourner que les hébergements à Nice."""
        data = client.get("/hebergements?ville=Nice").json()
        assert data["total"] >= 1
        for item in data["data"]:
            assert "nice" in item["ville"].lower()

    def test_filtre_par_source(self, client, inject_hebergements):
        """Filtre source=atout_france ne doit retourner que cette source."""
        data = client.get("/hebergements?source=atout_france").json()
        for item in data["data"]:
            assert item["source"] == "atout_france"

    def test_pagination_page_1(self, client, inject_hebergements):
        """page=1 doit retourner les premiers résultats."""
        data = client.get("/hebergements?page=1&per_page=2").json()
        assert data["page"]     == 1
        assert data["per_page"] == 2
        assert len(data["data"]) <= 2

    def test_pagination_page_vide(self, client, inject_hebergements, sample_hebergements_unified):
        """Une page au-delà des résultats doit retourner data vide."""
        total = len(sample_hebergements_unified)
        data  = client.get(f"/hebergements?page=999&per_page=100").json()
        assert data["data"] == []

    def test_recherche_geo_rayon(self, client, inject_hebergements):
        """
        Avec lat/lon/radius_km, seuls les hébergements dans le rayon
        doivent être retournés.
        """
        # Centre = Nice (43.71, 7.26), rayon 10 km
        # Hotel Du Lac est à Nice → dans le rayon
        # Camping Les Pins est à Bordeaux → hors rayon
        data = client.get(
            "/hebergements?lat=43.71&lon=7.26&radius_km=10"
        ).json()
        villes = [item["ville"] for item in data["data"] if item.get("ville")]
        # Il doit y avoir au moins Nice, pas Bordeaux
        if villes:
            assert any("nice" in v.lower() for v in villes)

    def test_per_page_max_100(self, client, inject_hebergements):
        """per_page > 100 doit retourner une erreur de validation."""
        response = client.get("/hebergements?per_page=200")
        assert response.status_code == 422  # Unprocessable Entity

    def test_etoiles_min_1_max_5(self, client, inject_hebergements):
        """etoiles < 1 ou > 5 → erreur de validation."""
        assert client.get("/hebergements?etoiles=0").status_code == 422
        assert client.get("/hebergements?etoiles=6").status_code == 422


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 3 — GET /hebergements/{id}
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetHebergement:
    """Tests du détail d'un hébergement par son ID."""

    def test_200_si_id_existe(self, client, inject_hebergements):
        """Un ID valide doit retourner 200 avec les données de l'hébergement."""
        response = client.get("/hebergements/AF_Hotel_Du_Lac_06000")
        assert response.status_code == 200
        data = response.json()
        assert data["id_source"] == "AF_Hotel_Du_Lac_06000"
        assert data["nom"] == "Hotel Du Lac"

    def test_404_si_id_inexistant(self, client, inject_hebergements):
        """Un ID qui n'existe pas → 404 Not Found."""
        response = client.get("/hebergements/ID_QUI_NEXISTE_PAS_999")
        assert response.status_code == 404

    def test_503_si_pas_de_donnees(self, client):
        """Sans données dans le cache → 503."""
        response = client.get("/hebergements/AF_Hotel_Du_Lac_06000")
        assert response.status_code == 503

    def test_retourne_tous_les_champs(self, client, inject_hebergements):
        """La réponse doit contenir les champs principaux."""
        data = client.get("/hebergements/AF_Hotel_Du_Lac_06000").json()
        for field in ["id_source", "type", "nom", "source"]:
            assert field in data


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 4 — GET /stats
# ═══════════════════════════════════════════════════════════════════════════════

class TestStats:
    """Tests de l'endpoint de statistiques agrégées."""

    def test_503_si_pas_de_donnees(self, client):
        response = client.get("/stats")
        assert response.status_code == 503

    def test_200_groupby_type(self, client, inject_hebergements):
        response = client.get("/stats?groupby=type")
        assert response.status_code == 200

    def test_structure_reponse(self, client, inject_hebergements):
        """La réponse doit avoir dimension, total, repartition."""
        data = client.get("/stats?groupby=type").json()
        assert "dimension"          in data
        assert "total_hebergements" in data
        assert "repartition"        in data
        assert isinstance(data["repartition"], list)

    def test_total_coherent(self, client, inject_hebergements, sample_hebergements_unified):
        data = client.get("/stats?groupby=type").json()
        assert data["total_hebergements"] == len(sample_hebergements_unified)

    def test_repartition_contient_nb_hebergements(self, client, inject_hebergements):
        data = client.get("/stats?groupby=type").json()
        for item in data["repartition"]:
            assert "nb_hebergements" in item
            assert item["nb_hebergements"] >= 0

    def test_groupby_region(self, client, inject_hebergements):
        response = client.get("/stats?groupby=region")
        assert response.status_code == 200

    def test_groupby_etoiles(self, client, inject_hebergements):
        response = client.get("/stats?groupby=etoiles")
        assert response.status_code == 200

    def test_groupby_invalide_retourne_400(self, client, inject_hebergements):
        response = client.get("/stats?groupby=champ_inexistant")
        assert response.status_code == 400


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 5 — GET /frequentation
# ═══════════════════════════════════════════════════════════════════════════════

class TestFrequentation:
    """Tests de l'endpoint des données de fréquentation INSEE."""

    def test_503_si_pas_de_donnees(self, client):
        response = client.get("/frequentation")
        assert response.status_code == 503

    def test_200_avec_donnees(self, client, inject_frequentation):
        response = client.get("/frequentation")
        assert response.status_code == 200

    def test_structure_reponse(self, client, inject_frequentation):
        data = client.get("/frequentation").json()
        assert "total"  in data
        assert "series" in data
        assert "data"   in data
        assert isinstance(data["data"], list)

    def test_filtre_par_serie(self, client, inject_frequentation):
        data = client.get("/frequentation?serie=hotels").json()
        # Toutes les lignes retournées doivent contenir 'hotels'
        for item in data["data"]:
            if "serie" in item:
                assert "hotels" in item["serie"].lower()

    def test_filtre_par_annee(self, client, inject_frequentation):
        data = client.get("/frequentation?annee=2023").json()
        for item in data["data"]:
            if "periode" in item:
                assert item["periode"].startswith("2023")

    def test_limit_respecte(self, client, inject_frequentation):
        data = client.get("/frequentation?limit=2").json()
        assert len(data["data"]) <= 2


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 6 — GET /geo/departements, /regions, /communes
# ═══════════════════════════════════════════════════════════════════════════════

class TestGeoEndpoints:
    """Tests des endpoints géographiques."""

    def test_503_departements_sans_donnees(self, client):
        assert client.get("/geo/departements").status_code == 503

    def test_200_departements_avec_donnees(self, client, inject_geo):
        assert client.get("/geo/departements").status_code == 200

    def test_departements_retourne_liste(self, client, inject_geo):
        data = client.get("/geo/departements").json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_filtre_departements_par_region(self, client, inject_geo):
        """Filtre par code région doit retourner uniquement les depts de cette région."""
        data = client.get("/geo/departements?region_code=93").json()
        for dept in data:
            assert dept["codeRegion"] == "93"

    def test_200_regions_avec_donnees(self, client, inject_geo):
        data = client.get("/geo/regions").json()
        assert isinstance(data, list)
        assert len(data) == 2  # On a injecté 2 régions

    def test_503_regions_sans_donnees(self, client):
        assert client.get("/geo/regions").status_code == 503

    def test_communes_avec_filtre_dept(self, client, inject_geo):
        """Filtre dept_code=06 doit retourner que les communes du 06."""
        data = client.get("/geo/communes?dept_code=06").json()
        assert isinstance(data, dict)
        assert "total" in data
        assert "data"  in data

    def test_communes_recherche_nom(self, client, inject_geo):
        """Recherche par nom partiel."""
        data = client.get("/geo/communes?nom=Nice").json()
        for commune in data["data"]:
            assert "nice" in commune["nom"].lower()

    def test_communes_limit_respecte(self, client, inject_geo):
        data = client.get("/geo/communes?limit=1").json()
        assert len(data["data"]) <= 1


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 7 — POST /admin/refresh-cache
# ═══════════════════════════════════════════════════════════════════════════════

class TestAdminRefreshCache:
    """Tests de la route d'administration."""

    def test_200_et_message(self, client):
        response = client.post("/admin/refresh-cache")
        assert response.status_code == 200
        assert "message" in response.json()

    def test_vide_le_cache(self, client, inject_hebergements):
        """Après refresh, le cache doit être vide."""
        # Avant : cache rempli
        from backend_api.app import _cache
        assert "hebergements_unifies" in _cache

        client.post("/admin/refresh-cache")

        # Après : cache vide
        assert "hebergements_unifies" not in _cache


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 8 — Cas limites et edge cases
# ═══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    """Tests de robustesse : cas aux limites, valeurs manquantes, etc."""

    def test_filtre_avec_donnees_null(self, client):
        """
        Si les données contiennent des valeurs None dans les colonnes filtrées,
        la fonction doit les ignorer sans lever d'exception.
        """
        df_avec_nulls = pd.DataFrame({
            "id_source": ["A1", "A2", "A3"],
            "source":    ["atout_france", "atout_france", "atout_france"],
            "type":      ["hotel", None, "camping"],
            "nom":       ["Hotel A", "Mystere", "Camping B"],
            "ville":     [None, "Paris", "Lyon"],
            "departement": [None, "75", "69"],
            "region":    [None, "IDF", "ARA"],
            "etoiles":   [4, None, 3],
            "latitude":  [43.71, None, 45.75],
            "longitude": [7.26,  None,  4.83],
            "adresse":   [None, None, None],
            "code_postal": ["06000", None, "69000"],
            "nb_chambres": [None, None, None],
            "site_web":  [None, None, None],
            "telephone": [None, None, None],
        })
        _cache["hebergements_unifies"] = df_avec_nulls

        client_instance = TestClient(app)
        resp = client_instance.get("/hebergements?type=hotel")
        # Ne doit pas lever d'exception
        assert resp.status_code in [200, 503]

    def test_hebergements_sans_coordonnees_geo_ignorees(self, client):
        """
        La recherche géographique doit ignorer les hébergements sans lat/lon.
        """
        df = pd.DataFrame({
            "id_source": ["A1", "A2"],
            "source":    ["atout_france"] * 2,
            "type":      ["hotel"] * 2,
            "nom":       ["Hotel Geo", "Hotel Sans Coords"],
            "latitude":  [43.71, None],
            "longitude": [7.26, None],
            "etoiles":   [4, 4],
            "ville":     ["Nice", "Paris"],
            "departement": ["AM", "75"],
            "region":    ["PACA", "IDF"],
            "adresse":   [None, None],
            "code_postal": ["06000", "75001"],
            "nb_chambres": [None, None],
            "site_web":  [None, None],
            "telephone": [None, None],
        })
        _cache["hebergements_unifies"] = df

        resp = TestClient(app).get("/hebergements?lat=43.71&lon=7.26&radius_km=10")
        assert resp.status_code == 200
        data = resp.json()
        # Seul l'hébergement avec coordonnées doit apparaître
        noms = [item["nom"] for item in data["data"]]
        assert "Hotel Geo" in noms
        assert "Hotel Sans Coords" not in noms

    def test_pagination_coherente(self, client, inject_hebergements,
                                   sample_hebergements_unified):
        """
        Avec 3 hébergements et per_page=2 :
        - page 1 → 2 résultats
        - page 2 → 1 résultat
        - pages total = 2
        """
        # 3 hébergements injectés
        data1 = client.get("/hebergements?page=1&per_page=2").json()
        data2 = client.get("/hebergements?page=2&per_page=2").json()

        assert len(data1["data"]) == 2
        assert len(data2["data"]) == 1
        assert data1["pages"] == 2

    def test_stats_avec_tous_types_manquants(self, client):
        """
        Si la colonne 'type' ne contient que des None/NaN,
        get_stats() ne doit pas planter.
        """
        df = pd.DataFrame({
            "id_source":   ["A1", "A2"],
            "source":      ["test"] * 2,
            "type":        [None, None],
            "etoiles":     [None, None],   # None → NaN → float64 (evite Int64 nullable)
            "latitude":    [None, None],
            "region":      [None, None],
            "departement": [None, None],
        })
        # Forcer les types numériques en float64 (compatible JSON)
        df["etoiles"]  = df["etoiles"].astype("float64")
        df["latitude"] = df["latitude"].astype("float64")
        _cache["hebergements_unifies"] = df

        resp = TestClient(app).get("/stats?groupby=type")
        # Peut retourner 200 (avec repartition vide) ou 400 si colonne absente
        assert resp.status_code in [200, 400]

