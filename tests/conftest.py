"""
conftest.py — Fixtures partagées entre tous les fichiers de tests
==================================================================

Ce fichier est chargé automatiquement par pytest avant chaque session.
Les fixtures définies ici sont disponibles dans TOUS les fichiers de tests
sans besoin de les importer.

Concepts clés :
  - fixture     : fonction qui prépare des données de test réutilisables
  - tmp_path    : fixture pytest intégrée — dossier temporaire nettoyé après chaque test
  - monkeypatch : fixture pytest — remplace temporairement des variables/fonctions
  - MagicMock   : objet qui simule un autre objet (HTTP response, DB connection…)
"""

import io
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock


# ─── Données de test communes ────────────────────────────────────────────────────

@pytest.fixture
def sample_atout_france_raw() -> pd.DataFrame:
    """
    DataFrame simulant un fichier CSV Atout France brut.
    Contient des cas variés : hotel avec coordonnées, camping sans email,
    résidence avec étoiles non numériques, doublon intentionnel.
    """
    return pd.DataFrame({
        "type_hebergement": [
            "HOTEL DE TOURISME",
            "CAMPING",
            "RÉSIDENCE DE TOURISME",
            "HOTEL DE TOURISME",   # doublon du 1er sur nom+code_postal
        ],
        "nom_hebergement":  ["Hotel Du Lac", "Camping Les Pins", "Résidence Plage", "Hotel Du Lac"],
        "adresse1":         ["12 Rue Riviera", "Route Forêt", "Bd Maritime", "12 Rue Riviera"],
        "code_postal":      ["06000", "33000", "13000", "06000"],
        "commune":          ["Nice", "Bordeaux", "Marseille", "Nice"],
        "departement":      ["Alpes-Maritimes", "Gironde", "Bouches-du-Rhône", "Alpes-Maritimes"],
        "region":           ["PACA", "Nouvelle-Aquitaine", "PACA", "PACA"],
        "classement":       ["4 étoiles", "3 étoiles", "2 étoiles", "4 étoiles"],
        "nb_chambres":      [120, None, 45, 120],
        "latitude":         [43.7102, 44.8378, 43.2965, 43.7102],
        "longitude":        [7.2620,  -0.5792, 5.3698,  7.2620],
        "telephone":        ["04 93 12 34 56", None, "04 91 00 11 22", "04 93 12 34 56"],
        "email":            ["contact@hotel.fr", None, "info@res.fr", "contact@hotel.fr"],
        "site_internet":    ["www.hotel.fr", None, "www.res.fr", "www.hotel.fr"],
        "date_classement":  ["2023-01-15", "2022-06-01", "2021-09-30", "2023-01-15"],
    })


@pytest.fixture
def sample_datatourisme_raw() -> pd.DataFrame:
    """
    DataFrame simulant les données brutes DATAtourisme après extraction API.
    """
    return pd.DataFrame({
        "id":        [
            "https://data.datatourisme.fr/abc123",
            "https://data.datatourisme.fr/def456",
            "https://data.datatourisme.fr/abc123",  # doublon intentionnel
        ],
        "type":      [
            "https://schema.org/Hotel",
            "https://schema.org/Campground",
            "https://schema.org/Hotel",
        ],
        "name":      ["Le Grand Hôtel", "Camping du Soleil", "Le Grand Hôtel"],
        "latitude":  [43.710, 45.188, 43.710],
        "longitude": [7.262,  5.724,  7.262],
        "postal_code": ["06000", "38000", "06000"],
        "city":      ["Nice", "Grenoble", "Nice"],
    })


@pytest.fixture
def sample_insee_raw() -> pd.DataFrame:
    """
    DataFrame simulant les données INSEE de fréquentation.
    Contient période, valeur, type de série.
    """
    return pd.DataFrame({
        "serie":    ["nuitees_hotels_france"] * 6,
        "idbank":   ["010777645"] * 6,
        "periode":  ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05", "2024-01"],
        "valeur":   [12_500_000, 11_000_000, 14_200_000, 13_100_000, 16_500_000, 13_000_000],
        "statut":   ["A", "A", "A", "A", "A", "P"],
    })


@pytest.fixture
def sample_geo_communes_raw() -> pd.DataFrame:
    """
    DataFrame simulant les communes après json_normalize depuis l'API Géo.
    Les coordonnées sont dans une colonne imbriquée 'centre.coordinates'.
    """
    return pd.DataFrame({
        "code":                  ["06088", "33063", "13055"],
        "nom":                   ["Nice", "Bordeaux", "Marseille"],
        "codesPostaux":          [["06000", "06100"], ["33000"], ["13001", "13002"]],
        "population":            [342522, 254436, 861635],
        "departement.code":      ["06", "33", "13"],
        "departement.nom":       ["Alpes-Maritimes", "Gironde", "Bouches-du-Rhône"],
        "region.code":           ["93", "75", "93"],
        "region.nom":            ["Provence-Alpes-Côte d'Azur", "Nouvelle-Aquitaine", "PACA"],
        "centre.coordinates":    [[7.262, 43.710], [-0.579, 44.837], [5.369, 43.296]],
    })


@pytest.fixture
def sample_hebergements_unified() -> pd.DataFrame:
    """
    DataFrame final unifié — tel que produit par build_hebergements_unified().
    Utilisé pour tester l'API et le chargement en base.
    """
    return pd.DataFrame({
        "id_source":   ["AF_Hotel_Du_Lac_06000", "AF_Camping_Les_Pins_33000", "DT_abc123"],
        "source":      ["atout_france",           "atout_france",              "datatourisme"],
        "type":        ["hotel",                  "camping",                   "hotel"],
        "nom":         ["Hotel Du Lac",           "Camping Les Pins",          "Le Grand Hôtel"],
        "adresse":     ["12 Rue Riviera",         "Route Forêt",               None],
        "code_postal": ["06000",                  "33000",                     "06000"],
        "ville":       ["Nice",                   "Bordeaux",                  "Nice"],
        "departement": ["Alpes-Maritimes",        "Gironde",                   None],
        "region":      ["PACA",                   "Nouvelle-Aquitaine",        None],
        "etoiles":     [4.0,                      3.0,                         float("nan")],  # float64 (compatible numpy.round)
        "latitude":    [43.7102,                  44.8378,                     43.710],
        "longitude":   [7.2620,                   -0.5792,                     7.262],
        "nb_chambres": [120.0,                    float("nan"),                float("nan")],  # float64
        "site_web":    ["www.hotel.fr",           None,                        None],
        "telephone":   ["04 93 12 34 56",         None,                        None],
    })



@pytest.fixture
def mock_http_response_csv(sample_atout_france_raw) -> MagicMock:
    """
    Simule une réponse HTTP contenant le CSV Atout France.
    Évite les vraies requêtes réseau dans les tests unitaires.
    """
    csv_content = sample_atout_france_raw.to_csv(sep=";", index=False).encode("utf-8")

    mock = MagicMock()
    mock.status_code = 200
    mock.content = csv_content
    mock.encoding = "utf-8"
    mock.raise_for_status = MagicMock(return_value=None)
    return mock


@pytest.fixture
def mock_http_response_json() -> MagicMock:
    """
    Simule une réponse HTTP JSON (pour l'API Géo ou DATAtourisme).
    """
    mock = MagicMock()
    mock.status_code = 200
    mock.raise_for_status = MagicMock(return_value=None)
    mock.json.return_value = [
        {"code": "75", "nom": "Paris", "codeRegion": "11"},
        {"code": "13", "nom": "Bouches-du-Rhône", "codeRegion": "93"},
    ]
    return mock


@pytest.fixture
def mock_http_error() -> MagicMock:
    """
    Simule une réponse HTTP d'erreur (404, timeout, etc.).
    Utilisé pour tester la robustesse des fonctions d'extraction.
    """
    import requests
    mock = MagicMock()
    mock.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
    return mock


@pytest.fixture
def temp_data_dirs(tmp_path) -> dict:
    """
    Crée la structure de dossiers data_lake/raw et data_lake/clean
    dans un répertoire temporaire pour ne pas polluer les données réelles.

    Returns:
        dict avec keys 'raw', 'clean', 'processed', 'lake'
    """
    raw_dir       = tmp_path / "data_lake" / "raw"
    clean_dir     = tmp_path / "data_lake" / "clean"
    processed_dir = tmp_path / "data_lake" / "processed"
    raw_dir.mkdir(parents=True)
    clean_dir.mkdir(parents=True)
    processed_dir.mkdir(parents=True)
    return {
        "raw":       raw_dir,
        "clean":     clean_dir,
        "processed": processed_dir,
        "lake":      tmp_path / "data_lake",
    }


@pytest.fixture
def csv_atout_france_in_raw(temp_data_dirs, sample_atout_france_raw) -> Path:
    """
    Écrit le CSV Atout France brut dans le dossier raw temporaire.
    Retourne le chemin du fichier créé.
    """
    path = temp_data_dirs["raw"] / "atout_france_hebergements.csv"
    sample_atout_france_raw.to_csv(path, index=False, encoding="utf-8")
    return path


@pytest.fixture
def csv_datatourisme_in_raw(temp_data_dirs, sample_datatourisme_raw) -> Path:
    """Écrit le CSV DATAtourisme brut dans le dossier raw temporaire."""
    path = temp_data_dirs["raw"] / "datatourisme.csv"
    sample_datatourisme_raw.to_csv(path, index=False, encoding="utf-8")
    return path


@pytest.fixture
def csv_insee_in_raw(temp_data_dirs, sample_insee_raw) -> Path:
    """Écrit le CSV INSEE brut dans le dossier raw temporaire."""
    path = temp_data_dirs["raw"] / "insee_frequentation_hebergements.csv"
    sample_insee_raw.to_csv(path, index=False, encoding="utf-8")
    return path


@pytest.fixture
def csv_geo_communes_in_raw(temp_data_dirs, sample_geo_communes_raw) -> Path:
    """Écrit le CSV géo communes brut dans le dossier raw temporaire."""
    path = temp_data_dirs["raw"] / "geo_communes.csv"
    sample_geo_communes_raw.to_csv(path, index=False, encoding="utf-8")
    return path


@pytest.fixture
def csv_unified_in_clean(temp_data_dirs, sample_hebergements_unified) -> Path:
    """Écrit le CSV unifié dans le dossier clean temporaire (pour l'API)."""
    path = temp_data_dirs["clean"] / "hebergements_unifies.csv"
    sample_hebergements_unified.to_csv(path, index=False, encoding="utf-8")
    return path
