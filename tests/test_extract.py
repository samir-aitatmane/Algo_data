"""
test_extract.py — Tests unitaires du module extract.py
=======================================================

Ce fichier teste CHAQUE FONCTION de data_pipline/extract.py de façon isolée.
On utilise des "mocks" pour simuler les appels réseau — les tests fonctionnent
même sans connexion internet, ce qui les rend rapides et reproductibles.

Structure :
  - TestExtractDatatourisme   — tests de extract_datatourisme()
  - TestExtractAtoutFrance    — tests de extract_atout_france()
  - TestExtractInsee          — tests de extract_insee_frequentation_file()
  - TestExtractGeo            — tests de extract_geo_communes/departements/regions()
  - TestRunAllExtractions     — tests de l'orchestrateur run_all_extractions()
"""

import io
import json
import zipfile
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock, call

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 1 — extract_datatourisme()
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtractDatatourisme:
    """
    Tests unitaires pour extract_datatourisme() et extract_datatourisme_all().

    Stratégie : on intercepte l'appel requests.post avec patch() pour éviter
    de vraies requêtes HTTP vers l'API DATAtourisme.
    """

    def test_retourne_dataframe_vide_si_pas_de_cle(self, monkeypatch):
        """
        Si DATATOURISME_API_KEY est absente ou vide,
        la fonction doit retourner un DataFrame vide sans lever d'exception.
        """
        # On supprime la variable d'environnement
        monkeypatch.setenv("DATATOURISME_API_KEY", "")

        # On réimporte après la modification d'env pour que le module recharge
        import importlib
        import data_pipline.extract as ext
        importlib.reload(ext)

        result = ext.extract_datatourisme()
        assert isinstance(result, pd.DataFrame), "Doit retourner un DataFrame"
        assert result.empty, "DataFrame doit être vide si clé absente"

    def test_parse_bindings_json_correctement(self, monkeypatch, tmp_path):
        """
        Vérifie que la fonction parse correctement la réponse JSON
        de l'API DATAtourisme (structure SPARQL bindings).
        """
        monkeypatch.setenv("DATATOURISME_API_KEY", "fake-api-key-test")

        fake_response = {
            "results": {
                "bindings": [
                    {
                        "id":        {"value": "https://data.datatourisme.fr/abc123"},
                        "type":      {"value": "https://schema.org/Hotel"},
                        "name":      {"value": "Hôtel Test"},
                        "latitude":  {"value": "43.710"},
                        "longitude": {"value": "7.262"},
                        "postalCode":{"value": "06000"},
                        "city":      {"value": "Nice"},
                    },
                    {
                        "id":        {"value": "https://data.datatourisme.fr/def456"},
                        "type":      {"value": "https://schema.org/Campground"},
                        "name":      {"value": "Camping Test"},
                        "latitude":  {"value": "45.188"},
                        "longitude": {"value": "5.724"},
                        # postalCode et city absents (champs optionnels)
                    },
                ]
            }
        }

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = fake_response

        with patch("data_pipline.extract.requests.post", return_value=mock_resp), \
             patch("data_pipline.extract.DATATOURISME_API_KEY", "fake-api-key-test"), \
             patch("data_pipline.extract.RAW_DIR", tmp_path):

            import data_pipline.extract as ext
            result = ext.extract_datatourisme(limit=10, offset=0)

        assert len(result) == 2, "Doit retourner 2 hébergements"
        assert "id"        in result.columns
        assert "name"      in result.columns
        assert "latitude"  in result.columns
        assert result.iloc[0]["name"] == "Hôtel Test"
        assert result.iloc[1]["city"] is None, "city absent → doit être None"

    def test_retourne_dataframe_vide_si_erreur_api(self, monkeypatch, tmp_path):
        """
        Si l'API retourne une erreur HTTP (ex: 401 Unauthorized),
        la fonction doit logger l'erreur et retourner un DataFrame vide.
        """
        import requests
        monkeypatch.setenv("DATATOURISME_API_KEY", "bad-key")

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("401 Unauthorized")

        with patch("data_pipline.extract.requests.post", return_value=mock_resp), \
             patch("data_pipline.extract.DATATOURISME_API_KEY", "bad-key"):

            import data_pipline.extract as ext
            result = ext.extract_datatourisme()

        assert result.empty, "Erreur API → DataFrame vide attendu"

    def test_pagination_s_arrete_si_derniere_page(self, monkeypatch, tmp_path):
        """
        extract_datatourisme_all() doit s'arrêter dès que la page
        retourne moins de résultats que le limit (indique fin des données).
        """
        monkeypatch.setenv("DATATOURISME_API_KEY", "fake-key")

        # Page 1 : 500 résultats (pleine)
        # Page 2 : 150 résultats (dernière)
        page1 = pd.DataFrame({"id": [f"h{i}" for i in range(500)], "name": ["x"] * 500})
        page2 = pd.DataFrame({"id": [f"h{i}" for i in range(150)], "name": ["y"] * 150})

        call_count = {"n": 0}
        def mock_extract(limit=500, offset=0):
            call_count["n"] += 1
            if offset == 0:
                return page1
            return page2

        with patch("data_pipline.extract.DATATOURISME_API_KEY", "fake-key"), \
             patch("data_pipline.extract.extract_datatourisme", side_effect=mock_extract), \
             patch("data_pipline.extract.RAW_DIR", tmp_path), \
             patch("data_pipline.extract.time.sleep"):  # pas de vraie pause

            import data_pipline.extract as ext
            result = ext.extract_datatourisme_all(max_pages=10)

        assert call_count["n"] == 2, "Doit interroger exactement 2 pages"
        assert len(result) == 650, "500 + 150 = 650 résultats attendus"


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 2 — extract_atout_france()
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtractAtoutFrance:
    """Tests pour le téléchargement du CSV Atout France."""

    def test_telechargement_csv_succes(self, sample_atout_france_raw, tmp_path):
        """
        Vérifie que la fonction télécharge, parse et sauvegarde le CSV
        correctement quand la requête HTTP réussit.
        """
        # Convertit notre DataFrame de test en bytes CSV (comme le serveur)
        csv_bytes = sample_atout_france_raw.to_csv(sep=";", index=False).encode("utf-8")

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.content  = csv_bytes
        mock_resp.encoding = "utf-8"

        with patch("data_pipline.extract.requests.get", return_value=mock_resp), \
             patch("data_pipline.extract.RAW_DIR", tmp_path):

            import data_pipline.extract as ext
            result = ext.extract_atout_france()

        assert not result.empty, "Doit retourner un DataFrame non vide"
        assert len(result) == len(sample_atout_france_raw)
        # Le fichier doit être sauvegardé
        assert (tmp_path / "atout_france_hebergements.csv").exists()

    def test_fallback_url_si_premiere_echoue(self, sample_atout_france_raw, tmp_path):
        """
        Si la première URL (Atout France) échoue, la fonction doit essayer
        l'URL de fallback data.gouv.fr automatiquement.
        """
        import requests as req_module

        csv_bytes = sample_atout_france_raw.to_csv(sep=";", index=False).encode("utf-8")

        mock_ok = MagicMock()
        mock_ok.raise_for_status = MagicMock()
        mock_ok.content  = csv_bytes
        mock_ok.encoding = "utf-8"

        mock_fail = MagicMock()
        mock_fail.raise_for_status.side_effect = req_module.HTTPError("503")

        # Premier appel échoue, deuxième réussit
        with patch("data_pipline.extract.requests.get", side_effect=[mock_fail, mock_ok]), \
             patch("data_pipline.extract.RAW_DIR", tmp_path):

            import data_pipline.extract as ext
            result = ext.extract_atout_france()

        assert not result.empty, "Doit réussir avec l'URL de fallback"

    def test_retourne_dataframe_vide_si_toutes_url_echouent(self, tmp_path):
        """
        Si toutes les URLs échouent, la fonction doit retourner un DataFrame vide.
        """
        import requests as req_module
        mock_fail = MagicMock()
        mock_fail.raise_for_status.side_effect = req_module.HTTPError("503")

        with patch("data_pipline.extract.requests.get", return_value=mock_fail), \
             patch("data_pipline.extract.RAW_DIR", tmp_path):

            import data_pipline.extract as ext
            result = ext.extract_atout_france()

        assert result.empty, "Toutes URLs échouent → DataFrame vide"

    def test_separateur_point_virgule(self, tmp_path):
        """
        Le CSV Atout France utilise ';' comme séparateur.
        Vérifie que le parsing utilise bien sep=';'.
        """
        # Fichier avec séparateur ; (standard Atout France)
        csv_content = "type_hebergement;nom_hebergement;code_postal\nHOTEL DE TOURISME;Hotel Test;06000\n"
        csv_bytes   = csv_content.encode("utf-8")

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.content  = csv_bytes
        mock_resp.encoding = "utf-8"

        with patch("data_pipline.extract.requests.get", return_value=mock_resp), \
             patch("data_pipline.extract.RAW_DIR", tmp_path):

            import data_pipline.extract as ext
            result = ext.extract_atout_france()

        assert len(result) == 1
        assert "type_hebergement" in result.columns
        assert result.iloc[0]["nom_hebergement"] == "Hotel Test"


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 3 — extract_insee_frequentation_file() et extract_insee_series_bdm()
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtractInsee:
    """Tests pour l'extraction des données INSEE (ZIP et séries BDM)."""

    def _make_zip_bytes(self, df: pd.DataFrame) -> bytes:
        """Helper : crée un ZIP en mémoire contenant un CSV."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            csv_bytes = df.to_csv(sep=";", index=False).encode("utf-8")
            z.writestr("frequentation.csv", csv_bytes)
        return buf.getvalue()

    def test_extraction_zip_reussie(self, sample_insee_raw, tmp_path):
        """
        Vérifie que la fonction extrait correctement le CSV depuis le ZIP INSEE.
        """
        zip_bytes = self._make_zip_bytes(sample_insee_raw)

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.content = zip_bytes

        with patch("data_pipline.extract.requests.get", return_value=mock_resp), \
             patch("data_pipline.extract.RAW_DIR", tmp_path):

            import data_pipline.extract as ext
            result = ext.extract_insee_frequentation_file()

        assert not result.empty, "Doit extraire le CSV du ZIP"
        assert len(result) == len(sample_insee_raw)
        assert (tmp_path / "insee_frequentation_hebergements.csv").exists()

    def test_fallback_bdm_si_zip_echoue(self, tmp_path):
        """
        Si le téléchargement ZIP échoue, la fonction doit appeler
        extract_insee_series_bdm() en fallback.
        """
        import requests as req_module
        mock_fail = MagicMock()
        mock_fail.raise_for_status.side_effect = req_module.HTTPError("404")

        mock_bdm_result = pd.DataFrame({
            "serie": ["nuitees_hotels_france"],
            "valeur": [12_500_000],
            "periode": ["2023-01"],
        })

        with patch("data_pipline.extract.requests.get", return_value=mock_fail), \
             patch("data_pipline.extract.extract_insee_series_bdm",
                   return_value=mock_bdm_result) as mock_bdm, \
             patch("data_pipline.extract.RAW_DIR", tmp_path):

            import data_pipline.extract as ext
            result = ext.extract_insee_frequentation_file()

        mock_bdm.assert_called_once(), "Fallback BDM doit être appelé"

    def test_series_bdm_parse_observations(self, tmp_path):
        """
        Vérifie que extract_insee_series_bdm() parse correctement
        la structure JSON SDMX de l'API BDM.
        """
        fake_sdmx_json = {
            "dataSets": [{
                "series": {
                    "0:0:0:0": {
                        "observations": {
                            "0": [12_500_000, 0],
                            "1": [11_000_000, 0],
                        }
                    }
                }
            }],
            "structure": {
                "dimensions": {
                    "observation": [{
                        "values": [
                            {"id": "2023-01"},
                            {"id": "2023-02"},
                        ]
                    }]
                }
            }
        }

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = fake_sdmx_json

        with patch("data_pipline.extract.requests.get", return_value=mock_resp), \
             patch("data_pipline.extract.RAW_DIR", tmp_path), \
             patch("data_pipline.extract.time.sleep"):

            import data_pipline.extract as ext
            result = ext.extract_insee_series_bdm()

        # 3 séries × 2 observations = 6 lignes
        assert len(result) == 6, "3 séries × 2 observations = 6 lignes"
        assert "periode" in result.columns
        assert "valeur"  in result.columns
        assert result.iloc[0]["periode"] == "2023-01"


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 4 — extract_geo_communes / departements / regions()
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtractGeo:
    """Tests pour les trois fonctions d'extraction géographique."""

    def test_communes_retourne_dataframe_normalise(self, tmp_path):
        """
        Vérifie que l'API Géo communes est bien interrogée
        et que json_normalize est appliqué sur la réponse.
        """
        fake_data = [
            {"code": "06088", "nom": "Nice",     "population": 342522,
             "centre": {"type": "Point", "coordinates": [7.262, 43.710]},
             "departement": {"code": "06", "nom": "Alpes-Maritimes"},
             "codesPostaux": ["06000"]},
            {"code": "75056", "nom": "Paris",    "population": 2161000,
             "centre": {"type": "Point", "coordinates": [2.347, 48.859]},
             "departement": {"code": "75", "nom": "Paris"},
             "codesPostaux": ["75001"]},
        ]
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = fake_data

        with patch("data_pipline.extract.requests.get", return_value=mock_resp), \
             patch("data_pipline.extract.RAW_DIR", tmp_path):

            import data_pipline.extract as ext
            result = ext.extract_geo_communes()

        assert len(result) == 2
        assert "code" in result.columns
        assert "nom"  in result.columns
        assert (tmp_path / "geo_communes.csv").exists()

    def test_departements_retourne_101_lignes_nominalement(self, tmp_path):
        """
        L'API Géo doit retourner ~101 départements.
        On vérifie que le DataFrame a les bonnes colonnes.
        """
        fake_depts = [
            {"code": "01", "nom": "Ain",   "codeRegion": "84"},
            {"code": "02", "nom": "Aisne", "codeRegion": "32"},
        ]
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = fake_depts

        with patch("data_pipline.extract.requests.get", return_value=mock_resp), \
             patch("data_pipline.extract.RAW_DIR", tmp_path):

            import data_pipline.extract as ext
            result = ext.extract_geo_departements()

        assert len(result) == 2
        assert "code"       in result.columns
        assert "nom"        in result.columns
        assert "codeRegion" in result.columns

    def test_retourne_vide_si_erreur_reseau(self, tmp_path):
        """
        Si l'API Géo est injoignable, la fonction doit retourner DataFrame vide.
        """
        import requests as req_module
        mock_fail = MagicMock()
        mock_fail.raise_for_status.side_effect = req_module.ConnectionError("Timeout")

        with patch("data_pipline.extract.requests.get", side_effect=req_module.ConnectionError), \
             patch("data_pipline.extract.RAW_DIR", tmp_path):

            import data_pipline.extract as ext
            result = ext.extract_geo_communes()

        assert result.empty


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 5 — Orchestrateur run_all_extractions()
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunAllExtractions:
    """
    Tests de l'orchestrateur global.

    On mock chaque fonction d'extraction individuellement
    pour tester que run_all_extractions() les appelle ALL et
    agrège correctement les résultats.
    """

    def test_appelle_toutes_les_sources(self, tmp_path):
        """
        Vérifie que run_all_extractions() appelle bien les 5 sous-fonctions.
        """
        empty_df  = pd.DataFrame()
        small_df  = pd.DataFrame({"a": [1, 2, 3]})

        with patch("data_pipline.extract.extract_datatourisme_all",   return_value=small_df) as m1, \
             patch("data_pipline.extract.extract_atout_france",        return_value=small_df) as m2, \
             patch("data_pipline.extract.extract_insee_frequentation_file", return_value=small_df) as m3, \
             patch("data_pipline.extract.extract_geo_communes",        return_value=small_df) as m4, \
             patch("data_pipline.extract.extract_geo_departements",    return_value=small_df) as m5, \
             patch("data_pipline.extract.extract_geo_regions",         return_value=small_df) as m6:

            import data_pipline.extract as ext
            results = ext.run_all_extractions()

        m1.assert_called_once()
        m2.assert_called_once()
        m3.assert_called_once()
        m4.assert_called_once()
        m5.assert_called_once()
        m6.assert_called_once()

    def test_resultats_contiennent_comptes(self, tmp_path):
        """
        Le dictionnaire retourné doit avoir une clé par source
        avec le nombre de lignes.
        """
        df_3 = pd.DataFrame({"x": [1, 2, 3]})
        df_5 = pd.DataFrame({"x": range(5)})

        with patch("data_pipline.extract.extract_datatourisme_all",        return_value=df_5), \
             patch("data_pipline.extract.extract_atout_france",             return_value=df_3), \
             patch("data_pipline.extract.extract_insee_frequentation_file", return_value=df_3), \
             patch("data_pipline.extract.extract_geo_communes",             return_value=df_5), \
             patch("data_pipline.extract.extract_geo_departements",         return_value=df_3), \
             patch("data_pipline.extract.extract_geo_regions",              return_value=df_3):

            import data_pipline.extract as ext
            results = ext.run_all_extractions()

        assert "datatourisme"       in results
        assert "atout_france"       in results
        assert "insee_frequentation" in results
        assert "geo_communes"       in results
        assert results["datatourisme"] == 5
        assert results["atout_france"] == 3

    def test_continue_si_une_source_echoue(self):
        """
        Si une source échoue (retourne DataFrame vide),
        l'orchestrateur doit continuer avec les autres sources.
        """
        df_ok = pd.DataFrame({"x": [1, 2]})

        with patch("data_pipline.extract.extract_datatourisme_all",        return_value=pd.DataFrame()), \
             patch("data_pipline.extract.extract_atout_france",             return_value=df_ok), \
             patch("data_pipline.extract.extract_insee_frequentation_file", return_value=df_ok), \
             patch("data_pipline.extract.extract_geo_communes",             return_value=df_ok), \
             patch("data_pipline.extract.extract_geo_departements",         return_value=df_ok), \
             patch("data_pipline.extract.extract_geo_regions",              return_value=df_ok):

            import data_pipline.extract as ext
            results = ext.run_all_extractions()

        assert results["datatourisme"] == 0, "Source échouée = 0 lignes"
        assert results["atout_france"] == 2, "Source OK = 2 lignes"
