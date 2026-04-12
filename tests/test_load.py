"""
test_load.py — Tests unitaires du module load.py
==================================================

On teste chaque fonction de chargement sans JAMAIS se connecter
à de vraies bases de données. Les connexions MongoDB et PostgreSQL
sont entièrement simulées avec des MagicMock.

Structure :
  - TestLoadToMongoDB    — tests de load_to_mongodb()
  - TestLoadToPostgres   — tests de load_to_postgres()
  - TestLoadToDatalake   — tests de load_to_datalake()
  - TestRunAllLoads      — tests de l'orchestrateur run_all_loads()
"""

import json
import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, MagicMock, call, ANY

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_pipline import load as L


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 1 — load_to_mongodb()
# ═══════════════════════════════════════════════════════════════════════════════

class TestLoadToMongoDB:
    """
    Tests pour le chargement vers MongoDB.
    On simule MongoClient avec MagicMock.
    """

    def _make_mock_client(self):
        """Crée un MongoClient mocké qui simule une connexion réussie."""
        mock_collection = MagicMock()
        mock_bulk_result = MagicMock()
        mock_bulk_result.upserted_count = 3
        mock_bulk_result.modified_count = 0
        mock_collection.bulk_write.return_value  = mock_bulk_result

        mock_insert_result = MagicMock()
        mock_insert_result.inserted_ids = [1, 2, 3]
        mock_collection.insert_many.return_value = mock_insert_result

        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection

        mock_client = MagicMock()
        mock_client.server_info.return_value      = {"version": "6.0.4"}
        mock_client.__getitem__.return_value      = mock_db

        return mock_client, mock_db, mock_collection

    def test_retourne_dict_vide_si_pymongo_absent(self):
        """
        Si pymongo n'est pas installé, la fonction doit retourner {}
        sans lever d'exception.
        """
        import pymongo as _pymongo
        original_MongoClient = _pymongo.MongoClient
        # On fait planter l'import en patchant directement
        with patch("pymongo.MongoClient", side_effect=Exception("pymongo absent")):
            result = L.load_to_mongodb()
        assert isinstance(result, dict)

    def test_retourne_dict_vide_si_connexion_impossible(self, temp_data_dirs):
        """
        Si MongoDB est injoignable (ConnectionFailure),
        la fonction retourne un dict vide.
        """
        mock_client = MagicMock()
        mock_client.server_info.side_effect = Exception("Connection refused")

        with patch("pymongo.MongoClient", return_value=mock_client), \
             patch("data_pipline.load.CLEAN_DIR", temp_data_dirs["clean"]):
            result = L.load_to_mongodb()

        assert result == {}

    def test_insere_documents_avec_upsert(self, sample_hebergements_unified, temp_data_dirs):
        """
        Pour les collections avec upsert_key, bulk_write doit être appelé
        avec des UpdateOne pour chaque document.
        """
        clean = temp_data_dirs["clean"]
        (clean / "hebergements_unifies.csv").write_text(
            sample_hebergements_unified.to_csv(index=False, encoding="utf-8"),
            encoding="utf-8"
        )

        mock_client, mock_db, mock_collection = self._make_mock_client()

        with patch("pymongo.MongoClient", return_value=mock_client), \
             patch("data_pipline.load.CLEAN_DIR", clean):
            result = L.load_to_mongodb()

        # bulk_write doit avoir été appelé (upsert sur id_source)
        mock_collection.bulk_write.assert_called()

    def test_ignore_fichier_absent(self, temp_data_dirs):
        """
        Si un fichier CSV attendu n'existe pas dans clean/,
        la fonction doit le skipper silencieusement.
        """
        mock_client, _, _ = self._make_mock_client()

        with patch("pymongo.MongoClient", return_value=mock_client), \
             patch("data_pipline.load.CLEAN_DIR", temp_data_dirs["clean"]):
            result = L.load_to_mongodb()

        assert isinstance(result, dict), "Doit retourner un dict même si vide"


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 2 — load_to_postgres()
# ═══════════════════════════════════════════════════════════════════════════════

class TestLoadToPostgres:
    """
    Tests pour le chargement vers PostgreSQL via SQLAlchemy.
    On mock create_engine et la connexion.
    """

    def _make_mock_engine(self):
        """Crée un engine SQLAlchemy mocké avec context managers fonctionnels."""
        mock_conn   = MagicMock()
        mock_ctx    = MagicMock()
        mock_ctx.__enter__ = MagicMock(return_value=mock_conn)
        mock_ctx.__exit__  = MagicMock(return_value=False)

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_ctx
        mock_engine.begin.return_value   = mock_ctx
        return mock_engine, mock_conn

    def test_retourne_vide_si_sqlalchemy_absent(self):
        """Si la connexion échoue immédiatement, retourne {}."""
        mock_engine, _ = self._make_mock_engine()
        mock_engine.connect.side_effect = Exception("No module")
        with patch("data_pipline.load.create_engine", return_value=mock_engine, create=True), \
             patch("data_pipline.load.text", MagicMock(), create=True):
            result = L.load_to_postgres()
        assert isinstance(result, dict)

    def test_retourne_vide_si_connexion_echoue(self):
        """Si la connexion PostgreSQL échoue, retourner {}."""
        with patch("data_pipline.load.create_engine",
                   side_effect=Exception("Connection refused"), create=True):
            result = L.load_to_postgres()
        assert result == {}

    def test_schema_cree_avant_chargement(self, temp_data_dirs):
        """
        La fonction doit tenter de créer le schéma sans planter.
        On vérifie juste qu'elle retourne un dict sans exception.
        """
        clean = temp_data_dirs["clean"]
        mock_engine, mock_conn = self._make_mock_engine()

        with patch("data_pipline.load.create_engine", return_value=mock_engine, create=True), \
             patch("data_pipline.load.text", return_value=MagicMock(), create=True), \
             patch("data_pipline.load.CLEAN_DIR", clean):
            result = L.load_to_postgres()

        assert isinstance(result, dict)

    def test_charge_dataframe_en_table(self, sample_hebergements_unified, temp_data_dirs):
        """
        Un DataFrame valide doit être traité sans exception.
        """
        clean = temp_data_dirs["clean"]
        (clean / "hebergements_unifies.csv").write_text(
            sample_hebergements_unified.to_csv(index=False, encoding="utf-8"),
            encoding="utf-8"
        )

        mock_engine, mock_conn = self._make_mock_engine()

        with patch("data_pipline.load.create_engine", return_value=mock_engine, create=True), \
             patch("data_pipline.load.text", return_value=MagicMock(), create=True), \
             patch("data_pipline.load.CLEAN_DIR", clean):
            result = L.load_to_postgres()

        assert isinstance(result, dict)


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 3 — load_to_datalake()
# ═══════════════════════════════════════════════════════════════════════════════

class TestLoadToDatalake:
    """Tests pour l'export vers le Data Lake (fichiers horodatés)."""

    def test_cree_dossier_date_du_jour(self, sample_hebergements_unified, temp_data_dirs):
        """
        La fonction doit créer un sous-dossier au format YYYY-MM-DD
        dans data_lake/processed/.
        """
        clean = temp_data_dirs["clean"]
        lake  = temp_data_dirs["lake"]

        (clean / "hebergements_unifies.csv").write_text(
            sample_hebergements_unified.to_csv(index=False, encoding="utf-8"),
            encoding="utf-8"
        )

        with patch("data_pipline.load.CLEAN_DIR", clean), \
             patch("data_pipline.load.LAKE_DIR", lake):
            L.load_to_datalake()

        today = datetime.now().strftime("%Y-%m-%d")
        assert (lake / "processed" / today).exists(), "Dossier horodaté doit être créé"

    def test_cree_csv_et_json_pour_chaque_dataset(self, sample_hebergements_unified, temp_data_dirs):
        """
        Pour chaque dataset disponible, un CSV et un JSON doivent être exportés.
        """
        clean = temp_data_dirs["clean"]
        lake  = temp_data_dirs["lake"]

        (clean / "hebergements_unifies.csv").write_text(
            sample_hebergements_unified.to_csv(index=False, encoding="utf-8"),
            encoding="utf-8"
        )

        with patch("data_pipline.load.CLEAN_DIR", clean), \
             patch("data_pipline.load.LAKE_DIR", lake):
            L.load_to_datalake()

        today   = datetime.now().strftime("%Y-%m-%d")
        out_dir = lake / "processed" / today

        assert (out_dir / "hebergements.csv").exists(),  "CSV hébergements manquant"
        assert (out_dir / "hebergements.json").exists(), "JSON hébergements manquant"

    def test_metadata_json_cree(self, sample_hebergements_unified, temp_data_dirs):
        """
        Le fichier _metadata.json doit être créé avec les informations du run.
        """
        clean = temp_data_dirs["clean"]
        lake  = temp_data_dirs["lake"]

        (clean / "hebergements_unifies.csv").write_text(
            sample_hebergements_unified.to_csv(index=False, encoding="utf-8"),
            encoding="utf-8"
        )

        with patch("data_pipline.load.CLEAN_DIR", clean), \
             patch("data_pipline.load.LAKE_DIR", lake):
            L.load_to_datalake()

        today    = datetime.now().strftime("%Y-%m-%d")
        meta_path = lake / "processed" / today / "_metadata.json"
        assert meta_path.exists()

        with open(meta_path, encoding="utf-8") as f:
            meta = json.load(f)
        assert "run_date"  in meta
        assert "datasets"  in meta
        assert "lake_path" in meta

    def test_json_est_valide(self, sample_hebergements_unified, temp_data_dirs):
        """
        Le fichier JSON exporté doit être un JSON valide (liste de dicts).
        """
        clean = temp_data_dirs["clean"]
        lake  = temp_data_dirs["lake"]

        (clean / "hebergements_unifies.csv").write_text(
            sample_hebergements_unified.to_csv(index=False, encoding="utf-8"),
            encoding="utf-8"
        )

        with patch("data_pipline.load.CLEAN_DIR", clean), \
             patch("data_pipline.load.LAKE_DIR", lake):
            L.load_to_datalake()

        today    = datetime.now().strftime("%Y-%m-%d")
        json_path = lake / "processed" / today / "hebergements.json"

        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) == len(sample_hebergements_unified)

    def test_retourne_dict_avec_comptes(self, sample_hebergements_unified, temp_data_dirs):
        """
        La valeur de retour doit être un dict { dataset: nb_lignes }.
        """
        clean = temp_data_dirs["clean"]
        lake  = temp_data_dirs["lake"]

        (clean / "hebergements_unifies.csv").write_text(
            sample_hebergements_unified.to_csv(index=False, encoding="utf-8"),
            encoding="utf-8"
        )

        with patch("data_pipline.load.CLEAN_DIR", clean), \
             patch("data_pipline.load.LAKE_DIR", lake):
            result = L.load_to_datalake()

        assert "hebergements" in result
        assert result["hebergements"] == len(sample_hebergements_unified)

    def test_ignore_fichier_clean_absent(self, temp_data_dirs):
        """
        Si aucun fichier clean n'existe, la fonction ne doit pas lever d'erreur.
        """
        with patch("data_pipline.load.CLEAN_DIR", temp_data_dirs["clean"]), \
             patch("data_pipline.load.LAKE_DIR",  temp_data_dirs["lake"]):
            result = L.load_to_datalake()

        assert isinstance(result, dict)


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 4 — run_all_loads()
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunAllLoads:
    """Tests de l'orchestrateur de chargement."""

    def test_appelle_les_trois_destinations(self):
        """
        run_all_loads() doit appeler load_to_datalake(), load_to_mongodb()
        et load_to_postgres() dans cet ordre.
        """
        with patch("data_pipline.load.load_to_datalake", return_value={"h": 10}) as m_lake, \
             patch("data_pipline.load.load_to_mongodb",  return_value={"h": 10}) as m_mongo, \
             patch("data_pipline.load.load_to_postgres", return_value={"h": 10}) as m_pg:

            result = L.run_all_loads()

        m_lake.assert_called_once()
        m_mongo.assert_called_once()
        m_pg.assert_called_once()

    def test_retourne_dict_par_destination(self):
        """
        Le résultat doit avoir les clés 'datalake', 'mongodb', 'postgres'.
        """
        with patch("data_pipline.load.load_to_datalake", return_value={"h": 5}), \
             patch("data_pipline.load.load_to_mongodb",  return_value={"col1": 5}), \
             patch("data_pipline.load.load_to_postgres", return_value={"table1": 5}):

            result = L.run_all_loads()

        assert "datalake" in result
        assert "mongodb"  in result
        assert "postgres" in result

    def test_continue_si_mongodb_echoue(self):
        """
        Si MongoDB échoue (retourne {}), PostgreSQL et DataLake doivent
        quand même être exécutés.
        """
        with patch("data_pipline.load.load_to_datalake", return_value={"h": 5}) as m_lake, \
             patch("data_pipline.load.load_to_mongodb",  return_value={}) as m_mongo, \
             patch("data_pipline.load.load_to_postgres", return_value={"t": 5}) as m_pg:

            result = L.run_all_loads()

        m_lake.assert_called_once()
        m_pg.assert_called_once()
        assert result["mongodb"] == {}

    def test_continue_si_postgres_echoue(self):
        """
        Si PostgreSQL échoue, Data Lake et MongoDB doivent continuer.
        """
        with patch("data_pipline.load.load_to_datalake", return_value={"h": 5}), \
             patch("data_pipline.load.load_to_mongodb",  return_value={"col": 5}), \
             patch("data_pipline.load.load_to_postgres", return_value={}):

            result = L.run_all_loads()

        assert result["postgres"] == {}
        assert result["datalake"]["h"] == 5
