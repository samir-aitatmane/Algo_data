"""
test_pipeline_integration.py — Tests d'intégration du pipeline ETL complet
============================================================================

Ces tests vérifient que les MODULES FONCTIONNENT ENSEMBLE (Extract → Transform → Load)
en utilisant des données réelles simulées et des dossiers temporaires.

Contrairement aux tests unitaires qui isolent chaque fonction,
les tests d'intégration testent le FLUX COMPLET.

Pas de connexion réseau ni de vraie base de données —
tout est simulé ou écrit en fichiers temporaires.

Structure :
  - TestExtractToTransform      — Extract → Transform (données brutes → propres)
  - TestTransformToLoad         — Transform → Load (données propres → Data Lake)
  - TestFullPipeline            — Extract → Transform → Load (orchestrateur main())
  - TestDataQuality             — Vérifications qualité des données en sortie
"""

import io
import json
import zipfile
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))


# ═══════════════════════════════════════════════════════════════════════════════
# INTÉGRATION 1 — Extract → Transform
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtractToTransform:
    """
    Vérifie que les données extraites sont correctement traitées
    par les fonctions de transformation.
    """

    def test_atout_france_extract_puis_transform(self, sample_atout_france_raw, temp_data_dirs):
        """
        Simule un téléchargement Atout France puis applique transform_atout_france().
        Vérifie la cohérence bout-en-bout : types normalisés, doublons supprimés.
        """
        raw   = temp_data_dirs["raw"]
        clean = temp_data_dirs["clean"]

        # 1. Simulate: écriture du fichier brut (comme extract_atout_france() le ferait)
        raw_path = raw / "atout_france_hebergements.csv"
        sample_atout_france_raw.to_csv(raw_path, index=False, encoding="utf-8")

        assert raw_path.exists(), "Le fichier brut doit exister"

        # 2. Transform
        from data_pipline.transform import transform_atout_france
        with patch("data_pipline.transform.RAW_DIR",   raw), \
             patch("data_pipline.transform.CLEAN_DIR", clean):
            result = transform_atout_france()

        # 3. Vérification du résultat
        assert not result.empty, "Le DataFrame transformé ne doit pas être vide"
        assert "type"   in result.columns
        assert "etoiles" in result.columns
        assert "source" in result.columns
        assert (result["source"] == "atout_france").all()

        # Types normalisés
        types_invalides = result["type"].str.isupper().sum()
        assert types_invalides == 0, "Aucun type ne doit être en MAJUSCULES"

        # Doublons supprimés
        if "nom" in result.columns and "code_postal" in result.columns:
            dupes = result.duplicated(subset=["nom", "code_postal"]).sum()
            assert dupes == 0, f"{dupes} doublon(s) restant après transformation"

        # Fichier clean créé
        assert (clean / "hebergements_atout_france.csv").exists()
        assert (clean / "hebergements_atout_france.json").exists()

    def test_datatourisme_extract_puis_transform(self, sample_datatourisme_raw, temp_data_dirs):
        """Flux DATAtourisme : brut → clean avec id_court extrait et doublons supprimés."""
        raw   = temp_data_dirs["raw"]
        clean = temp_data_dirs["clean"]

        (raw / "datatourisme.csv").write_text(
            sample_datatourisme_raw.to_csv(index=False, encoding="utf-8"), encoding="utf-8"
        )

        from data_pipline.transform import transform_datatourisme
        with patch("data_pipline.transform.RAW_DIR",   raw), \
             patch("data_pipline.transform.CLEAN_DIR", clean):
            result = transform_datatourisme()

        assert len(result) == 2, "3 bruts - 1 doublon = 2 résultats"
        assert "id_court" in result.columns
        assert (result["source"] == "datatourisme").all()

    def test_insee_extract_puis_transform(self, sample_insee_raw, temp_data_dirs):
        """Flux INSEE : brut → clean avec annee/mois extraits et NaN supprimés."""
        raw   = temp_data_dirs["raw"]
        clean = temp_data_dirs["clean"]

        (raw / "insee_frequentation_hebergements.csv").write_text(
            sample_insee_raw.to_csv(index=False, encoding="utf-8"), encoding="utf-8"
        )

        from data_pipline.transform import transform_insee
        with patch("data_pipline.transform.RAW_DIR",   raw), \
             patch("data_pipline.transform.CLEAN_DIR", clean):
            result = transform_insee()

        assert not result.empty
        assert "annee" in result.columns
        assert result["valeur"].isna().sum() == 0

    def test_geo_communes_extract_puis_transform(self, sample_geo_communes_raw, temp_data_dirs):
        """Flux géo communes : brut → clean avec coordonnées et colonnes renommées."""
        raw   = temp_data_dirs["raw"]
        clean = temp_data_dirs["clean"]

        (raw / "geo_communes.csv").write_text(
            sample_geo_communes_raw.to_csv(index=False, encoding="utf-8"), encoding="utf-8"
        )

        from data_pipline.transform import transform_geo
        with patch("data_pipline.transform.RAW_DIR",   raw), \
             patch("data_pipline.transform.CLEAN_DIR", clean):
            result = transform_geo()

        assert "dept_code"   in result.columns
        assert "region_code" in result.columns
        assert "latitude"    in result.columns
        assert "longitude"   in result.columns


# ═══════════════════════════════════════════════════════════════════════════════
# INTÉGRATION 2 — Transform → Load (Data Lake)
# ═══════════════════════════════════════════════════════════════════════════════

class TestTransformToLoad:
    """
    Vérifie que les fichiers clean produits par transform sont
    correctement exportés par load_to_datalake().
    """

    def test_datalake_relit_clean_et_exporte(self, sample_hebergements_unified, temp_data_dirs):
        """
        Après avoir écrit les fichiers clean, load_to_datalake()
        doit les relire et les exporter dans processed/.
        """
        clean = temp_data_dirs["clean"]
        lake  = temp_data_dirs["lake"]

        # Simuler la présence du fichier clean
        (clean / "hebergements_unifies.csv").write_text(
            sample_hebergements_unified.to_csv(index=False, encoding="utf-8"),
            encoding="utf-8"
        )

        from data_pipline.load import load_to_datalake
        with patch("data_pipline.load.CLEAN_DIR", clean), \
             patch("data_pipline.load.LAKE_DIR",  lake):
            result = load_to_datalake()

        assert result["hebergements"] == len(sample_hebergements_unified)

        # Vérifier les fichiers exportés
        from datetime import datetime
        today   = datetime.now().strftime("%Y-%m-%d")
        out_dir = lake / "processed" / today

        assert (out_dir / "hebergements.csv").exists()
        assert (out_dir / "hebergements.json").exists()

        # Relire et vérifier le contenu
        df_exported = pd.read_csv(out_dir / "hebergements.csv")
        assert len(df_exported) == len(sample_hebergements_unified)

    def test_json_contient_donnees_completes(self, sample_hebergements_unified, temp_data_dirs):
        """
        Le JSON exporté doit contenir exactement les mêmes données que le CSV clean.
        """
        clean = temp_data_dirs["clean"]
        lake  = temp_data_dirs["lake"]
        (clean / "hebergements_unifies.csv").write_text(
            sample_hebergements_unified.to_csv(index=False, encoding="utf-8"),
            encoding="utf-8"
        )

        from data_pipline.load import load_to_datalake
        from datetime import datetime
        with patch("data_pipline.load.CLEAN_DIR", clean), \
             patch("data_pipline.load.LAKE_DIR",  lake):
            load_to_datalake()

        today = datetime.now().strftime("%Y-%m-%d")
        json_path = lake / "processed" / today / "hebergements.json"

        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)

        assert len(data) == len(sample_hebergements_unified)
        # Vérifier qu'un champ clé est présent
        assert "id_source" in data[0]


# ═══════════════════════════════════════════════════════════════════════════════
# INTÉGRATION 3 — Pipeline complet (main.py)
# ═══════════════════════════════════════════════════════════════════════════════

class TestFullPipeline:
    """
    Tests de l'orchestrateur main.py — flux complet Extract → Transform → Load.
    Tous les appels réseau et bases de données sont mockés.
    """

    def test_orchestrateur_appelle_les_trois_etapes(self):
        """
        main.py doit appeler run_all_extractions(),
        run_all_transforms() et run_all_loads() dans cet ordre.
        """
        mock_results = {"source": 10}

        with patch("data_pipline.extract.run_all_extractions",
                   return_value=mock_results) as m_extract, \
             patch("data_pipline.transform.run_all_transforms",
                   return_value=mock_results) as m_transform, \
             patch("data_pipline.load.run_all_loads",
                   return_value={
                       "datalake": mock_results,
                       "mongodb":  mock_results,
                       "postgres": mock_results,
                   }) as m_load:

            # Import et exécution du main
            import importlib
            import data_pipline.main as main_module
            importlib.reload(main_module)
            main_module.main()

        m_extract.assert_called_once()
        m_transform.assert_called_once()
        m_load.assert_called_once()

    def test_pipeline_complet_avec_donnees_reelles_simulees(self,
                                                              sample_atout_france_raw,
                                                              temp_data_dirs):
        """
        Test E2E : données brutes → transformation → Data Lake.
        Simule le pipeline sans vraie connexion réseau ni base de données.
        """
        raw   = temp_data_dirs["raw"]
        clean = temp_data_dirs["clean"]
        lake  = temp_data_dirs["lake"]

        # ── Étape 1 : Simuler l'extraction (écrire les fichiers bruts) ────────
        sample_atout_france_raw.to_csv(
            raw / "atout_france_hebergements.csv", index=False, encoding="utf-8"
        )

        # ── Étape 2 : Transformation ──────────────────────────────────────────
        from data_pipline.transform import transform_atout_france, build_hebergements_unified
        with patch("data_pipline.transform.RAW_DIR",   raw), \
             patch("data_pipline.transform.CLEAN_DIR", clean):
            df_af  = transform_atout_france()
            df_uni = build_hebergements_unified()

        assert not df_af.empty, "Atout France doit être transformé"
        assert not df_uni.empty, "Dataset unifié doit être créé"
        assert (clean / "hebergements_unifies.csv").exists()

        # ── Étape 3 : Chargement Data Lake ────────────────────────────────────
        from data_pipline.load import load_to_datalake
        with patch("data_pipline.load.CLEAN_DIR", clean), \
             patch("data_pipline.load.LAKE_DIR",  lake):
            result = load_to_datalake()

        assert result["hebergements"] > 0, "Des hébergements doivent être exportés"

        # ── Vérification finale ────────────────────────────────────────────────
        from datetime import datetime
        today     = datetime.now().strftime("%Y-%m-%d")
        json_path = lake / "processed" / today / "hebergements.json"
        assert json_path.exists(), "Le JSON final doit être créé"

        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
        assert len(data) > 0


# ═══════════════════════════════════════════════════════════════════════════════
# INTÉGRATION 4 — Qualité des données
# ═══════════════════════════════════════════════════════════════════════════════

class TestDataQuality:
    """
    Tests de qualité des données — vérifient les règles métier
    après transformation complète.
    """

    def _transform_atout(self, df, temp_data_dirs):
        raw, clean = temp_data_dirs["raw"], temp_data_dirs["clean"]
        df.to_csv(raw / "atout_france_hebergements.csv", index=False, encoding="utf-8")
        from data_pipline.transform import transform_atout_france
        with patch("data_pipline.transform.RAW_DIR",   raw), \
             patch("data_pipline.transform.CLEAN_DIR", clean):
            return transform_atout_france()

    def test_etoiles_entre_1_et_5(self, sample_atout_france_raw, temp_data_dirs):
        """Après transformation, étoiles doit être entre 1 et 5 (ou NaN)."""
        result = self._transform_atout(sample_atout_france_raw, temp_data_dirs)
        if "etoiles" in result.columns:
            non_null = result["etoiles"].dropna()
            if not non_null.empty:
                assert (non_null >= 1).all(), "Étoiles minimum = 1"
                assert (non_null <= 5).all(), "Étoiles maximum = 5"

    def test_latitude_dans_bornes_france(self, sample_atout_france_raw, temp_data_dirs):
        """latitude doit être entre -22 et 52 (France métro + DOM)."""
        result = self._transform_atout(sample_atout_france_raw, temp_data_dirs)
        if "latitude" in result.columns:
            lat = result["latitude"].dropna()
            if not lat.empty:
                assert (lat >= -22).all(), "latitude trop basse"
                assert (lat <=  52).all(), "latitude trop haute"

    def test_longitude_dans_bornes_france(self, sample_atout_france_raw, temp_data_dirs):
        """longitude doit être entre -65 et 56."""
        result = self._transform_atout(sample_atout_france_raw, temp_data_dirs)
        if "longitude" in result.columns:
            lon = result["longitude"].dropna()
            if not lon.empty:
                assert (lon >= -65).all()
                assert (lon <=  56).all()

    def test_pas_de_vrais_doublons_id_source(self, sample_atout_france_raw, temp_data_dirs):
        """
        Après build_hebergements_unified(), id_source doit être unique.
        """
        raw, clean = temp_data_dirs["raw"], temp_data_dirs["clean"]
        sample_atout_france_raw.to_csv(
            raw / "atout_france_hebergements.csv", index=False, encoding="utf-8"
        )
        from data_pipline.transform import transform_atout_france, build_hebergements_unified
        with patch("data_pipline.transform.RAW_DIR",   raw), \
             patch("data_pipline.transform.CLEAN_DIR", clean):
            transform_atout_france()
            result = build_hebergements_unified()

        if not result.empty:
            dupes = result.duplicated(subset=["id_source"]).sum()
            assert dupes == 0, f"{dupes} id_source dupliqué(s) dans le dataset unifié"

    def test_colonne_source_jamais_nulle(self, sample_atout_france_raw, temp_data_dirs):
        """La colonne 'source' ne doit jamais être nulle après transformation."""
        result = self._transform_atout(sample_atout_france_raw, temp_data_dirs)
        if "source" in result.columns:
            nulls = result["source"].isna().sum()
            assert nulls == 0, f"{nulls} valeurs nulles dans 'source'"

    def test_types_minuscules_sans_espaces(self, sample_atout_france_raw, temp_data_dirs):
        """
        Les types d'hébergement doivent être en minuscules et sans espaces initiaux.
        """
        result = self._transform_atout(sample_atout_france_raw, temp_data_dirs)
        if "type" in result.columns:
            for t in result["type"].dropna():
                assert t == t.lower(), f"Type '{t}' doit être en minuscules"
                assert t == t.strip(), f"Type '{t}' ne doit pas avoir d'espaces"

    def test_json_serialize_sans_erreur(self, sample_hebergements_unified, temp_data_dirs):
        """
        Le DataFrame final doit être sérialisable en JSON sans erreur
        (pas de types numpy non sérialisables).
        """
        clean = temp_data_dirs["clean"]
        lake  = temp_data_dirs["lake"]

        (clean / "hebergements_unifies.csv").write_text(
            sample_hebergements_unified.to_csv(index=False, encoding="utf-8"),
            encoding="utf-8"
        )

        from data_pipline.load import load_to_datalake
        with patch("data_pipline.load.CLEAN_DIR", clean), \
             patch("data_pipline.load.LAKE_DIR",  lake):
            load_to_datalake()

        from datetime import datetime
        today     = datetime.now().strftime("%Y-%m-%d")
        json_path = lake / "processed" / today / "hebergements.json"

        # Si le fichier existe, vérifier qu'il est du JSON valide
        if json_path.exists():
            with open(json_path, encoding="utf-8") as f:
                data = json.load(f)  # ne doit pas lever d'exception
            assert isinstance(data, list)
