"""
test_transform.py — Tests unitaires du module transform.py
===========================================================

Teste chaque fonction de nettoyage/transformation de façon isolée.
On passe directement des DataFrames en entrée — le code ne lit
jamais de vrais fichiers grâce au patch de RAW_DIR et CLEAN_DIR.

Structure :
  - TestTransformAtoutFrance     — tests de transform_atout_france()
  - TestTransformDatatourisme    — tests de transform_datatourisme()
  - TestTransformInsee           — tests de transform_insee()
  - TestTransformGeo             — tests de transform_geo() et helpers
  - TestBuildHebergementsUnified — tests de build_hebergements_unified()
  - TestRunAllTransforms         — tests de l'orchestrateur
"""

import json
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_pipline import transform as T


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS COMMUNS
# ═══════════════════════════════════════════════════════════════════════════════

def _patch_dirs(raw_dir: Path, clean_dir: Path):
    """Context manager helper : remplace RAW_DIR et CLEAN_DIR."""
    return patch.multiple(
        "data_pipline.transform",
        RAW_DIR=raw_dir,
        CLEAN_DIR=clean_dir,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 1 — transform_atout_france()
# ═══════════════════════════════════════════════════════════════════════════════

class TestTransformAtoutFrance:
    """Tests de nettoyage du dataset Atout France."""

    def _run(self, df_input: pd.DataFrame, temp_data_dirs: dict) -> pd.DataFrame:
        """Helper : écrit le CSV brut et lance la transformation."""
        raw = temp_data_dirs["raw"]
        clean = temp_data_dirs["clean"]
        path = raw / "atout_france_hebergements.csv"
        df_input.to_csv(path, index=False, encoding="utf-8")

        with _patch_dirs(raw, clean):
            return T.transform_atout_france()

    def test_retourne_vide_si_fichier_absent(self, temp_data_dirs):
        """Si le fichier brut n'existe pas, retourne DataFrame vide."""
        with _patch_dirs(temp_data_dirs["raw"], temp_data_dirs["clean"]):
            result = T.transform_atout_france()
        assert result.empty

    def test_types_hebergement_normalises(self, sample_atout_france_raw, temp_data_dirs):
        """
        Les types d'hébergement doivent être convertis en minuscules normalisés
        'hotel', 'camping', 'residence', etc.
        """
        result = self._run(sample_atout_france_raw, temp_data_dirs)
        types = result["type"].unique().tolist()
        assert "hotel"    in types, "HOTEL DE TOURISME → hotel"
        assert "camping"  in types, "CAMPING → camping"
        assert "residence" in types, "RÉSIDENCE DE TOURISME → residence"

    def test_etoiles_converties_en_entier(self, sample_atout_france_raw, temp_data_dirs):
        """
        '4 étoiles' → 4 (Int64), '3 étoiles' → 3.
        Pas de valeur flottante ni de chaîne de caractères.
        """
        result = self._run(sample_atout_france_raw, temp_data_dirs)
        assert "etoiles" in result.columns
        # Pas de NaN non intentionnel
        non_null = result["etoiles"].dropna()
        assert non_null.dtype.name in ("Int64", "int64", "float64")
        assert (non_null >= 1).all() and (non_null <= 5).all(), "Étoiles entre 1 et 5"

    def test_coordonnees_gps_numeriques(self, sample_atout_france_raw, temp_data_dirs):
        """latitude et longitude doivent être float, pas des chaînes."""
        result = self._run(sample_atout_france_raw, temp_data_dirs)
        if "latitude" in result.columns and result["latitude"].notna().any():
            assert pd.api.types.is_float_dtype(result["latitude"]), "latitude doit être float"
            assert pd.api.types.is_float_dtype(result["longitude"]), "longitude doit être float"

    def test_coordonnees_hors_france_supprimees(self, temp_data_dirs):
        """
        Les hébergements avec des coordonnées en dehors des limites de la France
        (métro + DOM) doivent être filtrés.
        """
        df = pd.DataFrame({
            "type_hebergement": ["HOTEL DE TOURISME", "HOTEL DE TOURISME"],
            "nom_hebergement":  ["Hotel France", "Hotel Hors France"],
            "code_postal":      ["75001", "00000"],
            "latitude":   [48.8566,  60.0],   # 60° N → hors France
            "longitude":  [2.3522,   10.0],
        })
        result = self._run(df, temp_data_dirs)
        assert len(result) == 1, "L'hébergement hors France doit être supprimé"
        assert result.iloc[0]["nom"] == "Hotel France"

    def test_doublons_supprimes_sur_nom_code_postal(self, sample_atout_france_raw, temp_data_dirs):
        """
        Les lignes avec le même nom ET code postal doivent être dédoublonnées.
        sample_atout_france_raw contient un doublon intentionnel.
        """
        result = self._run(sample_atout_france_raw, temp_data_dirs)
        # 4 lignes brutes dont 1 doublon → 3 attendues
        assert len(result) == 3, f"Attendu 3 après dédup, obtenu {len(result)}"

    def test_colonne_source_remplie(self, sample_atout_france_raw, temp_data_dirs):
        """Chaque ligne doit avoir source='atout_france'."""
        result = self._run(sample_atout_france_raw, temp_data_dirs)
        assert (result["source"] == "atout_france").all()

    def test_texte_en_title_case(self, sample_atout_france_raw, temp_data_dirs):
        """Les noms de villes/hébergements doivent être en Title Case."""
        result = self._run(sample_atout_france_raw, temp_data_dirs)
        if "ville" in result.columns:
            for v in result["ville"].dropna():
                assert v == v.title(), f"'{v}' devrait être en Title Case"

    def test_fichiers_csv_et_json_sauvegardes(self, sample_atout_france_raw, temp_data_dirs):
        """Les fichiers CSV et JSON doivent être créés dans data_lake/clean/."""
        self._run(sample_atout_france_raw, temp_data_dirs)
        assert (temp_data_dirs["clean"] / "hebergements_atout_france.csv").exists()
        assert (temp_data_dirs["clean"] / "hebergements_atout_france.json").exists()

    def test_date_classement_parsee(self, sample_atout_france_raw, temp_data_dirs):
        """La colonne date doit être convertie en type datetime."""
        result = self._run(sample_atout_france_raw, temp_data_dirs)
        if "date_classement" in result.columns:
            non_null = result["date_classement"].dropna()
            if not non_null.empty:
                assert pd.api.types.is_datetime64_any_dtype(result["date_classement"])


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 2 — transform_datatourisme()
# ═══════════════════════════════════════════════════════════════════════════════

class TestTransformDatatourisme:
    """Tests de nettoyage du dataset DATAtourisme."""

    def _run(self, df_input, temp_data_dirs):
        raw, clean = temp_data_dirs["raw"], temp_data_dirs["clean"]
        (raw / "datatourisme.csv").write_text(
            df_input.to_csv(index=False, encoding="utf-8"), encoding="utf-8"
        )
        with _patch_dirs(raw, clean):
            return T.transform_datatourisme()

    def test_retourne_vide_si_fichier_absent(self, temp_data_dirs):
        with _patch_dirs(temp_data_dirs["raw"], temp_data_dirs["clean"]):
            result = T.transform_datatourisme()
        assert result.empty

    def test_id_court_extrait_depuis_uri(self, sample_datatourisme_raw, temp_data_dirs):
        """
        L'ID court doit être la partie finale de l'URI.
        Ex: 'https://data.datatourisme.fr/abc123' → 'abc123'
        """
        result = self._run(sample_datatourisme_raw, temp_data_dirs)
        assert "id_court" in result.columns
        assert "abc123" in result["id_court"].values
        assert "def456" in result["id_court"].values

    def test_type_simplifie_extrait(self, sample_datatourisme_raw, temp_data_dirs):
        """
        Le type simplifié doit extraire la fin de l'URI schema.org.
        Ex: 'https://schema.org/Hotel' → 'hotel'
        """
        result = self._run(sample_datatourisme_raw, temp_data_dirs)
        assert "type_simplifie" in result.columns
        types = result["type_simplifie"].str.lower().tolist()
        assert "hotel"   in types
        assert "camping" in types  # Campground → camping

    def test_doublons_supprimes_sur_id_court(self, sample_datatourisme_raw, temp_data_dirs):
        """3 lignes dont 1 doublon sur id → 2 résultats attendus."""
        result = self._run(sample_datatourisme_raw, temp_data_dirs)
        assert len(result) == 2

    def test_coordonnees_en_float(self, sample_datatourisme_raw, temp_data_dirs):
        """latitude et longitude doivent être des floats."""
        result = self._run(sample_datatourisme_raw, temp_data_dirs)
        if "latitude" in result.columns:
            assert pd.api.types.is_float_dtype(result["latitude"])

    def test_colonne_source_remplie(self, sample_datatourisme_raw, temp_data_dirs):
        """source doit être 'datatourisme'."""
        result = self._run(sample_datatourisme_raw, temp_data_dirs)
        assert (result["source"] == "datatourisme").all()


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 3 — transform_insee()
# ═══════════════════════════════════════════════════════════════════════════════

class TestTransformInsee:
    """Tests de nettoyage des séries de fréquentation INSEE."""

    def _run(self, df_input, temp_data_dirs):
        raw, clean = temp_data_dirs["raw"], temp_data_dirs["clean"]
        (raw / "insee_frequentation_hebergements.csv").write_text(
            df_input.to_csv(index=False, encoding="utf-8"), encoding="utf-8"
        )
        with _patch_dirs(raw, clean):
            return T.transform_insee()

    def test_retourne_vide_si_aucun_fichier(self, temp_data_dirs):
        with _patch_dirs(temp_data_dirs["raw"], temp_data_dirs["clean"]):
            result = T.transform_insee()
        assert result.empty

    def test_annee_extraite_depuis_periode(self, sample_insee_raw, temp_data_dirs):
        """
        La colonne 'annee' doit être extraite des 4 premiers caractères de 'periode'.
        '2023-01' → annee = 2023
        """
        result = self._run(sample_insee_raw, temp_data_dirs)
        if "annee" in result.columns:
            assert (result["annee"].dropna() >= 2000).all()
            assert 2023 in result["annee"].values

    def test_mois_extrait_pour_periodes_mensuelles(self, sample_insee_raw, temp_data_dirs):
        """
        Pour les périodes de format 'YYYY-MM', le mois doit être extrait.
        '2023-01' → mois = 1
        """
        result = self._run(sample_insee_raw, temp_data_dirs)
        if "mois" in result.columns:
            assert result["mois"].dropna().between(1, 12).all(), "Mois entre 1 et 12"

    def test_valeurs_nan_supprimees(self, temp_data_dirs):
        """Les lignes avec valeur NaN doivent être supprimées."""
        df = pd.DataFrame({
            "serie":   ["nuitees_hotels_france", "nuitees_hotels_france"],
            "periode": ["2023-01", "2023-02"],
            "valeur":  [12_500_000, None],  # 1 valeur manquante
        })
        result = self._run(df, temp_data_dirs)
        assert result["valeur"].isna().sum() == 0, "Pas de NaN dans valeur"
        assert len(result) == 1, "La ligne NaN doit être supprimée"

    def test_colonne_source_remplie(self, sample_insee_raw, temp_data_dirs):
        """source doit être 'insee'."""
        result = self._run(sample_insee_raw, temp_data_dirs)
        assert (result["source"] == "insee").all()

    def test_valeurs_numeriques(self, sample_insee_raw, temp_data_dirs):
        """La colonne 'valeur' doit être numérique (float64)."""
        result = self._run(sample_insee_raw, temp_data_dirs)
        if "valeur" in result.columns:
            assert pd.api.types.is_numeric_dtype(result["valeur"])


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 4 — transform_geo() et helpers
# ═══════════════════════════════════════════════════════════════════════════════

class TestTransformGeo:
    """Tests de nettoyage du référentiel géographique."""

    def _run_geo(self, df_input, temp_data_dirs):
        raw, clean = temp_data_dirs["raw"], temp_data_dirs["clean"]
        (raw / "geo_communes.csv").write_text(
            df_input.to_csv(index=False, encoding="utf-8"), encoding="utf-8"
        )
        with _patch_dirs(raw, clean):
            return T.transform_geo()

    def test_coordonnees_extraites_depuis_centre_coordinates(self,
                                                              sample_geo_communes_raw,
                                                              temp_data_dirs):
        """
        Si la colonne 'centre.coordinates' est présente,
        latitude et longitude doivent en être extraites.
        centre.coordinates = [lon, lat] donc longitude = val[0], latitude = val[1].
        """
        result = self._run_geo(sample_geo_communes_raw, temp_data_dirs)
        assert "latitude"  in result.columns, "latitude doit être créée"
        assert "longitude" in result.columns, "longitude doit être créée"
        assert "centre.coordinates" not in result.columns, "colonne brute doit être supprimée"

        # Nice : [7.262, 43.710] → lat=43.710, lon=7.262
        nice_row = result[result["code"] == "06088"]
        if not nice_row.empty:
            assert abs(nice_row.iloc[0]["latitude"]  - 43.710) < 0.01
            assert abs(nice_row.iloc[0]["longitude"] - 7.262)  < 0.01

    def test_colonnes_imbriquees_renommees(self, sample_geo_communes_raw, temp_data_dirs):
        """
        Les colonnes 'departement.code', 'region.code' etc.
        doivent être renommées en 'dept_code', 'region_code'.
        """
        result = self._run_geo(sample_geo_communes_raw, temp_data_dirs)
        assert "dept_code"   in result.columns
        assert "region_code" in result.columns
        assert "departement.code" not in result.columns

    def test_code_postal_principal_extrait(self, sample_geo_communes_raw, temp_data_dirs):
        """
        codesPostaux est une liste → code_postal_principal = premier élément.
        """
        result = self._run_geo(sample_geo_communes_raw, temp_data_dirs)
        assert "code_postal_principal" in result.columns
        # Nice : ["06000", "06100"] → "06000"
        nice_row = result[result["code"] == "06088"]
        if not nice_row.empty:
            assert nice_row.iloc[0]["code_postal_principal"] == "06000"

    def test_retourne_vide_si_fichier_absent(self, temp_data_dirs):
        with _patch_dirs(temp_data_dirs["raw"], temp_data_dirs["clean"]):
            result = T.transform_geo()
        assert result.empty

    def test_dept_sauvegarde(self, temp_data_dirs):
        """transform_geo_departements() doit sauvegarder le fichier clean."""
        raw, clean = temp_data_dirs["raw"], temp_data_dirs["clean"]
        (raw / "geo_departements.csv").write_text(
            pd.DataFrame({"code": ["01"], "nom": ["Ain"]}).to_csv(index=False),
            encoding="utf-8"
        )
        with _patch_dirs(raw, clean):
            T.transform_geo_departements()
        assert (clean / "geo_departements_clean.csv").exists()

    def test_regions_sauvegardees(self, temp_data_dirs):
        """transform_geo_regions() doit sauvegarder le fichier clean."""
        raw, clean = temp_data_dirs["raw"], temp_data_dirs["clean"]
        (raw / "geo_regions.csv").write_text(
            pd.DataFrame({"code": ["11"], "nom": ["Île-de-France"]}).to_csv(index=False),
            encoding="utf-8"
        )
        with _patch_dirs(raw, clean):
            T.transform_geo_regions()
        assert (clean / "geo_regions_clean.csv").exists()


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 5 — build_hebergements_unified()
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuildHebergementsUnified:
    """Tests de la fusion Atout France + DATAtourisme."""

    def test_fusion_deux_sources(self, temp_data_dirs,
                                  sample_atout_france_raw,
                                  sample_datatourisme_raw):
        """
        La fusion doit combiner les lignes des deux sources
        avec un schéma de colonnes uniforme.
        """
        raw, clean = temp_data_dirs["raw"], temp_data_dirs["clean"]

        # Préparer les fichiers clean (normalement produits par transform_*)
        af_clean = pd.DataFrame({
            "type": ["hotel", "camping"],
            "nom":  ["Hotel Du Lac", "Camping Les Pins"],
            "code_postal": ["06000", "33000"],
            "ville": ["Nice", "Bordeaux"],
            "departement": ["Alpes-Maritimes", "Gironde"],
            "region": ["PACA", "Nouvelle-Aquitaine"],
            "etoiles": [4, 3],
            "latitude": [43.7102, 44.8378],
            "longitude": [7.2620, -0.5792],
            "nb_chambres": [120, None],
            "site_web": ["www.hotel.fr", None],
            "telephone": ["04 93 12 34 56", None],
            "source": ["atout_france", "atout_france"],
            "adresse": ["12 Rue Riviera", "Route Forêt"],
        })
        dt_clean = pd.DataFrame({
            "id_court":        ["abc123", "def456"],
            "type_simplifie":  ["hotel",  "camping"],
            "nom":             ["Le Grand Hôtel", "Camping du Soleil"],
            "latitude":        [43.710, 45.188],
            "longitude":       [7.262,  5.724],
            "source":          ["datatourisme", "datatourisme"],
        })

        (clean / "hebergements_atout_france.csv").write_text(
            af_clean.to_csv(index=False, encoding="utf-8"), encoding="utf-8"
        )
        (clean / "hebergements_datatourisme.csv").write_text(
            dt_clean.to_csv(index=False, encoding="utf-8"), encoding="utf-8"
        )

        with _patch_dirs(raw, clean):
            result = T.build_hebergements_unified()

        assert len(result) == 4, "2 AF + 2 DT = 4 hébergements"
        assert "id_source" in result.columns
        assert result["id_source"].str.startswith("AF_").sum() == 2
        assert result["id_source"].str.startswith("DT_").sum() == 2

    def test_colonnes_communes_presentes(self, temp_data_dirs):
        """Le schéma de sortie doit contenir toutes les colonnes communes."""
        raw, clean = temp_data_dirs["raw"], temp_data_dirs["clean"]
        af = pd.DataFrame({
            "type": ["hotel"], "nom": ["Hotel Test"], "code_postal": ["06000"],
            "ville": ["Nice"], "departement": ["AM"], "region": ["PACA"],
            "etoiles": [4], "latitude": [43.7], "longitude": [7.2],
            "nb_chambres": [50], "site_web": [None], "telephone": [None],
            "source": ["atout_france"], "adresse": [None],
        })
        (clean / "hebergements_atout_france.csv").write_text(
            af.to_csv(index=False, encoding="utf-8"), encoding="utf-8"
        )
        with _patch_dirs(raw, clean):
            result = T.build_hebergements_unified()

        required_cols = ["id_source", "source", "type", "nom", "latitude", "longitude"]
        for col in required_cols:
            assert col in result.columns, f"Colonne '{col}' manquante"

    def test_doublons_id_source_supprimes(self, temp_data_dirs):
        """
        Si deux hébergements ont le même id_source (même nom + code postal),
        un seul doit rester.
        """
        raw, clean = temp_data_dirs["raw"], temp_data_dirs["clean"]
        af = pd.DataFrame({
            "type": ["hotel", "hotel"],
            "nom":  ["Hotel Test", "Hotel Test"],    # Même nom
            "code_postal": ["06000", "06000"],        # Même code postal
            "ville": ["Nice", "Nice"],
            "departement": ["AM", "AM"],
            "region": ["PACA", "PACA"],
            "etoiles": [4, 4],
            "latitude": [43.7, 43.7],
            "longitude": [7.2, 7.2],
            "nb_chambres": [50, 50],
            "site_web": [None, None], "telephone": [None, None],
            "source": ["atout_france", "atout_france"],
            "adresse": [None, None],
        })
        (clean / "hebergements_atout_france.csv").write_text(
            af.to_csv(index=False, encoding="utf-8"), encoding="utf-8"
        )
        with _patch_dirs(raw, clean):
            result = T.build_hebergements_unified()
        assert len(result) == 1, "Doublon sur id_source doit être supprimé"

    def test_retourne_vide_si_aucune_source(self, temp_data_dirs):
        """Si aucun fichier clean n'existe, retourne DataFrame vide."""
        raw, clean = temp_data_dirs["raw"], temp_data_dirs["clean"]
        with _patch_dirs(raw, clean):
            result = T.build_hebergements_unified()
        assert result.empty


# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE 6 — run_all_transforms()
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunAllTransforms:
    """Tests de l'orchestrateur de transformation."""

    def test_retourne_dictionnaire_par_source(self):
        """
        run_all_transforms() doit retourner un dict avec une clé par dataset
        et le nombre de lignes transformées.
        """
        df_3 = pd.DataFrame({"x": [1, 2, 3]})

        with patch("data_pipline.transform.transform_atout_france",     return_value=df_3), \
             patch("data_pipline.transform.transform_datatourisme",      return_value=df_3), \
             patch("data_pipline.transform.transform_insee",             return_value=df_3), \
             patch("data_pipline.transform.transform_geo",               return_value=df_3), \
             patch("data_pipline.transform.transform_geo_departements",  return_value=df_3), \
             patch("data_pipline.transform.transform_geo_regions",       return_value=df_3), \
             patch("data_pipline.transform.build_hebergements_unified",  return_value=df_3):

            result = T.run_all_transforms()

        assert "atout_france"        in result
        assert "datatourisme"        in result
        assert "insee"               in result
        assert "geo_communes"        in result
        assert "hebergements_unifies" in result
        assert result["atout_france"] == 3

    def test_appelle_toutes_les_fonctions(self):
        """Chaque fonction de transformation doit être appelée exactement une fois."""
        df = pd.DataFrame({"x": [1]})

        with patch("data_pipline.transform.transform_atout_france",     return_value=df) as m1, \
             patch("data_pipline.transform.transform_datatourisme",      return_value=df) as m2, \
             patch("data_pipline.transform.transform_insee",             return_value=df) as m3, \
             patch("data_pipline.transform.transform_geo",               return_value=df) as m4, \
             patch("data_pipline.transform.transform_geo_departements",  return_value=df) as m5, \
             patch("data_pipline.transform.transform_geo_regions",       return_value=df) as m6, \
             patch("data_pipline.transform.build_hebergements_unified",  return_value=df) as m7:

            T.run_all_transforms()

        for m in [m1, m2, m3, m4, m5, m6, m7]:
            m.assert_called_once()

    def test_continue_si_source_vide(self):
        """run_all_transforms() ne doit pas lever d'exception si une source est vide."""
        df = pd.DataFrame({"x": [1]})

        with patch("data_pipline.transform.transform_atout_france",     return_value=pd.DataFrame()), \
             patch("data_pipline.transform.transform_datatourisme",      return_value=df), \
             patch("data_pipline.transform.transform_insee",             return_value=df), \
             patch("data_pipline.transform.transform_geo",               return_value=df), \
             patch("data_pipline.transform.transform_geo_departements",  return_value=df), \
             patch("data_pipline.transform.transform_geo_regions",       return_value=df), \
             patch("data_pipline.transform.build_hebergements_unified",  return_value=df):

            result = T.run_all_transforms()

        assert result["atout_france"] == 0, "Source vide → 0 lignes"
        assert result["datatourisme"] == 1, "Source OK → 1 ligne"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS HELPERS INTERNES
# ═══════════════════════════════════════════════════════════════════════════════

class TestHelpers:
    """Tests des fonctions internes _save() et _load_raw()."""

    def test_save_cree_csv_et_json(self, temp_data_dirs):
        """_save() doit créer un fichier CSV et un fichier JSON."""
        clean = temp_data_dirs["clean"]
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

        with patch("data_pipline.transform.CLEAN_DIR", clean):
            from data_pipline.transform import _save
            path = _save(df, "test_output")

        assert (clean / "test_output.csv").exists(),  "CSV doit exister"
        assert (clean / "test_output.json").exists(), "JSON doit exister"

        # Vérifier que le JSON est valide et contient les bonnes données
        import json
        with open(clean / "test_output.json", encoding="utf-8") as f:
            data = json.load(f)
        assert len(data) == 2

    def test_load_raw_retourne_vide_si_absent(self, temp_data_dirs):
        """_load_raw() doit retourner un DataFrame vide si le fichier n'existe pas."""
        with patch("data_pipline.transform.RAW_DIR", temp_data_dirs["raw"]):
            from data_pipline.transform import _load_raw
            result = _load_raw("fichier_inexistant")
        assert result.empty

    def test_load_raw_lit_correctement(self, temp_data_dirs):
        """_load_raw() doit lire le CSV brut et retourner un DataFrame."""
        raw = temp_data_dirs["raw"]
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        (raw / "test_data.csv").write_text(df.to_csv(index=False), encoding="utf-8")

        with patch("data_pipline.transform.RAW_DIR", raw):
            from data_pipline.transform import _load_raw
            result = _load_raw("test_data")

        assert len(result) == 3
        assert list(result.columns) == ["a", "b"]
