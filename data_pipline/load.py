"""
load.py — Chargement dans MongoDB, PostgreSQL et Data Lake
===========================================================
Étape 3 du pipeline ETL : Clean → Stores

  1. load_to_mongodb()    — Staging NoSQL (documents JSON)
  2. load_to_postgres()   — Data Warehouse (tables relationnelles)
  3. load_to_datalake()   — Export final Data Lake (JSON + CSV)
  4. run_all_loads()      — Orchestrateur
"""

import os
import json
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

CLEAN_DIR = Path(__file__).parent.parent / "data_lake" / "clean"
LAKE_DIR  = Path(__file__).parent.parent / "data_lake"

# ─── Connexions (lues depuis .env) ────────────────────────────────────────────
MONGO_URI     = os.getenv("MONGO_URI",    "mongodb://localhost:27017")
MONGO_DB      = os.getenv("MONGO_DB",     "tourisdata")
POSTGRES_URI  = os.getenv("POSTGRES_URI", "postgresql://tourisme:tourisme@localhost:5432/tourisdata")


# ─── 1. MongoDB — Staging NoSQL ───────────────────────────────────────────────

def load_to_mongodb() -> dict:
    """
    Insère les données nettoyées dans MongoDB.
    Une collection par source de données.
    Utilise upsert sur l'identifiant pour éviter les doublons.

    Returns:
        dict { collection: nb_documents_insérés }
    """
    try:
        from pymongo import MongoClient, UpdateOne
        from pymongo.errors import ConnectionFailure, BulkWriteError
    except ImportError:
        log.error("[MongoDB] pymongo non installé — pip install pymongo")
        return {}

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()  # Test connexion
        db = client[MONGO_DB]
        log.info(f"[MongoDB] Connecté à {MONGO_URI} / base '{MONGO_DB}'")
    except Exception as e:
        log.error(f"[MongoDB] Connexion impossible : {e}")
        return {}

    results = {}
    collections_config = [
        ("hebergements_unifies",        "hebergements",  "id_source"),
        ("hebergements_atout_france",   "atout_france",  None),
        ("hebergements_datatourisme",   "datatourisme",  "id_court"),
        ("frequentation_insee",         "frequentation", None),
        ("geo_communes_clean",          "geo_communes",  "code"),
        ("geo_departements_clean",      "geo_departements", "code"),
        ("geo_regions_clean",           "geo_regions",   "code"),
    ]

    for filename, collection_name, upsert_key in collections_config:
        path = CLEAN_DIR / f"{filename}.csv"
        if not path.exists():
            log.warning(f"[MongoDB] Fichier absent : {filename}.csv")
            continue

        df = pd.read_csv(path, low_memory=False)
        if df.empty:
            continue

        # Nettoyage NaN → None pour MongoDB
        df = df.where(pd.notnull(df), None)
        records = df.to_dict(orient="records")

        collection = db[collection_name]

        if upsert_key and upsert_key in df.columns:
            # Upsert par clé unique
            operations = [
                UpdateOne(
                    {upsert_key: rec[upsert_key]},
                    {"$set": rec, "$setOnInsert": {"created_at": datetime.utcnow()}},
                    upsert=True
                )
                for rec in records
            ]
            try:
                result = collection.bulk_write(operations, ordered=False)
                n = result.upserted_count + result.modified_count
                log.info(f"[MongoDB] {collection_name} : {n} documents upsertés")
                results[collection_name] = n
            except BulkWriteError as bwe:
                log.warning(f"[MongoDB] {collection_name} partial error : {bwe.details}")
        else:
            # Insert classique (avec marquage timestamp)
            for rec in records:
                rec["loaded_at"] = datetime.utcnow().isoformat()
            try:
                collection.delete_many({})  # Refresh complet
                result = collection.insert_many(records)
                n = len(result.inserted_ids)
                log.info(f"[MongoDB] {collection_name} : {n} documents insérés")
                results[collection_name] = n
            except Exception as e:
                log.error(f"[MongoDB] {collection_name} erreur insert : {e}")

    client.close()
    return results


# ─── 2. PostgreSQL — Data Warehouse ───────────────────────────────────────────

POSTGRES_SCHEMA = """
-- Référentiel géographique
CREATE TABLE IF NOT EXISTS geo_regions (
    code        VARCHAR(10) PRIMARY KEY,
    nom         VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS geo_departements (
    code        VARCHAR(10) PRIMARY KEY,
    nom         VARCHAR(100),
    code_region VARCHAR(10) REFERENCES geo_regions(code) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS geo_communes (
    code            VARCHAR(10) PRIMARY KEY,
    nom             VARCHAR(150),
    code_postal     VARCHAR(10),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    population      INTEGER,
    dept_code       VARCHAR(10) REFERENCES geo_departements(code) ON DELETE SET NULL,
    region_code     VARCHAR(10)
);

-- Hébergements (table principale)
CREATE TABLE IF NOT EXISTS hebergements (
    id              SERIAL PRIMARY KEY,
    id_source       VARCHAR(200) UNIQUE,
    source          VARCHAR(50),
    type            VARCHAR(80),
    nom             VARCHAR(300),
    adresse         VARCHAR(300),
    code_postal     VARCHAR(10),
    ville           VARCHAR(150),
    departement     VARCHAR(100),
    region          VARCHAR(100),
    etoiles         SMALLINT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    nb_chambres     INTEGER,
    site_web        VARCHAR(300),
    telephone       VARCHAR(30),
    date_classement DATE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hebergements_type    ON hebergements(type);
CREATE INDEX IF NOT EXISTS idx_hebergements_dept    ON hebergements(departement);
CREATE INDEX IF NOT EXISTS idx_hebergements_etoiles ON hebergements(etoiles);
CREATE INDEX IF NOT EXISTS idx_hebergements_geo     ON hebergements(latitude, longitude);

-- Fréquentation (séries INSEE)
CREATE TABLE IF NOT EXISTS frequentation (
    id          SERIAL PRIMARY KEY,
    serie       VARCHAR(100),
    idbank      VARCHAR(50),
    periode     VARCHAR(20),
    annee       SMALLINT,
    mois        SMALLINT,
    valeur      DOUBLE PRECISION,
    source      VARCHAR(50),
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_frequentation_serie  ON frequentation(serie);
CREATE INDEX IF NOT EXISTS idx_frequentation_annee  ON frequentation(annee);
"""


def load_to_postgres() -> dict:
    """
    Crée le schéma et charge les données dans PostgreSQL.

    Returns:
        dict { table: nb_lignes_insérées }
    """
    try:
        from sqlalchemy import create_engine, text
        from sqlalchemy.exc import SQLAlchemyError
    except ImportError:
        log.error("[PostgreSQL] sqlalchemy non installé — pip install sqlalchemy psycopg2-binary")
        return {}

    try:
        engine = create_engine(POSTGRES_URI, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info(f"[PostgreSQL] Connecté : {POSTGRES_URI}")
    except Exception as e:
        log.error(f"[PostgreSQL] Connexion impossible : {e}")
        return {}

    results = {}

    # Création du schéma
    try:
        with engine.begin() as conn:
            for statement in POSTGRES_SCHEMA.split(";"):
                stmt = statement.strip()
                if stmt:
                    conn.execute(text(stmt))
        log.info("[PostgreSQL] Schéma créé/vérifié ✅")
    except Exception as e:
        log.error(f"[PostgreSQL] Erreur création schéma : {e}")
        return {}

    # Chargement par table
    tables_config = {
        "geo_regions":       ("geo_regions_clean.csv",      ["code", "nom"]),
        "geo_departements":  ("geo_departements_clean.csv", ["code", "nom", "codeRegion"]),
        "geo_communes":      ("geo_communes_clean.csv",
                              ["code", "nom", "code_postal_principal",
                               "latitude", "longitude", "population",
                               "dept_code", "region_code"]),
        "hebergements":      ("hebergements_unifies.csv",
                              ["id_source", "source", "type", "nom", "adresse",
                               "code_postal", "ville", "departement", "region",
                               "etoiles", "latitude", "longitude",
                               "nb_chambres", "site_web", "telephone"]),
        "frequentation":     ("frequentation_insee.csv",
                              ["serie", "idbank", "periode", "annee", "mois",
                               "valeur", "source"]),
    }

    for table, (filename, cols) in tables_config.items():
        path = CLEAN_DIR / filename
        if not path.exists():
            log.warning(f"[PostgreSQL] Fichier absent : {filename}")
            continue

        df = pd.read_csv(path, low_memory=False)
        if df.empty:
            continue

        # Garder seulement les colonnes qui existent
        available_cols = [c for c in cols if c in df.columns]
        df_load = df[available_cols].copy()

        # Renommage pour correspondre aux colonnes PostgreSQL
        if table == "geo_departements" and "codeRegion" in df_load.columns:
            df_load = df_load.rename(columns={"codeRegion": "code_region"})
        if table == "geo_communes" and "code_postal_principal" in df_load.columns:
            df_load = df_load.rename(columns={"code_postal_principal": "code_postal"})

        try:
            # Truncate + reload (idempotent)
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))

            df_load.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=500)
            log.info(f"[PostgreSQL] {table} : {len(df_load)} lignes chargées")
            results[table] = len(df_load)

        except Exception as e:
            log.error(f"[PostgreSQL] Erreur chargement {table} : {e}")

    return results


# ─── 3. Data Lake — Export final ──────────────────────────────────────────────

def load_to_datalake() -> dict:
    """
    Organise le Data Lake final avec une structure par zone et par date.

    Structure créée :
      data_lake/
        raw/          ← données brutes (extrait.py)
        clean/        ← données nettoyées (transform.py)
        processed/    ← données prêtes à l'usage (cette fonction)
          YYYY-MM-DD/
            hebergements.json
            hebergements.csv
            frequentation.json
            geo_communes.json

    Returns:
        dict { fichier: nb_lignes }
    """
    today = datetime.now().strftime("%Y-%m-%d")
    processed_dir = LAKE_DIR / "processed" / today
    processed_dir.mkdir(parents=True, exist_ok=True)

    results = {}
    exports = [
        ("hebergements_unifies",   "hebergements"),
        ("frequentation_insee",    "frequentation"),
        ("geo_communes_clean",     "geo_communes"),
        ("geo_departements_clean", "geo_departements"),
        ("geo_regions_clean",      "geo_regions"),
    ]

    for src_name, dest_name in exports:
        src_path = CLEAN_DIR / f"{src_name}.csv"
        if not src_path.exists():
            continue

        df = pd.read_csv(src_path, low_memory=False)
        if df.empty:
            continue

        # CSV
        csv_out = processed_dir / f"{dest_name}.csv"
        df.to_csv(csv_out, index=False, encoding="utf-8")

        # JSON (orienté records pour une API-friendly lecture)
        json_out = processed_dir / f"{dest_name}.json"
        df.where(pd.notnull(df), None).to_json(
            json_out, orient="records", force_ascii=False, indent=2
        )

        log.info(f"[Data Lake] {dest_name} → {processed_dir.name}/ ({len(df)} lignes)")
        results[dest_name] = len(df)

    # Fichier de métadonnées du run
    meta = {
        "run_date":   today,
        "run_ts":     datetime.now().isoformat(),
        "datasets":   results,
        "lake_path":  str(processed_dir),
    }
    meta_path = processed_dir / "_metadata.json"
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"[Data Lake] Métadonnées → {meta_path}")

    # Lien symbolique "latest" → dernier run
    latest = LAKE_DIR / "processed" / "latest"
    if latest.is_symlink() or latest.exists():
        try:
            latest.unlink()
        except Exception:
            pass
    try:
        latest.symlink_to(processed_dir, target_is_directory=True)
    except Exception:
        pass  # Windows peut nécessiter des droits admin pour les symlinks

    return results


# ─── Orchestrateur ─────────────────────────────────────────────────────────────

def run_all_loads() -> dict:
    """
    Lance le chargement dans toutes les destinations.

    Returns:
        dict { destination: { table/collection: nb_lignes } }
    """
    log.info("=" * 60)
    log.info("  PIPELINE DE CHARGEMENT")
    log.info("=" * 60)

    results = {}

    # 1. Data Lake (toujours)
    log.info("\n▶ Data Lake (JSON + CSV)…")
    results["datalake"] = load_to_datalake()

    # 2. MongoDB
    log.info("\n▶ MongoDB…")
    results["mongodb"] = load_to_mongodb()

    # 3. PostgreSQL
    log.info("\n▶ PostgreSQL…")
    results["postgres"] = load_to_postgres()

    # Résumé
    log.info("\n" + "=" * 60)
    log.info("  RÉSUMÉ CHARGEMENT")
    log.info("=" * 60)
    for dest, counts in results.items():
        total = sum(counts.values()) if counts else 0
        status = "✅" if total > 0 else "⚠️ "
        log.info(f"  {status}  {dest:<15} → {total:>6} enregistrements")

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_all_loads()
