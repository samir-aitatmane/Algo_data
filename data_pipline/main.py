"""
main.py — Orchestrateur du pipeline ETL TourisData
====================================================
Exécution complète : Extract → Transform → Load
"""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)


def main():
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║         PIPELINE ETL — TOURISDATA                       ║")
    log.info("║  Plateforme Airbnb-like sur données ouvertes françaises  ║")
    log.info("╚══════════════════════════════════════════════════════════╝")

    # ── 1. EXTRACT ────────────────────────────────────────────────────────────
    log.info("\n🔽  ÉTAPE 1 : EXTRACTION des sources")
    from data_pipline.extract import run_all_extractions
    extract_results = run_all_extractions()

    # ── 2. TRANSFORM ──────────────────────────────────────────────────────────
    log.info("\n🔄  ÉTAPE 2 : TRANSFORMATION / NETTOYAGE")
    from data_pipline.transform import run_all_transforms
    transform_results = run_all_transforms()

    # ── 3. LOAD ───────────────────────────────────────────────────────────────
    log.info("\n🔼  ÉTAPE 3 : CHARGEMENT (Data Lake + MongoDB + PostgreSQL)")
    from data_pipline.load import run_all_loads
    load_results = run_all_loads()

    # ── BILAN GLOBAL ──────────────────────────────────────────────────────────
    log.info("\n")
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║                   BILAN DU PIPELINE                     ║")
    log.info("╠══════════════════════════════════════════════════════════╣")

    extract_ok  = sum(1 for v in extract_results.values()  if v > 0)
    transform_ok = sum(1 for v in transform_results.values() if v > 0)

    mongo_ok    = len(load_results.get("mongodb",  {})) > 0
    postgres_ok = len(load_results.get("postgres", {})) > 0
    lake_ok     = len(load_results.get("datalake", {})) > 0

    log.info(f"║  ✅ Extraction  : {extract_ok}/{len(extract_results)} sources récupérées")
    log.info(f"║  ✅ Transform   : {transform_ok}/{len(transform_results)} jeux nettoyés")
    log.info(f"║  {'✅' if lake_ok     else '❌'} Data Lake  : {'OK' if lake_ok     else 'ÉCHEC'}")
    log.info(f"║  {'✅' if mongo_ok    else '⚠️'} MongoDB    : {'OK' if mongo_ok    else 'Non dispo / ignoré'}")
    log.info(f"║  {'✅' if postgres_ok else '⚠️'} PostgreSQL : {'OK' if postgres_ok else 'Non dispo / ignoré'}")
    log.info("╚══════════════════════════════════════════════════════════╝")
    log.info("  Log complet → pipeline.log")


if __name__ == "__main__":
    main()
