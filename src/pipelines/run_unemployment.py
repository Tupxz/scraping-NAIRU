"""Pipeline de desempleo (GEIH – fuente real DANE).

Orquesta: scraping → descarga → parsing → validación → guardado.
"""

from __future__ import annotations

import logging

from src.config import GEIH_CONFIG
from src.io_utils import setup_logging
from src.quality_checks import run_all_checks
from src.sources.dane.unemployment import run_geih_pipeline

logger = logging.getLogger("nairu_pipeline")


def run() -> None:
    """Ejecuta el pipeline de desempleo desde la fuente real DANE."""
    setup_logging()
    logger.info("── Pipeline DESEMPLEO (GEIH) ──")
    logger.info("Página fuente: %s", GEIH_CONFIG.page_url)

    df = run_geih_pipeline()
    run_all_checks(df)

    logger.info(
        "Desempleo: %d filas, rango: %s → %s",
        len(df), df["date"].min(), df["date"].max(),
    )
