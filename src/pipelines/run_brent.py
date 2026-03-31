"""Pipeline de Brent (FRED/EIA).

Orquesta: descarga CSV → parsing → agregación mensual → validación → guardado.
"""

from __future__ import annotations

import logging

from src.config import BRENT_CONFIG
from src.io_utils import setup_logging
from src.quality_checks import run_brent_checks
from src.sources.fred.brent import run_brent_pipeline

logger = logging.getLogger("nairu_pipeline")


def run() -> None:
    """Ejecuta el pipeline de precios del Brent."""
    setup_logging()
    logger.info("── Pipeline BRENT (FRED/EIA) ──")
    logger.info("Serie: %s", BRENT_CONFIG.series_id)
    logger.info("Fuente: %s", BRENT_CONFIG.source_base_url)

    df = run_brent_pipeline()
    run_brent_checks(df)

    logger.info(
        "Brent: %d filas, rango: %s → %s",
        len(df), df["date"].min(), df["date"].max(),
    )
