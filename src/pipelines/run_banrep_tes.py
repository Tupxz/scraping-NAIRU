"""Pipeline de TES Cero Cupón (BANREP/SUAMECA).

Orquesta: sesión → extracción diaria → agregación mensual → validación → guardado.
"""

from __future__ import annotations

import logging

from src.config import BANREP_TES_CONFIG
from src.io_utils import setup_logging
from src.quality_checks import run_banrep_tes_checks
from src.sources.banrep.tes import run_banrep_tes_pipeline

logger = logging.getLogger("nairu_pipeline")


def run() -> None:
    """Ejecuta el pipeline de TES BANREP."""
    setup_logging()
    logger.info("── Pipeline TES CERO CUPÓN (BANREP/SUAMECA) ──")
    logger.info("Endpoint: %s", BANREP_TES_CONFIG.endpoint_url)

    df = run_banrep_tes_pipeline()
    run_banrep_tes_checks(df)

    logger.info(
        "TES BANREP: %d filas, rango: %s → %s",
        len(df), df["date"].min(), df["date"].max(),
    )
