"""Pipeline de inflación (BANREP/SUAMECA).

Orquesta: sesión → extracción API → parsing → validación → guardado.
"""

from __future__ import annotations

import logging

from src.config import BANREP_INFLATION_CONFIG
from src.io_utils import setup_logging
from src.quality_checks import run_banrep_checks
from src.sources.banrep.inflation import run_banrep_inflation_pipeline

logger = logging.getLogger("nairu_pipeline")


def run() -> None:
    """Ejecuta el pipeline de inflación BANREP."""
    setup_logging()
    logger.info("── Pipeline INFLACIÓN (BANREP/SUAMECA) ──")
    logger.info("Endpoint: %s", BANREP_INFLATION_CONFIG.endpoint_url)

    df = run_banrep_inflation_pipeline()
    run_banrep_checks(df)

    logger.info(
        "Inflación BANREP: %d filas, rango: %s → %s",
        len(df), df["date"].min(), df["date"].max(),
    )
