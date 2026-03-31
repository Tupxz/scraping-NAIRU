"""Pipeline ANDI EOIC — Capacidad Instalada.

Orquesta: scraping → descarga PDF → extracción → validación → guardado.
"""

from __future__ import annotations

import logging

from src.io_utils import setup_logging
from src.quality_checks import run_andi_checks
from src.sources.andi.eoic import run_andi_pipeline

logger = logging.getLogger("nairu_pipeline")


def run(*, backfill: bool = False) -> None:
    """Ejecuta el pipeline ANDI EOIC.

    Parameters
    ----------
    backfill : bool
        Si ``True``, procesa todos los PDFs disponibles.
    """
    setup_logging()
    logger.info("── Pipeline ANDI (EOIC — Capacidad Instalada) ──")
    mode = "backfill" if backfill else "incremental"
    logger.info("Modo: %s", mode)

    df = run_andi_pipeline(backfill=backfill)

    if not df.empty:
        run_andi_checks(df)

    logger.info(
        "ANDI: %d filas, rango: %s → %s",
        len(df),
        df["date"].min() if not df.empty else "N/A",
        df["date"].max() if not df.empty else "N/A",
    )
