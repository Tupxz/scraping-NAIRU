"""Pipeline de IPC (DANE real).

Orquesta: scraping → descarga → parsing → validación → guardado.
"""

from __future__ import annotations

import logging

from src.config import IPC_CONFIG
from src.io_utils import setup_logging
from src.quality_checks import run_ipc_checks
from src.sources.dane.ipc import run_ipc_pipeline

logger = logging.getLogger("nairu_pipeline")


def run() -> None:
    """Ejecuta el pipeline de IPC."""
    setup_logging()
    logger.info("── Pipeline IPC ──")
    logger.info("Página fuente: %s", IPC_CONFIG.page_url)

    df_ipc = run_ipc_pipeline()
    run_ipc_checks(df_ipc)

    logger.info(
        "IPC: %d filas, rango: %s → %s",
        len(df_ipc),
        df_ipc["date"].min(),
        df_ipc["date"].max(),
    )
