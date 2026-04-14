"""Pipeline PWT 11.0 (Penn World Tables).

Orquesta: localizar/descargar CSV → parsing → guardado → validación de calidad.

Soporta dos formatos de entrada:
- Formato wide: exportación CSV de https://pwt-data-tool.streamlit.app/
- Formato largo: descarga clásica del Dataverse (requiere autenticación)
"""

from __future__ import annotations

import logging

from src.config import PWT_CONFIG
from src.io_utils import setup_logging
from src.quality_checks import run_pwt_checks
from src.sources.pwt.pwt import run_pwt_pipeline

logger = logging.getLogger("nairu_pipeline")


def run() -> None:
    """Ejecuta el pipeline de Stock de Capital y Capital Humano (PWT 11.0)."""
    setup_logging()
    logger.info("── Pipeline PWT 11.0 (Penn World Tables) ──")
    logger.info("País: %s", PWT_CONFIG.country_code)
    logger.info("Fuente: %s", PWT_CONFIG.source_url)

    df = run_pwt_pipeline()
    run_pwt_checks(df)

    logger.info(
        "PWT: %d filas, rango: %s → %s",
        len(df), df["date"].min(), df["date"].max(),
    )
