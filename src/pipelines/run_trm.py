"""Pipeline de la TRM (SFC vía datos.gov.co).

Orquesta: descarga paginada → parsing → promedio mensual → validación →
guardado (``data/processed/`` + copia en ``legacy/v7/``).  El CSV
procesado solo se escribe si pasan las validaciones de calidad.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from src.config import PROCESSED_DIR, PROJECT_ROOT, RAW_SFC_DIR, TRM_CONFIG, TRMConfig
from src.io_utils import setup_logging
from src.quality_checks import run_trm_checks
from src.sources.sfc.trm import Obtenedor, construir_trm_mensual, guardar_trm

logger = logging.getLogger("nairu_pipeline")


def run(
    config: TRMConfig = TRM_CONFIG,
    output_dir: Path = PROCESSED_DIR,
    raw_dir: Path | None = RAW_SFC_DIR,
    project_root: Path | None = PROJECT_ROOT,
    obtener: Obtenedor | None = None,
    hoy: date | None = None,
) -> pd.DataFrame:
    """Ejecuta el pipeline de la TRM promedio mensual."""
    setup_logging()
    logger.info("── Pipeline TRM (SFC / datos.gov.co) ──")
    logger.info("Fuente: %s", config.endpoint_url)
    logger.info("Ponderación del promedio mensual: %s", config.ponderacion)

    df = construir_trm_mensual(config, raw_dir=raw_dir, obtener=obtener, hoy=hoy)
    run_trm_checks(df, start_date=config.start_date)
    guardar_trm(df, output_dir=output_dir, project_root=project_root, config=config)

    logger.info(
        "TRM: %d filas, rango: %s → %s",
        len(df), df["date"].min().date(), df["date"].max().date(),
    )
    return df
