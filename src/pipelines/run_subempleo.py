"""Pipeline de subempleo GEIH (anexo principal, series brutas) para v7.

Escribe ``data/processed/geih_subempleo_bruto_mensual.csv`` solo si pasan
las validaciones.
"""

from __future__ import annotations

import logging

import pandas as pd

from src.io_utils import setup_logging
from src.quality_checks import run_subempleo_checks
from src.sources.dane.geih_subempleo import construir_subempleo, guardar_subempleo

logger = logging.getLogger("nairu_pipeline")


def run() -> pd.DataFrame:
    setup_logging()
    logger.info("── Pipeline SUBEMPLEO GEIH (anexo principal) ──")
    df = construir_subempleo()
    run_subempleo_checks(df)
    guardar_subempleo(df)
    return df
