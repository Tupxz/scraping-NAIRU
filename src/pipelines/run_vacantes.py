"""Pipeline de vacantes en prensa (Banrep / SUAMECA) para v7."""

from __future__ import annotations

import logging

import pandas as pd

from src.io_utils import setup_logging
from src.quality_checks import run_vacantes_checks
from src.sources.banrep.vacantes import construir_vacantes, guardar_vacantes

logger = logging.getLogger("nairu_pipeline")


def run() -> pd.DataFrame:
    setup_logging()
    logger.info("── Pipeline VACANTES en prensa (Banrep / SUAMECA) ──")
    df = construir_vacantes()
    run_vacantes_checks(df)
    guardar_vacantes(df)
    return df
