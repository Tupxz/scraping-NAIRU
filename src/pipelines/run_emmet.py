"""Pipeline EMMET (DANE): salario nominal por ocupado de la industria.

Descarga el anexo Total Nacional vigente, lo combina con el IPC total de
``data/processed/ipc_colombia.csv``, valida y recién entonces escribe
``data/processed/emmet_salarios_v7.csv``.
"""

from __future__ import annotations

import logging

import pandas as pd

from src.config import EMMET_CONFIG
from src.io_utils import setup_logging
from src.quality_checks import run_emmet_checks
from src.sources.dane.emmet import construir_emmet, guardar_emmet

logger = logging.getLogger("nairu_pipeline")


def run() -> pd.DataFrame:
    setup_logging()
    logger.info("── Pipeline EMMET (DANE, salarios de la industria) ──")
    df = construir_emmet()
    run_emmet_checks(df, EMMET_CONFIG.start_date)
    guardar_emmet(df)
    return df
