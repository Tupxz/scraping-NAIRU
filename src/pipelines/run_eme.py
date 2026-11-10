"""Pipeline de la EME (Banrep): expectativas de inflación de analistas.

Descarga el Excel de series históricas de SUAMECA, valida y recién entonces
escribe ``data/processed/eme_expectativas_inflacion_total_mensual_promedio.csv``.
"""

from __future__ import annotations

import logging

import pandas as pd

from src.config import BANREP_EME_CONFIG
from src.io_utils import setup_logging
from src.quality_checks import run_eme_checks
from src.sources.banrep.eme import construir_eme, guardar_eme

logger = logging.getLogger("nairu_pipeline")


def run() -> pd.DataFrame:
    setup_logging()
    logger.info("── Pipeline EME (Banrep / SUAMECA) ──")
    df = construir_eme()
    run_eme_checks(df, BANREP_EME_CONFIG.start_date)
    guardar_eme(df)
    logger.info("EME: %d meses, %s → %s", len(df), df["date"].min().date(), df["date"].max().date())
    return df
