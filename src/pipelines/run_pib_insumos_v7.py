"""Pipeline de insumos trimestrales del PIB para v7 (DANE): X_vapg y BR_tms."""

from __future__ import annotations

import logging

import pandas as pd

from src.io_utils import setup_logging
from src.quality_checks import run_pib_insumos_v7_checks
from src.sources.dane.gdp_insumos_v7 import construir_pib_insumos_v7, guardar_pib_insumos_v7

logger = logging.getLogger("nairu_pipeline")


def run() -> pd.DataFrame:
    setup_logging()
    logger.info("── Pipeline INSUMOS PIB v7 (VA petróleo y gas, impuestos menos subvenciones) ──")
    df = construir_pib_insumos_v7()
    run_pib_insumos_v7_checks(df)
    guardar_pib_insumos_v7(df)
    return df
