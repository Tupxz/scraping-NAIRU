"""Pipeline orquestador: PIB trimestral enfoque del ingreso (DANE).

Extrae Remuneración de asalariados, Excedente Bruto de Explotación
e Ingreso Mixto desde ``anex-PIB-EnfoqueCorriente-{trim}{YYYY}.xlsx``.

Notas:
  - Valores en miles de millones de pesos **corrientes** (no deflactados).
  - Cobertura desde 2016-Q1 (base metodológica nueva del DANE).
"""

from __future__ import annotations

import logging

from src.config import (
    DANE_GDP_INCOME_CONFIG,
    DANE_GDP_INCOME_PROCESSED_COLUMNS,
    PROCESSED_DIR,
    RAW_DANE_DIR,
)
from src.quality_checks import check_no_nulls_generic
from src.sources.dane.gdp_income import run_dane_gdp_income_pipeline

logger = logging.getLogger("nairu_pipeline.pipelines.dane_gdp_income")


def run() -> None:
    """Ejecuta el pipeline de PIB enfoque del ingreso del DANE."""
    logger.info("══ Pipeline PIB Ingreso DANE (Remuneración / EBE / Ingreso Mixto) ══")

    df = run_dane_gdp_income_pipeline(
        config=DANE_GDP_INCOME_CONFIG,
        raw_dir=RAW_DANE_DIR,
        processed_dir=PROCESSED_DIR,
    )

    check_no_nulls_generic(df, critical_cols=["date"])

    missing = set(DANE_GDP_INCOME_PROCESSED_COLUMNS) - set(df.columns)
    if missing:
        logger.warning("Columnas esperadas faltantes: %s", missing)

    logger.info(
        "Pipeline PIB Ingreso completado: %d trimestres, %s – %s",
        len(df),
        df["date"].min().strftime("%Y-%m"),
        df["date"].max().strftime("%Y-%m"),
    )
