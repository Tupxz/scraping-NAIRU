"""Pipeline orquestador: Inversión trimestral desestacionalizada del DANE.

Ejecuta el ciclo completo:
  scraping → descarga anex-GastoConstantes → parsing 'Cuadro 2'
  → validación → guardado en data/processed/dane_gdp_expenditure_colombia.csv

Indicador: Formación bruta de capital fijo (FBKF) a precios constantes
desestacionalizada, en miles de millones de pesos (base 2015).
Frecuencia: trimestral (Q1→Ene, Q2→Abr, Q3→Jul, Q4→Oct).
"""

from __future__ import annotations

import logging

from src.config import (
    DANE_GDP_EXPENDITURE_CONFIG,
    DANE_GDP_EXPENDITURE_PROCESSED_COLUMNS,
    PROCESSED_DIR,
    RAW_DANE_DIR,
)
from src.quality_checks import check_no_nulls_generic
from src.sources.dane.gdp_expenditure import run_dane_gdp_expenditure_pipeline

logger = logging.getLogger("nairu_pipeline.pipelines.dane_gdp_expenditure")


def run() -> None:
    """Ejecuta el pipeline de Inversión (FBKF) trimestral del DANE."""
    logger.info("══ Pipeline Inversión DANE (Cuentas Nacionales — Gasto) ══")

    df = run_dane_gdp_expenditure_pipeline(
        config=DANE_GDP_EXPENDITURE_CONFIG,
        raw_dir=RAW_DANE_DIR,
        processed_dir=PROCESSED_DIR,
    )

    check_no_nulls_generic(df, critical_cols=["date", "investment"])

    missing = set(DANE_GDP_EXPENDITURE_PROCESSED_COLUMNS) - set(df.columns)
    if missing:
        logger.warning("Columnas esperadas faltantes: %s", missing)

    inv = df["investment"]
    out_of_range = inv[inv < 0]
    if not out_of_range.empty:
        logger.warning("Valores negativos inesperados en investment: %s", out_of_range.tolist())

    logger.info(
        "Pipeline Inversión DANE completado: %d trimestres, %s – %s",
        len(df),
        df["date"].min().strftime("%Y-%m"),
        df["date"].max().strftime("%Y-%m"),
    )
