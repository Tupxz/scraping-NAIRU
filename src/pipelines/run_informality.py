"""Pipeline orquestador: informalidad laboral DANE (GEIH-EISS).

Ejecuta el ciclo completo:
  scraping → descarga Excel GEIHEISS → parsing hoja 'Prop informalidad'
  → validación → guardado en data/processed/dane_informality_colombia.csv

Frecuencia de salida: trimestre móvil (asignado al último mes del período).
Cobertura: 2021-03 en adelante (inicio publicación EISS).
Indicador: proporción de informalidad — 13 Ciudades y A.M.
"""

from __future__ import annotations

import logging

from src.config import (
    GEIH_INFORMALITY_CONFIG,
    INFORMALITY_PROCESSED_COLUMNS,
    INFORMALITY_RATE_MAX,
    INFORMALITY_RATE_MIN,
    PROCESSED_DIR,
    RAW_DANE_DIR,
)
from src.quality_checks import check_no_nulls_generic
from src.sources.dane.informality import run_informality_pipeline

logger = logging.getLogger("nairu_pipeline.pipelines.informality")


def run() -> None:
    """Ejecuta el pipeline de informalidad laboral (DANE GEIH-EISS)."""
    logger.info("══ Pipeline informalidad DANE ══")

    df = run_informality_pipeline(
        config=GEIH_INFORMALITY_CONFIG,
        raw_dir=RAW_DANE_DIR,
        processed_dir=PROCESSED_DIR,
    )

    # Validaciones básicas de calidad
    check_no_nulls_generic(df, critical_cols=["date", "informality_rate_13c"])

    rate = df["informality_rate_13c"]
    out_of_range = rate[(rate < INFORMALITY_RATE_MIN) | (rate > INFORMALITY_RATE_MAX)]
    if not out_of_range.empty:
        logger.warning(
            "Valores fuera de rango [%.1f, %.1f] en informality_rate_13c: %s",
            INFORMALITY_RATE_MIN, INFORMALITY_RATE_MAX, out_of_range.tolist(),
        )

    logger.info(
        "Pipeline informalidad completado: %d observaciones, %s – %s",
        len(df),
        df["date"].min().strftime("%Y-%m"),
        df["date"].max().strftime("%Y-%m"),
    )
