"""Pipeline orquestador: PIB trimestral desestacionalizado del DANE.

Ejecuta el ciclo completo:
  scraping → descarga anex-ProduccionConstantes → parsing 'Cuadro 4'
  → validación → guardado en data/processed/dane_gdp_colombia.csv

Frecuencia de salida: trimestral (asignada al primer mes del trimestre:
Q1→Ene, Q2→Abr, Q3→Jul, Q4→Oct). Cobertura típica: 2005-Q1 en adelante.
Indicador: PIB total a precios constantes desestacionalizado, en miles
de millones de pesos.

Este pipeline alimenta dos consumidores:
  1. ``data/inputs/PIB_CO.xlsx`` — usuario combina manualmente este
     output con el PIB potencial estimado por función de producción
     (input externo del compañero).
  2. ``run_viog`` — futuras versiones del VIOG-Colombia podrán leer
     directamente este CSV en lugar del Excel manual.
"""

from __future__ import annotations

import logging

from src.config import (
    DANE_GDP_CONFIG,
    DANE_GDP_MAX,
    DANE_GDP_MIN,
    DANE_GDP_PROCESSED_COLUMNS,
    PROCESSED_DIR,
    RAW_DANE_DIR,
)
from src.quality_checks import check_no_nulls_generic
from src.sources.dane.gdp import run_dane_gdp_pipeline

logger = logging.getLogger("nairu_pipeline.pipelines.dane_gdp")


def run() -> None:
    """Ejecuta el pipeline de PIB trimestral del DANE."""
    logger.info("══ Pipeline PIB DANE (Cuentas Nacionales Trimestrales) ══")

    df = run_dane_gdp_pipeline(
        config=DANE_GDP_CONFIG,
        raw_dir=RAW_DANE_DIR,
        processed_dir=PROCESSED_DIR,
    )

    # Validaciones básicas de calidad
    check_no_nulls_generic(df, critical_cols=["date", "gdp_observed"])

    # Verificar que las columnas estándar están presentes
    missing = set(DANE_GDP_PROCESSED_COLUMNS) - set(df.columns)
    if missing:
        logger.warning("Columnas esperadas faltantes: %s", missing)

    # Sanity check de rango (PIB no puede ser negativo ni absurdamente grande)
    gdp = df["gdp_observed"]
    out_of_range = gdp[(gdp < DANE_GDP_MIN) | (gdp > DANE_GDP_MAX)]
    if not out_of_range.empty:
        logger.warning(
            "Valores fuera de rango [%.1f, %.1f] en gdp_observed: %s",
            DANE_GDP_MIN, DANE_GDP_MAX, out_of_range.tolist(),
        )

    logger.info(
        "Pipeline PIB DANE completado: %d trimestres, %s – %s",
        len(df),
        df["date"].min().strftime("%Y-%m"),
        df["date"].max().strftime("%Y-%m"),
    )
