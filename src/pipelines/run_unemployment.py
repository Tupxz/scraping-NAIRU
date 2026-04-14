"""Pipeline laboral GEIH (DANE desestacionalizado).

Orquesta: scraping → descarga → parsing → validación → guardado.
Series extraídas: Tasa de Desocupación (TD), Tasa Global de Participación
(TGP) y Población en Edad de Trabajar (PET, si está disponible en el Excel).
"""

from __future__ import annotations

import logging

from src.config import GEIH_CONFIG
from src.io_utils import setup_logging
from src.quality_checks import run_labor_checks
from src.sources.dane.unemployment import run_geih_pipeline

logger = logging.getLogger("nairu_pipeline")


def run() -> None:
    """Ejecuta el pipeline laboral GEIH (TD, TGP, PET) desde la fuente real DANE."""
    setup_logging()
    logger.info("── Pipeline LABORAL GEIH (DANE) ──")
    logger.info("Series: Desempleo (TD), TGP, PET (si disponible)")
    logger.info("Página fuente: %s", GEIH_CONFIG.page_url)

    df = run_geih_pipeline()
    run_labor_checks(df)

    series_disponibles = [c for c in ("unemployment_rate", "tgp_rate", "pet_thousands")
                          if c in df.columns]
    logger.info(
        "GEIH laboral: %d filas, rango: %s → %s, series: %s",
        len(df), df["date"].min(), df["date"].max(), series_disponibles,
    )
