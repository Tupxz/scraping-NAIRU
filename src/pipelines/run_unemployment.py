"""Pipeline laboral GEIH (DANE desestacionalizado).

Orquesta: scraping → descarga → parsing → validación → guardado.
Series extraídas: Tasa de Desocupación (TD), Tasa Global de Participación
(TGP), Tasa de Ocupación (TO) y Población en Edad de Trabajar (PET, si está
disponible en el Excel).  Además escribe ``geih_tgp_to_td_desest_mensual.csv``
para v7 (``src/sources/dane/geih_tasas.py``).
"""

from __future__ import annotations

import logging

from src.config import GEIH_CONFIG
from src.io_utils import setup_logging
from src.quality_checks import run_labor_checks, run_tasas_geih_checks
from src.sources.dane.geih_tasas import construir_tasas_desest, guardar_tasas
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

    # Tasas desestacionalizadas para v7 (TGP, TO, TD; jul–ago 2006 en NaN).
    tasas = construir_tasas_desest(df, GEIH_CONFIG)
    run_tasas_geih_checks(tasas, GEIH_CONFIG.meses_empalme_nan)
    guardar_tasas(tasas, config=GEIH_CONFIG)

    series_disponibles = [c for c in ("unemployment_rate", "tgp_rate", "pet_thousands")
                          if c in df.columns]
    logger.info(
        "GEIH laboral: %d filas, rango: %s → %s, series: %s",
        len(df), df["date"].min(), df["date"].max(), series_disponibles,
    )
