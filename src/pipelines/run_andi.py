"""Pipeline ANDI EOIC — Capacidad Instalada (ICU).

Orquesta: página de la ANDI → informes de meses nuevos → extracción →
controles → validación de calidad → guardado.  Si algo falla se lanza
error y no se escribe nada (el workflow conserva el CSV cacheado).
"""

from __future__ import annotations

import logging

import pandas as pd

from src.io_utils import setup_logging
from src.quality_checks import run_andi_checks
from src.sources.andi.eoic import RespaldoICU, guardar_icu, run_andi_pipeline

logger = logging.getLogger("nairu_pipeline")


def run(*, backfill: bool = False, respaldo_ia: RespaldoICU | None = None) -> pd.DataFrame:
    """Ejecuta el pipeline ANDI EOIC.

    Parameters
    ----------
    backfill : bool
        Si ``True``, vuelve a extraer los meses nuevos ya agregados (el
        histórico de la ANDI nunca se toca).
    respaldo_ia : callable, opcional
        Respaldo para meses sin frase en el informe (``--andi-ia``, solo local).
    """
    setup_logging()
    logger.info("── Pipeline ANDI (EOIC — Capacidad Instalada) ──")
    logger.info("Modo: %s%s", "backfill" if backfill else "incremental",
                " + respaldo IA" if respaldo_ia else "")

    df = run_andi_pipeline(backfill=backfill, respaldo_ia=respaldo_ia)
    run_andi_checks(df)
    guardar_icu(df)

    logger.info(
        "ANDI: %d filas, rango: %s → %s",
        len(df), pd.Timestamp(df["date"].min()).date(), pd.Timestamp(df["date"].max()).date(),
    )
    return df
