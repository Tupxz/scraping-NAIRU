"""Validaciones de calidad para el pipeline.

Contiene funciones que verifican la integridad, completitud y
consistencia del dataset procesado.
"""

from __future__ import annotations

import logging

import pandas as pd

from src.config import (
    PROCESSED_COLUMNS,
    UNEMPLOYMENT_RATE_MAX,
    UNEMPLOYMENT_RATE_MIN,
)

logger = logging.getLogger("nairu_pipeline.quality")


class QualityCheckError(Exception):
    """Error lanzado cuando una validación de calidad falla."""


def check_columns(df: pd.DataFrame) -> None:
    """Verifica que el DataFrame tenga las columnas esperadas.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a validar.

    Raises
    ------
    QualityCheckError
        Si faltan columnas o hay columnas inesperadas.
    """
    expected = set(PROCESSED_COLUMNS)
    actual = set(df.columns)

    missing = expected - actual
    extra = actual - expected

    if missing:
        raise QualityCheckError(f"Columnas faltantes: {missing}")
    if extra:
        logger.warning("Columnas adicionales no esperadas: %s", extra)

    logger.info("✓ Validación de columnas: OK")


def check_no_nulls(df: pd.DataFrame) -> None:
    """Verifica que no haya valores nulos en columnas críticas.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a validar.

    Raises
    ------
    QualityCheckError
        Si hay valores nulos en columnas críticas.
    """
    critical_cols = ["date", "unemployment_rate", "year", "month"]
    null_counts = df[critical_cols].isnull().sum()
    cols_with_nulls = null_counts[null_counts > 0]

    if not cols_with_nulls.empty:
        raise QualityCheckError(
            f"Valores nulos en columnas críticas:\n{cols_with_nulls}"
        )

    logger.info("✓ Validación de nulos: OK")


def check_unemployment_rate_range(df: pd.DataFrame) -> None:
    """Verifica que la tasa de desempleo esté en un rango razonable.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a validar.

    Raises
    ------
    QualityCheckError
        Si hay valores fuera de rango.
    """
    rate = df["unemployment_rate"]
    out_of_range = rate[
        (rate < UNEMPLOYMENT_RATE_MIN) | (rate > UNEMPLOYMENT_RATE_MAX)
    ]

    if not out_of_range.empty:
        raise QualityCheckError(
            f"Valores de desempleo fuera de rango "
            f"[{UNEMPLOYMENT_RATE_MIN}, {UNEMPLOYMENT_RATE_MAX}]: "
            f"{out_of_range.values[:5]}..."
        )

    logger.info(
        "✓ Validación de rango [%.1f, %.1f]: OK",
        UNEMPLOYMENT_RATE_MIN,
        UNEMPLOYMENT_RATE_MAX,
    )


def check_no_duplicates(df: pd.DataFrame) -> None:
    """Verifica que no haya fechas duplicadas.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a validar.

    Raises
    ------
    QualityCheckError
        Si hay filas duplicadas por fecha.
    """
    duplicates = df[df.duplicated(subset=["date"], keep=False)]

    if not duplicates.empty:
        raise QualityCheckError(
            f"Fechas duplicadas encontradas: {duplicates['date'].unique()[:5]}"
        )

    logger.info("✓ Validación de duplicados: OK")


def check_date_continuity(df: pd.DataFrame) -> None:
    """Verifica que la serie temporal no tenga brechas grandes.

    Alerta (warning) si faltan más de 2 meses consecutivos, pero
    no lanza error ya que pueden existir brechas legítimas.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a validar.
    """
    dates = pd.to_datetime(df["date"]).sort_values()
    if len(dates) < 2:
        logger.warning("Menos de 2 fechas — no se puede verificar continuidad")
        return

    diffs = dates.diff().dt.days
    large_gaps = diffs[diffs > 62]  # Más de ~2 meses

    if not large_gaps.empty:
        logger.warning(
            "⚠ Se detectaron %d brechas temporales mayores a 2 meses",
            len(large_gaps),
        )
    else:
        logger.info("✓ Validación de continuidad temporal: OK")


def run_all_checks(df: pd.DataFrame) -> bool:
    """Ejecuta todas las validaciones de calidad.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame procesado a validar.

    Returns
    -------
    bool
        True si todas las validaciones pasan.

    Raises
    ------
    QualityCheckError
        Si alguna validación crítica falla.
    """
    logger.info("─── Iniciando validaciones de calidad ───")
    logger.info("Dataset: %d filas × %d columnas", *df.shape)

    check_columns(df)
    check_no_nulls(df)
    check_unemployment_rate_range(df)
    check_no_duplicates(df)
    check_date_continuity(df)

    logger.info("─── Todas las validaciones pasaron ✓ ───")
    return True
