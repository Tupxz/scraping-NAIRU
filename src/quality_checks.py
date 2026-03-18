"""Validaciones de calidad para el pipeline.

Contiene funciones que verifican la integridad, completitud y
consistencia del dataset procesado (desempleo e IPC).
"""

from __future__ import annotations

import logging

import pandas as pd

from src.config import (
    IPC_INDEX_MAX,
    IPC_INDEX_MIN,
    IPC_PROCESSED_COLUMNS,
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


# ═══════════════════════════════════════════════════════════════════════
# Validaciones de calidad para IPC
# ═══════════════════════════════════════════════════════════════════════


def check_ipc_columns(df: pd.DataFrame) -> None:
    """Verifica columnas esperadas del dataset IPC."""
    expected = set(IPC_PROCESSED_COLUMNS)
    actual = set(df.columns)
    missing = expected - actual
    if missing:
        raise QualityCheckError(f"Columnas IPC faltantes: {missing}")
    logger.info("✓ Validación de columnas IPC: OK")


def check_ipc_index_range(df: pd.DataFrame) -> None:
    """Verifica que el índice IPC esté en un rango razonable."""
    idx = df["ipc_index"]
    out_of_range = idx[
        (idx < IPC_INDEX_MIN) | (idx > IPC_INDEX_MAX)
    ]
    if not out_of_range.empty:
        raise QualityCheckError(
            f"Valores IPC fuera de rango [{IPC_INDEX_MIN}, {IPC_INDEX_MAX}]: "
            f"{out_of_range.values[:5]}..."
        )
    logger.info(
        "✓ Validación de rango IPC [%.1f, %.1f]: OK",
        IPC_INDEX_MIN, IPC_INDEX_MAX,
    )


def check_ipc_monotonic(df: pd.DataFrame, tolerance: float = 0.15) -> None:
    """Verifica que el IPC sea generalmente creciente (inflación positiva).

    Permite caídas mensuales de hasta `tolerance` (15%) para capturar
    deflación puntual, pero alerta si hay caídas mayores.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame IPC procesado y ordenado por fecha.
    tolerance : float
        Fracción máxima de caída mensual permitida sin alerta.
    """
    df_sorted = df.sort_values("date")
    pct_change = df_sorted["ipc_index"].pct_change()
    large_drops = pct_change[pct_change < -tolerance]

    if not large_drops.empty:
        logger.warning(
            "⚠ Se detectaron %d caídas del IPC mayores al %.0f%%",
            len(large_drops), tolerance * 100,
        )
    else:
        logger.info("✓ Validación de tendencia IPC: OK")


def run_ipc_checks(df: pd.DataFrame) -> bool:
    """Ejecuta todas las validaciones de calidad para IPC.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame IPC procesado.

    Returns
    -------
    bool
        True si todas las validaciones pasan.
    """
    logger.info("─── Validaciones de calidad IPC ───")
    logger.info("Dataset: %d filas × %d columnas", *df.shape)

    check_ipc_columns(df)
    check_no_nulls_generic(df, ["date", "ipc_index", "year", "month"])
    check_ipc_index_range(df)
    check_no_duplicates(df)
    check_date_continuity(df)
    check_ipc_monotonic(df)

    logger.info("─── Todas las validaciones IPC pasaron ✓ ───")
    return True


def check_no_nulls_generic(
    df: pd.DataFrame, critical_cols: list[str]
) -> None:
    """Verifica nulos en una lista arbitraria de columnas críticas."""
    null_counts = df[critical_cols].isnull().sum()
    cols_with_nulls = null_counts[null_counts > 0]
    if not cols_with_nulls.empty:
        raise QualityCheckError(
            f"Valores nulos en columnas críticas:\n{cols_with_nulls}"
        )
    logger.info("✓ Validación de nulos: OK")
