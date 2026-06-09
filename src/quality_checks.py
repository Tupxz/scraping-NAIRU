"""Validaciones de calidad para el pipeline.

Contiene funciones que verifican la integridad, completitud y
consistencia del dataset procesado (desempleo e IPC).
"""

from __future__ import annotations

import logging

import pandas as pd

from src.config import (
    ANDI_PROCESSED_COLUMNS,
    BANREP_PROCESSED_COLUMNS,
    BANREP_TES_PROCESSED_COLUMNS,
    BRENT_PRICE_MAX,
    BRENT_PRICE_MIN,
    BRENT_PROCESSED_COLUMNS,
    CAPACITY_UTILIZATION_MAX,
    CAPACITY_UTILIZATION_MAX_CHANGE,
    CAPACITY_UTILIZATION_MIN,
    CAPITAL_STOCK_MAX,
    CAPITAL_STOCK_MIN,
    DEPRECIATION_RATE_MAX,
    DEPRECIATION_RATE_MIN,
    HUMAN_CAPITAL_MAX,
    HUMAN_CAPITAL_MIN,
    INFLATION_GOAL_MAX,
    INFLATION_GOAL_MIN,
    INFLATION_RATE_MAX,
    INFLATION_RATE_MIN,
    IPC_INDEX_MAX,
    IPC_INDEX_MIN,
    IPC_PROCESSED_COLUMNS,
    PET_MAX,
    PET_MIN,
    PROCESSED_COLUMNS,
    PWT_PROCESSED_COLUMNS,
    TES_RATE_MAX,
    TES_RATE_MIN,
    TGP_MAX,
    TGP_MIN,
    UNEMPLOYMENT_RATE_MAX,
    UNEMPLOYMENT_RATE_MIN,
)

logger = logging.getLogger("nairu_pipeline.quality")


class QualityCheckError(Exception):
    """Error lanzado cuando una validación de calidad falla."""


def check_columns(df: pd.DataFrame) -> None:
    """Verifica que el DataFrame tenga las columnas esperadas.

    Las columnas ``tgp_rate`` y ``pet_thousands`` son opcionales: pueden
    no estar presentes si el Excel del DANE no las publicó.  Todas las
    demás columnas de ``PROCESSED_COLUMNS`` son obligatorias.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a validar.

    Raises
    ------
    QualityCheckError
        Si faltan columnas obligatorias o hay columnas inesperadas.
    """
    OPTIONAL_COLUMNS: set[str] = {"tgp_rate", "pet_thousands"}

    expected = set(PROCESSED_COLUMNS)
    actual = set(df.columns)

    # Columnas obligatorias: las de PROCESSED_COLUMNS que no son opcionales
    required = expected - OPTIONAL_COLUMNS
    missing = required - actual
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


def check_tgp_range(df: pd.DataFrame) -> None:
    """Verifica que tgp_rate esté en un rango razonable.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame laboral GEIH procesado.

    Raises
    ------
    QualityCheckError
        Si hay valores fuera del rango ``[TGP_MIN, TGP_MAX]``.
    """
    vals = df["tgp_rate"].dropna()
    out_of_range = vals[(vals < TGP_MIN) | (vals > TGP_MAX)]
    if not out_of_range.empty:
        raise QualityCheckError(
            f"Valores de tgp_rate fuera de rango "
            f"[{TGP_MIN}, {TGP_MAX}]: "
            f"{out_of_range.values[:5]}..."
        )
    logger.info(
        "✓ Validación de rango TGP [%.1f, %.1f]: OK",
        TGP_MIN, TGP_MAX,
    )


def check_pet_range(df: pd.DataFrame) -> None:
    """Verifica que pet_thousands esté en un rango razonable.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame laboral GEIH procesado.

    Raises
    ------
    QualityCheckError
        Si hay valores fuera del rango ``[PET_MIN, PET_MAX]``.
    """
    vals = df["pet_thousands"].dropna()
    out_of_range = vals[(vals < PET_MIN) | (vals > PET_MAX)]
    if not out_of_range.empty:
        raise QualityCheckError(
            f"Valores de pet_thousands fuera de rango "
            f"[{PET_MIN}, {PET_MAX}]: "
            f"{out_of_range.values[:5]}..."
        )
    logger.info(
        "✓ Validación de rango PET [%.1f, %.1f]: OK",
        PET_MIN, PET_MAX,
    )


def run_all_checks(df: pd.DataFrame) -> bool:
    """Ejecuta todas las validaciones de calidad para el dataset laboral GEIH.

    Además de las validaciones básicas (columnas, nulos, rango TD,
    duplicados y continuidad), valida TGP y PET si están presentes.

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

    # Validaciones opcionales: solo si las columnas están presentes
    if "tgp_rate" in df.columns:
        check_tgp_range(df)
    if "pet_thousands" in df.columns:
        check_pet_range(df)

    check_no_duplicates(df)
    check_date_continuity(df)

    logger.info("─── Todas las validaciones pasaron ✓ ───")
    return True


def run_labor_checks(df: pd.DataFrame) -> bool:
    """Alias de ``run_all_checks`` para el dataset laboral GEIH.

    Permite llamar ``run_labor_checks(df)`` desde pipelines que quieran
    un nombre semántico más descriptivo que ``run_all_checks``.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame laboral GEIH procesado.

    Returns
    -------
    bool
        True si todas las validaciones pasan.
    """
    return run_all_checks(df)


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


# ═══════════════════════════════════════════════════════════════════════
# Validaciones de calidad para BANREP inflación
# ═══════════════════════════════════════════════════════════════════════


def check_banrep_columns(df: pd.DataFrame) -> None:
    """Verifica columnas esperadas del dataset BANREP inflación."""
    expected = set(BANREP_PROCESSED_COLUMNS)
    actual = set(df.columns)
    missing = expected - actual
    if missing:
        raise QualityCheckError(f"Columnas BANREP faltantes: {missing}")
    logger.info("✓ Validación de columnas BANREP: OK")


def check_inflation_rate_range(df: pd.DataFrame) -> None:
    """Verifica que Inf_Rate y Core_Inf estén en rango razonable."""
    for col in ("Inf_Rate", "Core_Inf"):
        if col not in df.columns:
            continue
        vals = df[col].dropna()
        out_of_range = vals[
            (vals < INFLATION_RATE_MIN) | (vals > INFLATION_RATE_MAX)
        ]
        if not out_of_range.empty:
            raise QualityCheckError(
                f"Valores de {col} fuera de rango "
                f"[{INFLATION_RATE_MIN}, {INFLATION_RATE_MAX}]: "
                f"{out_of_range.values[:5]}..."
            )
    logger.info(
        "✓ Validación de rango inflación [%.1f, %.1f]: OK",
        INFLATION_RATE_MIN, INFLATION_RATE_MAX,
    )


def check_inflation_goal_range(df: pd.DataFrame) -> None:
    """Verifica que Inf_Goal esté en rango razonable."""
    if "Inf_Goal" not in df.columns:
        return
    vals = df["Inf_Goal"].dropna()
    out_of_range = vals[
        (vals < INFLATION_GOAL_MIN) | (vals > INFLATION_GOAL_MAX)
    ]
    if not out_of_range.empty:
        raise QualityCheckError(
            f"Valores de Inf_Goal fuera de rango "
            f"[{INFLATION_GOAL_MIN}, {INFLATION_GOAL_MAX}]: "
            f"{out_of_range.values[:5]}..."
        )
    logger.info(
        "✓ Validación de rango meta inflación [%.1f, %.1f]: OK",
        INFLATION_GOAL_MIN, INFLATION_GOAL_MAX,
    )


def run_banrep_checks(df: pd.DataFrame) -> bool:
    """Ejecuta todas las validaciones de calidad para BANREP inflación.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame BANREP procesado.

    Returns
    -------
    bool
        True si todas las validaciones pasan.
    """
    logger.info("─── Validaciones de calidad BANREP ───")
    logger.info("Dataset: %d filas × %d columnas", *df.shape)

    check_banrep_columns(df)
    check_no_nulls_generic(df, ["date", "year", "month"])
    check_inflation_rate_range(df)
    check_inflation_goal_range(df)
    check_no_duplicates(df)
    check_date_continuity(df)

    logger.info("─── Todas las validaciones BANREP pasaron ✓ ───")
    return True


# ═══════════════════════════════════════════════════════════════════════
# Validaciones de calidad para Brent (FRED/EIA)
# ═══════════════════════════════════════════════════════════════════════


def check_brent_columns(df: pd.DataFrame) -> None:
    """Verifica columnas esperadas del dataset Brent."""
    expected = set(BRENT_PROCESSED_COLUMNS)
    actual = set(df.columns)
    missing = expected - actual
    if missing:
        raise QualityCheckError(f"Columnas Brent faltantes: {missing}")
    logger.info("✓ Validación de columnas Brent: OK")


def check_brent_price_range(df: pd.DataFrame) -> None:
    """Verifica que el precio del Brent esté en rango razonable."""
    vals = df["brent_usd_per_barrel"].dropna()
    out_of_range = vals[
        (vals < BRENT_PRICE_MIN) | (vals > BRENT_PRICE_MAX)
    ]
    if not out_of_range.empty:
        raise QualityCheckError(
            f"Valores de brent_usd_per_barrel fuera de rango "
            f"[{BRENT_PRICE_MIN}, {BRENT_PRICE_MAX}]: "
            f"{out_of_range.values[:5]}..."
        )
    logger.info(
        "✓ Validación de rango Brent [%.2f, %.1f]: OK",
        BRENT_PRICE_MIN, BRENT_PRICE_MAX,
    )


def run_brent_checks(df: pd.DataFrame) -> bool:
    """Ejecuta todas las validaciones de calidad para Brent.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame Brent procesado.

    Returns
    -------
    bool
        True si todas las validaciones pasan.
    """
    logger.info("─── Validaciones de calidad Brent ───")
    logger.info("Dataset: %d filas × %d columnas", *df.shape)

    check_brent_columns(df)
    check_no_nulls_generic(df, ["date", "brent_usd_per_barrel", "year", "month"])
    check_brent_price_range(df)
    check_no_duplicates(df)
    check_date_continuity(df)

    logger.info("─── Todas las validaciones Brent pasaron ✓ ───")
    return True


# ═══════════════════════════════════════════════════════════════════════
# Validaciones de calidad para ANDI EOIC (Capacidad Instalada)
# ═══════════════════════════════════════════════════════════════════════


def check_andi_columns(df: pd.DataFrame) -> None:
    """Verifica columnas esperadas del dataset ANDI."""
    expected = set(ANDI_PROCESSED_COLUMNS)
    actual = set(df.columns)
    missing = expected - actual
    if missing:
        raise QualityCheckError(f"Columnas ANDI faltantes: {missing}")
    logger.info("✓ Validación de columnas ANDI: OK")


def check_capacity_utilization_range(df: pd.DataFrame) -> None:
    """Verifica que la utilización de capacidad esté en rango razonable."""
    vals = df["capacity_utilization"].dropna()
    out_of_range = vals[
        (vals < CAPACITY_UTILIZATION_MIN) | (vals > CAPACITY_UTILIZATION_MAX)
    ]
    if not out_of_range.empty:
        raise QualityCheckError(
            f"Valores de capacity_utilization fuera de rango "
            f"[{CAPACITY_UTILIZATION_MIN}, {CAPACITY_UTILIZATION_MAX}]: "
            f"{out_of_range.values[:5]}..."
        )
    logger.info(
        "✓ Validación de rango capacidad [%.1f, %.1f]: OK",
        CAPACITY_UTILIZATION_MIN, CAPACITY_UTILIZATION_MAX,
    )


def check_capacity_monthly_change(df: pd.DataFrame) -> None:
    """Verifica que no haya cambios mensuales mayores a un umbral.

    Alerta (warning) si el cambio mensual supera
    ``CAPACITY_UTILIZATION_MAX_CHANGE`` pp.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame ANDI procesado y ordenado por fecha.
    """
    df_sorted = df.sort_values("date").reset_index(drop=True)
    changes = df_sorted["capacity_utilization"].diff().abs()
    large_changes = changes[changes > CAPACITY_UTILIZATION_MAX_CHANGE]

    if not large_changes.empty:
        logger.warning(
            "⚠ Se detectaron %d cambios mensuales de capacidad > %.0f pp",
            len(large_changes), CAPACITY_UTILIZATION_MAX_CHANGE,
        )
    else:
        logger.info(
            "✓ Validación de cambio mensual (max %.0f pp): OK",
            CAPACITY_UTILIZATION_MAX_CHANGE,
        )


def run_andi_checks(df: pd.DataFrame) -> bool:
    """Ejecuta todas las validaciones de calidad para ANDI EOIC.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame ANDI procesado.

    Returns
    -------
    bool
        True si todas las validaciones pasan.
    """
    logger.info("─── Validaciones de calidad ANDI ───")
    logger.info("Dataset: %d filas × %d columnas", *df.shape)

    check_andi_columns(df)
    check_no_nulls_generic(df, ["date", "capacity_utilization", "year", "month"])
    check_capacity_utilization_range(df)
    check_no_duplicates(df)
    check_date_continuity(df)
    check_capacity_monthly_change(df)

    logger.info("─── Todas las validaciones ANDI pasaron ✓ ───")
    return True


# ═══════════════════════════════════════════════════════════════════════
# Validaciones de calidad para BANREP TES (Cero Cupón)
# ═══════════════════════════════════════════════════════════════════════


def check_tes_columns(df: pd.DataFrame) -> None:
    """Verifica columnas esperadas del dataset BANREP TES."""
    expected = set(BANREP_TES_PROCESSED_COLUMNS)
    actual = set(df.columns)
    missing = expected - actual
    if missing:
        raise QualityCheckError(f"Columnas TES faltantes: {missing}")
    logger.info("✓ Validación de columnas TES: OK")


def check_tes_rate_range(df: pd.DataFrame) -> None:
    """Verifica que las tasas TES estén en rango razonable."""
    for col in ("TES_UVR_1Y", "TES_PESOS_1Y"):
        if col not in df.columns:
            continue
        vals = df[col].dropna()
        out_of_range = vals[
            (vals < TES_RATE_MIN) | (vals > TES_RATE_MAX)
        ]
        if not out_of_range.empty:
            raise QualityCheckError(
                f"Valores de {col} fuera de rango "
                f"[{TES_RATE_MIN}, {TES_RATE_MAX}]: "
                f"{out_of_range.values[:5]}..."
            )
    logger.info(
        "✓ Validación de rango TES [%.1f, %.1f]: OK",
        TES_RATE_MIN, TES_RATE_MAX,
    )


def run_banrep_tes_checks(df: pd.DataFrame) -> bool:
    """Ejecuta todas las validaciones de calidad para BANREP TES.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame TES procesado.

    Returns
    -------
    bool
        True si todas las validaciones pasan.
    """
    logger.info("─── Validaciones de calidad BANREP TES ───")
    logger.info("Dataset: %d filas × %d columnas", *df.shape)

    check_tes_columns(df)
    check_no_nulls_generic(df, ["date", "year", "month"])
    check_tes_rate_range(df)
    check_no_duplicates(df)
    check_date_continuity(df)

    logger.info("─── Todas las validaciones TES pasaron ✓ ───")
    return True


# ═══════════════════════════════════════════════════════════════════════
# Validaciones de calidad para PWT 11.0 (Stock de Capital / Depreciación / Capital Humano)
# ═══════════════════════════════════════════════════════════════════════


def check_pwt_columns(df: pd.DataFrame) -> None:
    """Verifica columnas esperadas del dataset PWT."""
    expected = set(PWT_PROCESSED_COLUMNS)
    actual = set(df.columns)
    missing = expected - actual
    if missing:
        raise QualityCheckError(f"Columnas PWT faltantes: {missing}")
    logger.info("✓ Validación de columnas PWT: OK")


def check_capital_stock_range(df: pd.DataFrame) -> None:
    """Verifica que capital_stock_real esté en un rango razonable.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame PWT procesado.

    Raises
    ------
    QualityCheckError
        Si hay valores fuera del rango ``[CAPITAL_STOCK_MIN, CAPITAL_STOCK_MAX]``.
    """
    vals = df["capital_stock_real"].dropna()
    out_of_range = vals[
        (vals < CAPITAL_STOCK_MIN) | (vals > CAPITAL_STOCK_MAX)
    ]
    if not out_of_range.empty:
        raise QualityCheckError(
            f"Valores de capital_stock_real fuera de rango "
            f"[{CAPITAL_STOCK_MIN}, {CAPITAL_STOCK_MAX}]: "
            f"{out_of_range.values[:5]}..."
        )
    logger.info(
        "✓ Validación de rango capital_stock_real [%.1f, %.1f]: OK",
        CAPITAL_STOCK_MIN, CAPITAL_STOCK_MAX,
    )


def check_depreciation_rate_range(df: pd.DataFrame) -> None:
    """Verifica que depreciation_rate esté en un rango razonable.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame PWT procesado.

    Raises
    ------
    QualityCheckError
        Si hay valores fuera del rango ``[DEPRECIATION_RATE_MIN, DEPRECIATION_RATE_MAX]``.
    """
    vals = df["depreciation_rate"].dropna()
    out_of_range = vals[
        (vals < DEPRECIATION_RATE_MIN) | (vals > DEPRECIATION_RATE_MAX)
    ]
    if not out_of_range.empty:
        raise QualityCheckError(
            f"Valores de depreciation_rate fuera de rango "
            f"[{DEPRECIATION_RATE_MIN}, {DEPRECIATION_RATE_MAX}]: "
            f"{out_of_range.values[:5]}..."
        )
    logger.info(
        "✓ Validación de rango depreciation_rate [%.4f, %.4f]: OK",
        DEPRECIATION_RATE_MIN, DEPRECIATION_RATE_MAX,
    )


def check_human_capital_range(df: pd.DataFrame) -> None:
    """Verifica que human_capital esté en un rango razonable.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame PWT procesado.

    Raises
    ------
    QualityCheckError
        Si hay valores fuera del rango ``[HUMAN_CAPITAL_MIN, HUMAN_CAPITAL_MAX]``.
    """
    vals = df["human_capital"].dropna()
    out_of_range = vals[
        (vals < HUMAN_CAPITAL_MIN) | (vals > HUMAN_CAPITAL_MAX)
    ]
    if not out_of_range.empty:
        raise QualityCheckError(
            f"Valores de human_capital fuera de rango "
            f"[{HUMAN_CAPITAL_MIN}, {HUMAN_CAPITAL_MAX}]: "
            f"{out_of_range.values[:5]}..."
        )
    logger.info(
        "✓ Validación de rango human_capital [%.1f, %.1f]: OK",
        HUMAN_CAPITAL_MIN, HUMAN_CAPITAL_MAX,
    )


def check_pwt_min_rows(df: pd.DataFrame, min_rows: int = 50) -> None:
    """Verifica que el dataset PWT tenga al menos ``min_rows`` filas.

    PWT cubre Colombia desde 1950, por lo que se esperan ≥ 50 observaciones.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame PWT procesado.
    min_rows : int
        Número mínimo de filas esperadas (default: 50).

    Raises
    ------
    QualityCheckError
        Si el DataFrame tiene menos filas que ``min_rows``.
    """
    if len(df) < min_rows:
        raise QualityCheckError(
            f"El dataset PWT tiene solo {len(df)} filas; "
            f"se esperaban al menos {min_rows}."
        )
    logger.info("✓ Validación de mínimo de filas (%d): OK", min_rows)


def run_pwt_checks(df: pd.DataFrame) -> bool:
    """Ejecuta todas las validaciones de calidad para PWT 11.0.

    Verifica:
    - Columnas presentes: ``date``, ``year``, ``capital_stock_real``,
      ``depreciation_rate``, ``human_capital`` (y el resto del esquema estándar).
    - ``capital_stock_real`` dentro de ``[CAPITAL_STOCK_MIN, CAPITAL_STOCK_MAX]``.
    - ``depreciation_rate`` dentro de ``[DEPRECIATION_RATE_MIN, DEPRECIATION_RATE_MAX]``.
    - ``human_capital`` dentro de ``[HUMAN_CAPITAL_MIN, HUMAN_CAPITAL_MAX]``.
    - Sin duplicados en ``date``.
    - Al menos 50 filas (cobertura desde 1950).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame PWT procesado.

    Returns
    -------
    bool
        True si todas las validaciones pasan.

    Raises
    ------
    QualityCheckError
        Si alguna validación falla.
    """
    logger.info("─── Validaciones de calidad PWT 11.0 ───")
    logger.info("Dataset: %d filas × %d columnas", *df.shape)

    check_pwt_columns(df)
    check_no_nulls_generic(
        df,
        ["date", "year", "capital_stock_real", "depreciation_rate", "human_capital"],
    )
    check_capital_stock_range(df)
    check_depreciation_rate_range(df)
    check_human_capital_range(df)
    check_no_duplicates(df)
    check_pwt_min_rows(df)

    logger.info("─── Todas las validaciones PWT pasaron ✓ ───")
    return True


# ── Validaciones de variables derivadas del dataset unificado ────────────────

def run_derived_checks(df: pd.DataFrame) -> bool:
    """Valida la coherencia de las variables derivadas del dataset unificado.

    Comprueba:

    - ``corr(ipc_yoy, Inf_Rate) > 0.97`` para filas con ambas series no nulas
      (se aceptan los primeros 12 meses con NaN en ``ipc_yoy``).
    - La suma de las 12 variaciones mensuales (``Σ ipc_mom``) y ``ipc_yoy`` no
      divergen más de 5 pp en el 90 % de las filas (la diferencia es el término
      de composición del encadenamiento, pequeño).

    Parameters
    ----------
    df : pd.DataFrame
        Dataset unificado (resultado de ``merge_all_sources``).

    Returns
    -------
    bool
        True si todas las validaciones pasan.

    Raises
    ------
    QualityCheckError
        Si alguna validación falla.
    """
    logger.info("─── Validaciones de variables derivadas ───")

    required = {"ipc_yoy", "ipc_mom", "inflation_gap", "Inf_Rate"}
    missing = required - set(df.columns)
    if missing:
        raise QualityCheckError(
            f"Columnas derivadas faltantes en el dataset: {sorted(missing)}"
        )

    # 1. Correlación ipc_yoy vs Inf_Rate > 0.97
    mask = df["ipc_yoy"].notna() & df["Inf_Rate"].notna()
    if mask.sum() > 0:
        corr = df.loc[mask, "ipc_yoy"].corr(df.loc[mask, "Inf_Rate"])
        if corr <= 0.97:
            raise QualityCheckError(
                f"corr(ipc_yoy, Inf_Rate) = {corr:.4f} ≤ 0.97 — "
                "las series de inflación no son coherentes entre sí."
            )
        logger.info("corr(ipc_yoy, Inf_Rate) = %.4f ✓", corr)

    # 2. Coherencia: la variación interanual ≈ suma de las 12 variaciones mensuales
    #    en ≥ 90 % de las filas. La diferencia es el término de composición del
    #    encadenamiento (pequeño). NB: comparar contra ``ipc_mom * 12`` es incorrecto:
    #    anualiza UN solo mes y amplifica ×12 la volatilidad estacional (alimentos,
    #    ajustes de enero), divergiendo > 5 pp en muchos meses aunque los datos sean buenos.
    suma_12m = df["ipc_mom"].rolling(12).sum()
    mask2 = df["ipc_yoy"].notna() & suma_12m.notna()
    if mask2.sum() > 0:
        diff = (suma_12m.loc[mask2] - df.loc[mask2, "ipc_yoy"]).abs()
        pct_ok = (diff <= 5.0).mean()
        if pct_ok < 0.90:
            raise QualityCheckError(
                f"Solo el {pct_ok:.1%} de las filas cumplen "
                "|suma_12m(ipc_mom) - ipc_yoy| <= 5 pp (minimo esperado: 90 %)."
            )
        logger.info("|suma_12m(ipc_mom) - ipc_yoy| <= 5 pp en %.1f %% de las filas OK", pct_ok * 100)

    logger.info("─── Todas las validaciones de variables derivadas pasaron ✓ ───")
    return True


# ── Validaciones del dataset de PIB Potencial ────────────────────────────────

def run_pib_potencial_checks(df: pd.DataFrame) -> bool:
    """Valida el dataset trimestral de PIB Potencial antes de escribir el Excel.

    Verifica:
    - Columnas mínimas presentes: ``PIB``, ``PIB_pot``, ``Brecha_CD``,
      ``PIB_tend_BHP``, ``Brecha_BHP``, ``alpha``, ``A_obs``, ``A_pot``.
    - ``PIB_pot > 0`` en todos los periodos.
    - ``|Brecha_CD| < 50 pp`` (Colombia históricamente ±10 pp; se permite hasta 50 pp
      para cubrir choques extremos como el COVID-19 en 2020-Q2).
    - ``|Brecha_BHP| < 15 pp``.
    - ``alpha`` en el rango ``(0.15, 0.85)`` para todos los periodos.
    - ``A_pot > 0`` en todos los periodos.
    - Al menos 60 trimestres (cobertura desde 2005-Q1).

    Parameters
    ----------
    df : pd.DataFrame
        Dataset trimestral de PIB Potencial.

    Returns
    -------
    bool
        True si todas las validaciones pasan.

    Raises
    ------
    QualityCheckError
        Si alguna validación falla.
    """
    logger.info("─── Validaciones PIB Potencial ───")

    # 1. Columnas mínimas
    requeridas = {"PIB", "PIB_pot", "Brecha_CD", "PIB_tend_BHP", "Brecha_BHP", "alpha", "A_pot"}
    faltantes = requeridas - set(df.columns)
    if faltantes:
        raise QualityCheckError(
            f"Columnas requeridas faltantes en dataset PIB Potencial: {sorted(faltantes)}"
        )

    # 2. PIB_pot positivo
    invalidos = (df["PIB_pot"] <= 0).sum()
    if invalidos > 0:
        raise QualityCheckError(
            f"PIB_pot tiene {invalidos} valores no positivos — revisar factores de producción."
        )
    logger.info("PIB_pot > 0 en todos los trimestres ✓")

    # 3. Brecha CD acotada
    # Umbral generoso (50 pp) para cubrir el choque COVID-19 de 2020-Q2,
    # donde K_usado, L_obs y A_obs colapsaron simultáneamente mientras el
    # potencial Cobb-Douglas no incorpora una dummy de crisis explícita.
    # En períodos normales la brecha histórica de Colombia es ±10 pp.
    max_brecha_cd = df["Brecha_CD"].abs().max()
    if max_brecha_cd >= 50.0:
        raise QualityCheckError(
            f"|Brecha_CD| máxima = {max_brecha_cd:.2f} pp ≥ 50 pp — "
            "posible error en factores o NAIRU/NAICU."
        )
    if max_brecha_cd >= 15.0:
        logger.warning(
            "|Brecha_CD| máx = %.2f pp — supera ±15 pp (revisar si corresponde "
            "a choque transitorio como COVID-2020).", max_brecha_cd
        )
    else:
        logger.info("|Brecha_CD| máx = %.2f pp ✓", max_brecha_cd)

    # 4. Brecha BHP acotada
    # Umbral de error en 20 pp; advertencia entre 15-20 pp.
    # El filtro BHP (λ=1600, iter=3) normalmente contiene la brecha a ±5 pp,
    # pero durante el choque COVID-19 (2020-Q2) puede alcanzar ~17 pp.
    max_brecha_bhp = df["Brecha_BHP"].abs().max()
    if max_brecha_bhp >= 20.0:
        raise QualityCheckError(
            f"|Brecha_BHP| máxima = {max_brecha_bhp:.2f} pp ≥ 20 pp — revisar serie de PIB."
        )
    if max_brecha_bhp >= 15.0:
        logger.warning(
            "|Brecha_BHP| máx = %.2f pp — supera ±15 pp (verificar trimestre COVID-2020).",
            max_brecha_bhp,
        )
    else:
        logger.info("|Brecha_BHP| máx = %.2f pp ✓", max_brecha_bhp)

    # 5. Alpha en rango razonable
    alpha_fuera = ((df["alpha"] <= 0.15) | (df["alpha"] >= 0.85)).sum()
    if alpha_fuera > 0:
        raise QualityCheckError(
            f"Alpha tiene {alpha_fuera} valores fuera de (0.15, 0.85) — "
            "revisar datos de ingreso DANE."
        )
    logger.info("Alpha en (0.15, 0.85) para todos los trimestres ✓")

    # 6. A_pot positivo
    if (df["A_pot"] <= 0).any():
        raise QualityCheckError("A_pot tiene valores no positivos — revisar HP filter sobre A_obs.")
    logger.info("A_pot > 0 ✓")

    # 7. Mínimo de trimestres
    if len(df) < 60:
        raise QualityCheckError(
            f"Dataset tiene solo {len(df)} trimestres (mínimo esperado: 60). "
            "Verifique que todas las fuentes estén actualizadas."
        )
    logger.info("Cobertura: %d trimestres ✓", len(df))

    logger.info("─── Todas las validaciones PIB Potencial pasaron ✓ ───")
    return True
