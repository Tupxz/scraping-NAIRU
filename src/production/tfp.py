"""Cálculo de la Productividad Total de Factores (PTF) observada y tendencial.

La PTF (también llamada A o Residuo de Solow) se calcula como:

    A_obs = PIB / (K_usado^alpha × L_obs^(1 − alpha))

La tendencia de la PTF se obtiene con el filtro Boosted Hodrick-Prescott (BHP):

    A_pot = bhp_trend(A_obs, lambda=1600, iterations=3)

El BHP aplica el filtro HP iterativamente sobre el ciclo residual, extrayendo
tendencias de mayor frecuencia en cada pasada (Phillips & Shi, 2021).
``lambda = 1600`` es el valor estándar para datos trimestrales.
El filtro se aplica sobre los valores no nulos de la serie; los extremos con
NaN se reindexan al final.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from statsmodels.tsa.filters.hp_filter import hpfilter

logger = logging.getLogger("nairu_pipeline.production.tfp")

HP_LAMBDA_QUARTERLY: float = 1600.0
BHP_ITERATIONS: int = 3          # iteraciones por defecto del Boosted HP


# ── HP Filter (base) ──────────────────────────────────────────────────────────

def hp_filter(
    series: pd.Series,
    lamb: float = HP_LAMBDA_QUARTERLY,
) -> tuple[pd.Series, pd.Series]:
    """Aplica el filtro Hodrick-Prescott a una serie temporal.

    Maneja NaN al inicio/fin de la serie excluyéndolos del filtro y
    reindexando la tendencia al índice original.

    Parameters
    ----------
    series : pd.Series
        Serie temporal. Debe tener al menos 8 observaciones no nulas para
        que el filtro sea significativo.
    lamb : float
        Parámetro de suavizamiento (lambda). Default: 1600 (estándar trimestral).

    Returns
    -------
    tuple[pd.Series, pd.Series]
        ``(cycle, trend)`` con el mismo índice que la serie de entrada.
        ``cycle = series − trend``.

    Raises
    ------
    ValueError
        Si la serie tiene menos de 8 observaciones no nulas.
    """
    valid = series.dropna()
    if len(valid) < 8:
        raise ValueError(
            f"El filtro HP requiere al menos 8 observaciones no nulas; "
            f"la serie tiene {len(valid)}."
        )

    cycle_vals, trend_vals = hpfilter(valid.values, lamb=lamb)

    trend = pd.Series(trend_vals, index=valid.index, name=f"{series.name}_trend")
    cycle = pd.Series(cycle_vals, index=valid.index, name=f"{series.name}_cycle")

    # Reindexar al índice original (rellena con NaN donde había NaN)
    trend = trend.reindex(series.index)
    cycle = cycle.reindex(series.index)

    return cycle, trend


# ── Boosted HP Filter ─────────────────────────────────────────────────────────

def boosted_hp_filter(
    series: pd.Series,
    lamb: float = HP_LAMBDA_QUARTERLY,
    iterations: int = BHP_ITERATIONS,
) -> tuple[pd.Series, pd.Series]:
    """Aplica el filtro Boosted Hodrick-Prescott (BHP) a una serie temporal.

    El BHP (Phillips & Shi, 2021) aplica el filtro HP iterativamente: en cada
    pasada extrae el ciclo del residuo anterior, acumulando una tendencia de
    baja frecuencia más precisa que el HP estándar. El ciclo final es el
    residuo tras ``iterations`` aplicaciones; la tendencia es la serie
    original menos ese ciclo.

    Maneja NaN al inicio/fin de la serie excluyéndolos del filtro y
    reindexando la tendencia al índice original.

    Parameters
    ----------
    series : pd.Series
        Serie temporal. Debe tener al menos 8 observaciones no nulas.
    lamb : float
        Parámetro de suavizamiento lambda. Default: 1600 (trimestral).
    iterations : int
        Número de iteraciones del filtro HP. Default: 3.

    Returns
    -------
    tuple[pd.Series, pd.Series]
        ``(cycle, trend)`` con el mismo índice que la serie de entrada.
        ``cycle = series − trend``.

    Raises
    ------
    ValueError
        Si la serie tiene menos de 8 observaciones no nulas.
    """
    valid = series.dropna()
    if len(valid) < 8:
        raise ValueError(
            f"El filtro BHP requiere al menos 8 observaciones no nulas; "
            f"la serie tiene {len(valid)}."
        )

    # Aplicar HP iterativamente sobre el ciclo residual
    current_cycle = valid.values.copy().astype(float)
    for _ in range(iterations):
        current_cycle, _ = hpfilter(current_cycle, lamb=lamb)

    # Tendencia = serie original − ciclo final
    trend_vals = valid.values - current_cycle

    trend = pd.Series(trend_vals,   index=valid.index, name=f"{series.name}_trend")
    cycle = pd.Series(current_cycle, index=valid.index, name=f"{series.name}_cycle")

    trend = trend.reindex(series.index)
    cycle = cycle.reindex(series.index)

    return cycle, trend


# ── PTF observada ─────────────────────────────────────────────────────────────

def compute_tfp_observed(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula la PTF observada (Residuo de Solow).

    Fórmula
    -------
    A_obs = PIB / (K_usado^alpha × L_obs^(1 − alpha))

    Parameters
    ----------
    df : pd.DataFrame
        Requiere columnas: ``PIB``, ``K_usado``, ``L_obs``, ``alpha``.

    Returns
    -------
    pd.DataFrame
        Copia con columna ``A_obs`` añadida. Será NaN donde algún insumo sea
        NaN o donde ``K_usado`` o ``L_obs`` sean cero o negativos.
    """
    df = df.copy()

    # Protección contra divisiones por cero o valores negativos
    k = df["K_usado"].where(df["K_usado"] > 0)
    l = df["L_obs"].where(df["L_obs"] > 0)
    pib = df["PIB"].where(df["PIB"] > 0)
    alpha = df["alpha"]

    df["A_obs"] = pib / (k ** alpha * l ** (1.0 - alpha))

    n_nulo = df["A_obs"].isna().sum()
    if n_nulo > 0:
        logger.warning("A_obs tiene %d valores NaN (insumos faltantes o cero).", n_nulo)

    logger.debug(
        "PTF observada: media=%.4f, min=%.4f, max=%.4f",
        df["A_obs"].mean(), df["A_obs"].min(), df["A_obs"].max(),
    )
    return df


# ── PTF tendencial ────────────────────────────────────────────────────────────

def compute_tfp_trend(
    df: pd.DataFrame,
    lamb: float = HP_LAMBDA_QUARTERLY,
    iterations: int = BHP_ITERATIONS,
) -> pd.DataFrame:
    """Calcula la PTF tendencial con el filtro Boosted HP (BHP).

    Aplica el filtro BHP sobre ``A_obs`` para extraer la tendencia de largo
    plazo de la productividad. Esta tendencia (``A_pot``) se usa como proxy
    de la PTF potencial en el cálculo del PIB potencial.

    Parameters
    ----------
    df : pd.DataFrame
        Requiere columna ``A_obs`` (generada por ``compute_tfp_observed``).
    lamb : float
        Lambda del filtro BHP. Default: 1600 (trimestral).
    iterations : int
        Número de iteraciones del BHP. Default: 3.

    Returns
    -------
    pd.DataFrame
        Copia con columnas ``A_pot`` (tendencia) y ``A_cycle`` (ciclo) añadidas.
    """
    df = df.copy()

    if "A_obs" not in df.columns:
        raise KeyError("Se requiere la columna 'A_obs'. Llame compute_tfp_observed primero.")

    _, trend = boosted_hp_filter(df["A_obs"], lamb=lamb, iterations=iterations)
    df["A_pot"]   = trend.values
    df["A_cycle"] = df["A_obs"] - df["A_pot"]

    logger.debug(
        "PTF tendencial (BHP, λ=%.0f, iter=%d): media=%.4f, ciclo std=%.4f",
        lamb, iterations, df["A_pot"].mean(), df["A_cycle"].std(),
    )
    return df


# ── Función de conveniencia ───────────────────────────────────────────────────

def compute_tfp(
    df: pd.DataFrame,
    lamb: float = HP_LAMBDA_QUARTERLY,
    iterations: int = BHP_ITERATIONS,
) -> pd.DataFrame:
    """Calcula A_obs y A_pot en un solo paso.

    Equivale a llamar ``compute_tfp_observed`` seguido de ``compute_tfp_trend``
    con el filtro Boosted HP.

    Parameters
    ----------
    df : pd.DataFrame
        Requiere: ``PIB``, ``K_usado``, ``L_obs``, ``alpha``.
    lamb : float
        Lambda BHP. Default: 1600.
    iterations : int
        Iteraciones BHP. Default: 3.

    Returns
    -------
    pd.DataFrame
        DataFrame con columnas ``A_obs``, ``A_pot``, ``A_cycle`` añadidas.
    """
    df = compute_tfp_observed(df)
    df = compute_tfp_trend(df, lamb=lamb, iterations=iterations)
    return df
