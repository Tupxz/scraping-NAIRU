"""Cálculo del PIB Potencial Colombia y las brechas del producto.

Implementa dos metodologías complementarias:

1. **Cobb-Douglas (CD):** usa la PTF tendencial y los factores potenciales.

       PIB_pot   = A_pot × K_pot^alpha × (H · L_pot)^(1 − alpha)
       Brecha_CD = (PIB − PIB_pot) / PIB_pot × 100

2. **Boosted Hodrick-Prescott (BHP):** tendencia estadística del PIB observado
   con el filtro HP iterado (Phillips & Shi, 2021).

       PIB_tend_BHP = bhp_trend(PIB, lambda=1600, iterations=3)
       Brecha_BHP   = (PIB − PIB_tend_BHP) / PIB_tend_BHP × 100

La brecha CD es el indicador principal del pipeline (tiene interpretación
económica). La brecha BHP se incluye como referencia y chequeo de consistencia.

Columnas que produce ``compute_pib_potencial``
----------------------------------------------
    PIB_pot        — PIB Potencial (miles MM COP 2017, Cobb-Douglas)
    Brecha_CD      — Brecha del producto CD (%, positivo = sobre-calentamiento)
    PIB_tend_BHP   — Tendencia BHP del PIB (miles MM COP 2017)
    Brecha_BHP     — Brecha del producto BHP (%, positivo = sobre-calentamiento)
"""

from __future__ import annotations

import logging

import pandas as pd

from src.production.tfp import boosted_hp_filter, BHP_ITERATIONS, HP_LAMBDA_QUARTERLY

logger = logging.getLogger("nairu_pipeline.production.pib_potencial")


def compute_pib_potencial(
    df: pd.DataFrame,
    lamb: float = HP_LAMBDA_QUARTERLY,
    iterations: int = BHP_ITERATIONS,
) -> pd.DataFrame:
    """Calcula el PIB Potencial y las brechas del producto.

    El DataFrame de entrada debe haber pasado por ``compute_all_factors``
    y ``compute_tfp`` previamente (es decir, contener las columnas que
    esos módulos generan).

    Parameters
    ----------
    df : pd.DataFrame
        Requiere columnas: ``PIB``, ``A_pot``, ``K_pot``, ``L_pot``, ``alpha``.
        Opcional: ``H`` (capital humano; si falta se asume H = 1).
    lamb : float
        Lambda del filtro BHP para la brecha estadística. Default: 1600.
    iterations : int
        Iteraciones del filtro BHP. Default: 3.

    Returns
    -------
    pd.DataFrame
        Copia con columnas ``PIB_pot``, ``Brecha_CD``,
        ``PIB_tend_BHP`` y ``Brecha_BHP`` añadidas.

    Raises
    ------
    KeyError
        Si alguna columna requerida no está presente.
    """
    _requeridas = {"PIB", "A_pot", "K_pot", "L_pot", "alpha"}
    _faltantes = _requeridas - set(df.columns)
    if _faltantes:
        raise KeyError(
            f"Columnas requeridas faltantes para compute_pib_potencial: {sorted(_faltantes)}. "
            "Asegúrese de haber ejecutado compute_all_factors() y compute_tfp() primero."
        )

    df = df.copy()

    # ── 1. PIB Potencial Cobb-Douglas ─────────────────────────────────────
    k_pot = df["K_pot"].where(df["K_pot"] > 0)
    l_pot = df["L_pot"].where(df["L_pot"] > 0)
    alpha = df["alpha"]

    # Trabajo potencial efectivo: H · L_pot (mismo capital humano que en A_obs,
    # para que la PTF y el PIB potencial sean consistentes). H = 1 si no existe.
    h = df["H"] if "H" in df.columns else 1.0
    l_pot_ef = h * l_pot

    df["PIB_pot"] = df["A_pot"] * (k_pot ** alpha) * (l_pot_ef ** (1.0 - alpha))

    # ── 2. Brecha CD ──────────────────────────────────────────────────────
    df["Brecha_CD"] = (df["PIB"] - df["PIB_pot"]) / df["PIB_pot"] * 100.0

    # ── 3. Tendencia BHP del PIB y brecha estadística ────────────────────
    _, trend_pib = boosted_hp_filter(df["PIB"], lamb=lamb, iterations=iterations)
    df["PIB_tend_BHP"] = trend_pib.values
    df["Brecha_BHP"]   = (df["PIB"] - df["PIB_tend_BHP"]) / df["PIB_tend_BHP"] * 100.0

    logger.info(
        "PIB Potencial: rango [%.0f, %.0f] MM COP 2017 | "
        "Brecha_CD: media=%.2f%%, std=%.2f%% | "
        "Brecha_BHP: media=%.2f%%, std=%.2f%%",
        df["PIB_pot"].min(), df["PIB_pot"].max(),
        df["Brecha_CD"].mean(), df["Brecha_CD"].std(),
        df["Brecha_BHP"].mean(), df["Brecha_BHP"].std(),
    )
    return df


# ── Columnas de salida del dataset trimestral ─────────────────────────────────

QUARTERLY_OUTPUT_COLS: list[str] = [
    "date",
    # Contexto temporal
    "year", "quarter",
    # Insumos macroeconómicos
    "PIB",
    "K", "UCI", "NAICU_q", "H",
    "TD", "TGP", "PET", "NAIRU_q",
    "compensation_employees", "gross_operating_surplus", "mixed_income",
    # Factores calculados
    "alpha",
    "L_obs", "L_pot",
    "K_usado", "K_pot",
    # PTF
    "A_obs", "A_pot", "A_cycle",
    # PIB Potencial y brechas
    "PIB_pot", "Brecha_CD",
    "PIB_tend_BHP", "Brecha_BHP",
]
