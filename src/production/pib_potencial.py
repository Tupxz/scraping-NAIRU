"""Cálculo del PIB Potencial Colombia y las brechas del producto.

Implementa dos metodologías complementarias:

1. **Cobb-Douglas (CD):** usa la PTF tendencial y los factores potenciales.

       PIB_pot  = A_pot × K_pot^alpha × L_pot^(1 − alpha)
       Brecha_CD = (PIB − PIB_pot) / PIB_pot × 100

2. **Hodrick-Prescott puro (HP):** tendencia estadística del PIB observado.

       PIB_tend_HP  = hp_trend(PIB, lambda=1600)
       Brecha_HP    = (PIB − PIB_tend_HP) / PIB_tend_HP × 100

La brecha CD es el indicador principal del pipeline (tiene interpretación
económica). La brecha HP se incluye como referencia y chequeo de consistencia.

Columnas que produce ``compute_pib_potencial``
----------------------------------------------
    PIB_pot       — PIB Potencial (miles MM COP 2017, Cobb-Douglas)
    Brecha_CD     — Brecha del producto CD (%, positivo = sobre-calentamiento)
    PIB_tend_HP   — Tendencia HP del PIB (miles MM COP 2017)
    Brecha_HP     — Brecha del producto HP (%, positivo = sobre-calentamiento)
"""

from __future__ import annotations

import logging

import pandas as pd

from src.production.tfp import hp_filter, HP_LAMBDA_QUARTERLY

logger = logging.getLogger("nairu_pipeline.production.pib_potencial")


def compute_pib_potencial(
    df: pd.DataFrame,
    lamb: float = HP_LAMBDA_QUARTERLY,
) -> pd.DataFrame:
    """Calcula el PIB Potencial y las brechas del producto.

    El DataFrame de entrada debe haber pasado por ``compute_all_factors``
    y ``compute_tfp`` previamente (es decir, contener las columnas que
    esos módulos generan).

    Parameters
    ----------
    df : pd.DataFrame
        Requiere columnas: ``PIB``, ``A_pot``, ``K_pot``, ``L_pot``, ``alpha``.
    lamb : float
        Lambda del filtro HP para la brecha estadística. Default: 1600.

    Returns
    -------
    pd.DataFrame
        Copia con columnas ``PIB_pot``, ``Brecha_CD``,
        ``PIB_tend_HP`` y ``Brecha_HP`` añadidas.

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

    df["PIB_pot"] = df["A_pot"] * (k_pot ** alpha) * (l_pot ** (1.0 - alpha))

    # ── 2. Brecha CD ──────────────────────────────────────────────────────
    df["Brecha_CD"] = (df["PIB"] - df["PIB_pot"]) / df["PIB_pot"] * 100.0

    # ── 3. Tendencia HP del PIB y brecha estadística ──────────────────────
    _, trend_pib = hp_filter(df["PIB"], lamb=lamb)
    df["PIB_tend_HP"] = trend_pib.values
    df["Brecha_HP"]   = (df["PIB"] - df["PIB_tend_HP"]) / df["PIB_tend_HP"] * 100.0

    logger.info(
        "PIB Potencial: rango [%.0f, %.0f] MM COP 2017 | "
        "Brecha_CD: media=%.2f%%, std=%.2f%% | "
        "Brecha_HP: media=%.2f%%, std=%.2f%%",
        df["PIB_pot"].min(), df["PIB_pot"].max(),
        df["Brecha_CD"].mean(), df["Brecha_CD"].std(),
        df["Brecha_HP"].mean(), df["Brecha_HP"].std(),
    )
    return df


# ── Columnas de salida del dataset trimestral ─────────────────────────────────

QUARTERLY_OUTPUT_COLS: list[str] = [
    "date",
    # Contexto temporal
    "year", "quarter",
    # Insumos macroeconómicos
    "PIB",
    "K", "UCI", "NAICU_q",
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
    "PIB_tend_HP", "Brecha_HP",
]
