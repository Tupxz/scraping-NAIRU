"""Cálculo del PIB Potencial Colombia y las brechas del producto.

Metodología alineada con ``legacy/pib_potencial_integrado_v3.py`` (Módulo 2,
configuración v2). Todo se calcula sobre ÍNDICES base 100 en ``BASE_QUARTER``
(ver ``src/production/factors.py`` para la justificación de por qué el ancla
NO es 2005Q1 como en v3 original, sino 2007Q3, con datos reales del repo).

Tres brechas del producto, todas construidas con la misma función Cobb-Douglas
pero variantes en QUÉ tendencia de PTF usan (o ninguna):

    Brecha_CD  (principal) — usa ``ptf_star`` (tendencia ESTRUCTURAL estilo
        CBO, con tramos anclados en los picos del ciclo BBQ). Es el indicador
        económico principal del pipeline.

           pib_pot   = ptf_star × idx_K_star^alpha × idx_LH_star^(1-alpha)
           Brecha_CD = (idx_pib / pib_pot − 1) × 100

    brecha_pot_hp (diagnóstico) — igual fórmula pero con ``ptf_hp`` (PTF
        tendencial puramente estadística) en vez de ``ptf_star``. Aísla
        cuánto de la brecha CD viene de usar una tendencia de PTF
        estructural en vez de una puramente estadística.

    Brecha_BHP (referencia) — brecha puramente estadística del PIB mismo
        (sin pasar por la función de producción): tendencia HP de una sola
        pasada sobre ``idx_pib``.

           Brecha_BHP = (idx_pib / idx_trend − 1) × 100

``PIB_pot``/``Brecha_CD``/``PIB_tend_BHP``/``Brecha_BHP``/``A_obs``/``A_pot``
se conservan como nombres de columna (alias) por compatibilidad con
``excel_writer.py`` y ``quality_checks.py`` — su FÓRMULA cambió (ver arriba),
no solo su nombre.

Columnas que produce ``compute_pib_potencial``
----------------------------------------------
    pib_pot, pib_pot_hp             — PIB potencial, índice base 100 (principal y ref. HP)
    brecha_pot, brecha_pot_hp       — brechas correspondientes, FRACCIÓN (no %)
    idx_trend, brecha_hp            — tendencia HP del PIB observado y su brecha (fracción)
    PIB_pot, PIB_tend_BHP           — alias de pib_pot / idx_trend (índice, base 100)
    Brecha_CD, Brecha_BHP           — alias de brecha_pot / brecha_hp, en % (no fracción)
"""

from __future__ import annotations

import logging

import pandas as pd

from src.production.tfp import HP_LAMBDA_QUARTERLY, hp_filter

logger = logging.getLogger("nairu_pipeline.production.pib_potencial")


def calcular_tendencia_hp_producto(
    df: pd.DataFrame,
    T: pd.Timestamp,
    base_quarter: pd.Timestamp,
    lamb: float = HP_LAMBDA_QUARTERLY,
) -> pd.DataFrame:
    """Tendencia HP (una sola pasada) del PIB anualizado, indexada.

    Sirve de comparación puramente estadística (sin pasar por la función de
    producción) — la brecha correspondiente es ``Brecha_BHP``.

    Parameters
    ----------
    df : pd.DataFrame
        Requiere ``date``, ``W_pib_ann``, ``idx_pib`` (de ``factors.calcular_producto``).

    Returns
    -------
    pd.DataFrame
        Copia con columna ``idx_trend`` añadida.
    """
    df = df.copy()
    base_val = float(df.loc[df["date"] == base_quarter, "W_pib_ann"].iloc[0])

    muestra_mask = (df["date"] >= base_quarter) & (df["date"] <= T)
    muestra = df.loc[muestra_mask].copy()
    _, tendencia = hp_filter(muestra["W_pib_ann"], lamb=lamb)
    df.loc[muestra_mask, "hp_trend"] = tendencia.values
    df["idx_trend"] = 100 * df["hp_trend"] / base_val
    return df


def compute_pib_potencial(
    df: pd.DataFrame,
    T: pd.Timestamp,
    base_quarter: pd.Timestamp,
) -> pd.DataFrame:
    """Calcula el PIB Potencial y las tres brechas del producto.

    El DataFrame de entrada debe haber pasado por ``factors.compute_all_factors``
    y ``tfp.compute_tfp`` previamente.

    Parameters
    ----------
    df : pd.DataFrame
        Requiere: ``idx_pib``, ``idx_K_star``, ``idx_LH_star``, ``alpha``,
        ``ptf_star``, ``ptf_hp``, ``W_pib_ann``.
    T : pd.Timestamp
        Último trimestre completo (límite superior de la muestra).
    base_quarter : pd.Timestamp
        Ancla de índices (límite inferior de la muestra).

    Returns
    -------
    pd.DataFrame
        Copia con ``pib_pot``, ``pib_pot_hp``, ``brecha_pot``,
        ``brecha_pot_hp``, ``idx_trend``, ``brecha_hp`` añadidas, más los
        alias ``PIB_pot``, ``Brecha_CD``, ``PIB_tend_BHP``, ``Brecha_BHP``.

    Raises
    ------
    KeyError
        Si alguna columna requerida no está presente.
    """
    _requeridas = {"idx_pib", "idx_K_star", "idx_LH_star", "alpha", "ptf_star", "ptf_hp", "W_pib_ann"}
    _faltantes = _requeridas - set(df.columns)
    if _faltantes:
        raise KeyError(
            f"Columnas requeridas faltantes para compute_pib_potencial: {sorted(_faltantes)}. "
            "Asegúrese de haber ejecutado compute_all_factors() y compute_tfp() primero."
        )

    df = df.copy()
    muestra_mask = (df["date"] >= base_quarter) & (df["date"] <= T)
    k_star = df.loc[muestra_mask, "idx_K_star"]
    lh_star = df.loc[muestra_mask, "idx_LH_star"]
    alpha = df.loc[muestra_mask, "alpha"]

    # ── 1. PIB potencial principal (PTF* estructural CBO) ────────────────
    df.loc[muestra_mask, "pib_pot"] = (
        df.loc[muestra_mask, "ptf_star"] * (k_star ** alpha) * (lh_star ** (1.0 - alpha))
    )
    df.loc[muestra_mask, "brecha_pot"] = df.loc[muestra_mask, "idx_pib"] / df.loc[muestra_mask, "pib_pot"] - 1.0

    # ── 2. PIB potencial alternativo (PTF HP, diagnóstico) ────────────────
    df.loc[muestra_mask, "pib_pot_hp"] = (
        df.loc[muestra_mask, "ptf_hp"] * (k_star ** alpha) * (lh_star ** (1.0 - alpha))
    )
    df.loc[muestra_mask, "brecha_pot_hp"] = df.loc[muestra_mask, "idx_pib"] / df.loc[muestra_mask, "pib_pot_hp"] - 1.0

    # ── 3. Tendencia HP del PIB observado (estadística pura) ─────────────
    df = calcular_tendencia_hp_producto(df, T=T, base_quarter=base_quarter)
    df.loc[muestra_mask, "brecha_hp"] = df.loc[muestra_mask, "idx_pib"] / df.loc[muestra_mask, "idx_trend"] - 1.0

    # ── Alias de compatibilidad (excel_writer.py, quality_checks.py) ─────
    df["PIB_pot"] = df["pib_pot"]
    df["Brecha_CD"] = 100.0 * df["brecha_pot"]
    df["PIB_tend_BHP"] = df["idx_trend"]
    df["Brecha_BHP"] = 100.0 * df["brecha_hp"]

    logger.info(
        "PIB Potencial: idx_pib/pib_pot en %s→%s | "
        "Brecha_CD: media=%.2f%%, std=%.2f%% | Brecha_BHP: media=%.2f%%, std=%.2f%%",
        base_quarter.date(), T.date(),
        df["Brecha_CD"].mean(skipna=True), df["Brecha_CD"].std(skipna=True),
        df["Brecha_BHP"].mean(skipna=True), df["Brecha_BHP"].std(skipna=True),
    )
    return df


# ── Columnas de salida del dataset trimestral ─────────────────────────────────

QUARTERLY_OUTPUT_COLS: list[str] = [
    "date",
    # Contexto temporal
    "year", "quarter",
    # Insumos macroeconómicos (niveles, tal como llegan de data/processed)
    "V_pib", "K", "icu", "naicu", "nairu",
    "pet", "tgp", "td",
    "BQ_ra", "BS_ebe",
    # Mercado laboral y participación
    "brecha_u", "ma_brecha_u", "brecha_icu", "ma_brecha_icu",
    "tgp_star", "jornada", "festivos_q",
    # Factores calculados (índices, base 100 en BASE_QUARTER)
    "alpha", "alpha_t",
    "idx_L", "idx_L_star",
    "idx_hc", "idx_LH", "idx_LH_star",
    "idx_K", "idx_K_star",
    "idx_pib",
    # PTF
    "ptf", "ptf_hp", "ptf_star", "A_obs", "A_pot", "A_cycle",
    # PIB Potencial y brechas
    "pib_pot", "pib_pot_hp", "brecha_pot", "brecha_pot_hp",
    "idx_trend", "brecha_hp",
    "PIB_pot", "Brecha_CD", "PIB_tend_BHP", "Brecha_BHP",
]
