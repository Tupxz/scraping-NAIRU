"""Cálculo de los factores de producción Cobb-Douglas: Trabajo, Capital y Alpha.

Todas las funciones son puras (sin I/O): reciben un DataFrame trimestral y
retornan uno nuevo con columnas adicionales sin modificar las originales.

Columnas requeridas en el DataFrame de entrada
----------------------------------------------
    factor_trabajo  : PET, TGP, TD, NAIRU_q
    factor_capital  : K, UCI, NAICU_q
    alpha_dinamico  : compensation_employees, gross_operating_surplus, mixed_income

Columnas que produce cada función
----------------------------------
    factor_trabajo  → L_obs, L_pot      [miles de personas]
    factor_capital  → K_usado, K_pot    [millones COP 2017]
    alpha_dinamico  → alpha             [fracción 0–1]
"""

from __future__ import annotations

import logging
import warnings

import pandas as pd

logger = logging.getLogger("nairu_pipeline.production.factors")

# Alpha de respaldo cuando no hay datos de ingreso DANE (antes de 2016-Q1).
# Sólo se usa si ALPHA_FIXED es None y USE_CONSTANT_ALPHA = False.
ALPHA_FALLBACK: float = 0.40

# Si se define un valor (float), se usa ese alpha fijo para TODO el período,
# ignorando los datos de ingreso DANE y USE_CONSTANT_ALPHA.
# Fuente de verdad = Boceto / Función de Producción de los profesores (Alpha K = 0.4).
# Debe coincidir con ALPHA en build_production_function_dataset.py.
ALPHA_FIXED: float | None = 0.4

# Si True y ALPHA_FIXED es None: calcula la media de alpha desde los datos
# DANE (2016+) y la aplica uniformemente (elimina quiebre 2016).
# Si False y ALPHA_FIXED es None: usa ALPHA_FALLBACK pre-2016 (comportamiento original).
USE_CONSTANT_ALPHA: bool = True


# ── Factor Trabajo ────────────────────────────────────────────────────────────

def factor_trabajo(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula el Factor Trabajo observado y potencial.

    Fórmulas
    --------
    L_obs = PET × (TGP / 100) × (1 − TD / 100)
    L_pot = PET × (TGP / 100) × (1 − NAIRU_q / 100)

    ``L_pot`` refleja la fuerza laboral que estaría ocupada si la economía
    operara a su tasa de desempleo estructural (NAIRU*).

    Parameters
    ----------
    df : pd.DataFrame
        Requiere columnas: ``PET``, ``TGP``, ``TD``, ``NAIRU_q``.
        Si ``NAIRU_q`` no existe o es todo NaN, se usa ``TD`` como proxy
        y se emite un warning.

    Returns
    -------
    pd.DataFrame
        Copia del DataFrame con columnas ``L_obs`` y ``L_pot`` añadidas.
    """
    df = df.copy()

    fuerza = (df["TGP"] / 100.0) * df["PET"]
    df["L_obs"] = fuerza * (1.0 - df["TD"] / 100.0)

    if "NAIRU_q" not in df.columns or df["NAIRU_q"].isna().all():
        warnings.warn(
            "NAIRU_q no disponible — usando TD observada como proxy de NAIRU*. "
            "Ejecute '--nairu-estim' antes de '--pib-potencial' para resultados correctos.",
            stacklevel=2,
        )
        df["L_pot"] = df["L_obs"].copy()
    else:
        df["L_pot"] = fuerza * (1.0 - df["NAIRU_q"] / 100.0)

    logger.debug(
        "Factor Trabajo: L_obs=[%.1f, %.1f], L_pot=[%.1f, %.1f] (miles personas)",
        df["L_obs"].min(), df["L_obs"].max(),
        df["L_pot"].min(), df["L_pot"].max(),
    )
    return df


# ── Factor Capital ────────────────────────────────────────────────────────────

def factor_capital(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula el Factor Capital usado y potencial.

    Fórmulas
    --------
    K_usado = K × (UCI / 100)       ← capital efectivamente utilizado
    K_pot   = K × (NAICU_q / 100)   ← capital al nivel de utilización potencial

    ``K_pot`` usa la NAICU* (tasa de utilización de capacidad no aceleradora de
    inflación) como indicador de la utilización estructural del capital.

    Parameters
    ----------
    df : pd.DataFrame
        Requiere columnas: ``K``, ``UCI``, ``NAICU_q``.
        Si ``NAICU_q`` no existe o es todo NaN, se usa ``UCI`` como proxy
        y se emite un warning.

    Returns
    -------
    pd.DataFrame
        Copia del DataFrame con columnas ``K_usado`` y ``K_pot`` añadidas.
    """
    df = df.copy()

    df["K_usado"] = df["K"] * (df["UCI"] / 100.0)

    if "NAICU_q" not in df.columns or df["NAICU_q"].isna().all():
        warnings.warn(
            "NAICU_q no disponible — usando UCI observada como proxy de NAICU*. "
            "Ejecute '--nairu-estim' antes de '--pib-potencial' para resultados correctos.",
            stacklevel=2,
        )
        df["K_pot"] = df["K_usado"].copy()
    else:
        df["K_pot"] = df["K"] * (df["NAICU_q"] / 100.0)

    logger.debug(
        "Factor Capital: K_usado=[%.0f, %.0f], K_pot=[%.0f, %.0f] (MM COP 2017)",
        df["K_usado"].min(), df["K_usado"].max(),
        df["K_pot"].min(), df["K_pot"].max(),
    )
    return df


# ── Alpha dinámico ────────────────────────────────────────────────────────────

def alpha_dinamico(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula la participación del capital (alpha) desde el enfoque ingreso DANE.

    Fórmula
    -------
    α_t = (EBE_t + IM_t) / (RA_t + EBE_t + IM_t)   ← participación del capital

    donde:
        RA  = Remuneración de los Asalariados (compensation_employees)
        EBE = Excedente Bruto de Explotación  (gross_operating_surplus)
        IM  = Ingreso Mixto                    (mixed_income)

    El denominador es el Ingreso Nacional aproximado a precios corrientes.
    ``1 − α = RA / (RA+EBE+IM)`` es la participación del trabajo.

    Nota: por defecto el pipeline usa ``ALPHA_FIXED = 0.4`` (calibración de los
    profesores), por lo que esta rama dinámica queda disponible pero inactiva.

    Para periodos anteriores a 2016-Q1 (inicio de la serie de ingreso DANE),
    se usa ``ALPHA_FALLBACK = 0.40`` como calibración de respaldo.

    Parameters
    ----------
    df : pd.DataFrame
        Requiere columnas: ``compensation_employees``, ``gross_operating_surplus``,
        ``mixed_income``. Pueden ser NaN para periodos sin datos de ingreso.

    Returns
    -------
    pd.DataFrame
        Copia del DataFrame con columna ``alpha`` añadida (fracción 0–1).
    """
    df = df.copy()

    # ── Prioridad 1: ALPHA_FIXED definido por el usuario ─────────────────
    if ALPHA_FIXED is not None:
        df["alpha"] = ALPHA_FIXED
        logger.info("Alpha FIJO = %.4f (calibración manual)", ALPHA_FIXED)
        return df

    # ── Prioridad 2: calcular desde datos de ingreso DANE ─────────────────
    cols_ingreso = ["compensation_employees", "gross_operating_surplus", "mixed_income"]
    tiene_ingreso = all(c in df.columns for c in cols_ingreso)

    if not tiene_ingreso:
        logger.warning(
            "Columnas de ingreso DANE no disponibles — usando alpha de respaldo = %.2f",
            ALPHA_FALLBACK,
        )
        df["alpha"] = ALPHA_FALLBACK
        return df

    ra  = df["compensation_employees"]
    ebe = df["gross_operating_surplus"]
    im  = df["mixed_income"]

    ingreso_total = ra + ebe + im
    # α = (EBE + IM) / (RA + EBE + IM)  ← participación del capital
    alpha_serie = (ebe + im) / ingreso_total
    alpha_serie = alpha_serie.clip(0.20, 0.80)

    if USE_CONSTANT_ALPHA:
        # Usar la media del período con datos DANE (2016+) como constante uniforme.
        # Si todos los valores son NaN (sin datos de ingreso), usar ALPHA_FALLBACK.
        alpha_mean_raw = alpha_serie.dropna().mean()
        alpha_mean = ALPHA_FALLBACK if pd.isna(alpha_mean_raw) else float(alpha_mean_raw)
        df["alpha"] = alpha_mean
        logger.info(
            "Alpha CONSTANTE = %.4f (media de %d trimestres DANE 2016+)",
            alpha_mean, alpha_serie.notna().sum(),
        )
    else:
        # Comportamiento dinámico: reemplaza NaN pre-2016 con ALPHA_FALLBACK
        alpha_serie = alpha_serie.where(alpha_serie.notna(), other=ALPHA_FALLBACK)
        df["alpha"] = alpha_serie
        logger.debug(
            "Alpha DINÁMICO: media=%.3f, min=%.3f, max=%.3f (fallback=%.2f para %d trim.)",
            df["alpha"].mean(), df["alpha"].min(), df["alpha"].max(),
            ALPHA_FALLBACK, df["alpha"].isna().sum(),
        )

    return df


# ── Función de conveniencia ───────────────────────────────────────────────────

def compute_all_factors(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica las tres funciones de factores en secuencia.

    Equivale a llamar ``factor_trabajo``, ``factor_capital`` y
    ``alpha_dinamico`` en orden, propagando el DataFrame entre ellas.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame trimestral alineado con todas las columnas fuente.

    Returns
    -------
    pd.DataFrame
        DataFrame con columnas adicionales: ``L_obs``, ``L_pot``,
        ``K_usado``, ``K_pot``, ``alpha``.
    """
    df = factor_trabajo(df)
    df = factor_capital(df)
    df = alpha_dinamico(df)
    return df
