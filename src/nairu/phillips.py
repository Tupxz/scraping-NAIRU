"""Curva de Phillips parcial — la gráfica "de residuales" del tablero.

Qué es
------
La ecuación de medición del modelo NAIRU/NAICU (``model_core.py``, spec
"hysteresis" + "distributed_lag"; paper metodológico, sección 4.1) es::

    Δg_t = α + ρ1·Δg_{t-1} + ρ2·Δg_{t-2} + γe·E_t + γo·O_t
           + Σ_{j=0..2} β_u,j · (u_{t-j} − u*_t)      <- bloque desempleo/NAIRU
           + Σ_{j=0..2} β_c,j · (c_{t-j} − c*_t)      <- bloque ICU/NAICU
           + ε_t

con ``g_t`` = inflación básica_t − meta_t, ``Δg_t = g_t − g_{t-1}`` (la
variable dependiente real del modelo: el CAMBIO de la brecha de inflación, no
su nivel), ``u*_t`` = NAIRU y ``c*_t`` = NAICU. El MISMO nivel de estado
``*_t`` resta a los 3 rezagos de desempleo/ICU (no un ``*_{t-j}``): así está en
la ecuación del paper y así lo arma ``_kalman_pass`` (``signal`` con los
niveles crudos + ``observation_loadings @ state`` con los coeficientes sumados).

La "curva de Phillips parcial" aísla el efecto de la brecha de desempleo::

    ajustado_sin_desempleo_t = α + ρ1·Δg_{t-1} + ρ2·Δg_{t-2} + γe·E_t + γo·O_t
                               + Σ_j β_c,j · (c_{t-j} − c*_t)
    phillips_parcial_t       = Δg_t − ajustado_sin_desempleo_t
                             = Σ_j β_u,j · (u_{t-j} − u*_t) + ε̂_t

Es decir: la dependiente "limpia" de todos los regresores salvo el bloque
desempleo/NAIRU (más el error), que el tablero grafica contra la brecha de
desempleo contemporánea ``u_t − u*_t``. Es una gráfica de regresión parcial
("added-variable plot") simplificada: solo se limpia el eje Y, el eje X queda
en la brecha cruda — tal como se pidió.

Cómo leerla (importante)
------------------------
* NAIRU/NAICU son los estados SUAVIZADOS (RTS, muestra completa) que publica
  ``outputs/nairu/nairu_colombia.csv`` — los mismos del resto del tablero —, no
  el estado predicho un paso adelante que ve la verosimilitud. Por eso
  ``phillips_parcial`` no es exactamente la innovación del filtro; es la
  versión "de gráfica", consistente con el NAIRU que se ve en el eje X.
* La nube es ruidosa por construcción: la pendiente agregada del modelo
  (Σβ_u, fila ``unemployment_gap_coefficient`` de
  ``nairu_mle_coefficients.csv``) es pequeña y, con la estimación vigente
  (2026-09-17), NO es significativa (Σβ_u = −0.0201, p = 0.118), así que el
  ajuste OLS bivariado de la nube sale casi plano (R² ≈ 0.001). Se verificó
  que no es un artefacto del suavizado: con el estado predicho, el filtrado o
  el suavizado el R² de la nube es ≈ 0 en los tres casos.

Este módulo es solo cálculo (sin I/O salvo ``load_coefficient_table``); lo usa
``src/pipelines/export_web_data.py`` para agregar ``dg_inf`` y
``phillips_parcial`` a ``docs/data/nairu_monthly.csv``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

__all__ = [
    "REQUIRED_COLUMNS",
    "REQUIRED_COEFFICIENTS",
    "load_coefficient_table",
    "coefficients_from_table",
    "unemployment_slope_summary",
    "partial_phillips",
]

# Columnas de outputs/nairu/nairu_colombia.csv que necesita el cálculo.
REQUIRED_COLUMNS: tuple[str, ...] = (
    "inflation_gap_change",       # Δg_t   (dependiente)
    "inflation_gap_change_lag1",  # Δg_{t-1}
    "inflation_gap_change_lag2",  # Δg_{t-2}
    "unemployment_current",       # u_t
    "unemployment_lag1",          # u_{t-1}
    "unemployment_lag2",          # u_{t-2}
    "icu_current",                # c_t
    "icu_lag1",                   # c_{t-1}
    "icu_lag2",                   # c_{t-2}
    "expected_inflation_term",    # E_t
    "oil_shock",                  # O_t
    "nairu_estimate",             # u*_t (suavizado)
    "naicu_estimate",             # c*_t (suavizado)
)

# Coeficientes de outputs/nairu/nairu_mle_coefficients.csv (nombres de
# PARAMETER_NAMES en model_core.py).
REQUIRED_COEFFICIENTS: tuple[str, ...] = (
    "intercept",
    "inflation_lag1_coefficient",
    "inflation_lag2_coefficient",
    "unemployment_coefficient_0",
    "unemployment_coefficient_1",
    "unemployment_coefficient_2",
    "icu_coefficient_0",
    "icu_coefficient_1",
    "icu_coefficient_2",
    "expectations_coefficient",
    "oil_shock_coefficient",
)


def _require(available: Iterable[str], required: Iterable[str], what: str) -> None:
    available = set(available)
    missing = [name for name in required if name not in available]
    if missing:
        raise ValueError(f"{what}: faltan {missing} para la curva de Phillips parcial.")


def load_coefficient_table(path: Path | str) -> pd.DataFrame:
    """Lee ``nairu_mle_coefficients.csv`` (índice = nombre del parámetro)."""
    return pd.read_csv(path, index_col="parameter")


def coefficients_from_table(table: pd.DataFrame) -> dict[str, float]:
    """Estimaciones puntuales de ``REQUIRED_COEFFICIENTS`` como dict de floats."""
    _require(table.index, REQUIRED_COEFFICIENTS, "tabla de coeficientes")
    return {name: float(table.loc[name, "estimate"]) for name in REQUIRED_COEFFICIENTS}


def unemployment_slope_summary(table: pd.DataFrame) -> dict[str, float | None]:
    """Pendiente agregada del modelo, Σβ_u, y su p-valor (``None`` si no hay).

    Usa la fila derivada ``unemployment_gap_coefficient`` (= β_u0+β_u1+β_u2) de
    la tabla del MLE; si esa fila no existe (formato antiguo del CSV) suma los
    tres coeficientes a mano y deja el p-valor en ``None``.
    """
    row = "unemployment_gap_coefficient"
    if row in table.index:
        total = float(table.loc[row, "estimate"])
        p_value = table.loc[row, "p_value"] if "p_value" in table.columns else None
        p_value = float(p_value) if p_value is not None and pd.notna(p_value) else None
        return {"sum_beta_u": total, "p_value": p_value}
    betas = [f"unemployment_coefficient_{j}" for j in range(3)]
    _require(table.index, betas, "tabla de coeficientes")
    return {"sum_beta_u": float(sum(table.loc[b, "estimate"] for b in betas)), "p_value": None}


def partial_phillips(nairu: pd.DataFrame, coef: Mapping[str, float]) -> pd.DataFrame:
    """Curva de Phillips parcial a partir de ``nairu_colombia.csv`` + coeficientes.

    Devuelve un DataFrame con el mismo índice que ``nairu`` y columnas:

    * ``dg_inf``                 — Δg_t, cambio de la brecha de inflación (pp).
    * ``brecha_laboral``         — u_t − u*_t (pp), el eje X de la gráfica.
    * ``bloque_desempleo``       — Σ_j β_u,j·(u_{t-j} − u*_t): lo que el modelo
      le atribuye al desempleo/NAIRU en ese mes.
    * ``ajustado_sin_desempleo`` — valor ajustado con TODOS los demás regresores.
    * ``phillips_parcial``       — ``dg_inf − ajustado_sin_desempleo``, el eje Y.

    Una fila con cualquier regresor faltante da NaN (nunca se rellena con 0).
    """
    _require(nairu.columns, REQUIRED_COLUMNS, "nairu_colombia.csv")
    _require(coef.keys(), REQUIRED_COEFFICIENTS, "coeficientes")
    c = {name: float(coef[name]) for name in REQUIRED_COEFFICIENTS}

    nairu_t = nairu["nairu_estimate"]
    naicu_t = nairu["naicu_estimate"]

    unemployment_block = (
        c["unemployment_coefficient_0"] * (nairu["unemployment_current"] - nairu_t)
        + c["unemployment_coefficient_1"] * (nairu["unemployment_lag1"] - nairu_t)
        + c["unemployment_coefficient_2"] * (nairu["unemployment_lag2"] - nairu_t)
    )
    icu_block = (
        c["icu_coefficient_0"] * (nairu["icu_current"] - naicu_t)
        + c["icu_coefficient_1"] * (nairu["icu_lag1"] - naicu_t)
        + c["icu_coefficient_2"] * (nairu["icu_lag2"] - naicu_t)
    )
    fitted_without_unemployment = (
        c["intercept"]
        + c["inflation_lag1_coefficient"] * nairu["inflation_gap_change_lag1"]
        + c["inflation_lag2_coefficient"] * nairu["inflation_gap_change_lag2"]
        + c["expectations_coefficient"] * nairu["expected_inflation_term"]
        + c["oil_shock_coefficient"] * nairu["oil_shock"]
        + icu_block
    )

    return pd.DataFrame(
        {
            "dg_inf": nairu["inflation_gap_change"],
            "brecha_laboral": nairu["unemployment_current"] - nairu_t,
            "bloque_desempleo": unemployment_block,
            "ajustado_sin_desempleo": fitted_without_unemployment,
            "phillips_parcial": nairu["inflation_gap_change"] - fitted_without_unemployment,
        },
        index=nairu.index,
    )
