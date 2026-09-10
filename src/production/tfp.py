"""Cálculo de la Productividad Total de Factores (PTF) observada y tendencial.

La PTF (Residuo de Solow) se calcula, en el motor v3 (índices base 100), como:

    ptf = idx_pib / (idx_K^alpha × idx_LH^(1-alpha))

Dos tendencias de la PTF están disponibles, ambas producidas por
``compute_tfp``:

    ptf_hp   — filtro Hodrick-Prescott de una sola pasada (λ=1600) sobre
               ``ptf``. Es la tendencia "atemporal"/estadística, usada solo
               como referencia de comparación (columna ``pib_pot_hp`` /
               ``brecha_pot_hp`` en ``pib_potencial.py``).
    ptf_star — tendencia ESTRUCTURAL estilo CBO (Shackleton, 2018): regresión
               OLS de ln(PTF) sobre una tendencia por tramos (rampas-meseta
               ancladas en los picos del ciclo BBQ) más términos cíclicos
               (brecha de desempleo, contemporánea y rezagada) y dummies de
               pandemia. PTF* = valor ajustado con los términos cíclicos y
               dummies en cero. Es la que efectivamente entra al PIB
               potencial (Cambio 2 de SPEC_V2.md — reemplaza al filtro BHP
               puramente estadístico que usaba este módulo antes de la
               alineación con v3).

``hp_filter``/``boosted_hp_filter`` se conservan como utilidades generales
(el motor de PIB potencial ya no llama a ``boosted_hp_filter`` — v3 usa HP de
una sola pasada — pero se mantienen disponibles/exportadas para quien las
necesite).
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.filters.hp_filter import hpfilter

from src.production.business_cycle import construir_variables_ciclo

logger = logging.getLogger("nairu_pipeline.production.tfp")

HP_LAMBDA_QUARTERLY: float = 1600.0
BHP_ITERATIONS: int = 3          # iteraciones por defecto del Boosted HP

# Dummies de pandemia (Cambio 1 v2, ver SPEC_V2.md): una por trimestre; las
# series de PTF son sumas móviles 4T, por lo que el choque de 2020Q2 se
# "arrastra" hasta 2021Q1.
PANDEMIC_DUMMIES: list[pd.Timestamp] = [
    pd.Timestamp("2020-06-01"), pd.Timestamp("2020-09-01"),
    pd.Timestamp("2020-12-01"), pd.Timestamp("2021-03-01"),
]


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

    Nota: el motor de PIB potencial (``compute_tfp``) NO usa esta función —
    v3 usa HP de una sola pasada (``hp_filter``) para sus columnas de
    referencia ``ptf_hp``/``idx_trend``. Se conserva como utilidad general.

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

def compute_tfp_observed(df: pd.DataFrame, T: pd.Timestamp, base_quarter: pd.Timestamp) -> pd.DataFrame:
    """Calcula la PTF observada (Residuo de Solow) sobre índices base 100.

    Fórmula
    -------
    ptf = idx_pib / (idx_K^alpha × idx_LH^(1-alpha))

    Solo se calcula en la muestra ``[base_quarter, T]`` (fuera de esa
    ventana los índices no están definidos de forma comparable).

    Parameters
    ----------
    df : pd.DataFrame
        Requiere ``date``, ``idx_pib``, ``idx_K``, ``idx_LH``, ``alpha``.

    Returns
    -------
    pd.DataFrame
        Copia con columna ``ptf`` añadida (alias: ``A_obs``).
    """
    df = df.copy()
    muestra_mask = (df["date"] >= base_quarter) & (df["date"] <= T)

    df["ptf"] = np.nan
    df.loc[muestra_mask, "ptf"] = (
        df.loc[muestra_mask, "idx_pib"]
        / (df.loc[muestra_mask, "idx_K"] ** df.loc[muestra_mask, "alpha"]
           * df.loc[muestra_mask, "idx_LH"] ** (1 - df.loc[muestra_mask, "alpha"]))
    )
    df["A_obs"] = df["ptf"]  # alias de compatibilidad

    n_nulo = df.loc[muestra_mask, "ptf"].isna().sum()
    if n_nulo > 0:
        logger.warning("ptf tiene %d valores NaN dentro de la muestra %s→%s.",
                        n_nulo, base_quarter.date(), T.date())
    return df


# ── PTF tendencial ────────────────────────────────────────────────────────────

class ResultadoRegPTF:
    """Resultado de la regresión estructural de tendencia de PTF (diagnóstico)."""

    __slots__ = ("params", "bse", "tvalues", "pvalues", "r_squared", "nobs",
                 "knots", "dummies", "modelo")

    def __init__(self, params, bse, tvalues, pvalues, r_squared, nobs, knots, dummies, modelo=None):
        self.params, self.bse = params, bse
        self.tvalues, self.pvalues = tvalues, pvalues
        self.r_squared, self.nobs = r_squared, nobs
        self.knots, self.dummies, self.modelo = knots, dummies, modelo

    def tabla(self) -> pd.DataFrame:
        return pd.DataFrame({
            "Coeficiente": self.params, "Error Est.": self.bse,
            "t": self.tvalues, "p-valor": self.pvalues,
        })

    def __repr__(self) -> str:  # pragma: no cover
        return f"ResultadoRegPTF(r_squared={self.r_squared:.4f}, nobs={self.nobs}, knots={len(self.knots)})"


def estimar_ptf_tendencia_cbo(
    df: pd.DataFrame,
    T: pd.Timestamp,
    base_quarter: pd.Timestamp,
    picos_bbq: list[pd.Timestamp],
    pandemic_dummies: list[pd.Timestamp] = PANDEMIC_DUMMIES,
) -> tuple[pd.Series, ResultadoRegPTF]:
    """Tendencia de PTF estilo CBO (Shackleton, 2018, ecuación 29 adaptada):

        ln(PTF_t) = b0 + b1*tau_t + Σ_j gamma_j*S_j(tau_t)
                    + phi0*gap_t + phi1*gap_{t-1} + Σ_m delta_m*D_m,t + eps_t

    ``tau_t`` = índice entero de trimestre desde ``base_quarter``.
    ``gap_t`` = brecha_u (TD − NAIRU, en puntos porcentuales).
    ``S_j`` = rampas-meseta ancladas en los picos del ciclo BBQ (``picos_bbq``).
    ``D_m`` = dummies puntuales de pandemia.

    PTF*_t (tendencia potencial, sin cíclicos ni dummies), para todos los
    trimestres de la muestra:

        PTF*_t = exp(b0 + b1*tau_t + Σ_j gamma_j*S_j(tau_t))

    Returns
    -------
    (ptf_star, ResultadoRegPTF)
        ``ptf_star`` indexado por fecha.
    """
    muestra_mask = (df["date"] >= base_quarter) & (df["date"] <= T)
    d = df.loc[muestra_mask, ["date", "ptf", "brecha_u"]].copy().sort_values("date").reset_index(drop=True)
    d["tau"] = range(len(d))
    d["ln_ptf"] = np.log(d["ptf"])
    d["gap"] = d["brecha_u"]
    d["gap_lag1"] = d["gap"].shift(1)

    picos_muestra = [pd.Timestamp(k) for k in picos_bbq if base_quarter <= pd.Timestamp(k) <= T]
    cyc = construir_variables_ciclo(d["date"], picos_muestra)
    knot_cols = list(cyc.columns)
    for c in knot_cols:
        d[c] = cyc[c].values

    for dt_dummy in pandemic_dummies:
        dt_label = pd.Timestamp(dt_dummy)
        d[f"dummy_{dt_label.date()}"] = (d["date"] == dt_label).astype(float)

    estim = d.dropna(subset=["ln_ptf", "gap_lag1"]).copy()

    dummy_cols = [c for c in d.columns if c.startswith("dummy_")]
    regresor_cols = ["tau"] + knot_cols + ["gap", "gap_lag1"] + dummy_cols

    X = sm.add_constant(estim[regresor_cols])
    y = estim["ln_ptf"]
    modelo = sm.OLS(y, X, missing="raise").fit()

    resultado = ResultadoRegPTF(
        params=modelo.params, bse=modelo.bse, tvalues=modelo.tvalues, pvalues=modelo.pvalues,
        r_squared=modelo.rsquared, nobs=int(modelo.nobs),
        knots=list(picos_bbq), dummies=list(pandemic_dummies), modelo=modelo,
    )

    ln_ptf_star = modelo.params["const"] + modelo.params["tau"] * d["tau"]
    for c in knot_cols:
        ln_ptf_star = ln_ptf_star + modelo.params[c] * d[c]
    ptf_star = np.exp(ln_ptf_star)
    ptf_star.index = d["date"]

    logger.info("PTF* (CBO): R²=%.4f, nobs=%d, %d nudos BBQ", resultado.r_squared, resultado.nobs, len(knot_cols))
    return ptf_star, resultado


def compute_tfp_trend(
    df: pd.DataFrame,
    T: pd.Timestamp,
    base_quarter: pd.Timestamp,
    picos_bbq: list[pd.Timestamp],
) -> pd.DataFrame:
    """Calcula ambas tendencias de PTF: ``ptf_hp`` (HP, referencia) y
    ``ptf_star`` (estructural CBO — la que entra al PIB potencial).

    Parameters
    ----------
    df : pd.DataFrame
        Requiere columna ``ptf`` (generada por ``compute_tfp_observed``).

    Returns
    -------
    pd.DataFrame
        Copia con ``ptf_hp``, ``ptf_star`` añadidas (alias: ``A_pot`` =
        ``ptf_star``, ``A_cycle`` = ``ptf`` − ``ptf_star``).
    """
    if "ptf" not in df.columns:
        raise KeyError("Se requiere la columna 'ptf'. Llame compute_tfp_observed primero.")
    df = df.copy()
    muestra_mask = (df["date"] >= base_quarter) & (df["date"] <= T)

    muestra = df.loc[muestra_mask].copy()
    _, tendencia_hp = hp_filter(muestra["ptf"], lamb=HP_LAMBDA_QUARTERLY)
    df.loc[muestra_mask, "ptf_hp"] = tendencia_hp.values

    ptf_star, _reg_ptf = estimar_ptf_tendencia_cbo(df, T, base_quarter, picos_bbq)
    df.loc[muestra_mask, "ptf_star"] = df.loc[muestra_mask, "date"].map(ptf_star)

    df["A_pot"] = df["ptf_star"]              # alias de compatibilidad
    df["A_cycle"] = df["ptf"] - df["ptf_star"]  # alias de compatibilidad

    logger.debug(
        "PTF tendencial: ptf_hp media=%.4f, ptf_star media=%.4f",
        df["ptf_hp"].mean(skipna=True), df["ptf_star"].mean(skipna=True),
    )
    return df


# ── Función de conveniencia ───────────────────────────────────────────────────

def compute_tfp(
    df: pd.DataFrame,
    T: pd.Timestamp,
    base_quarter: pd.Timestamp,
    picos_bbq: list[pd.Timestamp],
) -> pd.DataFrame:
    """Calcula ``ptf``, ``ptf_hp`` y ``ptf_star`` en un solo paso.

    Equivale a llamar ``compute_tfp_observed`` seguido de ``compute_tfp_trend``.

    Parameters
    ----------
    df : pd.DataFrame
        Requiere: ``idx_pib``, ``idx_K``, ``idx_LH``, ``alpha``, ``brecha_u``.
    T : pd.Timestamp
        Último trimestre completo (límite superior de la muestra de estimación).
    base_quarter : pd.Timestamp
        Ancla de índices (límite inferior de la muestra de estimación).
    picos_bbq : list[pd.Timestamp]
        Picos del ciclo BBQ (nudos de la tendencia por tramos de PTF*).

    Returns
    -------
    pd.DataFrame
        DataFrame con columnas ``ptf``, ``A_obs``, ``ptf_hp``, ``ptf_star``,
        ``A_pot``, ``A_cycle`` añadidas.
    """
    df = compute_tfp_observed(df, T=T, base_quarter=base_quarter)
    df = compute_tfp_trend(df, T=T, base_quarter=base_quarter, picos_bbq=picos_bbq)
    return df
