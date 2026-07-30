"""Cálculo del VIOG (Variance-Inverse-Of-Gap weighted output gap).

Pipeline genérico: acepta cualquier serie con columnas (serie, tendencia_referencia).
Por defecto opera sobre PIB_USA.xlsx con columnas PIB / Potential_PIB.

Pasos:
  1. load_series      — Carga Excel, construye PeriodIndex trimestral.
  2. apply_filters    — BK, CF, Butterworth, BHP, Kalman/UCM.
  3. compute_gaps     — Logaritmos y brechas (ln serie − ln tendencia).
  4. compute_viog_weights — Ponderadores VIOG y 1/VIOG por error acumulado.
  5. compute_weighted_gap — Potencial ponderado y brecha final.
  6. plot_filters     — Gráficas de tendencias y brechas (opcional).

Notas:
  - Baxter-King recorta K=12 trimestres de cada extremo → NaN en extremos para gap_bk.
  - El divisor N usa len(df) (generalización del notebook original).
  - BHP (Boosted Hodrick-Prescott) usa iterations=3 y lambda=cfg.hp_lambda (1600).
  - Christiano-Fitzgerald: por defecto (cfg.cf_one_sided=True) se usa la versión
    de UNA COLA (causal / tiempo real): trend_cf en cada t depende SOLO de
    y_1..y_t — fórmula asimétrica de Christiano & Fitzgerald (2003, IER 44(2))
    con nf=0 adelantos, equivalente a evaluar el borde derecho del filtro sobre
    la muestra expansiva y[:t+1]. La brecha resultante es la que un hacedor de
    política habría visto en tiempo real y NO se revisa retroactivamente al
    llegar datos nuevos (crítica de Orphanides 2001 AER; 2003 JME) — es el
    análogo "filtered" del "smoothed" que ya se distingue para el Kalman.
    Warm-up: las primeras cf_min_obs−1 obs (default 2*cf_high = 64 trimestres)
    quedan en NaN y su peso VIOG es 0 (mismo mecanismo que los extremos de BK).
    Con cfg.cf_one_sided=False se reproduce la versión anterior de dos colas
    (statsmodels cffilter, drift=False) para comparación/depuración.
  - Kalman usa UnobservedComponents(level="local linear trend", drift=True, cycle=True).
    Pendiente estocástica (slope varía en el tiempo). Se ajusta en niveles.
    El nivel suavizado (result.level.smoothed) es la tendencia/potencial.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

logger = logging.getLogger("nairu_pipeline.viog")

_GAP_VARS_WITH_REF    = ["bk", "cf", "bw", "bhp", "kalman", "ref"]
_GAP_VARS_WITHOUT_REF = ["bk", "cf", "bw", "bhp", "kalman"]
_FILTER_LABELS = {
    "bk":     "Baxter-King",
    "cf":     "Christiano-Fitzgerald",
    "bw":     "Butterworth",
    "bhp":    "Boosted Hodrick-Prescott",
    "kalman": "Kalman/UCM",
    "ref":    "Referencia",
}


# ── 1. Carga ──────────────────────────────────────────────────────────

def load_series(
    path: Path,
    series_col: str = "PIB",           # tests usan "PIB"; archivo real (PIB_USA.xlsx) usa "Value(Billions)"
    ref_col: Optional[str] = "Potential_PIB",  # tests usan "Potential_PIB"; archivo real usa "Potential Value(Billions)"
) -> pd.DataFrame:
    """Carga un Excel y construye PeriodIndex trimestral.

    Parameters
    ----------
    path:       Ruta al Excel.
    series_col: Columna de la serie observada.
    ref_col:    Columna de tendencia de referencia (e.g. CBO, función de producción).
                Si es None, no se carga referencia y el VIOG usa solo los 5 filtros.
    """
    df = pd.read_excel(path)

    if "Year" in df.columns and "Quarter" in df.columns:
        df["_period"] = pd.PeriodIndex.from_fields(
            year=df["Year"], quarter=df["Quarter"], freq="Q"
        )
    else:
        df["_period"] = pd.DatetimeIndex(df["t"]).to_period("Q")

    df = df.sort_values("_period").set_index("_period")
    df.index.name = "t"
    rename_map = {series_col: "Y"}
    if ref_col:
        rename_map[ref_col] = "Y_ref"
    df = df.rename(columns=rename_map)
    df["_series_label"] = series_col
    return df


# Alias para compatibilidad con código anterior
def load_pib_usa(path: Path) -> pd.DataFrame:
    return load_series(path, series_col="PIB", ref_col="Potential_PIB")


# ── 2. Filtros ────────────────────────────────────────────────────────

def cf_filter_one_sided(
    x: np.ndarray,
    low: float = 6,
    high: float = 32,
    min_obs: Optional[int] = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Filtro Christiano-Fitzgerald de UNA COLA (causal / tiempo real).

    Implementa el filtro asimétrico "random walk" de Christiano & Fitzgerald
    (2003, "The Band Pass Filter", International Economic Review 44(2),
    435-465) restringido a nf=0 adelantos: el ciclo en cada t depende
    ÚNICAMENTE de x_1..x_t. Es la estimación que un analista podía calcular
    en el trimestre t, y no se revisa al llegar observaciones futuras
    (a diferencia de la versión de dos colas de ``statsmodels.cffilter``,
    que usa adelantos dentro de la muestra y revisa toda la historia con
    cada dato nuevo — crítica de Orphanides 2001 AER; 2003 JME).

    Identidad clave: para el filtro asimétrico de CF, el valor en el ÚLTIMO
    punto de cualquier muestra de tamaño n es causal por construcción:

        c_n = 0.5·B0·x_n + Σ_{j=1}^{n-2} B_j·x_{n-j} + B̃_{n-1}·x_1,
        B̃_{n-1} = −0.5·B0 − Σ_{j=1}^{n-2} B_j,

    con B0=(b−a)/π, B_j=(sin(j·b)−sin(j·a))/(π·j), a=2π/high, b=2π/low.
    Aplicar esta fórmula de borde sobre la muestra expansiva x[0..t] para
    cada t ES el filtro de una cola, y coincide a precisión de máquina con
    correr ``cffilter(x[:t+1], drift=False)`` y quedarse con el último valor
    (la ruta pragmática O(T²)), porque statsmodels implementa exactamente el
    filtro asimétrico random-walk de CF. La ruta analítica evita depender de
    ese detalle interno y solo requiere numpy.

    Parameters
    ----------
    x:        Serie 1-D a filtrar (niveles, igual que el uso actual de
              cffilter en apply_filters).
    low:      Período mínimo de oscilación (default 6 trimestres = 1.5 años).
    high:     Período máximo de oscilación (default 32 trimestres = 8 años).
    min_obs:  Warm-up — mínimo de observaciones acumuladas antes del primer
              valor no-NaN. Con menos historia el filtro de borde es poco
              fiable (la cola de pesos B̃ concentra demasiada masa en x_1).
              None → 2*high (64 trimestres con high=32).

    Returns
    -------
    (cycle, trend): tupla de arrays, mismo contrato que ``cffilter``.
        trend = x − cycle. NaN durante el warm-up.
    """
    x = np.asarray(x, dtype=float)
    if x.ndim == 2 and x.shape[1] == 1:
        x = x[:, 0]
    if x.ndim != 1:
        raise ValueError("cf_filter_one_sided solo soporta series 1-D")
    if low < 2:
        raise ValueError("low debe ser >= 2")
    nobs = x.shape[0]
    if min_obs is None:
        min_obs = int(np.ceil(2 * high))
    min_obs = max(int(min_obs), 3)

    a = 2 * np.pi / high
    b = 2 * np.pi / low

    j = np.arange(1, nobs)
    B = np.empty(nobs) if nobs else np.empty(0)
    if nobs:
        B[0] = (b - a) / np.pi
    if nobs > 1:
        B[1:] = (np.sin(b * j) - np.sin(a * j)) / (np.pi * j)
    csum = np.cumsum(B)  # csum[k] = B0 + B_1 + ... + B_k

    cycle = np.full(nobs, np.nan)
    for t in range(min_obs - 1, nobs):
        # c_t = 0.5·B0·x[t] + Σ_{j=1}^{t-1} B_j·x[t-j] + B̃_t·x[0]
        btilde = -0.5 * B[0] - (csum[t - 1] - B[0])
        cycle[t] = 0.5 * B[0] * x[t] + B[1:t] @ x[t - 1:0:-1] + btilde * x[0]

    trend = x - cycle
    return cycle, trend


def apply_filters(df: pd.DataFrame, cfg=None) -> pd.DataFrame:
    """Aplica los 5 filtros de tendencia a la columna Y."""
    from scipy.signal import butter, filtfilt
    from statsmodels.tsa.filters.bk_filter import bkfilter
    from statsmodels.tsa.filters.cf_filter import cffilter
    from statsmodels.tsa.statespace.structural import UnobservedComponents

    from src.production.tfp import BHP_ITERATIONS, boosted_hp_filter

    if cfg is None:
        from src.config import VIOG_CONFIG
        cfg = VIOG_CONFIG
    y = df["Y"].astype(float)

    # Baxter-King (recorta K obs de cada extremo → NaN en extremos)
    bk_cycle = bkfilter(y, low=cfg.bk_low, high=cfg.bk_high, K=cfg.bk_K)
    trend_bk = pd.Series(np.nan, index=df.index)
    valid = df.index[cfg.bk_K: len(df) - cfg.bk_K]
    trend_bk[valid] = y[valid].values - bk_cycle
    df["trend_bk"] = trend_bk

    # Christiano-Fitzgerald
    # cf_one_sided=True (default): versión causal / una cola — trend_cf en t
    # usa SOLO y_1..y_t (estimación de tiempo real, sin revisión retroactiva).
    # cf_one_sided=False: versión de dos colas de statsmodels (usa adelantos
    # dentro de la muestra; solo el último punto de la muestra es causal).
    if cfg.cf_one_sided:
        _, cf_trend_arr = cf_filter_one_sided(
            y.to_numpy(), low=cfg.cf_low, high=cfg.cf_high, min_obs=cfg.cf_min_obs
        )
        df["trend_cf"] = cf_trend_arr
    else:
        _, cf_trend = cffilter(y, low=cfg.cf_low, high=cfg.cf_high, drift=False)
        df["trend_cf"] = cf_trend.values

    # Butterworth
    b, a = butter(N=cfg.bw_order, Wn=cfg.bw_cutoff, btype="low")
    df["trend_bw"] = filtfilt(b, a, y.values)

    # Boosted Hodrick-Prescott (Phillips & Shi, 2021)
    y_series = pd.Series(y.values, index=df.index, name="Y")
    _, bhp_trend = boosted_hp_filter(y_series, lamb=cfg.hp_lambda, iterations=BHP_ITERATIONS)
    df["trend_bhp"] = bhp_trend.values

    # ── Kalman / UCM — Structural Time Series (equivalente a Stata ucm) ─────────
    # Modelo STS estimado por máxima verosimilitud vía filtro de Kalman:
    #
    #   Observación:  y_t = μ_t + c_t + ε_t        (irregular si cfg.kalman_irregular)
    #   Nivel:        μ_t = μ_{t-1} + β_{t-1} + η_t   (local linear trend)
    #   Pendiente:    β_t = β_{t-1} + ζ_t             (slope estocástico)
    #   Ciclo:        c_t sigue AR(2) con frecuencia λ = 2π/período y
    #                 amortiguamiento ρ < 1 si damped_cycle=True
    #
    # Stata equivalente:
    #   ucm lnpib, level slope cycle(1) cyclelen(32) cyc1rho(.) nolog
    #
    # NOTA sobre identificación: la función de log-verosimilitud tiene dos
    # máximos locales:
    #   (a) Solución DEGENERADA (llf≈296): σ²_ciclo→0, brecha≈0.  ← L-BFGS-B default
    #   (b) Solución ECONÓMICA  (llf≈290): σ²_ciclo≈Var(ΔlnPIB),  ← Stata
    #       brecha significativa (COVID 2020Q2 ≈ −19%).
    # Stata elige (b) por sus valores iniciales. Para replicarlo debemos
    # inicializar σ²_ciclo ≈ Var(ΔlnPIB) y ρ_ciclo ≈ 0.70.
    #
    # La tendencia (nivel suavizado) es el PIB potencial estimado.
    logger.info("[VIOG] Ajustando Kalman UCM (STS: level + slope + cycle)...")
    import warnings as _warnings
    # ── IMPORTANTE: aplicar UCM al log(PIB) igual que Stata 'ucm lnpib ...' ──────
    # Stata: gen lnpib = ln(pib)  →  ucm lnpib, level slope cycle(1) cyclelen(32)
    # El modelo STS es lineal → debe estimarse en logaritmos para ser equivalente.
    # La tendencia en logs se reconvierte al nivel: trend = exp(level.smoothed).
    y_log = np.log(y.values.astype(float))

    ucm_kwargs: dict = dict(
        endog=y_log,
        level="local linear trend",   # nivel + pendiente estocástica (= stata: level slope)
        cycle=True,
        stochastic_cycle=cfg.kalman_stochastic_cycle,
        damped_cycle=cfg.kalman_damped_cycle,
        irregular=cfg.kalman_irregular,
    )
    if cfg.kalman_cycle_period_bounds is not None:
        ucm_kwargs["cycle_period_bounds"] = cfg.kalman_cycle_period_bounds
    ucm = UnobservedComponents(**ucm_kwargs)

    # ── Valores iniciales en la cuenca del máximo económico (≈ Stata) ─────────
    # Var(ΔlnPIB) es la varianza de las primeras diferencias del log-PIB.
    # Inicializar σ²_ciclo ≈ Var(ΔlnPIB) y ρ ≈ 0.70 evita que L-BFGS-B
    # converja al máximo degenerado donde σ²_ciclo → 0.
    _var_dy = float(np.var(np.diff(y_log)))   # ← usa log-PIB, no nivel
    _sp = ucm.start_params.copy()
    _pnames = ucm.param_names
    _i_cyc = next((i for i, n in enumerate(_pnames) if "cycle" in n and "sigma" in n), None)
    _i_rho = next((i for i, n in enumerate(_pnames) if "damping" in n), None)
    _i_lev = next((i for i, n in enumerate(_pnames) if "level" in n and "sigma" in n), None)
    _i_trn = next((i for i, n in enumerate(_pnames) if "trend" in n and "sigma" in n), None)
    if _i_cyc is not None:
        _sp[_i_cyc] = _var_dy           # σ²_ciclo ≈ Var(ΔlnPIB) — cuenca económica
    if _i_rho is not None:
        _sp[_i_rho] = 0.70              # ρ ≈ 0.70 — persistencia cíclica razonable
    if _i_lev is not None:
        _sp[_i_lev] = _var_dy * 0.01   # σ²_nivel pequeña (tendencia suave)
    if _i_trn is not None:
        _sp[_i_trn] = _var_dy * 0.001  # σ²_pendiente muy pequeña
    # Frecuencia inicial ≈ ciclo largo (período dominante = upper bound)
    _i_freq = next((i for i, n in enumerate(_pnames) if "frequency" in n), None)
    if _i_freq is not None and cfg.kalman_cycle_period_bounds is not None:
        _sp[_i_freq] = 2 * np.pi / cfg.kalman_cycle_period_bounds[1]  # período máximo

    with _warnings.catch_warnings():
        _warnings.simplefilter("ignore")
        result = ucm.fit(start_params=_sp, disp=False)
    # Reconvertir tendencia en log → nivel original (exp del nivel suavizado)
    _trend_log = result.level.smoothed.copy()
    # Enmascarar burn-in: las primeras N obs tienen condiciones iniciales
    # no fiables (Stata también las omite, típicamente 2 obs).
    if cfg.kalman_burnin_periods > 0:
        _trend_log[: cfg.kalman_burnin_periods] = np.nan
    df["trend_kalman"] = np.exp(_trend_log)

    return df


# ── 3. Brechas ────────────────────────────────────────────────────────

def compute_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula logaritmos y brechas (ln Y − ln tendencia).

    Si la columna ``Y_ref`` no existe (no se pasó referencia), omite
    ``gap_ref`` y ``ln_Y_ref``.
    """
    df["ln_Y"] = np.log(df["Y"])
    for tag in ["bk", "cf", "bw", "bhp", "kalman"]:
        df[f"ln_trend_{tag}"] = np.log(df[f"trend_{tag}"])
        df[f"gap_{tag}"]      = df["ln_Y"] - df[f"ln_trend_{tag}"]
    if "Y_ref" in df.columns:
        df["ln_Y_ref"] = np.log(df["Y_ref"])
        df["gap_ref"]  = df["ln_Y"] - df["ln_Y_ref"]
    return df


# ── 4. Ponderadores VIOG ──────────────────────────────────────────────

def compute_viog_weights(df: pd.DataFrame, gap_vars: list[str] | None = None) -> pd.DataFrame:
    """Calcula ponderadores VIOG (rev) y 1/VIOG (inv_rev).

    Donde BK tiene NaN (extremos recortados), su peso es 0 y los otros
    filtros se renormalizan automáticamente para sumar 1.

    Parameters
    ----------
    gap_vars: Lista de filtros a incluir. Por defecto incluye "ref" si existe.
    """
    if gap_vars is None:
        gap_vars = _GAP_VARS_WITH_REF if "gap_ref" in df.columns else _GAP_VARS_WITHOUT_REF
    df["_active_gap_vars"] = str(gap_vars)  # guardamos para compute_weighted_gap

    N = len(df)
    for v in gap_vars:
        df[f"rev_{v}"]     = df[f"gap_{v}"].abs().cumsum() / N
        df[f"inv_rev_{v}"] = 1.0 / df[f"rev_{v}"]

    # fillna(0) → BK aporta 0 en extremos, los demás se renormalizan
    rev_filled     = {v: df[f"rev_{v}"].fillna(0)     for v in gap_vars}
    inv_rev_filled = {v: df[f"inv_rev_{v}"].fillna(0) for v in gap_vars}

    df["_rev_total"]     = sum(rev_filled[v]     for v in gap_vars)
    df["_inv_rev_total"] = sum(inv_rev_filled[v] for v in gap_vars)

    for v in gap_vars:
        df[f"weight_rev_{v}"]     = rev_filled[v]     / df["_rev_total"]
        df[f"weight_inv_rev_{v}"] = inv_rev_filled[v] / df["_inv_rev_total"]
    return df


# ── 5. Brecha VIOG final ──────────────────────────────────────────────

def compute_weighted_gap(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula el potencial ponderado y las brechas VIOG finales."""
    _trend_ln = {
        "bk": "ln_trend_bk", "cf": "ln_trend_cf", "bw": "ln_trend_bw",
        "bhp": "ln_trend_bhp", "kalman": "ln_trend_kalman", "ref": "ln_Y_ref",
    }
    # Determinar filtros activos (guardados en compute_viog_weights)
    import ast
    gap_vars = ast.literal_eval(df["_active_gap_vars"].iloc[0])

    # fillna(0): donde peso=0 y tendencia=NaN, contribución es 0
    df["ln_potential_viog"] = sum(
        (df[f"weight_rev_{v}"] * df[_trend_ln[v]]).fillna(0) for v in gap_vars
    )
    df["ln_potential_inv_viog"] = sum(
        (df[f"weight_inv_rev_{v}"] * df[_trend_ln[v]]).fillna(0) for v in gap_vars
    )
    df["gap_viog"]     = df["ln_Y"] - df["ln_potential_viog"]
    df["gap_inv_viog"] = df["ln_Y"] - df["ln_potential_inv_viog"]
    return df


# ── 6. Gráficas ───────────────────────────────────────────────────────

def plot_filters(
    df: pd.DataFrame,
    save_dir: Optional[Path] = None,
    show: bool = True,
) -> None:
    """Replica exactamente las gráficas del notebook original VIOG.

    Genera las siguientes figuras (igual que las celdas del notebook):
      1. viog_01_levels.png         — PIB y PIB Potencial en niveles
      2. viog_02_filter_bk.png      — Filtro Baxter-King (niveles)
      3. viog_03_filter_cf.png      — Filtro Christiano-Fitzgerald
      4. viog_04_filter_bw.png      — Filtro Butterworth
      5. viog_05_filter_bhp.png     — Filtro Boosted Hodrick-Prescott
      6. viog_06_filter_kalman.png  — Filtro Kalman/UCM
      7. viog_07_gaps.png           — Todas las brechas (ln)
      8. viog_08_ln_levels.png      — ln PIB vs ln Potencial (vline 2020Q2)
      9. viog_09_growth_gap.png     — Crecimiento PIB vs gap potencial
     10. viog_10_ln_trends.png      — PIB potencial ln por cada filtro
     11. viog_11_gaps_all.png       — Brecha del producto por cada filtro
     12. viog_12_viog_potential.png — ln PIB vs potencial VIOG y 1/VIOG
     13. viog_13_viog_gap.png       — Crecimiento vs brecha VIOG y 1/VIOG
     14. viog_14_kalman_gap.png     — Brecha Kalman únicamente
     15. viog_15_bhp_gap.png        — Brecha Boosted Hodrick-Prescott únicamente

    Parameters
    ----------
    save_dir: Si se indica, guarda los PNG en ese directorio.
    show:     Si True, llama plt.show().
    """
    x = df.index.to_timestamp()
    has_ref = "Y_ref" in df.columns

    # Crecimiento YoY del PIB (equivalente a Variation del notebook)
    variation = df["Y"].pct_change(4)

    def _save_show(fig: plt.Figure, name: str) -> None:
        plt.tight_layout()
        if save_dir:
            fig.savefig(save_dir / name, dpi=150)
        if show:
            plt.show()
        plt.close(fig)

    # ── Gráfica 1: PIB y PIB Potencial ───────────────────────────────
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(x, df["Y"], label="PIB")
    if has_ref:
        ax.plot(x, df["Y_ref"], label="Potential_PIB")
    ax.set_title("PIB y PIB Potencial")
    ax.legend()
    ax.grid(True)
    _save_show(fig, "viog_01_levels.png")

    # ── Gráficas 2-6: Cada filtro en niveles ─────────────────────────
    _filters = [
        ("bk",     "trend_bk",     "Filtro Baxter-King",                    "02"),
        ("cf",     "trend_cf",     "Filtro Christiano-Fitzgerald",           "03"),
        ("bw",     "trend_bw",     "Filtro Butterworth",                     "04"),
        ("bhp",    "trend_bhp",    "Filtro Boosted Hodrick-Prescott",        "05"),
        ("kalman", "trend_kalman", "Filtro Kalman / UCM",                    "06"),
    ]
    _trend_col_name = {
        "bk": "bkt_PIB", "cf": "cft_PIB", "bw": "bwt_PIB",
        "bhp": "bhpt_PIB", "kalman": "kalmant_PIB",
    }
    for tag, trend_col, title, num in _filters:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(x, df["Y"], label="PIB")
        if has_ref:
            ax.plot(x, df["Y_ref"], label="Potential_PIB")
        ax.plot(x, df[trend_col], label=_trend_col_name[tag])
        ax.set_title(title)
        ax.legend()
        ax.grid(True)
        _save_show(fig, f"viog_{num}_filter_{tag}.png")

    # ── Gráfica 7: Todas las brechas (ln) ────────────────────────────
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(x, df["gap_bk"],    label="gap_bk")
    ax.plot(x, df["gap_cf"],    label="gap_cf")
    ax.plot(x, df["gap_bw"],    label="gap_bw")
    ax.plot(x, df["gap_bhp"],   label="gap_bhp")
    ax.plot(x, df["gap_kalman"], label="gap_kalman")
    if has_ref:
        ax.plot(x, df["gap_ref"], label="gap_potential")
    ax.set_title("Gaps")
    ax.legend()
    plt.tight_layout()
    _save_show(fig, "viog_07_gaps.png")

    # ── Gráfica 8: ln PIB vs ln Potencial (vline 2020Q2) ─────────────
    tline = pd.Period("2020Q2", freq="Q").to_timestamp()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(x, df["ln_Y"], label="Ln real GDP")
    if has_ref:
        ax.plot(x, df["ln_Y_ref"], label="Ln Potential GDP")
    ax.axvline(tline, linestyle="--", color="black")
    ax.set_title("Potential GDP - Real GDP")
    ax.legend(loc="lower left", fontsize="small")
    ax.grid(False)
    _save_show(fig, "viog_08_ln_levels.png")

    # ── Gráfica 9: Crecimiento vs gap potencial ───────────────────────
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(x, variation, label="GDP growth")
    if has_ref:
        ax.plot(x, df["gap_ref"], label="GDP gap")
    ax.set_title("Growth - GDP gap")
    ax.legend(loc="lower center", fontsize="small")
    ax.grid(False)
    _save_show(fig, "viog_09_growth_gap.png")

    # ── Gráfica 10: PIB potencial ln por cada filtro ──────────────────
    fig, ax = plt.subplots(figsize=(10, 5))
    if has_ref:
        ax.plot(x, df["ln_Y_ref"],       label="Production Function")
    ax.plot(x, df["ln_trend_bk"],    label="Baxter and King")
    ax.plot(x, df["ln_trend_cf"],    label="Christiano and Fitzgerald")
    ax.plot(x, df["ln_trend_bw"],    label="Butterworth")
    ax.plot(x, df["ln_trend_bhp"],   label="Boosted Hodrick-Prescott")
    ax.plot(x, df["ln_trend_kalman"], label="Kalman")
    ax.set_title("Potential GDP")
    ax.legend(loc="center right", fontsize="small")
    ax.grid(False)
    _save_show(fig, "viog_10_ln_trends.png")

    # ── Gráfica 11: Brecha del producto por filtro ────────────────────
    fig, ax = plt.subplots(figsize=(10, 5))
    if has_ref:
        ax.plot(x, df["gap_ref"],    label="Production Function")
    ax.plot(x, df["gap_bk"],     label="Baxter and King")
    ax.plot(x, df["gap_cf"],     label="Christiano and Fitzgerald")
    ax.plot(x, df["gap_bw"],     label="Butterworth")
    ax.plot(x, df["gap_bhp"],    label="Boosted Hodrick-Prescott")
    ax.plot(x, df["gap_kalman"], label="Kalman")
    ax.set_title("GDP gap")
    ax.legend(loc="lower center", ncol=2, fontsize="small")
    ax.grid(False)
    _save_show(fig, "viog_11_gaps_all.png")

    # ── Gráfica 12: ln PIB vs potencial VIOG y 1/VIOG ────────────────
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(x, df["ln_Y"],                  label="Ln real GDP")
    ax.plot(x, df["ln_potential_viog"],     label="Ln potential GDP (VIOG)")
    ax.plot(x, df["ln_potential_inv_viog"], label="Ln potential GDP (1/VIOG)")
    ax.set_title("GDP - Potential GDP based on VIOG and 1/VIOG")
    ax.legend(loc="lower left", fontsize="small")
    ax.grid(False)
    _save_show(fig, "viog_12_viog_potential.png")

    # ── Gráfica 13: Crecimiento vs brecha VIOG y 1/VIOG ──────────────
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(x, variation,          label="Growth")
    ax.plot(x, df["gap_viog"],     label="GDP gap (VIOG)")
    ax.plot(x, df["gap_inv_viog"], label="GDP gap (1/VIOG)")
    ax.set_title("GDP growth - GDP gap based on VIOG and 1/VIOG")
    ax.legend(loc="lower center", fontsize="small")
    ax.grid(False)
    _save_show(fig, "viog_13_viog_gap.png")

    # ── Gráfica 14: Brecha Kalman únicamente ─────────────────────────
    _gk = df["gap_kalman"].dropna()
    _q01, _q99 = _gk.quantile(0.01), _gk.quantile(0.99)
    _margin = (_q99 - _q01) * 0.15
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(x, df["gap_kalman"], color="steelblue", label="gap_kalman")
    ax.axhline(0, linestyle="--", color="black", linewidth=0.8)
    ax.set_ylim(_q01 - _margin, _q99 + _margin)
    ax.set_title("Brecha del producto — Filtro Kalman / UCM")
    ax.set_ylabel("Brecha (log)")
    ax.legend()
    ax.grid(True)
    _save_show(fig, "viog_14_kalman_gap.png")

    # ── Gráfica 15: Brecha Boosted Hodrick-Prescott únicamente ───────────
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(x, df["gap_bhp"], color="darkorange", label="gap_bhp")
    ax.axhline(0, linestyle="--", color="black", linewidth=0.8)
    ax.set_title("Brecha del producto — Filtro Boosted Hodrick-Prescott")
    ax.set_ylabel("Brecha (log)")
    ax.legend()
    ax.grid(True)
    _save_show(fig, "viog_15_bhp_gap.png")


# ── API de alto nivel ─────────────────────────────────────────────────

def compute_viog(
    df: pd.DataFrame,
    series_col: str,
    ref_col: Optional[str] = None,
    period_col: Optional[str] = None,
    year_col: str = "Year",
    quarter_col: str = "Quarter",
    plot: bool = False,
    plot_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """Calcula el VIOG directamente desde un DataFrame ya cargado.

    Uso sin serie de referencia (solo 5 filtros estadísticos)::

        resultado = compute_viog(df, series_col="PIB")

    Uso con serie de referencia (6 filtros, incluye función de producción/CBO)::

        resultado = compute_viog(df, series_col="PIB", ref_col="PIB_potencial")

    Parameters
    ----------
    df:           DataFrame con los datos. Puede tener PeriodIndex ya,
                  o columnas de año/trimestre, o una columna datetime.
    series_col:   Nombre de la columna de la serie observada.
    ref_col:      Nombre de la columna de tendencia de referencia (opcional).
                  Si es None, el VIOG se calcula solo con los 5 filtros
                  estadísticos (BK, CF, BW, HP, Kalman). No se genera gap_ref.
    period_col:   Columna datetime/period a usar como índice (opcional).
                  Si es None, se buscan year_col + quarter_col.
    year_col:     Columna de año (default "Year").
    quarter_col:  Columna de trimestre (default "Quarter").
    plot:         Si True, genera las gráficas.
    plot_dir:     Directorio donde guardar los PNG.

    Returns
    -------
    DataFrame con columnas:
        date, year, quarter,
        gap_viog, gap_inv_viog,
        gap_bhp, gap_cf, gap_bk, gap_bw, gap_kalman,
        [gap_ref]  ← solo si se pasó ref_col
    """
    df = df.copy()

    # ── Construir PeriodIndex si no existe ────────────────────────────
    if not isinstance(df.index, pd.PeriodIndex):
        if period_col and period_col in df.columns:
            df.index = pd.DatetimeIndex(df[period_col]).to_period("Q")
        elif year_col in df.columns and quarter_col in df.columns:
            df.index = pd.PeriodIndex.from_fields(
                year=df[year_col], quarter=df[quarter_col], freq="Q"
            )
        else:
            raise ValueError(
                f"No se puede construir el índice trimestral. "
                f"Proporciona '{year_col}'+'{quarter_col}' o 'period_col'."
            )
        df = df.sort_index()
        df.index.name = "t"

    # ── Estandarizar nombres internos ─────────────────────────────────
    rename_map = {series_col: "Y"}
    if ref_col:
        rename_map[ref_col] = "Y_ref"
    df = df.rename(columns=rename_map)
    df["_series_label"] = series_col

    # ── Calcular ──────────────────────────────────────────────────────
    df = apply_filters(df)   # usa VIOG_CONFIG por defecto (cfg=None → fallback)
    df = compute_gaps(df)       # gap_ref solo se crea si Y_ref existe
    df = compute_viog_weights(df)
    df = compute_weighted_gap(df)

    if plot:
        if plot_dir:
            plot_dir = Path(plot_dir)
            plot_dir.mkdir(parents=True, exist_ok=True)
        plot_filters(df, save_dir=plot_dir, show=(plot_dir is None))

    out = {
        "date":         df.index.to_timestamp(),
        "year":         df.index.year,
        "quarter":      df.index.quarter,
        "gap_viog":     df["gap_viog"],
        "gap_inv_viog": df["gap_inv_viog"],
        "gap_bhp":      df["gap_bhp"],
        "gap_cf":       df["gap_cf"],
        "gap_bk":       df["gap_bk"],
        "gap_bw":       df["gap_bw"],
        "gap_kalman":   df["gap_kalman"],
    }
    if ref_col:
        out["gap_ref"] = df["gap_ref"]

    return pd.DataFrame(out).reset_index(drop=True)


# ── Pipeline completo (basado en archivo) ─────────────────────────────

def run_viog_pipeline(
    input_path: Path,
    output_path: Path,
    series_col: str = "Value(Billions)",
    ref_col: Optional[str] = "Potential Value(Billions)",
    source_label: Optional[str] = None,
    plot: bool = False,
    plot_dir: Optional[Path] = None,
    cfg=None,
) -> pd.DataFrame:
    """Pipeline completo VIOG. Genérico para cualquier serie trimestral.

    Parameters
    ----------
    input_path:  Ruta al Excel de entrada.
    output_path: Ruta donde guardar el CSV procesado.
    series_col:  Columna de la serie observada en el Excel.
    ref_col:     Columna de tendencia de referencia en el Excel.
    plot:        Si True, genera y guarda gráficas en plot_dir.
    plot_dir:    Directorio para PNG (por defecto: mismo directorio que output_path).
    cfg:         VIOGConfig a usar en apply_filters. None → VIOG_CONFIG (USA),
                 que era el comportamiento anterior (los filtros SIEMPRE usaban
                 VIOG_CONFIG aunque el runner pasara VIOG_CO_CONFIG; era inocuo
                 porque ambas configs comparten parámetros econométricos, pero
                 con flags como cf_one_sided conviene respetar la config del
                 país — run_viog.py ahora la pasa explícitamente).
    """
    from src.config import VIOG_CONFIG

    if cfg is None:
        cfg = VIOG_CONFIG
    logger.info("[VIOG] Cargando %s (serie=%s, ref=%s)", input_path, series_col, ref_col)
    df = load_series(input_path, series_col=series_col, ref_col=ref_col)
    df = apply_filters(df, cfg=cfg)
    df = compute_gaps(df)
    df = compute_viog_weights(df)
    df = compute_weighted_gap(df)

    if plot:
        _plot_dir = plot_dir or output_path.parent
        _plot_dir.mkdir(parents=True, exist_ok=True)
        plot_filters(df, save_dir=_plot_dir, show=False)
        logger.info("[VIOG] Gráficas guardadas en %s", _plot_dir)

    out = {
        "date":         df.index.to_timestamp(),
        "year":         df.index.year,
        "quarter":      df.index.quarter,
        "gap_viog":     df["gap_viog"],
        "gap_inv_viog": df["gap_inv_viog"],
        "gap_bhp":      df["gap_bhp"],
        "gap_cf":       df["gap_cf"],
        "gap_bk":       df["gap_bk"],
        "gap_bw":       df["gap_bw"],
        "gap_kalman":   df["gap_kalman"],
        "source":       source_label or cfg.source_label,
    }
    if ref_col and "gap_ref" in df.columns:
        out["gap_ref"] = df["gap_ref"]

    out_df = pd.DataFrame(out).reset_index(drop=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output_path, index=False)
    logger.info("[VIOG] %d observaciones guardadas en %s", len(out_df), output_path)
    return out_df
