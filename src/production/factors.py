"""Cálculo de los factores de producción Cobb-Douglas: Trabajo, Capital y Alpha.

Metodología alineada con ``legacy/pib_potencial_integrado_v3.py`` (Módulo 2,
configuración v2 / SPEC_V2.md — ver ``docs/integracion_v3.md`` para el mapeo
completo). A diferencia de la versión anterior de este archivo (que trabajaba
en NIVELES: miles de personas, millones COP), el motor v3 trabaja en ÍNDICES
(base 100 en ``BASE_QUARTER``) construidos sobre SUMAS MÓVILES DE 4 TRIMESTRES
(series "anualizadas"). Esto no es un detalle de implementación: es la forma
en que v3 suaviza el ruido trimestral antes de comparar factores en la función
Cobb-Douglas, y por lo tanto es intencional que ``idx_L``, ``idx_K``, etc. NO
sean directamente comparables en unidades a un trimestre individual.

Todas las funciones que reciben DataFrames son puras (sin I/O): quien orquesta
(``src/pipelines/run_pib_potencial.py``) lee los archivos (CSV, Excel) y pasa
DataFrames/Series ya cargados.

Cadena de cálculo (``compute_all_factors``)
--------------------------------------------
    1. Agregación mensual → trimestral (Ocupados: promedio simple, v2).
    2. Mercado laboral observado: TGP, TD, brecha de desempleo y de uso de
       capital, con sus medias móviles (8 trimestres, v2).
    3. TGP* (participación potencial) por regresión OLS con tendencia por
       tramos (picos del ciclo BBQ) + términos cíclicos.
    4. Factor trabajo: FL*, Ocupados*, horas trabajadas (ajustadas por
       festivos efectivos), sumas móviles anualizadas e índices.
    5. Capital humano (PWT hc): extrapolación OLS anclada + interpolación
       intra-anual.
    6. Producto: PIB total anualizado (suma móvil 4T) e índice.
    7. Capital físico: stock de capital productivo DANE (interpolación
       PCHIP a trimestral) — reemplaza el inventario permanente (PIM).
    8. Alpha (participación del capital): EBE/(RA+EBE), estilo CBO,
       promediado en la ventana 2016Q1..T.

Columnas que produce ``compute_all_factors``
----------------------------------------------
    tgp, td, brecha_u, ma_brecha_u, brecha_icu, ma_brecha_icu   [mercado laboral]
    tgp_star                                                     [participación potencial, %]
    fl_star, ocup_star, jornada, festivos_q, horas_worker_q,
    horas_star_q, horas_q, horas_star_ann, horas_ann             [factor trabajo]
    idx_L, idx_L_star                                            [índice trabajo, base=100]
    idx_hc, idx_LH, idx_LH_star                                  [índice trabajo × capital humano]
    W_pib_ann, idx_pib                                           [PIB anualizado e índice]
    K, K_used, K_used_star, idx_K, idx_K_star                    [capital físico e índice]
    alpha_t, alpha                                               [participación del capital]
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import statsmodels.api as sm

from src.production.business_cycle import construir_variables_ciclo

logger = logging.getLogger("nairu_pipeline.production.factors")

# ---------------------------------------------------------------------------
# Ancla de índices y ventanas (ver docs/integracion_v3.md, sección "Fase 0")
# ---------------------------------------------------------------------------
#
# v3 ancla los índices (=100) en 2005Q1 porque el boceto Excel original del
# autor tenía historia desde 2004. Los datos REALES de este repo (los mismos
# que usa el resto del pipeline) tienen dos huecos genuinos que 2005Q1 no
# puede sortear:
#   (a) el PIB desestacionalizado del DANE empieza EXACTAMENTE en 2005Q1, sin
#       ningún trimestre previo (confirmado en el Excel crudo del DANE);
#   (b) la GEIH mensual (y por lo tanto NAIRU/NAICU/ICU derivados) tiene un
#       hueco real de 2 meses en 2006-07 y 2006-08 → 2006Q3 completo queda NaN.
# Cualquier ventana móvil de 4 trimestres que incluya 2006Q3 (es decir, hasta
# 2007Q3 inclusive) queda contaminada. 2007Q3 es el primer trimestre cuya
# ventana [2006Q4..2007Q3] ya no toca el hueco.
#
# Reanclar el "=100" de un índice multiplicativo NO cambia ninguna razón ni
# brecha calculada con él (identidad algebraica: numerador y denominador se
# reescalan por la misma constante) — solo cambia qué trimestre vale 100.
BASE_QUARTER: pd.Timestamp = pd.Timestamp("2007-09-01")

# Ventana de estimación de alpha (Cambio 5 v2): desde el primer trimestre con
# datos de ingreso DANE (2016Q1) hasta T (se resuelve en tiempo de ejecución).
ALPHA_WINDOW_START: pd.Timestamp = pd.Timestamp("2016-03-01")

# Cambio 1 (v2): agregación trimestral de Ocupados = promedio simple (no
# valor puntual de fin de trimestre, como en el legado).
OCUPADOS_TRIMESTRAL: str = "promedio"

# Cambio 2 (v2): ventana móvil de la brecha de desempleo/capital en la
# regresión TGP* (24 meses ≈ especificación de la curva de Phillips).
PARTICIPATION_MA_WINDOW: int = 8

# Cambio 3 (v2): horas trabajadas ajustadas por festivos efectivos.
HOURS_FESTIVOS: bool = True
VACACIONES_SEMANAS: float = 6.6
DIAS_LABORALES_SEMANA: float = 6.0

# Jornada legal semanal (horas), Ley 2101 de 2021.
JORNADA_SCHEDULE: list[tuple[pd.Timestamp, int]] = [
    (pd.Timestamp("2023-09-01"), 47),
    (pd.Timestamp("2024-09-01"), 46),
    (pd.Timestamp("2025-09-01"), 44),
    (pd.Timestamp("2026-09-01"), 42),
]
JORNADA_BASE: int = 48

# Cambio 4 (v2): capital humano — extrapolación OLS anclada en el último
# año PWT observado (sin salto de nivel, a diferencia del boceto legado).
PWT_HC_LAST_YEAR: int = 2023
HC_EXTRAP_SLOPE_YEARS: int = 10

# Cambio 5 (v2): alpha por el método CBO — EBE/(RA+EBE), sin impuestos-
# subsidios ni ingreso mixto.
ALPHA_METHOD: str = "cbo"


# ---------------------------------------------------------------------------
# Utilidades de fecha / calendario laboral
# ---------------------------------------------------------------------------

def quarter_label(date: pd.Timestamp) -> pd.Timestamp:
    """Etiqueta de trimestre: primer día del mes FINAL del trimestre (mar/jun/sep/dic).

    Convención de v3 (distinta de ``resample("QS")``, que etiqueta con el
    primer día del trimestre). Se usa de forma consistente en todo el motor
    de PIB potencial para que las fechas calcen con las de v3.
    """
    q_end_month = ((date.month - 1) // 3 + 1) * 3
    return pd.Timestamp(year=date.year, month=q_end_month, day=1)


def jornada_legal(date: pd.Timestamp) -> int:
    """Jornada legal semanal (horas) vigente en el trimestre ``date`` (Ley 2101/2021)."""
    horas = JORNADA_BASE
    for umbral, valor in JORNADA_SCHEDULE:
        if date >= umbral:
            horas = valor
    return horas


# ---------------------------------------------------------------------------
# 1. Agregación mensual → trimestral
# ---------------------------------------------------------------------------

def aggregate_monthly_to_quarterly(
    monthly: pd.DataFrame,
    ocupados_trimestral: str = OCUPADOS_TRIMESTRAL,
) -> pd.DataFrame:
    """Agrega insumos mensuales (pet, fl, ocup, nairu, naicu, icu) a trimestral.

    Promedio simple de los 3 meses del trimestre (solo trimestres completos).
    Ocupados (Cambio 1, v2): ``ocupados_trimestral="promedio"`` promedia igual
    que las demás columnas; ``"fin_de_trimestre"`` (legado) toma el valor
    puntual del último mes.

    Parameters
    ----------
    monthly : pd.DataFrame
        Requiere ``date`` y las columnas a agregar (típicamente ``pet``,
        ``fl``, ``ocup``, ``nairu``, ``naicu``, ``icu``). ``hc_anual`` se
        ignora aquí (se trata aparte, solo tiene dato en enero).

    Returns
    -------
    pd.DataFrame
        Una fila por trimestre completo, columna ``date`` = ``quarter_label``.
    """
    df = monthly.copy()
    df["quarter"] = df["date"].apply(quarter_label)

    if ocupados_trimestral == "promedio":
        cols_promedio = [c for c in df.columns if c not in ("date", "quarter", "hc_anual")]
    elif ocupados_trimestral == "fin_de_trimestre":
        cols_promedio = [c for c in df.columns if c not in ("date", "quarter", "hc_anual", "ocup")]
    else:
        raise ValueError(f"ocupados_trimestral desconocido: {ocupados_trimestral!r}")

    grouped = df.groupby("quarter")[cols_promedio].agg(["mean", "count"])
    out = pd.DataFrame(index=grouped.index)
    n_meses_esperados = 3
    for col in cols_promedio:
        completos = grouped[(col, "count")] == n_meses_esperados
        out[col] = grouped[(col, "mean")].where(completos)

    if ocupados_trimestral == "fin_de_trimestre":
        ocup_directo = df.loc[df["date"] == df["quarter"], ["quarter", "ocup"]].set_index("quarter")["ocup"]
        out["ocup"] = ocup_directo

    out = out.reset_index().rename(columns={"quarter": "date"})
    return out.sort_values("date").reset_index(drop=True)


# ---------------------------------------------------------------------------
# 2. Mercado laboral observado
# ---------------------------------------------------------------------------

def calcular_mercado_laboral(df: pd.DataFrame, ma_window: int = PARTICIPATION_MA_WINDOW) -> pd.DataFrame:
    """TGP, TD, brecha de desempleo (TD-NAIRU) y de uso de capital (ICU-NAICU).

    Cambio 2 (v2): se agrega la brecha de utilización de capital y la
    ventana móvil pasa de 4 a 8 trimestres (24 meses ≈ curva de Phillips).

    Parameters
    ----------
    df : pd.DataFrame
        Requiere ``fl``, ``pet``, ``ocup``, ``nairu``, ``icu``, ``naicu``.
    """
    df = df.copy()
    df["tgp"] = 100 * df["fl"] / df["pet"]
    df["td"] = 100 * (df["fl"] - df["ocup"]) / df["fl"]
    df["brecha_u"] = df["td"] - df["nairu"]
    df["brecha_icu"] = df["icu"] - df["naicu"]
    df["ma_brecha_u"] = df["brecha_u"].rolling(window=ma_window, min_periods=ma_window).mean()
    df["ma_brecha_icu"] = df["brecha_icu"].rolling(window=ma_window, min_periods=ma_window).mean()
    return df


# ---------------------------------------------------------------------------
# 3. TGP* (participación potencial) — regresión OLS por tramos BBQ
# ---------------------------------------------------------------------------

class ResultadoRegTGP:
    """Resultado de la regresión de TGP* (para diagnóstico/verificación)."""

    __slots__ = ("beta0", "beta1", "beta2", "beta3", "r_squared", "nobs", "modelo")

    def __init__(self, beta0, beta1, beta2, beta3, r_squared, nobs, modelo=None):
        self.beta0, self.beta1, self.beta2, self.beta3 = beta0, beta1, beta2, beta3
        self.r_squared, self.nobs, self.modelo = r_squared, nobs, modelo

    def __repr__(self) -> str:  # pragma: no cover
        return (f"ResultadoRegTGP(beta0={self.beta0:.4f}, beta1={self.beta1:.4f}, "
                f"r_squared={self.r_squared:.4f}, nobs={self.nobs})")


def estimar_tgp_star(
    df: pd.DataFrame,
    picos: list[pd.Timestamp] | None,
    base_quarter: pd.Timestamp = BASE_QUARTER,
) -> tuple[pd.Series, ResultadoRegTGP]:
    """Estima TGP* (participación potencial) por OLS, en NIVELES (Cambio 2, v2):

        TGP = b0 + [tendencia por tramos BBQ] + b2*brecha_u + b3*MA8_brecha_u
              + b4*brecha_icu + b5*MA8_brecha_icu + eps

    TGP* = valor ajustado con los términos cíclicos en cero (solo tendencia),
    para todos los trimestres.

    Parameters
    ----------
    df : pd.DataFrame
        Requiere ``date``, ``tgp``, ``brecha_u``, ``ma_brecha_u``,
        ``brecha_icu``, ``ma_brecha_icu``.
    picos : list[pd.Timestamp] | None
        Picos del ciclo BBQ (tendencia por tramos). Si ``None`` o vacío, la
        tendencia es lineal en el índice de trimestre.

    Returns
    -------
    (tgp_star, ResultadoRegTGP)
    """
    df = df.copy()
    df["dep_tgp"] = df["tgp"]

    ciclicos = ["brecha_u", "ma_brecha_u", "brecha_icu", "ma_brecha_icu"]

    if picos:
        df["tau_q"] = (
            (df["date"].dt.year - base_quarter.year) * 12
            + (df["date"].dt.month - base_quarter.month)
        ) / 3.0
        cyc = construir_variables_ciclo(df["date"], picos)
        cyc_cols = list(cyc.columns)
        for c in cyc_cols:
            df[c] = cyc[c].values
        trend_cols = ["tau_q"] + cyc_cols
    else:
        df["t_serial"] = (
            (df["date"].dt.year - base_quarter.year) * 12
            + (df["date"].dt.month - base_quarter.month)
        ) / 3.0
        trend_cols = ["t_serial"]

    reg_cols = trend_cols + ciclicos
    muestra = df.dropna(subset=reg_cols + ["dep_tgp"]).copy()
    X = sm.add_constant(muestra[reg_cols])
    y = muestra["dep_tgp"]
    modelo = sm.OLS(y, X, missing="raise").fit()

    pred = float(modelo.params["const"]) + sum(
        float(modelo.params[c]) * df[c] for c in trend_cols
    )
    tgp_star = pred  # en niveles (%)

    resultado = ResultadoRegTGP(
        beta0=float(modelo.params["const"]),
        beta1=float(modelo.params[trend_cols[0]]),
        beta2=float(modelo.params["brecha_u"]),
        beta3=float(modelo.params["ma_brecha_u"]),
        r_squared=modelo.rsquared, nobs=int(modelo.nobs), modelo=modelo,
    )
    logger.info(
        "TGP*: R²=%.4f, nobs=%d, beta0=%.3f, beta1=%.4f",
        resultado.r_squared, resultado.nobs, resultado.beta0, resultado.beta1,
    )
    return tgp_star, resultado


# ---------------------------------------------------------------------------
# 4. Factor trabajo (horas, ajustadas por festivos)
# ---------------------------------------------------------------------------

def calcular_factor_trabajo(
    df: pd.DataFrame,
    festivos: pd.Series | None = None,
    hours_festivos: bool = HOURS_FESTIVOS,
    base_quarter: pd.Timestamp = BASE_QUARTER,
) -> pd.DataFrame:
    """FL*, Ocupados*, horas trimestrales (obs. y potenciales), sumas móviles e índices.

    Cambio 3 (v2): además de vacaciones (6.6 semanas/año), descuenta el
    tiempo no laborado por FESTIVOS efectivos del trimestre — cada festivo
    resta ``jornada/DIAS_LABORALES_SEMANA`` horas.

    Parameters
    ----------
    df : pd.DataFrame
        Requiere ``date``, ``pet``, ``tgp_star``, ``nairu``, ``ocup``.
    festivos : pd.Series | None
        Festivos efectivos por trimestre (índice = fecha-etiqueta de
        trimestre). Si ``None`` o ``hours_festivos=False``, no se descuentan.

    Returns
    -------
    pd.DataFrame
        Copia con ``fl_star``, ``ocup_star``, ``jornada``, ``festivos_q``,
        ``horas_worker_q``, ``horas_star_q``, ``horas_q``, ``horas_star_ann``,
        ``horas_ann``, ``idx_L_star``, ``idx_L`` añadidas.
    """
    df = df.copy()
    df["fl_star"] = df["pet"] * df["tgp_star"] / 100
    df["ocup_star"] = df["fl_star"] * (1 - df["nairu"] / 100)

    df["jornada"] = df["date"].apply(jornada_legal)
    semanas_efectivas = (52 - VACACIONES_SEMANAS) / 4

    if hours_festivos and festivos is not None:
        df["festivos_q"] = df["date"].map(festivos).fillna(0.0)
        df["horas_worker_q"] = (
            df["jornada"] * semanas_efectivas
            - df["festivos_q"] * df["jornada"] / DIAS_LABORALES_SEMANA
        )
    else:
        df["festivos_q"] = 0.0
        df["horas_worker_q"] = df["jornada"] * semanas_efectivas

    df["horas_star_q"] = df["ocup_star"] * df["horas_worker_q"]
    df["horas_q"] = df["ocup"] * df["horas_worker_q"]

    df["horas_star_ann"] = df["horas_star_q"].rolling(window=4, min_periods=4).sum()
    df["horas_ann"] = df["horas_q"].rolling(window=4, min_periods=4).sum()

    base_val = df.loc[df["date"] == base_quarter, "horas_star_ann"].iloc[0]
    df["idx_L_star"] = 100 * df["horas_star_ann"] / base_val
    df["idx_L"] = 100 * df["horas_ann"] / base_val
    return df


# ---------------------------------------------------------------------------
# 5. Capital humano (PWT)
# ---------------------------------------------------------------------------

def extrapolar_hc_anual(
    hc_anual: pd.Series,
    ultimo_anio_pwt: int,
    anio_final_necesario: int,
    anios_pendiente: int = HC_EXTRAP_SLOPE_YEARS,
) -> pd.Series:
    """Extrapola hc (capital humano PWT) más allá del último año observado.

    Extrapolación ANCLADA (Cambio 4, v2): ``hc(y) = hc(último) + s*(y-último)``
    con ``s`` = pendiente OLS de hc sobre año en los últimos ``anios_pendiente``
    años observados — sin salto de nivel en el punto de empalme.
    """
    hc_anual = hc_anual.dropna().sort_index()
    observado = hc_anual[hc_anual.index.year <= ultimo_anio_pwt]
    anio_ini_pendiente = ultimo_anio_pwt - anios_pendiente + 1
    ventana = observado[observado.index.year >= anio_ini_pendiente]
    anios_x = ventana.index.year.values.astype(float)
    X = sm.add_constant(anios_x)
    modelo = sm.OLS(ventana.values, X).fit()
    slope = float(np.asarray(modelo.params)[1])

    hc_ultimo = observado.loc[pd.Timestamp(ultimo_anio_pwt, 1, 1)]
    extra_years = range(ultimo_anio_pwt + 1, anio_final_necesario + 1)
    extrapolados = pd.Series(
        {pd.Timestamp(y, 1, 1): hc_ultimo + slope * (y - ultimo_anio_pwt) for y in extra_years}
    )
    return pd.concat([observado, extrapolados]).sort_index()


def calcular_capital_humano(quarterly_dates: pd.Series, hc_anual: pd.Series) -> pd.DataFrame:
    """Índice de capital humano con ancla en Q1 de cada año e interpolación intra-anual:

        a_y = 100*hc(y)/hc(2005)
        Q1=a_y ; Q2=(3a_y+a_{y+1})/4 ; Q3=(2a_y+2a_{y+1})/4 ; Q4=(a_y+3a_{y+1})/4
    """
    hc_by_year = hc_anual.dropna()
    hc_2005 = hc_by_year.loc[hc_by_year.index.year == 2005].iloc[0]
    a = 100 * hc_by_year / hc_2005

    registros = []
    for fecha in quarterly_dates:
        year = fecha.year
        q = (fecha.month - 1) // 3 + 1
        a_y = a.get(pd.Timestamp(year, 1, 1))
        a_y1 = a.get(pd.Timestamp(year + 1, 1, 1))
        if a_y is None:
            idx_hc = np.nan
        elif q == 1:
            idx_hc = a_y
        elif q == 2:
            idx_hc = (3 * a_y + a_y1) / 4 if a_y1 is not None else np.nan
        elif q == 3:
            idx_hc = (2 * a_y + 2 * a_y1) / 4 if a_y1 is not None else np.nan
        else:
            idx_hc = (a_y + 3 * a_y1) / 4 if a_y1 is not None else np.nan
        registros.append({"date": fecha, "idx_hc": idx_hc})
    return pd.DataFrame(registros)


# ---------------------------------------------------------------------------
# 6. Producto (PIB anualizado e índice)
# ---------------------------------------------------------------------------

def calcular_producto(df: pd.DataFrame, base_quarter: pd.Timestamp = BASE_QUARTER) -> tuple[pd.DataFrame, float]:
    """PIB total anualizado (suma móvil 4T) e índice base ``base_quarter``=100.

    Parameters
    ----------
    df : pd.DataFrame
        Requiere ``date``, ``V_pib`` (PIB real trimestral desestacionalizado).

    Returns
    -------
    (df, base_val)
        ``base_val`` = ``W_pib_ann`` en ``base_quarter`` (para referencia/tests).
    """
    df = df.copy()
    df["W_pib_ann"] = df["V_pib"].rolling(window=4, min_periods=4).sum()
    df.loc[df["date"] < base_quarter, "W_pib_ann"] = np.nan

    base_val = df.loc[df["date"] == base_quarter, "W_pib_ann"].iloc[0]
    df["idx_pib"] = 100 * df["W_pib_ann"] / base_val
    return df, float(base_val)


# ---------------------------------------------------------------------------
# 7. Capital físico (stock de capital productivo DANE)
# ---------------------------------------------------------------------------

def calcular_capital_dane(df: pd.DataFrame, dane_capital: pd.DataFrame) -> pd.DataFrame:
    """Capital = stock de capital productivo DANE (anual) interpolado a
    trimestral por PCHIP centrado en el año (el promedio de 4 trimestres
    reproduce el dato anual). Extrapola años posteriores al último
    observado con el crecimiento log medio de los últimos 4 años.

    Parameters
    ----------
    df : pd.DataFrame
        Requiere ``date`` (trimestral).
    dane_capital : pd.DataFrame
        Requiere ``year``, ``K_prod_mmp`` (stock de capital productivo,
        miles de millones de pesos, anual).

    Returns
    -------
    pd.DataFrame
        Copia con columna ``K`` añadida.
    """
    from scipy.interpolate import PchipInterpolator

    years = dane_capital["year"].to_numpy(dtype=float)
    Kv = dane_capital["K_prod_mmp"].to_numpy(dtype=float)
    g = float(np.mean(np.diff(np.log(Kv))[-4:]))
    last_year = int(years[-1])
    need_years = sorted({d.year for d in df["date"]})
    ext_y, ext_v, yy = [], [], last_year
    while yy < max(need_years) + 1:
        yy += 1
        ext_y.append(float(yy))
        ext_v.append(Kv[-1] * np.exp(g * (yy - last_year)))
    yr = np.concatenate([years, ext_y])
    Kall = np.concatenate([Kv, ext_v])
    tc = yr + 0.5  # centro del año

    def q_center(d):
        q = {3: 1, 6: 2, 9: 3, 12: 4}[d.month]
        return d.year + (q - 0.5) / 4.0

    qt = np.array([q_center(d) for d in df["date"]])
    Kq = PchipInterpolator(tc, Kall)(qt)
    out = df.copy()
    out["K"] = Kq
    return out


def calcular_capital_usado(df: pd.DataFrame, base_quarter: pd.Timestamp = BASE_QUARTER) -> pd.DataFrame:
    """Capital usado (potencial y observado), sumas móviles 4T e índices."""
    df = df.copy()
    df["K_used_star"] = df["K"] * df["naicu"] / 100
    df["K_used"] = df["K"] * df["icu"] / 100

    df["K_star_ann"] = df["K_used_star"].rolling(window=4, min_periods=4).sum()
    df["K_ann"] = df["K_used"].rolling(window=4, min_periods=4).sum()

    base_val = df.loc[df["date"] == base_quarter, "K_star_ann"].iloc[0]
    df["idx_K_star"] = 100 * df["K_star_ann"] / base_val
    df["idx_K"] = 100 * df["K_ann"] / base_val
    return df


# ---------------------------------------------------------------------------
# 8. Alpha (participación del capital) — método CBO
# ---------------------------------------------------------------------------

def calcular_alpha(
    df: pd.DataFrame,
    ventana_inicio: pd.Timestamp,
    ventana_fin: pd.Timestamp,
    metodo: str = ALPHA_METHOD,
) -> tuple[pd.Series, float]:
    """alpha_t (participación del capital) por trimestre; alpha = promedio en la ventana.

    Cambio 5 (v2): ``metodo="cbo"`` → ``alpha_t = EBE/(RA+EBE)`` (quita
    impuestos-subsidios netos e ingreso mixto del reparto — estilo CBO).

    Parameters
    ----------
    df : pd.DataFrame
        Requiere ``BQ_ra`` (remuneración asalariados) y ``BS_ebe`` (excedente
        bruto de explotación).
    """
    df = df.copy()
    if metodo == "cbo":
        alpha_t = df["BS_ebe"] / (df["BQ_ra"] + df["BS_ebe"])
    else:
        raise ValueError(f"alpha metodo desconocido: {metodo!r} (solo 'cbo' está implementado)")
    ventana = df[(df["date"] >= ventana_inicio) & (df["date"] <= ventana_fin)]
    alpha = alpha_t.loc[ventana.index].mean()
    logger.info(
        "Alpha CBO = %.4f (promedio %s -> %s, %d trimestres)",
        alpha, ventana_inicio.date(), ventana_fin.date(), len(ventana),
    )
    return alpha_t, float(alpha)


# ---------------------------------------------------------------------------
# 9. Último trimestre completo
# ---------------------------------------------------------------------------

def detect_last_complete_quarter(
    quarterly: pd.DataFrame,
    required_cols: tuple[str, ...] = ("V_pib", "BE_inv", "BQ_ra", "BS_ebe"),
) -> pd.Timestamp:
    """Determina T = último trimestre con todos los insumos trimestrales completos."""
    completos = quarterly.dropna(subset=list(required_cols))
    return completos["date"].max()


# ---------------------------------------------------------------------------
# Orquestador
# ---------------------------------------------------------------------------

def compute_all_factors(
    monthly: pd.DataFrame,
    quarterly: pd.DataFrame,
    nairu_monthly: pd.DataFrame,
    dane_capital: pd.DataFrame,
    *,
    festivos: pd.Series | None = None,
    picos_bbq: list[pd.Timestamp] | None = None,
    T: pd.Timestamp | None = None,
    base_quarter: pd.Timestamp = BASE_QUARTER,
    ocupados_trimestral: str = OCUPADOS_TRIMESTRAL,
    ma_window: int = PARTICIPATION_MA_WINDOW,
    hours_festivos: bool = HOURS_FESTIVOS,
    alpha_window_start: pd.Timestamp = ALPHA_WINDOW_START,
    alpha_window_end: pd.Timestamp | None = None,
    pwt_hc_last_year: int = PWT_HC_LAST_YEAR,
) -> pd.DataFrame:
    """Calcula todos los factores de producción (trabajo, capital, alpha) y el
    índice de producto, siguiendo la cadena de v3 (Módulo 2, pasos 2-9).

    Parameters
    ----------
    monthly : pd.DataFrame
        ``date``, ``pet``, ``fl``, ``ocup``, ``hc_anual`` (mensual; ``hc_anual``
        solo tiene dato en enero de cada año).
    quarterly : pd.DataFrame
        ``date``, ``V_pib``, ``BE_inv``, ``BQ_ra``, ``BS_ebe`` (trimestral,
        etiquetado con ``quarter_label``).
    nairu_monthly : pd.DataFrame
        ``date``, ``nairu``, ``naicu``, ``icu`` (mensual).
    dane_capital : pd.DataFrame
        ``year``, ``K_prod_mmp`` (stock de capital productivo DANE, anual).
    festivos : pd.Series | None
        Festivos efectivos por trimestre (ver ``run_pib_potencial._load_festivos``).
    picos_bbq : list[pd.Timestamp] | None
        Picos del ciclo (ver ``business_cycle.detect_bbq_peaks``). Si es
        ``None``, se detectan aquí mismo sobre ``quarterly.V_pib``.
    T : pd.Timestamp | None
        Último trimestre completo. Si es ``None``, se detecta aquí mismo.

    Returns
    -------
    pd.DataFrame
        DataFrame trimestral con todas las columnas de factores añadidas
        (ver docstring del módulo).
    """
    from src.production.business_cycle import detect_bbq_peaks

    if T is None:
        T = detect_last_complete_quarter(quarterly)
    if alpha_window_end is None:
        alpha_window_end = T
    if picos_bbq is None:
        picos_bbq = detect_bbq_peaks(quarterly)
        logger.info("Picos BBQ detectados: %s", [str(p.date()) for p in picos_bbq])

    # ── 1. Mensual: fuerza laboral + NAIRU/NAICU/ICU ────────────────────
    monthly_full = monthly.merge(nairu_monthly, on="date", how="left")

    # ── 2. Agregación mensual → trimestral (Ocupados) ──────────────────
    quarterly_agg = aggregate_monthly_to_quarterly(monthly_full, ocupados_trimestral=ocupados_trimestral)
    df = quarterly.merge(quarterly_agg, on="date", how="left")

    # ── 3. Mercado laboral observado ────────────────────────────────────
    df = calcular_mercado_laboral(df, ma_window=ma_window)

    # ── 4. TGP* óptima ───────────────────────────────────────────────────
    df["tgp_star"], _reg_tgp = estimar_tgp_star(df, picos=picos_bbq, base_quarter=base_quarter)

    # ── 5. Factor trabajo (horas ajustadas por festivos) ────────────────
    df = calcular_factor_trabajo(df, festivos=festivos, hours_festivos=hours_festivos, base_quarter=base_quarter)

    # ── 6. Capital humano ────────────────────────────────────────────────
    hc_anual_crudo = monthly.dropna(subset=["hc_anual"]).set_index("date")["hc_anual"]
    anio_final_necesario = T.year + 1
    hc_anual = extrapolar_hc_anual(hc_anual_crudo, pwt_hc_last_year, anio_final_necesario)
    hc_df = calcular_capital_humano(df["date"], hc_anual)
    df = df.merge(hc_df, on="date", how="left")
    df["idx_LH_star"] = df["idx_L_star"] * df["idx_hc"] / 100
    df["idx_LH"] = df["idx_L"] * df["idx_hc"] / 100

    # ── 7. Producto (PIB anualizado e índice) ───────────────────────────
    df, _base_out = calcular_producto(df, base_quarter=base_quarter)

    # ── 8. Capital físico (DANE) ─────────────────────────────────────────
    df = calcular_capital_dane(df, dane_capital)
    df = calcular_capital_usado(df, base_quarter=base_quarter)

    # ── 9. Alpha (CBO) ────────────────────────────────────────────────────
    alpha_t, alpha = calcular_alpha(df, alpha_window_start, alpha_window_end, metodo=ALPHA_METHOD)
    df["alpha_t"] = alpha_t
    df["alpha"] = alpha

    logger.info(
        "Factores: idx_L=[%.1f,%.1f] idx_K=[%.1f,%.1f] idx_pib=[%.1f,%.1f] alpha=%.4f",
        df["idx_L"].min(skipna=True), df["idx_L"].max(skipna=True),
        df["idx_K"].min(skipna=True), df["idx_K"].max(skipna=True),
        df["idx_pib"].min(skipna=True), df["idx_pib"].max(skipna=True),
        alpha,
    )
    return df
