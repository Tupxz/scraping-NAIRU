# =========================================================================== #
#  src/v7/motor.py -- COPIA LITERAL de legacy/v7/pib_potencial_integrado_v7.py  #
#                                                                               #
#  Unico cambio: la raiz de insumos/salidas (_HERE) se toma de la variable de   #
#  entorno V7_ROOT, que fija src/pipelines/run_v7.py despues de armar la        #
#  carpeta de insumos con src/v7/insumos.py. No editar la metodologia aqui:     #
#  si el script de referencia cambia, regenerar con scripts/sync_motor_v7.py.   #
# =========================================================================== #
# -*- coding: utf-8 -*-
"""
pib_potencial_integrado_v7.py -- PIB potencial de Colombia. Version 7 (propuesta: TGP*, Phillips de precios y de salarios reespecificadas).

Este archivo es AUTOCONTENIDO: contiene todo el procedimiento y no importa ningun
otro modulo del proyecto. Sus unicas dependencias son librerias de terceros
(numpy, pandas, scipy, statsmodels, matplotlib, openpyxl) y los archivos de DATOS
de entrada.

Estructura
----------
  MODULO A -- Fechado del ciclo (Bry-Boschan) y PIB POTENCIAL por funcion de
              produccion estilo CBO: participacion potencial, horas efectivas
              ajustadas por festivos, capital humano PWT, stock de capital
              productivo del DANE, alpha por el enfoque del ingreso y tendencia
              de la PTF, mas el contraste con los compuestos VIOG / 1-VIOG.

  MODULO B -- NAIRU y NAICU: seis estados [u*, ICU*, l, l(-1), TGP*, g_TGP*] y
              cinco ecuaciones de medicion (desempleo, participacion, uso de
              capacidad, salarios y precios). El PIB no interviene en ninguna de ellas, de
              modo que el contraste posterior contra los filtros estadisticos del
              PIB es una prueba externa y no una validacion circular. El nivel se
              ancla con una regla objetiva y reproducible: los episodios en que la
              inflacion nucleo se mantuvo dentro de +-0.5 pp de la meta durante
              seis meses o mas (`calcular_ancla_reversion`).

  MODULO C -- Orquestacion en cuatro pasos:
              PASO 1  estimacion de NAIRU y NAICU;
              PASO 2  PIB potencial y brecha del producto, con banda de 95 %;
              PASO 3  contraste EX POST contra los compuestos VIOG y 1/VIOG, en
                      niveles y por bandas de frecuencia. Es un DIAGNOSTICO:
                      ningun parametro del modelo se elige para mejorarlo.

Tendencia de la PTF
-------------------
Se obtiene con una regresion de tendencia lineal POR TRAMOS al estilo de la CBO
sobre el residuo de Solow. Los quiebres de pendiente son los picos del ciclo
fechados con Bry-Boschan mas un quiebre exogeno en 2014Q3 (fin del auge de
materias primas), necesario porque el algoritmo no data ningun pico entre 2005 y
2019 y sin el la tendencia seria una sola recta que leeria como ciclica la
desaceleracion estructural posterior a 2014.

Cambios de v7 frente a v6 (solo en el MODULO B)
----------------------------------------------
  1. TGP: la referencia de largo plazo ya no es una media movil de 36 meses. La
     ecuacion se escribe en niveles contra un estado latente TGP*_t (participacion
     estructural, tendencia suave estimada con un prior de suavidad como u* e
     ICU*), mas un escalon por el empalme ECH->GEIH de 2006 y un efecto COVID.
  2. Phillips de precios: dependiente = inflacion nucleo - meta del BanRep;
     regresor nuevo = expectativa EME formada hace 12 meses - meta.
  3. Phillips de salarios: dependiente = variacion anual del salario - inflacion
     nucleo de precios (su rezago reemplaza al de la inflacion salarial).
  Todo lo demas (modulo de PIB, ancla, priors de u*/ICU*, diagnostico) es v6.

Uso:
  py -3.10 pib_potencial_integrado_v7.py
"""
from __future__ import annotations

import os
import math
import json
import time
import datetime as dt
from datetime import datetime
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.optimize import minimize

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import openpyxl
import statsmodels.api as sm
from statsmodels.tsa.filters.hp_filter import hpfilter
from statsmodels.tsa.filters.cf_filter import cffilter
from statsmodels.tsa.statespace.mlemodel import MLEModel

_HERE = Path(os.environ["V7_ROOT"]).resolve() if os.environ.get("V7_ROOT") else Path(__file__).resolve().parent




# ##########################################################################
# ###  MODULO A: CICLO (BRY-BOSCHAN) Y PIB POTENCIAL (FUNCION DE PRODUCCION)  ###
# ##########################################################################

BBQ_K_WINDOW = 2       # ventana de extremos locales (Harding & Pagan 2002)


BBQ_MIN_PHASE = 2      # duracion minima de fase (trimestres)


BBQ_MIN_CYCLE = 5      # duracion minima de ciclo completo (trimestres)


BBQ_INCREMENTO = 0.25  # incremento por trimestre de la rampa de ciclo


def _bbq_extremos_locales(y: np.ndarray, k: int = BBQ_K_WINDOW):
    n = len(y)
    cands = []
    for t in range(k, n - k):
        vent = y[t - k: t + k + 1]
        if y[t] == vent.max() and np.sum(vent == y[t]) == 1:
            cands.append((t, "P"))
        elif y[t] == vent.min() and np.sum(vent == y[t]) == 1:
            cands.append((t, "T"))
    return cands


def _bbq_censurar(cands, n, k: int = BBQ_K_WINDOW):
    return [(t, tipo) for (t, tipo) in cands if k <= t <= n - 1 - k]


def _bbq_alternar(cands, y: np.ndarray):
    out = []
    for t, tipo in cands:
        if out and out[-1][1] == tipo:
            pt, _ = out[-1]
            if tipo == "P" and y[t] > y[pt]:
                out[-1] = (t, tipo)
            elif tipo == "T" and y[t] < y[pt]:
                out[-1] = (t, tipo)
        else:
            out.append((t, tipo))
    return out


def _bbq_fase_minima(turns, y, min_phase: int = BBQ_MIN_PHASE):
    while True:
        idx = next((i for i in range(len(turns) - 1)
                    if turns[i + 1][0] - turns[i][0] < min_phase), None)
        if idx is None:
            return turns
        turns = [tv for k, tv in enumerate(turns) if k not in (idx, idx + 1)]
        turns = _bbq_alternar(turns, y)


def _bbq_ciclo_minimo(turns, y, min_cycle: int = BBQ_MIN_CYCLE):
    while True:
        idx = next((i for i in range(len(turns) - 2)
                    if turns[i + 2][0] - turns[i][0] < min_cycle), None)
        if idx is None:
            return turns
        t0, tipo = turns[idx]
        t2, _ = turns[idx + 2]
        if tipo == "P":
            drop = idx + 2 if y[t0] >= y[t2] else idx
        else:
            drop = idx + 2 if y[t0] <= y[t2] else idx
        turns = [tv for k, tv in enumerate(turns) if k not in (idx + 1, drop)]
        turns = _bbq_alternar(turns, y)


def bbq_turning_points(y: np.ndarray):
    """Algoritmo BBQ completo (identico a bry_boschan_pib.py). -> [(t,'P'|'T')]."""
    n = len(y)
    cands = _bbq_extremos_locales(y, BBQ_K_WINDOW)
    cands = _bbq_censurar(cands, n, BBQ_K_WINDOW)
    turns = _bbq_alternar(cands, y)
    turns = _bbq_fase_minima(turns, y, BBQ_MIN_PHASE)
    turns = _bbq_ciclo_minimo(turns, y, BBQ_MIN_CYCLE)
    turns = _bbq_fase_minima(turns, y, BBQ_MIN_PHASE)
    turns = _bbq_alternar(turns, y)
    return turns


# --- Deteccion de picos sobre el (log) PIB real desestacionalizado ---
_BBQ_PICOS_CACHE = None


def detectar_picos_bbq(insumos_csv: str = None):
    """
    Fecha el ciclo con el algoritmo de Bry-Boschan trimestral sobre el logaritmo del
    PIB real desestacionalizado y devuelve la lista ordenada de fechas de PICO.
    Cachea el resultado (el insumo no cambia dentro de una corrida).
    """
    global _BBQ_PICOS_CACHE
    if _BBQ_PICOS_CACHE is not None:
        return _BBQ_PICOS_CACHE
    ruta = INSUMOS_TRIM_CSV if insumos_csv is None else insumos_csv
    d = pd.read_csv(ruta, parse_dates=["date"]).dropna(subset=["V_pib"]).sort_values("date")
    fechas = list(d["date"])
    y = np.log(d["V_pib"].to_numpy(dtype=float))
    turns = bbq_turning_points(y)
    _BBQ_PICOS_CACHE = sorted(fechas[t] for t, tipo in turns if tipo == "P")
    return _BBQ_PICOS_CACHE


def _quarters_elapsed(date: pd.Timestamp, ref: pd.Timestamp) -> float:
    """Trimestres (float) transcurridos de `ref` a `date` (meses/3; sirve para
    frecuencia mensual o trimestral por igual)."""
    return ((date.year - ref.year) * 12 + (date.month - ref.month)) / 3.0


def construir_variables_ciclo(dates, picos, incremento: float = BBQ_INCREMENTO):
    """
    Construye las variables de ciclo (rampas-meseta) al estilo CBO para las
    fechas `dates` dados los `picos` (lista ordenada de Timestamps).

    Devuelve un DataFrame indexado por `dates` con una columna 'ciclo_j' por
    cada pico j:
        ciclo_j(t) = incremento * clip(q_elapsed(t, pico_j), 0, L_j)
      donde L_j = q_elapsed(pico_{j+1}, pico_j) para ciclos completos, y
      sin cota superior (rampa en curso) para el ultimo pico.
    """
    dates = pd.DatetimeIndex(dates)
    picos = sorted(pd.Timestamp(p) for p in picos)
    out = pd.DataFrame(index=dates)
    for j, p in enumerate(picos):
        qe = np.array([_quarters_elapsed(d, p) for d in dates], dtype=float)
        if j < len(picos) - 1:
            L = _quarters_elapsed(picos[j + 1], p)
            ramp = incremento * np.clip(qe, 0.0, L)
        else:
            ramp = incremento * np.clip(qe, 0.0, None)  # ciclo en curso
        out[f"ciclo_{j+1}"] = ramp
    return out


UNEMPLOYMENT_COL = "Unemp_Desest"


CORE_INFLATION_COL = "Core_Inf"


INFLATION_TARGET_COL = "Inf_Goal"


OIL_PRICE_COL = "Brent_Oil_Price"


NOMINAL_RATE_COL = "TES_Rate_1yr_COP"


REAL_RATE_COL = "TES_Rate_1yr_UVR"


ICU_COL = "ICU"


# v7: el ICU de Data_NAIRU.xlsx NO viene desestacionalizado (ene-feb y abr bajos,
# ago-oct altos: ~3 pts entre extremos; autocorrelacion de dICU en el rezago 12 = 0.19),
# a diferencia del desempleo, la TGP y el PIB. Se desestacionaliza con STL robusto
# (periodo 12) sobre toda su historia; el resultado alimenta el ancla, el bloque de
# tasas naturales y el modulo de PIB.
ICU_DESESTACIONALIZAR = True


def desestacionalizar_icu(fechas: pd.Series, icu: pd.Series) -> pd.Series:
    from statsmodels.tsa.seasonal import STL
    y = pd.Series(icu.to_numpy(dtype=float), index=pd.DatetimeIndex(fechas))
    validos = y.dropna()
    serie = validos.asfreq("MS")
    if serie.isna().any():
        raise ValueError("El ICU tiene huecos internos; no se puede desestacionalizar con STL.")
    est = STL(serie, period=12, robust=True, seasonal=13).fit().seasonal
    return pd.Series((y - est.reindex(y.index)).to_numpy(), index=icu.index)


# Choque de oferta petrolero en PESOS. El Brent de Data_NAIRU.xlsx esta en USD
# (FRED POILBREUSDM); para Colombia el choque relevante es en pesos, asi que se
# multiplica por la TRM promedio mensual (Superintendencia Financiera via
# datos.gov.co, ver download_trm_cop_usd.py):
#   "cop" -> oil_shock = 100*dln(Brent_USD * TRM) = dln(Brent) + dln(TRM)
#   "usd" -> oil_shock = 100*dln(Brent_USD)
OIL_SHOCK_MODE = "cop"


TRM_CSV = "trm_cop_usd_promedio_mensual.csv"


EXPECTATIONS_HORIZON_MONTHS = 12


MIN_NAIRU_LEVEL = 3.0


MAX_NAIRU_LEVEL = 20.0


MIN_NAICU_LEVEL = 55.0


MAX_NAICU_LEVEL = 90.0


Z_95 = 1.959963984540054


# --------------------------------------------------------------------------- #
#  Datos                                                                        #
# --------------------------------------------------------------------------- #
MAX_LAGS = 6  # numero maximo de terminos en la holgura de rezagos distribuidos



@dataclass(slots=True)
class ModelData:
    inflation_gap_change: np.ndarray
    inflation_gap_change_lag1: np.ndarray
    inflation_gap_change_lag2: np.ndarray
    unemployment_current: np.ndarray
    unemployment_lag1: np.ndarray
    unemployment_lag2: np.ndarray
    unemployment_ma24: np.ndarray
    icu_current: np.ndarray
    icu_lag1: np.ndarray
    icu_lag2: np.ndarray
    icu_ma24: np.ndarray
    unemployment_lags: np.ndarray  # (n_obs, MAX_LAGS): columna j = rezago j
    icu_lags: np.ndarray           # (n_obs, MAX_LAGS)
    expected_inflation_term: np.ndarray
    oil_shock: np.ndarray
    dates: pd.Series
    n_obs: int


def _to_float_array(series: pd.Series) -> np.ndarray:
    return np.ascontiguousarray(series.to_numpy(dtype=float))


#  Columnas que SOLO usa la especificacion de tendencia "ma24" (la variante de ancla por media movil):
#  el ancla de reversion a la media movil de 24 meses. Las specs de histeresis y
#  de random walk NO las usan, asi que exigirlas en el dropna truncaria la muestra
#  por una variable que el modelo final no emplea (ver `require_ma24`).
MA24_COLS = ("unemployment_ma24", "icu_ma24")


def load_and_prepare_data(path: Path, require_ma24: bool = True,
                          oil_shock_mode: str = None) -> pd.DataFrame:
    """Construye la muestra de estimacion de la curva de Phillips.

    `require_ma24=True` (default) exige que existan las medias moviles de 24 meses
    y por tanto recorta el inicio de la muestra; solo es necesario cuando se va a
    estimar una spec con `trend="ma24"` (la variante de ancla por media movil) o cuando se comparan specs
    contra ella (logLik/AIC/BIC exigen MISMA muestra). Las corridas que solo usan
    histeresis o random walk deben pasar `require_ma24=False`: las columnas ma24 se siguen
    calculando, pero con NaN al inicio y sin condicionar la muestra.

    Nota: los rezagos de desempleo/ICU se exigen hasta MAX_LAGS (no solo hasta el
    n_lags de la spec) a proposito, para que el barrido de rezagos compare todos
    los L sobre una unica muestra.
    """
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    df = pd.read_excel(path)
    required = [
        "Year", "Month", UNEMPLOYMENT_COL, CORE_INFLATION_COL, INFLATION_TARGET_COL,
        OIL_PRICE_COL, NOMINAL_RATE_COL, REAL_RATE_COL, ICU_COL,
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    for col in [UNEMPLOYMENT_COL, CORE_INFLATION_COL, INFLATION_TARGET_COL,
                OIL_PRICE_COL, NOMINAL_RATE_COL, REAL_RATE_COL, ICU_COL]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["Date"] = pd.to_datetime(
        dict(year=pd.to_numeric(df["Year"], errors="coerce"),
             month=pd.to_numeric(df["Month"], errors="coerce"), day=1),
        errors="coerce",
    )
    df = df.sort_values("Date").dropna(subset=["Date"]).reset_index(drop=True)
    if ICU_DESESTACIONALIZAR:
        df[ICU_COL] = desestacionalizar_icu(df["Date"], df[ICU_COL])

    nominal_rate = df[NOMINAL_RATE_COL] / 100.0
    real_rate = df[REAL_RATE_COL] / 100.0
    df["expected_inflation_fisher_12m_ahead"] = 100.0 * (
        (1.0 + nominal_rate) / (1.0 + real_rate) - 1.0
    )
    df["expected_inflation_current_period"] = df[
        "expected_inflation_fisher_12m_ahead"
    ].shift(EXPECTATIONS_HORIZON_MONTHS)

    df["inflation_gap"] = df[CORE_INFLATION_COL] - df[INFLATION_TARGET_COL]
    df["inflation_gap_change"] = df["inflation_gap"].diff()
    df["inflation_gap_change_lag1"] = df["inflation_gap_change"].shift(1)
    df["inflation_gap_change_lag2"] = df["inflation_gap_change"].shift(2)

    df["unemployment_current"] = df[UNEMPLOYMENT_COL]
    df["icu_current"] = df[ICU_COL]
    for j in range(1, MAX_LAGS):
        df[f"unemployment_lag{j}"] = df[UNEMPLOYMENT_COL].shift(j)
        df[f"icu_lag{j}"] = df[ICU_COL].shift(j)
    df["unemployment_ma24"] = (
        df[UNEMPLOYMENT_COL].shift(1).rolling(window=24, min_periods=24).mean()
    )
    df["icu_ma24"] = (
        df[ICU_COL].shift(1).rolling(window=24, min_periods=24).mean()
    )

    oil_level = df[OIL_PRICE_COL].where(df[OIL_PRICE_COL] > 0.0)
    df["oil_shock_usd"] = 100.0 * np.log(oil_level).diff()
    # TRM promedio mensual (COP por USD) -> choque petrolero en pesos
    mode = OIL_SHOCK_MODE if oil_shock_mode is None else oil_shock_mode
    trm_path = path.parent / TRM_CSV
    if not trm_path.exists():
        raise FileNotFoundError(f"Falta {trm_path}; correr download_trm_cop_usd.py")
    trm = pd.read_csv(trm_path, parse_dates=["date"])[["date", "trm_cop_usd"]]
    trm["Date"] = trm["date"].dt.to_period("M").dt.to_timestamp()
    df = df.merge(trm[["Date", "trm_cop_usd"]], on="Date", how="left")
    df["fx_shock"] = 100.0 * np.log(df["trm_cop_usd"].where(df["trm_cop_usd"] > 0.0)).diff()
    df["oil_shock_cop"] = df["oil_shock_usd"] + df["fx_shock"]
    if mode == "cop":
        df["oil_shock"] = df["oil_shock_cop"]
    elif mode == "usd":
        df["oil_shock"] = df["oil_shock_usd"]
    else:
        raise ValueError(f"oil_shock_mode desconocido: {mode}")

    df["expected_inflation_term"] = (
        df["expected_inflation_current_period"] - df[CORE_INFLATION_COL].shift(1)
    )

    cols = [
        "Date", "inflation_gap", "inflation_gap_change",
        "inflation_gap_change_lag1", "inflation_gap_change_lag2",
        "unemployment_current", "icu_current",
        "unemployment_ma24", "icu_ma24", "oil_shock", "expected_inflation_term",
        "oil_shock_usd", "fx_shock", "oil_shock_cop", "trm_cop_usd",
    ]
    for j in range(1, MAX_LAGS):
        cols += [f"unemployment_lag{j}", f"icu_lag{j}"]
    # Las columnas ma24 se conservan siempre (la spec la variante de ancla por media movil las necesita),
    # pero solo condicionan la muestra si se van a usar. Las columnas auxiliares
    # del choque (usd/fx/cop/trm) tampoco condicionan la muestra.
    aux_cols = ("oil_shock_usd", "fx_shock", "oil_shock_cop", "trm_cop_usd")
    subset = [c for c in cols if (require_ma24 or c not in MA24_COLS) and c not in aux_cols]
    model_df = df[cols].dropna(subset=subset).reset_index(drop=True)
    if len(model_df) < 60:
        raise ValueError("Not enough observations after preprocessing.")
    return model_df


def build_model_data(df: pd.DataFrame) -> ModelData:
    n = len(df)
    u_lags = np.empty((n, MAX_LAGS), dtype=float)
    icu_lags = np.empty((n, MAX_LAGS), dtype=float)
    u_lags[:, 0] = _to_float_array(df["unemployment_current"])
    icu_lags[:, 0] = _to_float_array(df["icu_current"])
    for j in range(1, MAX_LAGS):
        u_lags[:, j] = _to_float_array(df[f"unemployment_lag{j}"])
        icu_lags[:, j] = _to_float_array(df[f"icu_lag{j}"])
    return ModelData(
        inflation_gap_change=_to_float_array(df["inflation_gap_change"]),
        inflation_gap_change_lag1=_to_float_array(df["inflation_gap_change_lag1"]),
        inflation_gap_change_lag2=_to_float_array(df["inflation_gap_change_lag2"]),
        unemployment_current=u_lags[:, 0].copy(),
        unemployment_lag1=u_lags[:, 1].copy(),
        unemployment_lag2=u_lags[:, 2].copy(),
        unemployment_ma24=_to_float_array(df["unemployment_ma24"]),
        icu_current=icu_lags[:, 0].copy(),
        icu_lag1=icu_lags[:, 1].copy(),
        icu_lag2=icu_lags[:, 2].copy(),
        icu_ma24=_to_float_array(df["icu_ma24"]),
        unemployment_lags=u_lags,
        icu_lags=icu_lags,
        expected_inflation_term=_to_float_array(df["expected_inflation_term"]),
        oil_shock=_to_float_array(df["oil_shock"]),
        dates=df["Date"].reset_index(drop=True),
        n_obs=len(df),
    )


# Raiz = carpeta de este script (v7/): los insumos se copiaron a v7/ y v7/datos/ y las
# salidas van a v7/outputs/, de modo que v7 no lee ni escribe nada fuera de su carpeta.
PROJECT_DIR = str(_HERE)


# Insumos del modulo de PIB, construidos desde las fuentes originales por
# datos/build_insumos_pib.py (DANE cuentas nacionales y GEIH, Penn World Table).
INSUMOS_TRIM_CSV = os.path.join(PROJECT_DIR, "datos", "insumos_pib_trimestral.csv")
INSUMOS_MENS_CSV = os.path.join(PROJECT_DIR, "datos", "insumos_pib_mensual.csv")
INSUMOS_HC_CSV = os.path.join(PROJECT_DIR, "datos", "insumos_pib_hc_anual.csv")






NAIRU_CSV_DEFAULT = os.path.join(PROJECT_DIR, "outputs", "nairu_estimates_v7.csv")   # lo sobrescribe la orquestacion


OUTPUT_DIR = os.path.join(PROJECT_DIR, "outputs")




# festivos efectivos por trimestre (para ajustar horas trabajadas)
FESTIVOS_XLSX = os.path.join(PROJECT_DIR, "festivos_efectivos_colombia_2001_2026.xlsx")


# stock de capital productivo DANE (anual), fuente principal de K
DANE_CAPITAL_CSV = os.path.join(PROJECT_DIR, "datos", "dane_stock_capital_productivo.csv")






# Parámetros del modelo
HP_LAMBDA = 1600








ALPHA_WINDOW_START = pd.Timestamp("2016-03-01")  # 2016Q1 (trimestre etiquetado 2016-03-01)


ALPHA_WINDOW_END = None                  # None => hasta el último trimestre disponible T


BASE_QUARTER = pd.Timestamp("2005-03-01")  # base de índices = 2005Q1 (etiqueta = primer día del mes final del trimestre)


# Vacaciones/festivos (semanas) y jornada legal (horas/semana) por Ley 2101 de 2021
VACACIONES_SEMANAS = 6.6


JORNADA_SCHEDULE = [
    (pd.Timestamp("2023-09-01"), 47),
    (pd.Timestamp("2024-09-01"), 46),
    (pd.Timestamp("2025-09-01"), 44),
    (pd.Timestamp("2026-09-01"), 42),
]


JORNADA_BASE = 48  # hasta 2023Q2


# agregación trimestral de Ocupados
#   "promedio"        -> promedio simple de los 3 meses del trimestre.
#   "fin_de_trimestre" -> valor puntual del último mes del trimestre.
OCUPADOS_TRIMESTRAL_DEFAULT = "promedio"


# método de tendencia de la PTF que entra al PIB potencial
#   "cbo" -> regresión OLS con tendencia lineal por tramos + cíclicos + dummies.
#   "hp"  -> filtro HP puro (pib_pot = pib_pot_hp).
TFP_TREND_METHOD_DEFAULT = "cbo"


# Nudos ("sticks" tipo CBO) de la tendencia por tramos de PTF (picos de ciclo colombiano).
# Especificación S1: se conserva únicamente el nudo de 2008Q1 (el único individualmente
# significativo). El F-test conjunto sobre los nudos de 2014Q4 y 2019Q4 (F=0.55, p=0.58) no
# rechaza que sean cero, y S1 es preferida por BIC y R² ajustado frente a la spec de 3 nudos
# (ver el ejercicio de sensibilidad de nudos en la nota metodologica).
KNOTS = [pd.Timestamp("2008-03-01")]


# Dummies de pandemia (una por trimestre; las series son sumas móviles 4T)
PANDEMIC_DUMMIES = [
    pd.Timestamp("2020-06-01"), pd.Timestamp("2020-09-01"),
    pd.Timestamp("2020-12-01"), pd.Timestamp("2021-03-01"),
]


# capital humano — extrapolación de años PWT no observados
# Extrapolacion del indice de capital humano mas alla del ultimo anio observado.
HC_EXTRAP_DEFAULT = "ols_anclado"


PWT_HC_LAST_YEAR = 2023          # ultimo anio con dato observado de la Penn World Table


HC_EXTRAP_SLOPE_YEARS = 10       # años usados para estimar la pendiente de extrapolación (2014-2023)


# ---------------------------------------------------------------------------
# OPCIONES DEL MODULO DE PIB (continuacion)
# ---------------------------------------------------------------------------
#
# Nudos de la tendencia: fuente de los nudos y forma de la tendencia por tramos de la PTF
#   tfp_knots_source: "fixed" -> nudo(s) predefinido(s) en KNOTS .
#                     "bbq"   -> picos detectados por Bry-Boschan.
#   tfp_piecewise:    "cumulative" -> base spline max(0, tau-tau_k) .
#                     "plateau"    -> rampas-meseta estilo CBO/NBER.
TFP_KNOTS_SOURCE_DEFAULT = "bbq"


TFP_PIECEWISE_DEFAULT = "plateau"


# Quiebre EXOGENO de la tendencia de PTF (2026-10-14): fin del auge de materias
# primas, 2014Q3 (el Brent empieza a caer: 110 USD en 2014Q2, 103 en Q3, 77 en Q4).
# Bry-Boschan no fecha ningun pico entre 2005 y 2019 (no hubo recesion clasica), asi
# que sin este nudo la tendencia de PTF es UNA recta 2005Q1-2019Q4 y lee como ciclica
# la desaceleracion estructural posterior a 2014 (PTF observada: +0.2%/ano en 2006-14,
# -1.3%/ano en 2015-19). Se suma a los picos BBQ SOLO en la tendencia de PTF (no en
# TGP* ni en el fechado del ciclo). Fecha fijada por criterio economico, no por busqueda:
# el minimo de SSR esta en 2015Q4 con un valle plano 2014Q4-2016Q1
# (outputs/diagnostico_quiebre_ptf.csv).
TFP_QUIEBRES_EXOGENOS_DEFAULT = (pd.Timestamp("2014-09-01"),)


# tasa de participación potencial (TGP*)
#   participation_form: "logit"  | "levels" (en niveles).
#   participation_ma_window: 4  | 8 (24 meses).
#   participation_add_capital_gap: añade brecha de uso de capital (icu-naicu).
#   participation_piecewise: usa tendencia por tramos BBQ en vez de tendencia lineal.
PARTICIPATION_FORM_DEFAULT = "levels"


PARTICIPATION_MA_WINDOW_DEFAULT = 8


PARTICIPATION_ADD_CAPITAL_GAP_DEFAULT = True


PARTICIPATION_PIECEWISE_DEFAULT = True


# horas trabajadas ajustadas por festivos efectivos
HOURS_FESTIVOS_DEFAULT = True


# Días laborales por semana usados para valorar un festivo en horas (jornada/DIAS_SEM)
DIAS_LABORALES_SEMANA = 6.0


# fuente del capital físico
#   "dane"        -> stock de capital productivo DANE (opcion vigente).
# Fuente del capital fisico: stock de capital productivo publicado por el DANE.
CAPITAL_SOURCE_DEFAULT = "dane"


# método de alpha (participación del capital)
#   "cbo"  -> EBE/(RA+EBE) (quita impuestos-subsidios e ingreso mixto).
#   "full" -> EBE/(RA+TmS+EBE+IM) .
ALPHA_METHOD_DEFAULT = "cbo"


# medida de producto en la función de producción
#   "total"  -> PIB total (opcion vigente).
#   "ex_oil" -> PIB sin petróleo-gas .
OUTPUT_MEASURE_DEFAULT = "total"



@dataclass
class Config:
    """Configuracion del pipeline de PIB potencial."""
    ocupados_trimestral: str = OCUPADOS_TRIMESTRAL_DEFAULT
    tfp_trend_method: str = TFP_TREND_METHOD_DEFAULT
    hc_extrap: str = HC_EXTRAP_DEFAULT
    knots: list = field(default_factory=lambda: list(KNOTS))
    pandemic_dummies: list = field(default_factory=lambda: list(PANDEMIC_DUMMIES))
    pwt_hc_last_year: int = PWT_HC_LAST_YEAR
    hc_extrap_slope_years: int = HC_EXTRAP_SLOPE_YEARS
    # ---  ---
    tfp_knots_source: str = TFP_KNOTS_SOURCE_DEFAULT
    tfp_piecewise: str = TFP_PIECEWISE_DEFAULT
    # nudos exogenos que se suman a los picos BBQ solo en la tendencia de PTF
    tfp_quiebres_exogenos: list = field(default_factory=lambda: list(TFP_QUIEBRES_EXOGENOS_DEFAULT))
    participation_form: str = PARTICIPATION_FORM_DEFAULT
    participation_ma_window: int = PARTICIPATION_MA_WINDOW_DEFAULT
    participation_add_capital_gap: bool = PARTICIPATION_ADD_CAPITAL_GAP_DEFAULT
    participation_piecewise: bool = PARTICIPATION_PIECEWISE_DEFAULT
    hours_festivos: bool = HOURS_FESTIVOS_DEFAULT
    capital_source: str = CAPITAL_SOURCE_DEFAULT
    alpha_method: str = ALPHA_METHOD_DEFAULT
    output_measure: str = OUTPUT_MEASURE_DEFAULT
    nairu_csv: str = NAIRU_CSV_DEFAULT               # CSV de NAIRU/NAICU que consume el pipeline
    # picos BBQ (se inyectan en ejecutar_pipeline; None => se fechan de los insumos)
    picos_bbq: list = field(default=None)

    @property
    def label(self) -> str:
        return (f"ocupados={self.ocupados_trimestral}, tfp_trend={self.tfp_trend_method}/"
                f"{self.tfp_knots_source}/{self.tfp_piecewise}, hc_extrap={self.hc_extrap}, "
                f"tgp*={self.participation_form}(ma{self.participation_ma_window}), "
                f"festivos={self.hours_festivos}, K={self.capital_source}, "
                f"alpha={self.alpha_method}, producto={self.output_measure}")


CONFIG_PRODUCCION = Config()


# Serie DANE de crecimiento anual de PTF publicada (%), para comparación
DANE_PTF_GROWTH = {
    2005: -0.3200, 2006: 4.2308, 2007: -2.6822, 2008: -1.6541, 2009: -1.0518,
    2010: -1.7028, 2011: 0.4646, 2012: -0.4509, 2013: 0.4602, 2014: -1.4143,
    2015: 0.7723, 2016: -1.1601, 2017: 0.1938, 2018: 0.2351, 2019: 0.2295,
    2020: -1.5029, 2021: 1.2977, 2022: 0.1325, 2023: -0.9234, 2024: 1.2060,
    2025: 0.7537,
}


EXCEL_EPOCH = dt.date(1899, 12, 30)



def excel_serial(date) -> int:
    """Convierte una fecha a número serial de Excel (días desde 1899-12-30)."""
    if isinstance(date, pd.Timestamp):
        date = date.date()
    return (date - EXCEL_EPOCH).days


def quarter_label(date: pd.Timestamp) -> pd.Timestamp:
    """Devuelve la etiqueta de trimestre (primer día del mes final: mar/jun/sep/dic)."""
    q_end_month = ((date.month - 1) // 3 + 1) * 3
    return pd.Timestamp(year=date.year, month=q_end_month, day=1)


def jornada_legal(date: pd.Timestamp) -> int:
    """Jornada legal semanal (horas) vigente en el trimestre `date` (Ley 2101/2021)."""
    horas = JORNADA_BASE
    for umbral, valor in JORNADA_SCHEDULE:
        if date >= umbral:
            horas = valor
    return horas


def leer_insumos_mensuales(path: str = None) -> pd.DataFrame:
    """Insumos mensuales del mercado de trabajo: fecha, PET, fuerza laboral y ocupados.
    Los construye datos/build_insumos_pib.py a partir de la GEIH desestacionalizada."""
    d = pd.read_csv(INSUMOS_MENS_CSV if path is None else path, parse_dates=["date"])
    faltan = {"date", "pet", "fl", "ocup"} - set(d.columns)
    if faltan:
        raise ValueError(f"Faltan columnas en los insumos mensuales: {sorted(faltan)}")
    return d.sort_values("date").reset_index(drop=True)


def leer_insumos_trimestrales(path: str = None) -> pd.DataFrame:
    """Insumos trimestrales de cuentas nacionales: PIB real desestacionalizado, valor
    agregado de petroleo y gas, y enfoque del ingreso (remuneracion de asalariados,
    impuestos menos subvenciones, excedente bruto de explotacion e ingreso mixto)."""
    d = pd.read_csv(INSUMOS_TRIM_CSV if path is None else path, parse_dates=["date"])
    faltan = {"date", "V_pib", "X_vapg", "BQ_ra", "BR_tms", "BS_ebe", "BT_im"} - set(d.columns)
    if faltan:
        raise ValueError(f"Faltan columnas en los insumos trimestrales: {sorted(faltan)}")
    return d.sort_values("date").reset_index(drop=True)


def leer_hc_anual(path: str = None) -> pd.Series:
    """Indice de capital humano de la Penn World Table, por anio observado.
    Devuelve una serie indexada por el 1 de enero de cada anio."""
    d = pd.read_csv(INSUMOS_HC_CSV if path is None else path)
    idx = pd.to_datetime(dict(year=d["year"].astype(int), month=1, day=1))
    return pd.Series(d["hc"].to_numpy(dtype=float), index=idx).sort_index()


def leer_nairu_csv(path: str) -> pd.DataFrame:
    """Lee NAIRU, NAICU e ICU mensuales del CSV de tasas naturales."""
    df = pd.read_csv(path, usecols=["Date", "nairu_estimate", "naicu_estimate", "icu_current"])
    df["date"] = pd.to_datetime(df["Date"])
    df = df.rename(
        columns={"nairu_estimate": "nairu", "naicu_estimate": "naicu", "icu_current": "icu"}
    )
    return df[["date", "nairu", "naicu", "icu"]].sort_values("date").reset_index(drop=True)


def agregar_mensual_a_trimestral(monthly: pd.DataFrame, ocupados_trimestral: str = OCUPADOS_TRIMESTRAL_DEFAULT) -> pd.DataFrame:
    """
    Promedio simple de los 3 meses del trimestre (solo trimestres completos) para
    PET, FL, NAIRU, NAICU, ICU.

    Ocupados (col Q):
      - ocupados_trimestral="promedio" (opcion vigente): promedio simple de los 3 meses,
        igual tratamiento que PET y FL.
      - ocupados_trimestral="fin_de_trimestre" (valor puntual de fin de trimestre,
        verificado contra la fórmula cacheada en celda AO5 = XLOOKUP directo al mes
        de cierre del trimestre): se toma el valor puntual del ÚLTIMO MES del
        trimestre (mar/jun/sep/dic).
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
        # Ocupados: valor puntual del mes de cierre del trimestre (no promedio)
        ocup_directo = df.loc[df["date"] == df["quarter"], ["quarter", "ocup"]].set_index("quarter")["ocup"]
        out["ocup"] = ocup_directo

    out = out.reset_index().rename(columns={"quarter": "date"})
    return out.sort_values("date").reset_index(drop=True)


def calcular_mercado_laboral(df: pd.DataFrame, ma_window: int = 4) -> pd.DataFrame:
    """
    TGP, TD, brecha de desempleo (TD - NAIRU) y brecha de uso de capital
    (ICU - NAICU), con sus medias móviles de `ma_window` trimestres.

    además de la brecha de desempleo se construye la brecha de
    utilización de capital (icu - naicu) y la ventana móvil pasa de 4 a 8
    trimestres (24 meses ~ especificación de la curva de Phillips). Se conserva
    `ma4_brecha_u` para el la variante con ventana de 4 trimestres.
    """
    df = df.copy()
    df["tgp"] = 100 * df["fl"] / df["pet"]
    df["td"] = 100 * (df["fl"] - df["ocup"]) / df["fl"]
    df["brecha_u"] = df["td"] - df["nairu"]
    df["brecha_icu"] = df["icu"] - df["naicu"]
    df["ma4_brecha_u"] = df["brecha_u"].rolling(window=4, min_periods=4).mean()
    df["ma_brecha_u"] = df["brecha_u"].rolling(window=ma_window, min_periods=ma_window).mean()
    df["ma_brecha_icu"] = df["brecha_icu"].rolling(window=ma_window, min_periods=ma_window).mean()
    return df


# ---------------------------------------------------------------------------
# 4. TGP* ÓPTIMA (REGRESIÓN CBO, HOJA RegTGP)
# ---------------------------------------------------------------------------

@dataclass
class ResultadoRegTGP:
    beta0: float
    beta1: float
    beta2: float
    beta3: float
    r_squared: float
    nobs: int
    modelo: object = field(repr=False, default=None)


def estimar_tgp_star(df: pd.DataFrame, config: "Config" = None,
                     picos: list = None) -> tuple[pd.Series, ResultadoRegTGP]:
    """
    Estima TGP* (participación potencial) por OLS.

    Variante en logit (config.participation_form="logit", sin tramos, ma=4):
        logit(TGP) = b0 + b1*t + b2*brecha_u + b3*MA4_brecha_u          

    Modo de produccion: en NIVELES y con brecha de uso de capital y tramos BBQ:
        TGP = b0 + [tendencia por tramos BBQ] + b2*brecha_u + b3*MA8_brecha_u
              + b4*brecha_icu + b5*MA8_brecha_icu + eps
      La participación es una tasa (55-65%) lejos de 0/1, por lo que el logit no
      aporta y se estima directamente en niveles. La brecha de uso de capital
      (icu - naicu) entra igual que la brecha laboral. Ventana móvil = 8 trim.
      (24 meses, como la curva de Phillips).

    TGP* (potencial) = valor ajustado con TODOS los términos cíclicos en cero
    (solo la tendencia, por tramos o lineal), para todos los trimestres.
    """
    if config is None:
        config = CONFIG_PRODUCCION
    df = df.copy()
    df["t_serial"] = df["date"].apply(excel_serial)

    # --- variable dependiente (niveles o logit) ---
    if config.participation_form == "logit":
        df["dep_tgp"] = np.log((df["tgp"] / 100) / (1 - df["tgp"] / 100))
    elif config.participation_form == "levels":
        df["dep_tgp"] = df["tgp"]
    else:
        raise ValueError(f"participation_form desconocido: {config.participation_form!r}")

    # --- regresores cíclicos ---
    ciclicos = ["brecha_u", "ma_brecha_u"]
    if config.participation_add_capital_gap:
        ciclicos += ["brecha_icu", "ma_brecha_icu"]

    # --- regresores de tendencia (lineal o por tramos BBQ) ---
    if config.participation_piecewise and picos:
        df["tau_q"] = (
            (df["date"].dt.year - BASE_QUARTER.year) * 12
            + (df["date"].dt.month - BASE_QUARTER.month)
        ) / 3.0
        cyc = construir_variables_ciclo(df["date"], picos)
        cyc_cols = list(cyc.columns)
        for c in cyc_cols:
            df[c] = cyc[c].values
        trend_cols = ["tau_q"] + cyc_cols
    else:
        trend_cols = ["t_serial"]

    reg_cols = trend_cols + ciclicos
    muestra = df.dropna(subset=reg_cols + ["dep_tgp"]).copy()
    X = sm.add_constant(muestra[reg_cols])
    y = muestra["dep_tgp"]
    modelo = sm.OLS(y, X, missing="raise").fit()

    # --- componente potencial: solo tendencia (cíclicos = 0) ---
    pred = float(modelo.params["const"]) + sum(
        float(modelo.params[c]) * df[c] for c in trend_cols
    )
    if config.participation_form == "logit":
        tgp_star = 100 * np.exp(pred) / (1 + np.exp(pred))
    else:
        tgp_star = pred  # ya en niveles (%)

    # beta1 = coef. del primer término de tendencia (t_serial o tau_q segun la forma elegida)
    b0 = float(modelo.params["const"])
    b1 = float(modelo.params[trend_cols[0]])
    b2 = float(modelo.params["brecha_u"])
    b3 = float(modelo.params["ma_brecha_u"])
    resultado = ResultadoRegTGP(
        beta0=b0, beta1=b1, beta2=b2, beta3=b3,
        r_squared=modelo.rsquared, nobs=int(modelo.nobs), modelo=modelo,
    )
    return tgp_star, resultado


def leer_festivos_trimestrales(path: str) -> pd.Series:
    """
    lee la hoja 'Resumen trimestral' de festivos_efectivos_...xlsx
    y devuelve una Serie indexada por fecha-etiqueta de trimestre (primer día del
    mes de cierre) con el número de FESTIVOS EFECTIVOS del trimestre (fechas
    festivas que sí generan descanso; los festivos fijos en domingo no cuentan).
    """
    import openpyxl
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    ws = wb["Resumen trimestral"]
    q_month = {"T1": 3, "T2": 6, "T3": 9, "T4": 12}
    registros = {}
    for row in ws.iter_rows(min_row=5, values_only=True):
        anio, trim, festivos = row[0], row[1], row[4]
        if anio is None or trim not in q_month or festivos is None:
            continue
        fecha = pd.Timestamp(year=int(anio), month=q_month[trim], day=1)
        registros[fecha] = float(festivos)
    return pd.Series(registros).sort_index()


def calcular_factor_trabajo(df: pd.DataFrame, config: "Config" = None,
                            festivos: pd.Series = None) -> pd.DataFrame:
    """
    FL*, OCUP*, horas trimestrales (observadas y potenciales), sumas móviles e índices.

    (config.hours_festivos): además de descontar vacaciones (6.6
    semanas/año), se descuenta el tiempo no laborado por FESTIVOS efectivos del
    trimestre. Cada festivo resta un día de trabajo = jornada/DIAS_LABORALES_SEMANA
    horas. Con hours_festivos=False solo se descuentan vacaciones, con semanas
    efectivas constantes.
    """
    if config is None:
        config = CONFIG_PRODUCCION
    df = df.copy()
    df["fl_star"] = df["pet"] * df["tgp_star"] / 100
    df["ocup_star"] = df["fl_star"] * (1 - df["nairu"] / 100)

    df["jornada"] = df["date"].apply(jornada_legal)
    semanas_efectivas = (52 - VACACIONES_SEMANAS) / 4

    if config.hours_festivos and festivos is not None:
        df["festivos_q"] = df["date"].map(festivos).fillna(0.0)
        # horas por trabajador en el trimestre, netas de vacaciones y festivos
        df["horas_worker_q"] = (
            df["jornada"] * semanas_efectivas
            - df["festivos_q"] * df["jornada"] / DIAS_LABORALES_SEMANA
        )
    else:
        df["festivos_q"] = 0.0
        df["horas_worker_q"] = df["jornada"] * semanas_efectivas

    df["horas_star_q"] = df["ocup_star"] * df["horas_worker_q"]
    df["horas_q"] = df["ocup"] * df["horas_worker_q"]

    # Sumas móviles 4 trimestres (anualizadas), disponibles desde 2004Q4
    df["horas_star_ann"] = df["horas_star_q"].rolling(window=4, min_periods=4).sum()
    df["horas_ann"] = df["horas_q"].rolling(window=4, min_periods=4).sum()

    # Índices base 2005Q1, normalizados por el denominador POTENCIAL
    base_val = df.loc[df["date"] == BASE_QUARTER, "horas_star_ann"].iloc[0]
    df["idx_L_star"] = 100 * df["horas_star_ann"] / base_val
    df["idx_L"] = 100 * df["horas_ann"] / base_val
    return df


def extrapolar_hc_anual(hc_anual: pd.Series, hc_extrap: str, ultimo_anio_pwt: int,
                         anios_pendiente: int, anio_final_necesario: int) -> pd.Series:
    """
    Construye la serie anual de hc (índice de capital humano PWT) usada aguas abajo.

    hc_extrap="ols_anclado": de la serie observada solo se toman
    los años <= ultimo_anio_pwt (dato PWT observado). Los años posteriores, hasta
    anio_final_necesario, se extrapolan de forma ANCLADA en el último valor observado:
        hc(y) = hc(ultimo_anio_pwt) + s*(y - ultimo_anio_pwt)
    donde s es la pendiente OLS de hc sobre año, estimada en los últimos
    `anios_pendiente` años observados (p.ej. 2014-2023), de modo que no hay salto de
    nivel en el punto de empalme .
    """
    hc_anual = hc_anual.dropna().sort_index()
    if hc_extrap != "ols_anclado":
        raise ValueError(f"hc_extrap desconocido: {hc_extrap!r}")

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
    """
    Índice de capital humano (PWT) con ancla en Q1 de cada año y interpolación intra-anual:
      a_y = 100*hc(y)/hc(2005)
      Q1 = a_y ; Q2 = (3a_y + a_{y+1})/4 ; Q3 = (2a_y + 2a_{y+1})/4 ; Q4 = (a_y + 3a_{y+1})/4
    """
    hc_by_year = hc_anual.dropna()
    hc_2005 = hc_by_year.loc[hc_by_year.index.year == 2005].iloc[0]
    a = 100 * hc_by_year / hc_2005  # índice anual "a_y"

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
        else:  # q == 4
            idx_hc = (a_y + 3 * a_y1) / 4 if a_y1 is not None else np.nan
        registros.append({"date": fecha, "idx_hc": idx_hc})
    return pd.DataFrame(registros)


def calcular_producto(df: pd.DataFrame, config: "Config" = None) -> pd.DataFrame:
    """
    W (PIB total anualizado), Y (VAPG anualizado), Z = W-Y (PIB sin petróleo-gas),
    índice de producto e insumo para la tendencia HP.

    (config.output_measure): la medida de producto que entra a la
    función de producción (idx_pib, PTF, tendencia) es:
      "total"  -> W_pib_ann (PIB total, opcion vigente).
      "ex_oil" -> Z_pib_exoil_ann (PIB sin petróleo-gas).
    Se conservan ambas columnas; `output_ann` es la elegida.
    """
    if config is None:
        config = CONFIG_PRODUCCION
    df = df.copy()
    df["W_pib_ann"] = df["V_pib"].rolling(window=4, min_periods=4).sum()
    df["Y_vapg_ann"] = df["X_vapg"].rolling(window=4, min_periods=4).sum()

    # W e Y quedan definidas desde 2005Q1 (primera ventana 2004Q2..2005Q1)
    df.loc[df["date"] < BASE_QUARTER, ["W_pib_ann", "Y_vapg_ann"]] = np.nan

    df["Z_pib_exoil_ann"] = df["W_pib_ann"] - df["Y_vapg_ann"]

    if config.output_measure == "total":
        df["output_ann"] = df["W_pib_ann"]
    elif config.output_measure == "ex_oil":
        df["output_ann"] = df["Z_pib_exoil_ann"]
    else:
        raise ValueError(f"output_measure desconocido: {config.output_measure!r}")

    base_val = df.loc[df["date"] == BASE_QUARTER, "output_ann"].iloc[0]
    df["idx_pib"] = 100 * df["output_ann"] / base_val
    return df, base_val


def calcular_tendencia_hp(df: pd.DataFrame, base_out: float, T: pd.Timestamp) -> pd.DataFrame:
    """Tendencia HP del producto elegido (output_ann) sobre 2005Q1..T, lambda=1600."""
    df = df.copy()
    muestra = df[(df["date"] >= BASE_QUARTER) & (df["date"] <= T)].copy()
    ciclo, tendencia = hpfilter(muestra["output_ann"].values, lamb=HP_LAMBDA)
    muestra["hp_trend"] = tendencia
    df = df.merge(muestra[["date", "hp_trend"]], on="date", how="left")
    df["idx_trend"] = 100 * df["hp_trend"] / base_out
    return df


def construir_capital_dane(df: pd.DataFrame, dane_csv: str = None) -> pd.DataFrame:
    """
    Capital = stock de capital productivo DANE (anual, 1990+) interpolado a
    trimestral por PCHIP posicionada en el centro del año (el promedio de 4
    trimestres reproduce el dato anual). Extrapola 2025+ con el crecimiento log
    medio de los últimos 4 años observados. (Fuente principal de K.)
    """
    if dane_csv is None:
        dane_csv = DANE_CAPITAL_CSV
    from scipy.interpolate import PchipInterpolator
    dane = pd.read_csv(dane_csv)
    years = dane["year"].values.astype(float)
    Kv = dane["K_prod_mmp"].values.astype(float)
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


def construir_capital(df: pd.DataFrame, T: pd.Timestamp, config: "Config") -> pd.DataFrame:
    """Dispatcher de la fuente de capital fisico."""
    if config.capital_source == "dane":
        return construir_capital_dane(df)
    raise ValueError(
        f"capital_source desconocido: {config.capital_source!r}. La unica fuente "
        "disponible es 'dane' (stock de capital productivo publicado).")


def calcular_capital_usado(df: pd.DataFrame) -> pd.DataFrame:
    """Capital usado (potencial y observado), sumas móviles e índices base 2005Q1 (denominador potencial)."""
    df = df.copy()
    df["K_used_star"] = df["K"] * df["naicu"] / 100
    df["K_used"] = df["K"] * df["icu"] / 100

    df["K_star_ann"] = df["K_used_star"].rolling(window=4, min_periods=4).sum()
    df["K_ann"] = df["K_used"].rolling(window=4, min_periods=4).sum()

    base_val = df.loc[df["date"] == BASE_QUARTER, "K_star_ann"].iloc[0]
    df["idx_K_star"] = 100 * df["K_star_ann"] / base_val
    df["idx_K"] = 100 * df["K_ann"] / base_val
    return df


def calcular_alpha(df: pd.DataFrame, ventana_inicio: pd.Timestamp, ventana_fin: pd.Timestamp,
                   metodo: str = "full") -> tuple[pd.Series, float]:
    """
    alpha_t (participación del capital) por trimestre; alpha = promedio en la ventana.

    
      metodo="cbo"  -> alpha_t = EBE/(RA+EBE)          (opcion vigente, estilo CBO:
                       quita impuestos-subsidios netos e ingreso mixto del reparto).
      metodo="full" -> alpha_t = EBE/(RA+TmS+EBE+IM)   (alternativa).
    BQ=RA (remuneración asalariados), BR=TmS (impuestos-subsidios), BS=EBE
    (excedente bruto de explotación), BT=IM (ingreso mixto).
    """
    df = df.copy()
    if metodo == "cbo":
        alpha_t = df["BS_ebe"] / (df["BQ_ra"] + df["BS_ebe"])
    elif metodo == "full":
        alpha_t = df["BS_ebe"] / (df["BQ_ra"] + df["BR_tms"] + df["BS_ebe"] + df["BT_im"])
    else:
        raise ValueError(f"alpha metodo desconocido: {metodo!r}")
    ventana = df[(df["date"] >= ventana_inicio) & (df["date"] <= ventana_fin)]
    alpha = alpha_t.loc[ventana.index].mean()
    return alpha_t, alpha


# ---------------------------------------------------------------------------
# 10. PTF Y PIB POTENCIAL
# ---------------------------------------------------------------------------

@dataclass
class ResultadoRegPTF:
    """Resultado de la regresión de tendencia de PTF estilo CBO."""
    params: "pd.Series" = field(repr=False, default=None)
    bse: "pd.Series" = field(repr=False, default=None)
    tvalues: "pd.Series" = field(repr=False, default=None)
    pvalues: "pd.Series" = field(repr=False, default=None)
    r_squared: float = np.nan
    nobs: int = 0
    knots: list = field(default_factory=list)
    dummies: list = field(default_factory=list)
    modelo: object = field(repr=False, default=None)

    def tabla(self) -> pd.DataFrame:
        return pd.DataFrame({
            "Coeficiente": self.params,
            "Error Est.": self.bse,
            "t": self.tvalues,
            "p-valor": self.pvalues,
        })


def estimar_ptf_tendencia_cbo(df: pd.DataFrame, T: pd.Timestamp, knots: list, dummies: list,
                              piecewise_basis: str = "cumulative") -> tuple[pd.Series, ResultadoRegPTF]:
    """
    Tendencia de PTF estilo CBO (ecuación 29 de Shackleton 2018, adaptada):

        ln(PTF_t) = b0 + b1*tau_t + sum_j gamma_j*S_j(tau_t)
                    + phi0*gap_t + phi1*gap_{t-1} + sum_m delta_m*D_m,t + eps_t

    tau_t = índice entero de trimestre (0,1,2,...) desde 2005Q1 (BASE_QUARTER).
    gap_t = brecha_u (TD - NAIRU, en puntos porcentuales).
    D_m   = dummies puntuales de pandemia.

    Base por tramos S_j(tau):
      piecewise_basis="cumulative": spline acumulativo max(0, tau - tau_kj)
        (variante; los quiebres son los `knots` predefinidos).
      piecewise_basis="plateau": rampas-meseta estilo CBO/NBER, donde `knots` son
        los PICOS del ciclo (Bry-Boschan). Cada rampa vale 0 antes del pico, crece
        0.25/trim durante el ciclo y se congela (meseta) en el pico siguiente; el
        último pico origina una rampa en curso sin meseta.

    PTF*_t (tendencia potencial, sin cíclicos ni dummies) para todos los trimestres:
        PTF*_t = exp(b0 + b1*tau_t + sum_j gamma_j*S_j(tau_t))
    """
    muestra_mask = (df["date"] >= BASE_QUARTER) & (df["date"] <= T)
    d = df.loc[muestra_mask, ["date", "ptf", "brecha_u"]].copy().sort_values("date").reset_index(drop=True)
    d["tau"] = range(len(d))
    d["ln_ptf"] = np.log(d["ptf"])
    d["gap"] = d["brecha_u"]
    d["gap_lag1"] = d["gap"].shift(1)

    knot_cols = []
    if piecewise_basis == "cumulative":
        tau_by_date = d.set_index("date")["tau"]
        for j, k in enumerate(knots):
            k_label = pd.Timestamp(k)
            tau_k = tau_by_date.get(k_label)
            if tau_k is None:
                raise ValueError(f"Nudo {k_label.date()} fuera de la muestra 2005Q1..T")
            d[f"knot_{j+1}"] = np.maximum(0, d["tau"] - tau_k)
            knot_cols.append(f"knot_{j+1}")
    elif piecewise_basis == "plateau":
        # rampas-meseta BBQ (los `knots` son los picos dentro de la muestra)
        picos_muestra = [pd.Timestamp(k) for k in knots
                         if BASE_QUARTER <= pd.Timestamp(k) <= T]
        cyc = construir_variables_ciclo(d["date"], picos_muestra)
        for c in cyc.columns:
            d[c] = cyc[c].values
            knot_cols.append(c)
    else:
        raise ValueError(f"piecewise_basis desconocido: {piecewise_basis!r}")

    for m, dt_dummy in enumerate(dummies):
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
        r_squared=modelo.rsquared, nobs=int(modelo.nobs), knots=list(knots), dummies=list(dummies),
        modelo=modelo,
    )

    # PTF* para TODOS los trimestres 2005Q1..T (términos cíclicos y dummies en cero)
    ln_ptf_star = modelo.params["const"] + modelo.params["tau"] * d["tau"]
    for c in knot_cols:
        ln_ptf_star = ln_ptf_star + modelo.params[c] * d[c]
    ptf_star = np.exp(ln_ptf_star)
    ptf_star.index = d["date"]

    return ptf_star, resultado


def calcular_ptf_y_potencial(df: pd.DataFrame, alpha: float, T: pd.Timestamp,
                              config: Config) -> tuple[pd.DataFrame, ResultadoRegPTF | None]:
    """
    PTF (residuo de Solow), tendencias de PTF (HP, y CBO por tramos si
    config.tfp_trend_method == "cbo"), PIB potencial (principal y alternativo) y brechas.
    """
    df = df.copy()
    muestra_mask = (df["date"] >= BASE_QUARTER) & (df["date"] <= T)

    df["ptf"] = np.nan
    df.loc[muestra_mask, "ptf"] = (
        df.loc[muestra_mask, "idx_pib"]
        / (df.loc[muestra_mask, "idx_K"] ** alpha * df.loc[muestra_mask, "idx_LH"] ** (1 - alpha))
    )

    muestra = df.loc[muestra_mask].copy()
    ciclo, tendencia = hpfilter(muestra["ptf"].values, lamb=HP_LAMBDA)
    df.loc[muestra_mask, "ptf_hp"] = tendencia

    # --- tendencia de PTF estilo CBO (por tramos) ---
    reg_ptf = None
    if config.tfp_trend_method == "cbo":
        # Nudos de la tendencia: los nudos son los picos BBQ (source="bbq") o los fijos (source="fixed")
        if config.tfp_knots_source == "bbq":
            knots = sorted({pd.Timestamp(p) for p in (config.picos_bbq or [])}
                           | {pd.Timestamp(p) for p in (config.tfp_quiebres_exogenos or [])})
        else:
            knots = config.knots
        ptf_star, reg_ptf = estimar_ptf_tendencia_cbo(
            df, T, knots, config.pandemic_dummies, piecewise_basis=config.tfp_piecewise
        )
        df.loc[muestra_mask, "ptf_star"] = df.loc[muestra_mask, "date"].map(ptf_star)
        ptf_para_potencial = df.loc[muestra_mask, "ptf_star"]
    elif config.tfp_trend_method == "hp":
        df.loc[muestra_mask, "ptf_star"] = df.loc[muestra_mask, "ptf_hp"]
        ptf_para_potencial = df.loc[muestra_mask, "ptf_hp"]
    else:
        raise ValueError(f"tfp_trend_method desconocido: {config.tfp_trend_method!r}")

    # PIB potencial principal (usa ptf_star: PTF* por tramos, o HP si se elige esa variante)
    df.loc[muestra_mask, "pib_pot"] = (
        ptf_para_potencial
        * df.loc[muestra_mask, "idx_K_star"] ** alpha
        * df.loc[muestra_mask, "idx_LH_star"] ** (1 - alpha)
    )

    # PIB potencial alternativo (siempre con ptf_hp, para comparación)
    df.loc[muestra_mask, "pib_pot_hp"] = (
        df.loc[muestra_mask, "ptf_hp"]
        * df.loc[muestra_mask, "idx_K_star"] ** alpha
        * df.loc[muestra_mask, "idx_LH_star"] ** (1 - alpha)
    )

    df.loc[muestra_mask, "brecha_pot"] = df.loc[muestra_mask, "idx_pib"] / df.loc[muestra_mask, "pib_pot"] - 1
    df.loc[muestra_mask, "brecha_pot_hp"] = df.loc[muestra_mask, "idx_pib"] / df.loc[muestra_mask, "pib_pot_hp"] - 1
    df.loc[muestra_mask, "brecha_hp"] = df.loc[muestra_mask, "idx_pib"] / df.loc[muestra_mask, "idx_trend"] - 1

    return df, reg_ptf


def detectar_ultimo_trimestre_completo(quarterly_raw: pd.DataFrame) -> pd.Timestamp:
    """Determina T = último trimestre con todos los insumos trimestrales completos."""
    cols_requeridas = ["V_pib", "X_vapg", "BQ_ra", "BR_tms", "BS_ebe", "BT_im"]
    completos = quarterly_raw.dropna(subset=cols_requeridas)
    return completos["date"].max()


def ejecutar_pipeline(config: Config = CONFIG_PRODUCCION):
    """Ejecuta el pipeline completo de estimación del PIB potencial, parametrizado por `config`."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # picos BBQ (fechado del ciclo) — se detectan de los insumos si no vienen dados.
    if config.picos_bbq is None:
        try:
            config = replace(config, picos_bbq=detectar_picos_bbq())
        except Exception as exc:  # pragma: no cover
            print(f"[AVISO] No se pudieron detectar picos BBQ ({exc}); sin tramos BBQ.")
            config = replace(config, picos_bbq=[])

    # --- 1. Lectura de insumos ---
    mensual = leer_insumos_mensuales()
    trimestral = leer_insumos_trimestrales()
    nairu_monthly = leer_nairu_csv(config.nairu_csv)

    monthly = mensual.merge(nairu_monthly, on="date", how="left")

    T = detectar_ultimo_trimestre_completo(trimestral)
    ventana_fin = ALPHA_WINDOW_END if ALPHA_WINDOW_END is not None else T

    # festivos efectivos por trimestre (para horas trabajadas)
    festivos = None
    if config.hours_festivos:
        try:
            festivos = leer_festivos_trimestrales(FESTIVOS_XLSX)
        except Exception as exc:  # pragma: no cover
            print(f"[AVISO] No se pudieron leer festivos ({exc}); horas sin ajuste.")

    # --- 2. Agregación mensual -> trimestral (Ocupados) ---
    quarterly_agg = agregar_mensual_a_trimestral(monthly, ocupados_trimestral=config.ocupados_trimestral)
    df = trimestral.merge(quarterly_agg, on="date", how="left")

    # --- 3. Mercado laboral observado (brecha de capital + MA8) ---
    df = calcular_mercado_laboral(df, ma_window=config.participation_ma_window)

    # --- 4. TGP* óptima (niveles, brecha de capital, tramos BBQ) ---
    df["tgp_star"], reg_resultado = estimar_tgp_star(df, config, picos=config.picos_bbq)

    # --- 5. Factor trabajo (horas ajustadas por festivos) ---
    df = calcular_factor_trabajo(df, config, festivos=festivos)

    # --- 6. Capital humano (extrapolación) ---
    hc_anual_crudo = leer_hc_anual()
    anio_final_necesario = T.year + 1  # Q2-Q4 del año T requieren hc(año_T + 1)
    hc_anual = extrapolar_hc_anual(
        hc_anual_crudo, config.hc_extrap, config.pwt_hc_last_year,
        config.hc_extrap_slope_years, anio_final_necesario,
    )
    hc_df = calcular_capital_humano(df["date"], hc_anual)
    df = df.merge(hc_df, on="date", how="left")
    df["idx_LH_star"] = df["idx_L_star"] * df["idx_hc"] / 100
    df["idx_LH"] = df["idx_L"] * df["idx_hc"] / 100

    # --- 7. Producto y tendencia HP (PIB total vs ex-petróleo) ---
    df, base_out = calcular_producto(df, config)
    df = calcular_tendencia_hp(df, base_out, T)

    # --- 8. Capital físico (DANE principal / PIM alternativo) ---
    df = construir_capital(df, T, config)
    df = calcular_capital_usado(df)

    # --- 9. Alpha (EBE/(RA+EBE) estilo CBO) ---
    alpha_t, alpha = calcular_alpha(df, ALPHA_WINDOW_START, ventana_fin, metodo=config.alpha_method)
    df["alpha_t"] = alpha_t
    df["alpha"] = alpha

    # --- 10. PTF y PIB potencial (tramos BBQ; sin HP en PTF potencial) ---
    df, reg_ptf = calcular_ptf_y_potencial(df, alpha, T, config)

    return df, reg_resultado, alpha, T, reg_ptf


def construir_hoja_series(df: pd.DataFrame, T: pd.Timestamp) -> pd.DataFrame:
    """Construye la tabla 'Series' con nombres de columnas en español para el Excel de salida."""
    d = df[(df["date"] >= pd.Timestamp("2004-03-01")) & (df["date"] <= T)].copy()
    salida = pd.DataFrame({
        "Fecha": d["date"],
        "PIB (V)": d["V_pib"],
        "VA petróleo-gas (X)": d["X_vapg"],
        "PIB sin petróleo anualizado (Z)": d["Z_pib_exoil_ann"],
        "PET": d["pet"],
        "FL": d["fl"],
        "Ocupados": d["ocup"],
        "TGP (%)": d["tgp"],
        "TD (%)": d["td"],
        "NAIRU (%)": d["nairu"],
        "NAICU (%)": d["naicu"],
        "ICU (%)": d["icu"],
        "TGP* (%)": d["tgp_star"],
        "FL*": d["fl_star"],
        "Ocupados*": d["ocup_star"],
        "Jornada legal (h/sem)": d["jornada"],
        "hc (PWT)": d["idx_hc"],
        "Trabajo* (idx_L*)": d["idx_L_star"],
        "Trabajo (idx_L)": d["idx_L"],
        "Capital Humano (idx_hc)": d["idx_hc"],
        "LH* (idx_LH*)": d["idx_LH_star"],
        "LH (idx_LH)": d["idx_LH"],
        "K* (idx_K*)": d["idx_K_star"],
        "K (idx_K)": d["idx_K"],
        "PIB observado (idx_pib)": d["idx_pib"],
        "PIB tendencial HP (idx_trend)": d["idx_trend"],
        "PTF": d["ptf"],
        "PTF HP": d["ptf_hp"],
        "PTF* (CBO)": d["ptf_star"],
        "PIB potencial (principal)": d["pib_pot"],
        "PIB potencial (PTF HP, alternativo)": d["pib_pot_hp"],
        "Brecha vs potencial (%)": 100 * d["brecha_pot"],
        "Brecha vs potencial HP (alt.) (%)": 100 * d["brecha_pot_hp"],
        "Brecha vs HP (%)": 100 * d["brecha_hp"],
        "Alpha": d["alpha"],
        "Capital físico K": d["K"],
    })
    return salida.reset_index(drop=True)


def _sub_periodos():
    return [
        ("2005-2009", pd.Timestamp("2005-01-01"), pd.Timestamp("2009-12-31")),
        ("2010-2014", pd.Timestamp("2010-01-01"), pd.Timestamp("2014-12-31")),
        ("2015-2019", pd.Timestamp("2015-01-01"), pd.Timestamp("2019-12-31")),
        ("2020-2025", pd.Timestamp("2020-01-01"), pd.Timestamp("2025-12-31")),
    ]


def construir_resumen_crecimiento_potencial(df: pd.DataFrame, T: pd.Timestamp) -> pd.DataFrame:
    """Estadísticas de crecimiento del PIB potencial (q/q anualizado y a/a) por subperíodo."""
    d = df[(df["date"] >= BASE_QUARTER) & (df["date"] <= T)].copy().sort_values("date")
    d["g_qq_ann"] = 100 * ((d["pib_pot"] / d["pib_pot"].shift(1)) ** 4 - 1)
    d["g_yy"] = 100 * (d["pib_pot"] / d["pib_pot"].shift(4) - 1)

    filas = []
    periodos = _sub_periodos() + [("2005-" + str(T.year), BASE_QUARTER, T)]
    for nombre, ini, fin in periodos:
        sub = d[(d["date"] >= ini) & (d["date"] <= fin)]
        for etiqueta, col in [("q/q anualizado (%)", "g_qq_ann"), ("a/a (%)", "g_yy")]:
            serie = sub[col].dropna()
            if len(serie) == 0:
                continue
            filas.append({
                "Subperíodo": nombre, "Medida": etiqueta,
                "Media": serie.mean(), "Desv. Est.": serie.std(),
                "Mín.": serie.min(), "Máx.": serie.max(), "N obs": len(serie),
            })
    return pd.DataFrame(filas)


def construir_resumen_ptf(df: pd.DataFrame, T: pd.Timestamp) -> pd.DataFrame:
    """Estadísticas de nivel y crecimiento de la PTF (observada y tendencia HP) por subperíodo."""
    d = df[(df["date"] >= BASE_QUARTER) & (df["date"] <= T)].copy().sort_values("date")
    d["g_ptf"] = 100 * (d["ptf"] / d["ptf"].shift(1) - 1)
    d["g_ptf_hp"] = 100 * (d["ptf_hp"] / d["ptf_hp"].shift(1) - 1)

    filas = []
    periodos = _sub_periodos() + [("2005-" + str(T.year), BASE_QUARTER, T)]
    for nombre, ini, fin in periodos:
        sub = d[(d["date"] >= ini) & (d["date"] <= fin)]
        if len(sub) == 0:
            continue
        filas.append({
            "Subperíodo": nombre,
            "PTF nivel (media)": sub["ptf"].mean(),
            "PTF nivel (DE)": sub["ptf"].std(),
            "Cto. trimestral PTF (media, %)": sub["g_ptf"].dropna().mean(),
            "Cto. trimestral PTF HP (media, %)": sub["g_ptf_hp"].dropna().mean(),
            "N obs": len(sub),
        })
    return pd.DataFrame(filas)


def construir_resumen_brechas(df: pd.DataFrame, T: pd.Timestamp) -> pd.DataFrame:
    """Estadísticas de las brechas del producto (vs potencial y vs tendencia HP)."""
    d = df[(df["date"] >= BASE_QUARTER) & (df["date"] <= T)].copy().sort_values("date")
    bp = 100 * d["brecha_pot"].dropna()
    bh = 100 * d["brecha_hp"].dropna()

    def autocorr1(s):
        return s.autocorr(lag=1)

    comun = d.dropna(subset=["brecha_pot", "brecha_hp"])
    corr = np.corrcoef(comun["brecha_pot"], comun["brecha_hp"])[0, 1]

    filas = [
        {"Serie": "Brecha vs potencial (%)", "Media": bp.mean(), "Desv. Est.": bp.std(),
         "Mín.": bp.min(), "Máx.": bp.max(), "Autocorr.(1)": autocorr1(bp)},
        {"Serie": "Brecha vs HP (%)", "Media": bh.mean(), "Desv. Est.": bh.std(),
         "Mín.": bh.min(), "Máx.": bh.max(), "Autocorr.(1)": autocorr1(bh)},
    ]
    resumen = pd.DataFrame(filas)
    corr_df = pd.DataFrame({"Correlación brecha_pot vs brecha_hp": [corr]})
    return resumen, corr_df


def construir_resumen_comparacion_brechas(df: pd.DataFrame, T: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Estadísticas y correlaciones cruzadas entre las 3 medidas de brecha
    (potencial CBO, potencial HP alternativo, tendencial HP).
    """
    d = df[(df["date"] >= BASE_QUARTER) & (df["date"] <= T)].copy().sort_values("date")
    series = {
        "brecha_pot_cbo": 100 * d["brecha_pot"],
        "brecha_pot_hp": 100 * d["brecha_pot_hp"],
        "brecha_hp": 100 * d["brecha_hp"],
    }
    filas = []
    for nombre, serie in series.items():
        s = serie.dropna()
        if len(s) == 0:
            continue
        filas.append({"Serie": nombre, "Media": s.mean(), "Desv. Est.": s.std(),
                       "Mín.": s.min(), "Máx.": s.max(), "N obs": len(s)})
    resumen = pd.DataFrame(filas)

    comun = pd.DataFrame(series).dropna()
    corr_mat = comun.corr()

    return resumen, corr_mat


def construir_resumen_ptf_anual_dane(df: pd.DataFrame, T: pd.Timestamp) -> tuple[pd.DataFrame, float]:
    """PTF anual calculada (Q4/Q4) vs serie DANE publicada, y su correlación."""
    d = df[(df["date"].dt.month == 12) & (df["date"] >= BASE_QUARTER) & (df["date"] <= T)].copy()
    d["year"] = d["date"].dt.year
    d = d.sort_values("year")
    d["g_calc"] = 100 * (d["ptf"] / d["ptf"].shift(1) - 1)

    filas = []
    for _, row in d.iterrows():
        year = int(row["year"])
        filas.append({
            "Año": year,
            "PTF calculada (Q4, F1A+FLT+HCI)": row["ptf"],
            "Cto. % calculado (a/a)": row["g_calc"],
            "Cto. % DANE (publicado)": DANE_PTF_GROWTH.get(year, np.nan),
        })
    tabla = pd.DataFrame(filas)

    comun = tabla.dropna(subset=["Cto. % calculado (a/a)", "Cto. % DANE (publicado)"])
    corr = np.corrcoef(comun["Cto. % calculado (a/a)"], comun["Cto. % DANE (publicado)"])[0, 1] \
        if len(comun) > 1 else np.nan
    return tabla, corr


def construir_resumen_parametros(reg: ResultadoRegTGP, alpha: float, T: pd.Timestamp,
                                  config: Config = None) -> pd.DataFrame:
    """Parámetros clave del modelo: alpha, coeficientes de la regresión TGP*, R², etc."""
    filas = [
        {"Parámetro": "Alpha (participación del capital)", "Valor": alpha},
        {"Parámetro": "Beta0 (intercepto, reg. TGP*)", "Valor": reg.beta0},
        {"Parámetro": "Beta1 (tendencia t, reg. TGP*)", "Valor": reg.beta1},
        {"Parámetro": "Beta2 (brecha_u, reg. TGP*)", "Valor": reg.beta2},
        {"Parámetro": "Beta3 (MA4 brecha_u, reg. TGP*)", "Valor": reg.beta3},
        {"Parámetro": "R² (reg. TGP*)", "Valor": reg.r_squared},
        {"Parámetro": "N obs (reg. TGP*)", "Valor": reg.nobs},
        {"Parámetro": "Muestra (reg. TGP*)", "Valor": "2004Q4 - " + str(T.year) + "Q" + str((T.month - 1) // 3 + 1)},
        {"Parámetro": "T (último trimestre completo)", "Valor": str(T.date())},
        {"Parámetro": "Ventana alpha (inicio)", "Valor": str(ALPHA_WINDOW_START.date())},
        {"Parámetro": "Ventana alpha (fin)", "Valor": str((ALPHA_WINDOW_END or T).date())},
    ]
    if config is not None:
        filas.extend([
            {"Parámetro": "Config: Ocupados trimestral", "Valor": config.ocupados_trimestral},
            {"Parámetro": "Config: Método tendencia PTF", "Valor": config.tfp_trend_method},
            {"Parámetro": "Config: Extrapolación hc", "Valor": config.hc_extrap},
            {"Parámetro": "Config: Fuente nudos PTF ", "Valor": config.tfp_knots_source},
            {"Parámetro": "Config: Base por tramos PTF", "Valor": config.tfp_piecewise},
            {"Parámetro": "Config: Forma TGP* ", "Valor": config.participation_form},
            {"Parámetro": "Config: Ventana MA TGP* (trim.)", "Valor": config.participation_ma_window},
            {"Parámetro": "Config: TGP* incluye brecha capital", "Valor": config.participation_add_capital_gap},
            {"Parámetro": "Config: TGP* por tramos BBQ", "Valor": config.participation_piecewise},
            {"Parámetro": "Config: Horas ajustadas por festivos ", "Valor": config.hours_festivos},
            {"Parámetro": "Config: Fuente de capital ", "Valor": config.capital_source},
            {"Parámetro": "Config: Método alpha ", "Valor": config.alpha_method},
            {"Parámetro": "Config: Medida de producto ", "Valor": config.output_measure},
            {"Parámetro": "Picos BBQ (fechado del ciclo)",
             "Valor": ", ".join(str(pd.Timestamp(p).date()) for p in (config.picos_bbq or []))},
            {"Parámetro": "Quiebres exógenos tendencia PTF",
             "Valor": ", ".join(str(pd.Timestamp(p).date()) for p in (config.tfp_quiebres_exogenos or []))},
        ])
    return pd.DataFrame(filas)


def construir_resumen_regresion_ptf(reg_ptf: ResultadoRegPTF | None) -> pd.DataFrame:
    """Tabla completa de la regresión de tendencia de PTF estilo CBO."""
    if reg_ptf is None:
        return pd.DataFrame({"Nota": ["Variante HP (TFP_TREND_METHOD='hp'): no se estima la regresión CBO."]})
    tabla = reg_ptf.tabla().reset_index().rename(columns={"index": "Regresor"})
    return tabla


def escribir_excel_salida(df: pd.DataFrame, reg: ResultadoRegTGP, alpha: float, T: pd.Timestamp,
                           path: str, config: Config = None, reg_ptf: ResultadoRegPTF | None = None):
    """Escribe el archivo Excel de salida con hojas 'Series' y 'Resumen'."""
    hoja_series = construir_hoja_series(df, T)
    resumen_crec = construir_resumen_crecimiento_potencial(df, T)
    resumen_ptf = construir_resumen_ptf(df, T)
    resumen_brechas, resumen_corr_brechas = construir_resumen_brechas(df, T)
    resumen_ptf_dane, corr_dane = construir_resumen_ptf_anual_dane(df, T)
    resumen_parametros = construir_resumen_parametros(reg, alpha, T, config)
    resumen_reg_ptf = construir_resumen_regresion_ptf(reg_ptf)
    resumen_comp_brechas, corr_mat_brechas = construir_resumen_comparacion_brechas(df, T)

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        hoja_series.to_excel(writer, sheet_name="Series", index=False, float_format="%.6f")

        fila = 0
        resumen_crec.round(4).to_excel(writer, sheet_name="Resumen", startrow=fila, index=False)
        fila += len(resumen_crec) + 3

        pd.DataFrame({"(a) Crecimiento del PIB potencial por subperíodo": []}).to_excel(
            writer, sheet_name="Resumen", startrow=max(fila - len(resumen_crec) - 3, 0), index=False)

        ws = writer.sheets["Resumen"]
        ws.cell(row=1, column=1, value="(a) Crecimiento del PIB potencial (q/q anualizado y a/a) por subperíodo")

        fila_actual = len(resumen_crec) + 3
        ws.cell(row=fila_actual + 1, column=1, value="(b) Estadísticas de la PTF por subperíodo")
        resumen_ptf.round(4).to_excel(writer, sheet_name="Resumen", startrow=fila_actual + 2, index=False)
        fila_actual += len(resumen_ptf) + 5

        ws.cell(row=fila_actual + 1, column=1, value="(c) Brecha del producto vs potencial y vs tendencia HP")
        resumen_brechas.round(4).to_excel(writer, sheet_name="Resumen", startrow=fila_actual + 2, index=False)
        fila_actual += len(resumen_brechas) + 4
        resumen_corr_brechas.round(4).to_excel(writer, sheet_name="Resumen", startrow=fila_actual, index=False)
        fila_actual += 3

        ws.cell(row=fila_actual + 1, column=1, value="(d) PTF anual (Q4/Q4) calculada vs DANE publicada")
        resumen_ptf_dane.round(4).to_excel(writer, sheet_name="Resumen", startrow=fila_actual + 2, index=False)
        fila_actual += len(resumen_ptf_dane) + 4
        ws.cell(row=fila_actual + 1, column=1, value="Correlación (calculada vs DANE, F1A+FLT+HCI)")
        ws.cell(row=fila_actual + 1, column=2, value=round(float(corr_dane), 4))
        fila_actual += 2
        ws.cell(
            row=fila_actual + 1, column=1,
            value=("Nota: la correlacion se calcula entre el crecimiento anual (Q4/Q4) de la PTF "
                   "tendencial del modelo y el de la PTF que publica el DANE en su anexo de "
                   "productividad."),
        )
        fila_actual += 3

        ws.cell(row=fila_actual + 1, column=1, value="(e) Parámetros clave del modelo")
        resumen_parametros.to_excel(writer, sheet_name="Resumen", startrow=fila_actual + 2, index=False)
        fila_actual += len(resumen_parametros) + 4

        ws.cell(row=fila_actual + 1, column=1,
                value="(f) Regresión de tendencia de PTF estilo CBO (ln PTF ~ tau + nudos + brecha_u + brecha_u(-1) + dummies pandemia)")
        resumen_reg_ptf.round(6).to_excel(writer, sheet_name="Resumen", startrow=fila_actual + 2, index=False)
        fila_actual += len(resumen_reg_ptf) + 3
        if reg_ptf is not None:
            ws.cell(row=fila_actual + 1, column=1, value=f"R² = {reg_ptf.r_squared:.6f}   N obs = {reg_ptf.nobs}")
            fila_actual += 1
            ws.cell(row=fila_actual + 1, column=1,
                    value="Nudos: " + ", ".join(str(pd.Timestamp(k).date()) for k in reg_ptf.knots)
                    + "  |  Dummies: " + ", ".join(str(pd.Timestamp(dd).date()) for dd in reg_ptf.dummies))
            fila_actual += 2
        fila_actual += 2

        ws.cell(row=fila_actual + 1, column=1,
                value="(g) Comparación de brechas entre métodos (potencial CBO, potencial HP alt., tendencial HP)")
        resumen_comp_brechas.round(4).to_excel(writer, sheet_name="Resumen", startrow=fila_actual + 2, index=False)
        fila_actual += len(resumen_comp_brechas) + 4
        ws.cell(row=fila_actual + 1, column=1, value="Matriz de correlaciones cruzadas entre brechas:")
        fila_actual += 1
        corr_mat_out = corr_mat_brechas.reset_index().rename(columns={"index": "Serie"})
        corr_mat_out.round(4).to_excel(writer, sheet_name="Resumen", startrow=fila_actual + 1, index=False)


FUENTE_NOTA = "Cálculos propios con base en DANE, BanRep, PWT"


def graficar_niveles(df: pd.DataFrame, T: pd.Timestamp, path: str):
    """
    Gráfico de niveles: PIB observado, tendencial HP y PIB potencial (CBO)
    -- índices, 2005Q1=100.
    """
    d = df[(df["date"] >= BASE_QUARTER) & (df["date"] <= T)].sort_values("date")
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.plot(d["date"], d["idx_pib"], label="PIB observado", color="#1f77b4", linewidth=1.8)
    ax.plot(d["date"], d["idx_trend"], label="Tendencial HP", color="#ff7f0e", linewidth=1.8, linestyle="--")
    ax.plot(d["date"], d["pib_pot"], label="PIB potencial (CBO)", color="#2ca02c", linewidth=1.8, linestyle="-.")
    ax.set_title("Colombia: PIB observado, tendencial (HP) y potencial\n(Índice 2005T1 = 100)", fontsize=13)
    ax.set_xlabel("Fecha")
    ax.set_ylabel("Índice (2005T1 = 100)")
    ax.legend(loc="upper left", frameon=False)
    ax.grid(alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.text(0.01, 0.01, FUENTE_NOTA, fontsize=8, color="gray")
    fig.tight_layout(rect=[0, 0.03, 1, 1])
    fig.savefig(path, dpi=150)
    plt.close(fig)


def graficar_brechas(df: pd.DataFrame, T: pd.Timestamp, path: str):
    """
    Brecha del producto del modelo frente a la de los compuestos VIOG y 1/VIOG,
    que son la referencia externa del ejercicio.
    """
    d = df[(df["date"] >= BASE_QUARTER) & (df["date"] <= T)].sort_values("date")
    refc = _referencia_compuestos(d)
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.plot(d["date"], 100 * d["brecha_pot"], label="Brecha vs potencial (CBO)", color="#d62728", linewidth=1.8)
    if refc is not None:
        ax.plot(refc["date"], refc["gap4_viog"], label="Brecha compuesto VIOG",
                color="#7f7f7f", linewidth=1.5, linestyle="--")
        ax.plot(refc["date"], refc["gap4_inv_viog"], label="Brecha compuesto 1/VIOG",
                color="#7f7f7f", linewidth=1.5, linestyle=":")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("Colombia: brecha del producto\n"
                 "(función de producción frente a los compuestos de filtros estadísticos)", fontsize=13)
    ax.set_xlabel("Fecha")
    ax.set_ylabel("Brecha (%)")
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f%%"))
    ax.legend(loc="upper left", frameon=False)
    ax.grid(alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.text(0.01, 0.01, FUENTE_NOTA, fontsize=8, color="gray")
    fig.tight_layout(rect=[0, 0.03, 1, 1])
    fig.savefig(path, dpi=150)
    plt.close(fig)


VALIDATION_MAP = {
    "tgp_star": "tgp_star",
    "idx_pib": "idx_pib",
    "idx_trend": "idx_trend",
    "K": "K",
    "idx_L": "idx_L",
    "idx_LH": "idx_LH",
    "idx_K": "idx_K",
    "ptf": "ptf",
    "ptf_hp": "ptf_hp",
    "pib_pot": "pib_pot",
    "alpha": "alpha",
}


HP_SERIES = {"idx_trend", "ptf_hp", "pib_pot"}


def construir_reporte(df: pd.DataFrame, reg: ResultadoRegTGP, alpha: float, T: pd.Timestamp,
                          reg_ptf: ResultadoRegPTF, corr_dane: float, config: Config) -> list:
    """Construye las líneas de texto del resumen produccion (sección ii del reporte de validación)."""
    lineas = []
    lineas.append("")
    lineas.append("RESUMEN MODO V2 (config default)")
    lineas.append("=" * 70)
    lineas.append(f"Config: {config.label}")
    lineas.append(f"T (último trimestre completo) = {T.date()}")
    lineas.append(f"Alpha ({config.alpha_method}) = {float(alpha):.10g}   "
                  f"Capital = {config.capital_source}   Producto = {config.output_measure}")
    lineas.append(f"Picos BBQ (tramos) = {[str(pd.Timestamp(p).date()) for p in (config.picos_bbq or [])]}")
    lineas.append(f"Quiebres exogenos de la tendencia de PTF = "
                  f"{[str(pd.Timestamp(p).date()) for p in (config.tfp_quiebres_exogenos or [])]}")
    lineas.append("")
    _tgp_dep = "logit(TGP)" if config.participation_form == "logit" else "TGP (niveles)"
    _tgp_trend = "tramos BBQ" if config.participation_piecewise else "tendencia lineal"
    _tgp_cap = " + brecha_icu + MA_brecha_icu" if config.participation_add_capital_gap else ""
    lineas.append(f"Regresión TGP* : {_tgp_dep} ~ {_tgp_trend} + brecha_u + "
                  f"MA{config.participation_ma_window}_brecha_u{_tgp_cap}")
    lineas.append(f"  const = {float(reg.beta0):.10g}   coef_tendencia_1 = {float(reg.beta1):.10g}")
    lineas.append(f"  brecha_u = {float(reg.beta2):.10g}   MA_brecha_u = {float(reg.beta3):.10g}")
    lineas.append(f"  R² = {reg.r_squared:.6f}   N obs = {reg.nobs}")
    if reg.modelo is not None:
        lineas.append("  Coeficientes completos (TGP*):")
        for nombre in reg.modelo.params.index:
            lineas.append(f"    {nombre:<26}{float(reg.modelo.params[nombre]):>14.6g}"
                          f"{float(reg.modelo.pvalues[nombre]):>12.4g}  (p)")
    lineas.append("")

    _basis = ("rampas-meseta BBQ (CBO/NBER)" if config.tfp_piecewise == "plateau"
              else "spline acumulativo max(0,·)")
    lineas.append(f"Regresión de tendencia de PTF estilo CBO (base = {_basis}):")
    lineas.append(f"  ln(PTF_t) = b0 + b1*tau_t + sum gamma_j*ciclo_j(tau_t) + phi0*gap_t + phi1*gap_(t-1) + sum delta_m*D_m + eps_t")
    lineas.append(f"  N obs = {reg_ptf.nobs}    R² = {reg_ptf.r_squared:.6f}")
    lineas.append("")
    tabla = reg_ptf.tabla()
    lineas.append(f"  {'Regresor':<28}{'Coef.':>14}{'Error Est.':>14}{'t':>10}{'p-valor':>12}")
    for nombre, fila in tabla.iterrows():
        lineas.append(f"  {nombre:<28}{fila['Coeficiente']:>14.6g}{fila['Error Est.']:>14.6g}{fila['t']:>10.3f}{fila['p-valor']:>12.4g}")
    lineas.append("")
    phi0 = tabla.loc["gap", "Coeficiente"] if "gap" in tabla.index else np.nan
    signo_ok = "negativo, como se esperaba" if phi0 < 0 else "POSITIVO (inesperado; revisar)"
    lineas.append(f"  Signo de phi0 (coef. de gap = brecha_u contemporánea): {phi0:.6g} -> {signo_ok}")
    lineas.append("")

    d = df[(df["date"] >= BASE_QUARTER) & (df["date"] <= T)].copy().sort_values("date")
    lineas.append("Chequeos de sanidad (muestra 2005Q1..T):")
    cols_potenciales = ["pib_pot", "ptf_star", "idx_K_star", "idx_LH_star"]
    n_nan = {c: int(d[c].isna().sum()) for c in cols_potenciales if c in d.columns}
    lineas.append(f"  NaN en series principales del PIB potencial: {n_nan}")
    ptf_star_min = d["ptf_star"].min()
    lineas.append(f"  Min(PTF*) = {ptf_star_min:.6g}  ({'> 0 OK' if ptf_star_min > 0 else 'PROBLEMA: <= 0'})")
    lineas.append(f"  Media brecha vs potencial (CBO) = {100*d['brecha_pot'].mean():.4f}%   "
                  f"DE = {100*d['brecha_pot'].std():.4f}%")
    # perfil de la brecha: 2020 (negativo profundo) y 2022-2023 (positivo)
    b2020 = d.loc[d["date"].dt.year == 2020, "brecha_pot"]
    b2022_23 = d.loc[d["date"].dt.year.isin([2022, 2023]), "brecha_pot"]
    lineas.append(f"  Brecha media 2020 (esperado muy negativo) = {100*b2020.mean():.4f}%")
    lineas.append(f"  Brecha media 2022-2023 (esperado positivo) = {100*b2022_23.mean():.4f}%")
    lineas.append("")

    resumen_comp, corr_mat = construir_resumen_comparacion_brechas(df, T)
    lineas.append("Comparación de brechas entre métodos:")
    lineas.append(resumen_comp.round(4).to_string(index=False))
    lineas.append("")
    lineas.append("Matriz de correlaciones cruzadas entre brechas:")
    lineas.append(corr_mat.round(4).to_string())
    lineas.append("")

    lineas.append(f"Correlación PTF anual calculada (Q4/Q4) vs DANE publicada  = {corr_dane:.4f}")
    lineas.append("(referencia la primera version: 0.3967)")

    return lineas




def _escribir_nairu_csv(out: pd.DataFrame, path: Path) -> None:
    csv = pd.DataFrame({
        "Date": pd.to_datetime(out["Date"]),
        "nairu_estimate": np.asarray(out["nairu_smooth"], dtype=float),
        "naicu_estimate": np.asarray(out["naicu_smooth"], dtype=float),
        "icu_current": np.asarray(out["icu"], dtype=float),
        "unemployment_current": np.asarray(out["unemployment"], dtype=float),
        "nairu_ci_lower_95": np.asarray(out["nairu_smooth_lo95"], dtype=float),
        "nairu_ci_upper_95": np.asarray(out["nairu_smooth_hi95"], dtype=float),
        "naicu_ci_lower_95": np.asarray(out["naicu_smooth_lo95"], dtype=float),
        "naicu_ci_upper_95": np.asarray(out["naicu_smooth_hi95"], dtype=float),
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    csv.to_csv(path, index=False)


def _pib_pot_con_nairu(nairu, naicu, icu, dates, tag) -> pd.DataFrame:
    """Corre el pipeline PIB con una NAIRU/NAICU dada y devuelve (date, pib_pot).
    Usado para la banda de confianza del PIB potencial (delta method)."""
    tmp = _HERE / "outputs" / f"_ci_{tag}.csv"
    pd.DataFrame({"Date": pd.to_datetime(dates), "nairu_estimate": nairu,
                  "naicu_estimate": naicu, "icu_current": icu}).to_csv(tmp, index=False)
    prev = CONFIG_PRODUCCION.nairu_csv
    CONFIG_PRODUCCION.nairu_csv = str(tmp)
    dfp = ejecutar_pipeline(CONFIG_PRODUCCION)[0]
    CONFIG_PRODUCCION.nairu_csv = prev
    try:
        tmp.unlink()
    except OSError:
        pass
    return dfp[["date", "pib_pot"]].rename(columns={"pib_pot": tag})


def _ci_pib_potencial(out: pd.DataFrame, df_base: pd.DataFrame) -> pd.DataFrame:
    """Banda 95% del PIB potencial por DELTA METHOD numerico: se re-corre el
    pipeline con NAIRU y NAICU fijadas en sus cotas de 95% (una a la vez) y se
    combinan en cuadratura las contribuciones. Devuelve date, pib_pot_lo/hi.
    Nota: propaga solo la incertidumbre de NAIRU/NAICU (suavizador), tratando el
    desplazamiento de cada trayectoria como su IC 95% puntual; es una aproximacion
    conservadora (ignora correlacion temporal y otras fuentes, p.ej. la tendencia
    de PTF)."""
    dts = pd.to_datetime(out["Date"])
    icu = out["icu"].to_numpy(dtype=float)
    nb = out["nairu_smooth"].to_numpy(dtype=float)
    cb = out["naicu_smooth"].to_numpy(dtype=float)
    n_hi = np.clip(out["nairu_smooth_hi95"].to_numpy(dtype=float), 4.0, 18.0)
    n_lo = np.clip(out["nairu_smooth_lo95"].to_numpy(dtype=float), 4.0, 18.0)
    c_hi = np.clip(out["naicu_smooth_hi95"].to_numpy(dtype=float), 55.0, 90.0)
    c_lo = np.clip(out["naicu_smooth_lo95"].to_numpy(dtype=float), 55.0, 90.0)
    m = df_base[["date", "pib_pot"]].rename(columns={"pib_pot": "base"})
    for tag, nn, cc in (("Nhi", n_hi, cb), ("Nlo", n_lo, cb),
                        ("Chi", nb, c_hi), ("Clo", nb, c_lo)):
        m = m.merge(_pib_pot_con_nairu(nn, cc, icu, dts, tag), on="date", how="left")
    dN = (m["Nlo"] - m["Nhi"]).abs()   # NAIRU baja => potencial sube
    dC = (m["Chi"] - m["Clo"]).abs()   # NAICU sube => potencial sube
    half = 0.5 * np.sqrt(dN ** 2 + dC ** 2)
    return pd.DataFrame({"date": m["date"], "pib_pot_lo": m["base"] - half,
                         "pib_pot_hi": m["base"] + half})


def _referencia_compuestos(d: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Brechas y nivel de tendencia de los compuestos VIOG y 1/VIOG, alineados a los
    trimestres de `d`. Son la referencia externa del ejercicio: proceden de una bateria
    de filtros estadisticos aplicados al PIB, independiente de este modelo.
    Devuelve None si el archivo de filtros no esta disponible."""
    try:
        ref = leer_compuestos_filtros()
    except Exception as exc:  # pragma: no cover
        print(f"[v7][AVISO] No se pudieron leer los compuestos de filtros: {exc!r}")
        return None
    # Las sumas moviles de 4 trimestres se calculan sobre la serie COMPLETA de los
    # compuestos (que arranca en 1994), no sobre la ventana del modelo: de lo contrario
    # las primeras cuatro observaciones quedarian vacias y la base del indice se correria.
    ref = ref.sort_values("q").copy()
    for c in ("viog", "inv_viog"):
        ref[f"s4_{c}"] = ref[c].rolling(4).sum()
    q = pd.DataFrame({"date": pd.to_datetime(d["date"])})
    q["q"] = q["date"].dt.to_period("Q")
    m = q.merge(ref, on="q", how="left")
    # Nivel en el mismo indice que el PIB del modelo: 100 en el primer trimestre de `d`.
    for c in ("viog", "inv_viog"):
        s4 = m[f"s4_{c}"]
        base_s4 = s4.iloc[0]
        m[f"idx_{c}"] = 100.0 * s4 / base_s4 if pd.notna(base_s4) else np.nan
    return m


def _graficar_pib_paneles(df: pd.DataFrame, T, ci: pd.DataFrame, path) -> None:
    """2 paneles: (izq) niveles con el PIB potencial y su banda de 95 %; (der) brecha
    del producto con su banda de 95 %. En ambos, la referencia externa son los
    compuestos VIOG y 1/VIOG de la bateria de filtros estadisticos."""
    d = df[(df["date"] >= BASE_QUARTER) & (df["date"] <= T)].sort_values("date")
    refc = _referencia_compuestos(d)
    fig, ax = plt.subplots(1, 2, figsize=(16, 6))
    if ci is not None:
        c = ci[(ci["date"] >= BASE_QUARTER) & (ci["date"] <= T)].sort_values("date")
        ax[0].fill_between(c["date"], c["pib_pot_lo"], c["pib_pot_hi"],
                           color="#2ca02c", alpha=0.18, label="PIB potencial IC 95%")
    ax[0].plot(d["date"], d["idx_pib"], color="#1f77b4", lw=1.8, label="PIB observado")
    if refc is not None:
        base = 1.0  # el indice de los compuestos ya esta en base 100 = primer trimestre
        for col, etiqueta, estilo in (("idx_viog", "Tendencia VIOG (filtros)", "--"),
                                      ("idx_inv_viog", "Tendencia 1/VIOG (filtros)", ":")):
            if col in refc and refc[col].notna().any():
                ax[0].plot(refc["date"], refc[col] * base, color="#7f7f7f", lw=1.3,
                           ls=estilo, label=etiqueta)
    ax[0].plot(d["date"], d["pib_pot"], color="#2ca02c", lw=2.0, ls="-.", label="PIB potencial (CBO)")
    ax[0].set_title("Niveles (Índice 2005T1 = 100)"); ax[0].set_ylabel("Índice")
    ax[0].legend(loc="upper left", frameon=False, fontsize=9); ax[0].grid(alpha=0.3)
    ax[0].spines["top"].set_visible(False); ax[0].spines["right"].set_visible(False)

    ax[1].axhline(0, color="black", lw=0.8)
    if ci is not None:
        b = d.merge(c, on="date", how="inner")
        gap_hi = 100.0 * (b["idx_pib"] / b["pib_pot_lo"] - 1.0)  # potencial bajo => brecha alta
        gap_lo = 100.0 * (b["idx_pib"] / b["pib_pot_hi"] - 1.0)
        ax[1].fill_between(b["date"], gap_lo, gap_hi, color="#d62728", alpha=0.15, label="Brecha IC 95%")
    ax[1].plot(d["date"], 100.0 * d["brecha_pot"], color="#d62728", lw=1.8, label="Brecha vs potencial (CBO)")
    if refc is not None:
        ax[1].plot(refc["date"], refc["gap4_viog"], color="#7f7f7f", lw=1.4, ls="--",
                   label="Brecha compuesto VIOG")
        ax[1].plot(refc["date"], refc["gap4_inv_viog"], color="#7f7f7f", lw=1.4, ls=":",
                   label="Brecha compuesto 1/VIOG")
    ax[1].set_title("Brecha del producto (%)"); ax[1].set_ylabel("%")
    ax[1].yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f%%"))
    ax[1].legend(loc="upper left", frameon=False, fontsize=9); ax[1].grid(alpha=0.3)
    ax[1].spines["top"].set_visible(False); ax[1].spines["right"].set_visible(False)

    fig.suptitle("Colombia: PIB potencial y brecha del producto", fontsize=15)
    fig.text(0.01, 0.005, FUENTE_NOTA, fontsize=8, color="gray")
    fig.tight_layout(rect=(0.0, 0.02, 1.0, 0.97))
    fig.savefig(path, dpi=200, bbox_inches="tight"); plt.close(fig)


def _graficar_ptf_tendencia(df: pd.DataFrame, T, path) -> None:
    """PTF observada (residuo de Solow) y su tendencia por tramos, en indice y en
    crecimiento anual. Marca el quiebre exogeno del fin del auge de materias primas."""
    d = df.dropna(subset=["ptf"]).copy()
    d = d[d["date"] <= T]
    base = float(d["ptf"].iloc[0])
    idx_obs = 100.0 * d["ptf"] / base
    idx_ten = 100.0 * d["ptf_star"] / base
    fig, ax = plt.subplots(2, 1, figsize=(13, 9), sharex=True)
    ax[0].plot(d["date"], idx_obs, color="#7f7f7f", lw=1.1, label="PTF observada (residuo de Solow)")
    ax[0].plot(d["date"], idx_ten, color="#196B24", lw=2.4, label="PTF tendencial (regresion por tramos)")
    ax[0].set_ylabel(f"indice ({pd.Timestamp(d['date'].iloc[0]).year}Q1 = 100)")
    ax[0].set_title("Productividad total de los factores: dato y tendencia")
    g_obs = 100.0 * (d["ptf"] / d["ptf"].shift(4) - 1.0)
    g_ten = 100.0 * (d["ptf_star"] / d["ptf_star"].shift(4) - 1.0)
    ax[1].axhline(0, color="black", lw=0.8)
    ax[1].plot(d["date"], g_obs, color="#7f7f7f", lw=1.1, label="PTF observada")
    ax[1].plot(d["date"], g_ten, color="#196B24", lw=2.4, label="PTF tendencial")
    ax[1].set_ylabel("crecimiento anual (%)")
    ax[1].set_title("Crecimiento anual")
    for a in ax:
        for q in TFP_QUIEBRES_EXOGENOS_DEFAULT:
            a.axvline(pd.Timestamp(q), color="#d62728", ls="--", lw=1.2)
        a.grid(alpha=0.3)
        a.legend(fontsize=9, frameon=False)
        a.spines["top"].set_visible(False); a.spines["right"].set_visible(False)
    ax[0].annotate("quiebre 2014Q3 (fin del auge de materias primas)",
                   xy=(pd.Timestamp(TFP_QUIEBRES_EXOGENOS_DEFAULT[0]), float(idx_ten.max())),
                   xytext=(10, -18), textcoords="offset points", fontsize=9, color="#d62728")
    fig.tight_layout()
    fig.savefig(path, dpi=200, bbox_inches="tight"); plt.close(fig)


def _graficar_ptf_vs_dane(df: pd.DataFrame, T, path) -> None:
    """Crecimiento anual (Q4/Q4) de la PTF del modelo vs la PTF publicada por DANE."""
    tabla, corr = construir_resumen_ptf_anual_dane(df, T)
    t = tabla.dropna(subset=["Cto. % calculado (a/a)"]).copy()
    x = t["Año"].to_numpy(dtype=float)
    fig, ax = plt.subplots(figsize=(13, 6))
    ax.bar(x - 0.2, t["Cto. % calculado (a/a)"], width=0.4, color="#2ca02c", label="PTF modelo (Q4/Q4)")
    ax.bar(x + 0.2, t["Cto. % DANE (publicado)"], width=0.4, color="#7f7f7f", label="PTF DANE (publicado)")
    ax.axhline(0, color="black", lw=0.8)
    ctxt = f"corr = {corr:.2f}" if np.isfinite(corr) else "corr = n/d"
    ax.set_title(f"Crecimiento anual de la PTF: modelo vs DANE ({ctxt})", fontsize=13)
    ax.set_xlabel("Año"); ax.set_ylabel("Crecimiento (%)")
    ax.legend(frameon=False); ax.grid(alpha=0.3, axis="y")
    ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(False)
    fig.text(0.01, 0.01, FUENTE_NOTA, fontsize=8, color="gray")
    fig.tight_layout(); fig.savefig(path, dpi=200, bbox_inches="tight"); plt.close(fig)


FILTROS_XLSX = os.path.join(PROJECT_DIR, "Filtros_PIB_Colombia_CF_dos_colas.xlsx")


FILTROS_SHEET = "Tendencias"


CALIB_WINDOW = (pd.Period("2006Q1", freq="Q"), pd.Period("2024Q4", freq="Q"))


CALIB_COVID_EXCL = (pd.Period("2020Q2", freq="Q"), pd.Period("2021Q2", freq="Q"))


def leer_compuestos_filtros(path: str = FILTROS_XLSX, sheet: str = FILTROS_SHEET) -> pd.DataFrame:
    """Lee la hoja Tendencias y devuelve, por trimestre (Period Q), el PIB
    observado, las tendencias VIOG y 1/VIOG y sus brechas en base trimestral
    (gapq_*) y en base suma movil 4T (gap4_*), en pp."""
    f = pd.read_excel(path, sheet_name=sheet, header=4)
    f["Fecha"] = pd.to_datetime(f["Fecha"], errors="coerce")
    f = f.dropna(subset=["Fecha"]).reset_index(drop=True)
    f = f.rename(columns={"PIB observado": "pib_obs", "Compuesto VIOG": "viog",
                          "Compuesto 1/VIOG": "inv_viog"})
    for c in ("pib_obs", "viog", "inv_viog"):
        f[c] = f[c].astype(float)
    f["q"] = f["Fecha"].dt.to_period("Q")
    f = f.sort_values("q").reset_index(drop=True)
    s4_pib = f["pib_obs"].rolling(4).sum()
    for c in ("viog", "inv_viog"):
        f[f"gapq_{c}"] = 100.0 * (np.log(f["pib_obs"]) - np.log(f[c]))
        f[f"gap4_{c}"] = 100.0 * (np.log(s4_pib) - np.log(f[c].rolling(4).sum()))
    return f[["q", "Fecha", "pib_obs", "viog", "inv_viog",
              "gapq_viog", "gapq_inv_viog", "gap4_viog", "gap4_inv_viog"]]


def _brechas_comparables(df_pipe: pd.DataFrame, ref: pd.DataFrame) -> pd.DataFrame:
    """Une nuestra brecha (pp) con las de referencia por trimestre."""
    ours = pd.DataFrame({"q": pd.to_datetime(df_pipe["date"]).dt.to_period("Q"),
                         "gap_ours": 100.0 * df_pipe["brecha_pot"].to_numpy(dtype=float)})
    m = ours.merge(ref, on="q", how="inner").dropna(subset=["gap_ours"]).reset_index(drop=True)
    m["en_ventana"] = (m["q"] >= CALIB_WINDOW[0]) & (m["q"] <= CALIB_WINDOW[1])
    m["covid"] = (m["q"] >= CALIB_COVID_EXCL[0]) & (m["q"] <= CALIB_COVID_EXCL[1])
    return m


def metricas_vs_compuestos(df_pipe: pd.DataFrame, ref: pd.DataFrame) -> Dict[str, float]:
    """RMSE (pp) y correlacion de nuestra brecha frente a VIOG y 1/VIOG en la
    ventana de calibracion, sin COVID (metrica objetivo = rmse_mean) y, como
    diagnostico, con COVID (sufijo _full)."""
    m = _brechas_comparables(df_pipe, ref)
    res: Dict[str, float] = {}
    for tag, mask in (("", m["en_ventana"] & ~m["covid"]), ("_full", m["en_ventana"])):
        w = m[mask]
        for c in ("viog", "inv_viog"):
            d = w["gap_ours"] - w[f"gap4_{c}"]
            res[f"rmse_{c}{tag}"] = float(np.sqrt(np.mean(d ** 2))) if len(w) > 1 else float("nan")
            res[f"corr_{c}{tag}"] = float(w["gap_ours"].corr(w[f"gap4_{c}"])) if len(w) > 2 else float("nan")
            res[f"bias_{c}{tag}"] = float(d.mean()) if len(w) else float("nan")
        res[f"rmse_mean{tag}"] = 0.5 * (res[f"rmse_viog{tag}"] + res[f"rmse_inv_viog{tag}"])
        res[f"corr_mean{tag}"] = 0.5 * (res[f"corr_viog{tag}"] + res[f"corr_inv_viog{tag}"])
        res[f"n_obs{tag}"] = int(len(w))
    return res

# ##########################################################################
# ###  MODULO B: NAIRU Y NAICU (BLOQUE LABORAL Y DE PRECIOS, SIN EL PIB)     ###
# ##########################################################################

# --- Rutas de insumos del bloque laboral (todas derivadas de la raiz del proyecto) ---
# Carpeta con los insumos mensuales del bloque laboral y de precios (datos, no codigo).
_DATOS_LAB_DIR = _HERE / "datos"
DATA_XLSX = _HERE / "Data_NAIRU.xlsx"
EME_CSV = _DATOS_LAB_DIR / "eme_expectativas_inflacion_total_mensual_promedio.csv"
# Salarios EMMET: indice REAL del DANE y nominal reconstruido con el IPC total
# (construir_salarios_emmet.py). La serie anterior, emmet_salarios_industria_mensual.csv,
# era la misma columna real tratada por error como nominal.
WAGE_CSV = _DATOS_LAB_DIR / "emmet_salarios_v7.csv"
SUBEMPLEO_CSV = _DATOS_LAB_DIR / "geih_subempleo_bruto_mensual.csv"
TGP_CSV = _DATOS_LAB_DIR / "geih_tgp_to_td_desest_mensual.csv"
VACANTES_CSV = _DATOS_LAB_DIR / "banrep_vacantes_prensa_mensual.csv"


COVID_START = pd.Timestamp("2020-04-01")


COVID_END = pd.Timestamp("2021-06-01")


# Empalme ECH -> GEIH de la TGP: la fuente no tiene 2006-07 ni 2006-08 y la serie
# cae ~3.9 pp de 2006-06 a 2006-09 por el cambio de encuesta, no por el ciclo.
TGP_EMPALME_GEIH = pd.Timestamp("2006-07-01")


# =========================================================================== #
#  1. Datos                                                                    #
# =========================================================================== #
@dataclass(slots=True)
class SampleV7:
    dates: pd.DatetimeIndex
    n: int
    unemployment: np.ndarray
    icu: np.ndarray
    covid_dummy: np.ndarray
    subempleo_yoy: np.ndarray
    tgp: np.ndarray                # TGP desestacionalizada en NIVELES (v7: sin media movil)
    tgp_empalme_geih: np.ndarray   # escalon 0/1 desde el empalme ECH->GEIH (2006-07)
    log_vacantes: np.ndarray
    vacantes_gap: np.ndarray       # log_vacantes - MA(60) (opcion 2 del critico: colapso estructural)
    wage_annual: np.ndarray        # variacion anual del salario (EMMET), insumo
    wage_rel: np.ndarray           # v7: dependiente salarial = salario - inflacion nucleo
    wage_rel_lag1: np.ndarray
    eme_used: np.ndarray           # EME a 12 meses formada en t-12
    eme_gap_meta: np.ndarray       # v7: EME(t-12) - meta(t), regresor de la Phillips de precios
    eme_err_nucleo: np.ndarray     # v7: EME(t-12) - nucleo(t), regresor de la Phillips de salarios
    pasada_menos_nucleo: np.ndarray  # v7: inflacion total(t-12) - nucleo(t), indexacion a inflacion pasada
    y_phillips: np.ndarray         # v7: inflacion nucleo - meta
    y_phillips_lag1: np.ndarray
    y_phillips_lag2: np.ndarray
    oil_shock: np.ndarray
    u_bar: float                   # ancla de reversion a la media de u* (episodios de inflacion estable)
    icu_bar: float                 # idem para ICU*
    episodios_ancla: List[Tuple[str, str]]  # (inicio, fin) de cada episodio detectado


def calcular_ancla_reversion(dates: pd.DatetimeIndex, core_inf: np.ndarray, inf_goal: np.ndarray,
                              unemployment: np.ndarray, icu: np.ndarray, min_meses: int = 6,
                              tol: float = 0.5) -> Tuple[float, float, List[Tuple[str, str]]]:
    """Regla OBJETIVA y REPLICABLE (criterio acordado
    para fijar el ancla de reversion a la media de u*/ICU*: se detectan
    episodios en que |Core_Inf - Inf_Goal| < `tol` puntos porcentuales de forma
    SOSTENIDA (>= `min_meses` consecutivos) -- momentos en que la inflacion
    estuvo efectivamente anclada a la meta, evidencia de que la economia
    operaba cerca del pleno empleo -- y se promedia el desempleo/ICU
    observados SOLO en esos meses. No es un numero magico: el script calcula
    esto desde los datos e imprime los episodios detectados."""
    estable = np.abs(core_inf - inf_goal) < tol
    n = len(estable)
    episodios: List[Tuple[str, str]] = []
    meses_ancla: List[int] = []
    i = 0
    while i < n:
        if estable[i] and np.isfinite(core_inf[i]) and np.isfinite(inf_goal[i]):
            j = i
            while j < n and estable[j]:
                j += 1
            if (j - i) >= min_meses:
                episodios.append((str(dates[i].date()), str(dates[j - 1].date())))
                meses_ancla.extend(range(i, j))
            i = j
        else:
            i += 1
    idx = np.array(meses_ancla, dtype=int)
    u_bar = float(np.mean(unemployment[idx])) if len(idx) else float(np.mean(unemployment))
    icu_bar = float(np.mean(icu[idx])) if len(idx) else float(np.mean(icu))
    print(f"[ancla] {len(episodios)} episodios de inflacion estable (|core-meta|<{tol}pp, "
          f">={min_meses}m consecutivos), {len(idx)} meses en total:")
    for ini, fin in episodios:
        print(f"    {ini} .. {fin}")
    print(f"[ancla] u_bar={u_bar:.4f}%  icu_bar={icu_bar:.4f}")
    return u_bar, icu_bar, episodios


def cargar_datos(u_bar_override: Optional[float] = None,
                 ancla_tol: float = 0.5, ancla_min_meses: int = 6,
                 wage_col: str = "var_anual_salario_nominal") -> SampleV7:
    """Muestra mensual del bloque de tasas naturales. `wage_col` es la variacion
    anual del salario NOMINAL por ocupado de la EMMET (reconstruido: indice real
    del DANE por IPC total, ver construir_salarios_emmet.py)."""
    df_base = load_and_prepare_data(DATA_XLSX, require_ma24=False)
    md = build_model_data(df_base)
    dates = pd.DatetimeIndex(md.dates)
    covid = ((dates >= COVID_START) & (dates <= COVID_END)).astype(float)

    # --- Phillips de precios (v7): la dependiente es la desviacion de la inflacion
    # nucleo frente a la META del Banco de la Republica, pi_t - pi*_t (ya no
    # pi_t - E_{t-12}pi_t). La desviacion de las expectativas formadas hace un ano
    # frente a la meta, E_{t-12}pi_t - pi*_t, pasa a ser regresor con coeficiente
    # libre (gamma_pe). La meta es la vigente en t, el periodo al que se refiere
    # la expectativa.
    raw = pd.read_excel(DATA_XLSX)
    raw["date"] = pd.to_datetime(dict(year=raw["Year"], month=raw["Month"], day=1))
    core = raw[["date", "Core_Inf", "Inf_Rate", "Inf_Goal"]]
    eme = pd.read_csv(EME_CSV, parse_dates=["date"])[["date", "doce_meses"]]
    eme["eme_t_menos_12"] = eme["doce_meses"].shift(12)
    base = pd.DataFrame({"date": dates}).merge(core, on="date", how="left").merge(
        eme[["date", "eme_t_menos_12"]], on="date", how="left")
    y_phillips = (base["Core_Inf"] - base["Inf_Goal"]).to_numpy(dtype=float)
    y_lag1 = pd.Series(y_phillips).shift(1).to_numpy(dtype=float)
    y_lag2 = pd.Series(y_phillips).shift(2).to_numpy(dtype=float)
    eme_used = base["eme_t_menos_12"].to_numpy(dtype=float)
    eme_gap_meta = (base["eme_t_menos_12"] - base["Inf_Goal"]).to_numpy(dtype=float)
    # la ecuacion solo se evalua donde existe el regresor de expectativas
    y_phillips = np.where(np.isfinite(eme_gap_meta), y_phillips, np.nan)
    inf_total = base["Inf_Rate"].to_numpy(dtype=float)
    inf_goal = base["Inf_Goal"].to_numpy(dtype=float)
    core_inf_arr = base["Core_Inf"].to_numpy(dtype=float)

    # --- Salarios (v7): variacion anual del salario NOMINAL por ocupado (EMMET) ---
    wage = pd.read_csv(WAGE_CSV, parse_dates=["date"])[["date", wage_col]]
    base_w = pd.DataFrame({"date": dates}).merge(wage, on="date", how="left")
    wage_annual = base_w[wage_col].to_numpy(dtype=float)
    # La dependiente de la Phillips de salarios es w_t - pi^c_t (salario nominal
    # menos inflacion NUCLEO); su rezago reemplaza al de la inflacion salarial. El
    # regresor coherente con esa dependiente es el ERROR de expectativas,
    # E_{t-12}pi_t - pi^c_t: los salarios se pactan con la inflacion esperada y una
    # inflacion mayor a la esperada erosiona el salario real.
    eme_err_nucleo = eme_used - core_inf_arr
    # Inflacion TOTAL de hace 12 meses (la del ano anterior, con la que se ajustan el
    # salario minimo y muchos contratos en Colombia), menos la nucleo actual: regresor
    # de la indexacion a inflacion pasada.
    inf_hist = raw[["date", "Inf_Rate"]].sort_values("date").set_index("date")["Inf_Rate"]
    inf_hist = inf_hist.asfreq("MS")
    inf_total_t12 = inf_hist.shift(12).reindex(dates).to_numpy(dtype=float)
    pasada_menos_nucleo = inf_total_t12 - core_inf_arr
    wage_rel = wage_annual - core_inf_arr
    wage_rel_lag1 = pd.Series(wage_rel).shift(1).to_numpy(dtype=float)
    # primer mes con dato salarial: sin rezago no se evalua la ecuacion (evita rezago=0)
    wage_rel = np.where(np.isfinite(wage_rel_lag1), wage_rel, np.nan)

    # --- Subempleo (bruto, GEIH): variacion interanual (elimina estacionalidad) ---
    sub = pd.read_csv(SUBEMPLEO_CSV, parse_dates=["date"])[["date", "tasa_subocupacion"]]
    base_s = pd.DataFrame({"date": dates}).merge(sub, on="date", how="left")
    tasa_sub = base_s["tasa_subocupacion"].to_numpy(dtype=float)
    subempleo_yoy = np.full(len(dates), np.nan)
    subempleo_yoy[12:] = tasa_sub[12:] - tasa_sub[:-12]

    # --- TGP desestacionalizada (v7): en NIVELES. La referencia de largo plazo ya
    # no es una media movil de 36 meses sino un estado latente TGP*_t (tasa de
    # participacion estructural) estimado conjuntamente con u* e ICU*; ver
    # NairuV7Model. El escalon `tgp_empalme_geih` recoge el salto de nivel de la
    # serie en el empalme ECH->GEIH (2006-07 y 2006-08 faltan en la fuente).
    tgp = pd.read_csv(TGP_CSV, parse_dates=["date"])[["date", "tgp_desest"]]
    base_p = pd.DataFrame({"date": dates}).merge(tgp, on="date", how="left")
    tgp_nivel = base_p["tgp_desest"].to_numpy(dtype=float)
    tgp_empalme_geih = (dates >= TGP_EMPALME_GEIH).astype(float)

    # --- Vacantes de prensa (Banrep): suma de 7 ciudades, log ---
    # ALERTA del critico (2026-09-23): el indice tiene un COLAPSO ESTRUCTURAL, no
    # ciclico (Bogota pasa de 387 a 13 avisos: migracion de clasificados a internet).
    # Se calcula la MA(60) sobre la serie COMPLETA desde 1976 (no solo 2004-2025)
    # para tener historia suficiente y se ofrecen DOS variantes ("trend": log crudo
    # + tendencia propia v*_t; "gap": desviacion de esa MA(60), sin estado propio)
    # que se comparan explicitamente en la Seccion de resultados (opcion 1 vs 2 del
    # critico).
    vac = pd.read_csv(VACANTES_CSV, parse_dates=["date"])
    vac["date"] = vac["date"].dt.to_period("M").dt.to_timestamp()
    city_cols = [c for c in vac.columns if c != "date"]
    vac["total_vacantes"] = vac[city_cols].sum(axis=1, skipna=True)
    vac = vac.sort_values("date").reset_index(drop=True)
    vac["log_total"] = np.log(np.where(vac["total_vacantes"] > 0, vac["total_vacantes"], np.nan))
    vac["log_total_ma60"] = vac["log_total"].rolling(window=60, min_periods=36, center=False).mean()
    vac["vac_gap"] = vac["log_total"] - vac["log_total_ma60"]
    base_v = pd.DataFrame({"date": dates}).merge(vac[["date", "log_total", "vac_gap"]], on="date", how="left")
    log_vacantes = base_v["log_total"].to_numpy(dtype=float)
    vacantes_gap = base_v["vac_gap"].to_numpy(dtype=float)

    for name, arr in [("unemployment_current", md.unemployment_current), ("icu_current", md.icu_current),
                       ("oil_shock", md.oil_shock)]:
        if not np.all(np.isfinite(arr)):
            raise ValueError(f"Columna {name} tiene NaN dentro de la muestra 2004-2025.")

    print(f"[datos v7] {len(dates)} meses ({dates[0].date()}..{dates[-1].date()}); "
          f"subempleo valido={int(np.isfinite(subempleo_yoy).sum())}; "
          f"tgp valida={int(np.isfinite(tgp_nivel).sum())}; "
          f"vacantes validas={int(np.isfinite(log_vacantes).sum())}; "
          f"salarios validos={int(np.isfinite(wage_rel).sum())}; "
          f"phillips-precios valida={int(np.isfinite(y_phillips).sum())}")

    if u_bar_override is None:
        u_bar, icu_bar, episodios = calcular_ancla_reversion(
            dates, core_inf_arr, inf_goal, md.unemployment_current, md.icu_current,
            min_meses=ancla_min_meses, tol=ancla_tol)
    else:
        u_bar = float(u_bar_override)
        _, icu_bar, episodios = calcular_ancla_reversion(
            dates, core_inf_arr, inf_goal, md.unemployment_current, md.icu_current,
            min_meses=ancla_min_meses, tol=ancla_tol)
        print(f"[ancla] u_bar SOBRESCRITO a {u_bar:.4f}% (sensibilidad)")

    return SampleV7(
        dates=dates, n=len(dates), unemployment=md.unemployment_current, icu=md.icu_current,
        covid_dummy=covid, subempleo_yoy=subempleo_yoy, tgp=tgp_nivel,
        tgp_empalme_geih=tgp_empalme_geih, log_vacantes=log_vacantes,
        vacantes_gap=vacantes_gap,
        wage_annual=wage_annual, wage_rel=wage_rel, wage_rel_lag1=wage_rel_lag1,
        eme_used=eme_used, eme_gap_meta=eme_gap_meta, eme_err_nucleo=eme_err_nucleo,
        pasada_menos_nucleo=pasada_menos_nucleo,
        y_phillips=y_phillips, y_phillips_lag1=y_lag1, y_phillips_lag2=y_lag2, oil_shock=md.oil_shock,
        u_bar=u_bar, icu_bar=icu_bar, episodios_ancla=episodios,
    )


# =========================================================================== #
#  2. Tasas naturales: 6 estados, 5 ecuaciones, sin subempleo ni vacantes      #
#  Estados: [u*, ICU*, l, l(-1), TGP*, g_TGP*]. Ecuaciones de medicion:        #
#   desempleo   u_t   = u*_t - l_t + d_u*COVID_t + e                           #
#   TGP         TGP_t = TGP*_t + beta_p*l_t + d_geih*1[t>=2006-07]             #
#                       + d_tgp*COVID_t + e                                    #
#   ICU         ICU_t = ICU*_t + beta_c*l_t [+ d_icu*COVID_t] + e              #
#   salarios    (w - pi^c)_t = c_w + gamma_w*(w - pi^c)_{t-1}                  #
#                       + gamma_we*X_t + kappa_w*(u_t - u*_t) + e              #
#               X_t = E_{t-12}pi_t - pi^c_t (error de expectativas) o          #
#                     E_{t-12}pi_t (nivel), segun la especificacion            #
#   precios     (pi^c - pi*)_t = a0 + rho1*(pi^c - pi*)_{t-1}                  #
#                       + rho2*(pi^c - pi*)_{t-2} + gamma_pe*(E_{t-12}pi_t - pi*_t)
#                       + theta*oil_t + kappa*(u_t - u*_t) + e                 #
#  w es el salario NOMINAL por ocupado de la EMMET (indice real del DANE por   #
#  IPC total). El ciclo l es un AR(2) estacionario, como es costumbre en la    #
#  literatura (Clark 1987; Laubach-Williams 2003). u* e ICU* revierten a un    #
#  ancla objetiva; TGP* es una tendencia sin ancla.                            #
#  Las opciones de especificacion (priors, intercepto, estacionariedad de la   #
#  Phillips de precios, COVID en ICU, forma del regresor de expectativas       #
#  salariales) estan en `EspecV7`; ESPEC_PRODUCCION es la elegida en el        #
#  gauntlet (gauntlet/RESULTADOS.md).                                          #
# =========================================================================== #
IX_USTAR, IX_ICUSTAR, IX_L, IX_LLAG, IX_TGPSTAR, IX_TGPSLOPE, IX_ICUERR = 0, 1, 2, 3, 4, 5, 6


# El error de medicion del ICU se lleva como estado AR(1): e_t = rho_icu_err*e_{t-1} + v_t.
# Con rho_icu_err = 0 (fijo) es exactamente el ruido blanco de medicion de v6.
N_STATES = 7


INIT_VAR = np.array([4.0, 16.0, 25.0, 25.0, 25.0, 0.01, 4.0])


# Horizonte (meses) con el que se traducen las creencias economicas sobre la
# variabilidad de una tasa natural en una decada a la desviacion de su innovacion.
HORIZONTE_PRIOR_MESES = 120


def moda_sigma_por_decada(d_decada: float, ley: str, h: int = HORIZONTE_PRIOR_MESES) -> float:
    """Convierte 'la tasa natural cambia, con desviacion d_decada pp, en una decada'
    en la moda de sigma de su innovacion mensual (sin contar la reversion a la
    media, es decir, como cota superior):
      paseo aleatorio en el nivel ('rw'):   sd(cambio en h) = sigma*sqrt(h)
      paseo aleatorio integrado ('irw'):    sd(cambio en h) ~= sigma*sqrt(h^3/3)."""
    if ley == "rw":
        return d_decada / math.sqrt(h)
    if ley == "irw":
        return d_decada / math.sqrt(h ** 3 / 3.0)
    raise ValueError(ley)


# Esquemas de prior: {estado: (moda de sigma, forma a) o None (sin prior)}.
# "heredado": el de v6 (u*: moda 0.05, a=8; ICU*: moda 0.05, a=25) sin prior en TGP*.
PRIORS_HEREDADOS = {"ustar": (0.05, 8.0), "icustar": (0.05, 25.0), "tgpstar": None}
PRIORS_NINGUNO = {"ustar": None, "icustar": None, "tgpstar": None}

# Creencias economicas (desviacion del cambio en una decada, pp):
#   u*   1.5 pp  -- las estimaciones de NAIRU para Colombia se mueven 1-2 pp por decada
#   ICU* 2.0 pts -- el uso de capacidad de largo plazo (ANDI/Fedesarrollo) se mueve ~2 pts
#   TGP* 3.0 pp  -- la participacion subio ~4 pp en 2008-2012 y cayo ~4 pp en 2012-2021
DECADA_PP = {"ustar": 1.5, "icustar": 2.0, "tgpstar": 3.0}


def priors_economicos(a: float, tgp_tendencia: str = "irw", decada: Dict[str, float] = None) -> Dict:
    """Mismo principio para las tres tendencias: moda derivada de la variabilidad
    decenal plausible (`DECADA_PP`) y una misma forma `a` (cuanto pesa la creencia)."""
    d = DECADA_PP if decada is None else decada
    return {"ustar": (moda_sigma_por_decada(d["ustar"], "rw"), a),
            "icustar": (moda_sigma_por_decada(d["icustar"], "rw"), a),
            "tgpstar": (moda_sigma_por_decada(d["tgpstar"], tgp_tendencia), a)}


@dataclass
class EspecV7:
    """Opciones de especificacion del bloque de tasas naturales."""
    nombre: str = "v7"
    tgp_tendencia: str = "irw"            # ley de TGP*: "irw" (tendencia suave) o "rw"
    # Indexacion salarial. Con la dependiente w - pi^c, los pesos sobre la inflacion
    # suman 1 y la opcion decide A CUAL se indexan los salarios:
    #   "error"       gamma_we libre sobre E_{t-12}pi - pi^c   (mezcla esperada / realizada)
    #   "error_fijo"  gamma_we = 1: indexacion ex ante a la inflacion esperada
    #   "expost"      gamma_we = 0: los salarios siguen a la inflacion nucleo realizada
    #   "pasada"      gamma_wp libre sobre pi^{total}_{t-12} - pi^c (inflacion del ano anterior)
    #   "pasada_fijo" gamma_wp = 1: indexacion plena a la inflacion del ano anterior
    #   "mixta"       gamma_we y gamma_wp libres (esperada, pasada y realizada)
    #   "nivel"       gamma_we libre sobre el NIVEL de E_{t-12}pi (forma de la v7 inicial)
    salarios_expectativa: str = "error"
    a0_libre: bool = True                 # intercepto de la Phillips de precios
    # Phillips de precios: "libre" (rho1 <= 0.98, rho2 y gamma_pe libres);
    # "estacionaria" (persistencia s = rho1 + rho2 < 1: la brecha vuelve a la meta);
    # "hibrida" (estacionaria y homogenea: gamma_pe = 1 - s, los pesos de la
    # expectativa y de la inflacion pasada suman 1 -- sin dilema de largo plazo).
    precios_forma: str = "libre"
    covid_icu: bool = False               # dummy COVID tambien en la ecuacion de ICU
    icu_error_ar1: bool = False           # error de medicion del ICU persistente (AR(1))
    naicu_constante: bool = False         # ICU* constante, nivel estimado por los datos (lambda=0, sigma~0)
    # Pandemia (abr-2020..jun-2021):
    #   "escalon"   dummy constante en desempleo y TGP (y en ICU si covid_icu), como v6
    #   "decae"     salto en abr-2020 que se desvanece a la tasa mensual estimada covid_decaimiento
    #   "faltante_cantidades"  los meses de pandemia son DATOS FALTANTES en las ecuaciones de
    #               desempleo, TGP e ICU (el filtro interpola u*, l, TGP*); sin dummies
    #   "faltante_todas"       idem en las cinco ecuaciones (tambien salarios y precios)
    #   "faltante_ciclo"       como "faltante_cantidades", pero en la ventana la holgura de las
    #               Phillips de salarios y precios se toma del ciclo latente (u - u* = -l) y no
    #               del desempleo observado, que el modelo trata como no informativo esos meses
    covid_forma: str = "escalon"
    covid_fin: str = "2021-06-01"         # ultimo mes de la ventana de pandemia (formas "faltante_*")
    priors: Dict = field(default_factory=lambda: dict(PRIORS_HEREDADOS))


PARAM_NAMES = [
    "rho_l1", "rho_l2", "log_sigma_l",
    "log_sigma_ustar_trend", "log_sigma_icustar_trend", "log_sigma_tgpstar_trend",
    "lambda_u", "lambda_icu",
    "delta_u_covid", "log_sigma_u",
    "beta_p", "delta_tgp_geih", "delta_tgp_covid", "log_sigma_p",
    "beta_c", "delta_icu_covid", "log_sigma_icu", "rho_icu_err",
    "c_w", "gamma_w", "gamma_we", "kappa_w", "log_sigma_w",
    "a0", "rho1", "rho2", "gamma_pe", "theta_oil", "kappa", "log_sigma_pi",
    "covid_decaimiento", "gamma_wp",
]


IX = {nm: i for i, nm in enumerate(PARAM_NAMES)}


N_PARAMS = len(PARAM_NAMES)


BOUNDS = [
    (-1.98, 1.98), (-0.98, 0.98), (-6.0, 4.0),
    (-8.0, 1.0), (-8.0, 2.0), (-12.0, 1.0),
    (0.0, 0.15), (0.0, 0.15),
    (-15.0, 25.0), (-6.0, 4.0),
    (-5.0, 5.0), (-10.0, 10.0), (-15.0, 15.0), (-6.0, 4.0),
    (-5.0, 5.0), (-40.0, 40.0), (-6.0, 4.0), (0.0, 0.98),
    (-10.0, 15.0), (-0.9, 0.9), (-2.0, 2.0), (-6.0, 6.0), (-6.0, 5.0),
    (-5.0, 5.0), (-0.98, 0.98), (-0.98, 0.98), (-2.0, 2.0), (-2.0, 2.0), (-3.0, 3.0), (-6.0, 4.0),
    (0.01, 1.0), (-2.0, 2.0),
]


def bounds_espec(e: "EspecV7") -> List[Tuple[float, float]]:
    """Cotas segun la especificacion. Un parametro con cota inferior = superior
    queda FIJO (L-BFGS-B lo respeta) y no se reporta como estimado."""
    b = list(BOUNDS)
    if not e.covid_icu:
        b[IX["delta_icu_covid"]] = (0.0, 0.0)
    if not e.icu_error_ar1:
        b[IX["rho_icu_err"]] = (0.0, 0.0)
    if e.covid_forma != "decae":
        b[IX["covid_decaimiento"]] = (0.0, 0.0)
    if e.covid_forma.startswith("faltante"):
        for nm in ("delta_u_covid", "delta_tgp_covid", "delta_icu_covid"):
            b[IX[nm]] = (0.0, 0.0)   # sin datos en la ventana, la dummy no tiene papel
    if e.naicu_constante:
        b[IX["lambda_icu"]] = (0.0, 0.0)
        b[IX["log_sigma_icustar_trend"]] = (-8.0, -8.0)   # sigma = 3e-4: constante a efectos practicos
    if not e.a0_libre:
        b[IX["a0"]] = (0.0, 0.0)
    if e.salarios_expectativa == "error_fijo":
        b[IX["gamma_we"]] = (1.0, 1.0)
    if e.salarios_expectativa in ("expost", "pasada", "pasada_fijo"):
        b[IX["gamma_we"]] = (0.0, 0.0)
    if e.salarios_expectativa not in ("pasada", "pasada_fijo", "mixta"):
        b[IX["gamma_wp"]] = (0.0, 0.0)
    if e.salarios_expectativa == "pasada_fijo":
        b[IX["gamma_wp"]] = (1.0, 1.0)
    if e.precios_forma == "estacionaria":
        b[IX["rho1"]] = (-0.98, 0.995)   # en estas formas el parametro "rho1" es la persistencia s
    elif e.precios_forma == "hibrida":
        b[IX["rho1"]] = (0.0, 0.995)     # s >= 0 para que gamma_pe = 1 - s quede en [0, 1]
        b[IX["gamma_pe"]] = (0.0, 0.0)   # derivado, no libre
    return b


def coef_precios(params: np.ndarray, e: "EspecV7") -> Dict[str, float]:
    """Coeficientes efectivos de la Phillips de precios segun la forma: en
    'estacionaria' e 'hibrida' el parametro "rho1" es la persistencia s = rho1 + rho2."""
    r2 = float(params[IX["rho2"]])
    if e.precios_forma == "libre":
        r1 = float(params[IX["rho1"]])
    else:
        r1 = float(params[IX["rho1"]]) - r2
    gpe = (1.0 - (r1 + r2)) if e.precios_forma == "hibrida" else float(params[IX["gamma_pe"]])
    return {"rho1": r1, "rho2": r2, "persistencia": r1 + r2, "gamma_pe": gpe, "a0": float(params[IX["a0"]])}


def parametros_fijos(e: "EspecV7") -> List[str]:
    return [nm for nm, (lo, hi) in zip(PARAM_NAMES, bounds_espec(e)) if lo == hi]


def _stationary_ok(rho1: float, rho2: float) -> bool:
    return (abs(rho2) < 0.999) and (rho1 + rho2 < 0.999) and (rho2 - rho1 < 0.999)


# Especificacion de PRODUCCION. Gauntlet 1 (gauntlet/RESULTADOS.md) fijo la estructura;
# el gauntlet 2 (gauntlet/RESULTADOS2.md, ganadora g2_r2_ciclo_error_fijo, 20/21) corrigio
# la pandemia y reviso la indexacion salarial:
#  - sin priors de suavidad: u* y TGP* quedan identificados por los datos (sin asimetria);
#  - NAICU constante (nivel estimado): ninguna especificacion identifico una ICU* variable;
#  - ICU desestacionalizado (STL) con error de medicion AR(1);
#  - PANDEMIA (abr-2020..jun-2021) como DATOS FALTANTES en desempleo, TGP e ICU, sin
#    dummies; en esa ventana la holgura de las Phillips de salarios y precios sale del
#    ciclo latente (u - u* = -l) y no del desempleo observado ("faltante_ciclo");
#  - Phillips de precios hibrida y homogenea (gamma_pe = 1 - rho1 - rho2) con a0 = 0;
#  - Phillips de salarios con indexacion MIXTA: pesos libres sobre la inflacion esperada
#    (gamma_we), la inflacion total del ano anterior (gamma_wp) y la inflacion nucleo
#    realizada (1 - gamma_we - gamma_wp). Adoptada por el usuario el 2026-10-21 tras
#    experimento_indexacion_tgp/: con la misma ventana de pandemia domina a la indexacion
#    ex ante (LR = 29, p < 0.001) y no deja parametros en su cota. La ex ante anterior
#    (salarios_expectativa="error_fijo") esta respaldada en _previo/ex_ante_2026-10-21/.
ESPEC_PRODUCCION = EspecV7(
    nombre="v7_produccion", tgp_tendencia="irw", salarios_expectativa="mixta",
    a0_libre=False, precios_forma="hibrida", covid_icu=False, icu_error_ar1=True,
    naicu_constante=True, covid_forma="faltante_ciclo", covid_fin="2021-06-01",
    priors=dict(PRIORS_NINGUNO))


class NairuV7Model(MLEModel):
    """6 estados [u*, ICU*, l, l_lag, TGP*, g_TGP*]; 5 ecuaciones de medicion
    (desempleo, TGP en niveles, ICU, salarios nominales relativos a la inflacion
    nucleo, inflacion nucleo relativa a la meta), ninguna usa el PIB.
    `wage_holgura` in {'gap','cycle'} controla si kappa_w carga sobre (u_t-u*_t)
    o directamente sobre l_t."""

    def __init__(self, s: SampleV7, wage_holgura: str = "gap",
                 disable_cols: Tuple[int, ...] = (),
                 col_overrides: Optional[Dict[int, np.ndarray]] = None,
                 espec: Optional[EspecV7] = None):
        self.s = s
        self.wage_holgura = wage_holgura
        self.espec = ESPEC_PRODUCCION if espec is None else espec
        self.tgp_tendencia = self.espec.tgp_tendencia
        if self.tgp_tendencia not in ("irw", "rw"):
            raise ValueError(f"tgp_tendencia desconocida: {self.tgp_tendencia!r}")
        self.bounds = bounds_espec(self.espec)
        fechas = pd.DatetimeIndex(s.dates)
        self._ventana_covid = ((fechas >= COVID_START) & (fechas <= pd.Timestamp(self.espec.covid_fin)))
        self._meses_covid = ((fechas.year - COVID_START.year) * 12 + (fechas.month - COVID_START.month)).to_numpy(float)
        self.x_salarios = (s.eme_used if self.espec.salarios_expectativa == "nivel" else s.eme_err_nucleo)
        cols = [s.unemployment, s.tgp, s.icu, s.wage_rel, s.y_phillips]
        if col_overrides:
            cols = list(cols)
            for i, arr in col_overrides.items():
                cols[i] = arr
        endog = np.column_stack(cols)
        for col in disable_cols:
            endog[:, col] = np.nan
        if self.espec.covid_forma.startswith("faltante"):
            ventana = self._ventana_covid
            cols_covid = [0, 1, 2, 3, 4] if self.espec.covid_forma == "faltante_todas" else [0, 1, 2]
            for col in cols_covid:
                endog[ventana, col] = np.nan
        super().__init__(endog, k_states=N_STATES, k_posdef=5)
        R = np.zeros((N_STATES, 5))
        R[IX_USTAR, 0] = 1.0; R[IX_ICUSTAR, 1] = 1.0; R[IX_L, 2] = 1.0
        R[IX_TGPSLOPE if self.tgp_tendencia == "irw" else IX_TGPSTAR, 3] = 1.0
        R[IX_ICUERR, 4] = 1.0
        self["selection"] = R
        init_state = np.zeros(N_STATES)
        init_state[IX_USTAR] = float(np.mean(s.unemployment[:12]))
        init_state[IX_ICUSTAR] = float(np.mean(s.icu[:12]))
        init_state[IX_TGPSTAR] = float(np.nanmean(s.tgp[:12]))
        init_var = INIT_VAR.copy()
        if self.tgp_tendencia == "rw":
            init_var[IX_TGPSLOPE] = 1e-12   # la pendiente no existe en el paseo aleatorio
        self.initialize_known(init_state, np.diag(init_var))

    @property
    def param_names(self):
        return PARAM_NAMES

    @property
    def start_params(self):
        p = np.zeros(N_PARAMS); ix = IX
        p[ix["rho_l1"]] = 1.1; p[ix["rho_l2"]] = -0.3; p[ix["log_sigma_l"]] = math.log(0.5)
        p[ix["log_sigma_ustar_trend"]] = math.log(0.1); p[ix["log_sigma_icustar_trend"]] = math.log(0.08)
        p[ix["log_sigma_tgpstar_trend"]] = math.log(0.003 if self.tgp_tendencia == "irw" else 0.1)
        p[ix["lambda_u"]] = 0.02; p[ix["lambda_icu"]] = 0.02
        p[ix["delta_u_covid"]] = 3.0; p[ix["log_sigma_u"]] = math.log(0.3)
        p[ix["beta_p"]] = 0.1; p[ix["delta_tgp_geih"]] = -3.0; p[ix["delta_tgp_covid"]] = -3.0
        p[ix["log_sigma_p"]] = math.log(0.5)
        p[ix["beta_c"]] = 1.0; p[ix["delta_icu_covid"]] = -5.0 if self.espec.covid_icu else 0.0
        p[ix["log_sigma_icu"]] = math.log(2.0); p[ix["rho_icu_err"]] = 0.5 if self.espec.icu_error_ar1 else 0.0
        p[ix["c_w"]] = 1.0; p[ix["gamma_w"]] = 0.5; p[ix["gamma_we"]] = 0.5
        p[ix["kappa_w"]] = -0.3; p[ix["log_sigma_w"]] = math.log(2.0)
        p[ix["a0"]] = 0.0; p[ix["rho1"]] = 0.9; p[ix["rho2"]] = 0.0; p[ix["gamma_pe"]] = 0.1
        p[ix["theta_oil"]] = -0.001; p[ix["kappa"]] = -0.03; p[ix["log_sigma_pi"]] = -1.27
        p[ix["covid_decaimiento"]] = 0.15 if self.espec.covid_forma == "decae" else 0.0
        p[ix["gamma_wp"]] = 0.5 if self.espec.salarios_expectativa in ("pasada", "mixta") else 0.0
        if self.espec.covid_forma.startswith("faltante"):
            p[ix["delta_u_covid"]] = p[ix["delta_tgp_covid"]] = p[ix["delta_icu_covid"]] = 0.0
        return np.clip(p, [b[0] for b in self.bounds], [b[1] for b in self.bounds])

    def update(self, params, **kwargs):
        params = super().update(params, **kwargs)
        s = self.s; ix = IX; dt = params.dtype
        rho_l1, rho_l2 = params[ix["rho_l1"]], params[ix["rho_l2"]]
        log_sig_l = params[ix["log_sigma_l"]]
        log_sig_ustar, log_sig_icustar, log_sig_tgpstar = (params[ix["log_sigma_ustar_trend"]],
                                                           params[ix["log_sigma_icustar_trend"]],
                                                           params[ix["log_sigma_tgpstar_trend"]])
        lambda_u, lambda_icu = params[ix["lambda_u"]], params[ix["lambda_icu"]]
        delta_u, log_sig_u = params[ix["delta_u_covid"]], params[ix["log_sigma_u"]]
        beta_p, d_geih, d_tgp_covid, log_sig_p = (params[ix["beta_p"]], params[ix["delta_tgp_geih"]],
                                                  params[ix["delta_tgp_covid"]], params[ix["log_sigma_p"]])
        beta_c, d_icu_covid, log_sig_icu, rho_icu_err = (params[ix["beta_c"]], params[ix["delta_icu_covid"]],
                                                         params[ix["log_sigma_icu"]], params[ix["rho_icu_err"]])
        c_w, gamma_w, gamma_we, kappa_w, log_sig_w = (params[ix["c_w"]], params[ix["gamma_w"]],
                                                       params[ix["gamma_we"]], params[ix["kappa_w"]],
                                                       params[ix["log_sigma_w"]])
        a0, rho1p, rho2p, gamma_pe, theta_oil, kappa, log_sig_pi = (
            params[ix["a0"]], params[ix["rho1"]], params[ix["rho2"]], params[ix["gamma_pe"]],
            params[ix["theta_oil"]], params[ix["kappa"]], params[ix["log_sigma_pi"]])
        if self.espec.precios_forma != "libre":       # "rho1" es la persistencia s
            if self.espec.precios_forma == "hibrida":
                gamma_pe = 1.0 - rho1p
            rho1p = rho1p - rho2p

        # u*_t = u*_{t-1} + lambda_u*(u_bar - u*_{t-1}) + eta_t (reversion a la
        # media hacia el ancla objetiva); idem ICU*.
        # TGP*_t = TGP*_{t-1} + g_{t-1};  g_t = g_{t-1} + zeta_t   ("irw")
        # TGP*_t = TGP*_{t-1} + zeta_t                              ("rw")
        T = np.zeros((N_STATES, N_STATES), dtype=dt)
        T[IX_USTAR, IX_USTAR] = 1.0 - lambda_u
        T[IX_ICUSTAR, IX_ICUSTAR] = 1.0 - lambda_icu
        T[IX_L, IX_L] = rho_l1; T[IX_L, IX_LLAG] = rho_l2; T[IX_LLAG, IX_L] = 1.0
        T[IX_TGPSTAR, IX_TGPSTAR] = 1.0
        if self.tgp_tendencia == "irw":
            T[IX_TGPSTAR, IX_TGPSLOPE] = 1.0
            T[IX_TGPSLOPE, IX_TGPSLOPE] = 1.0
        T[IX_ICUERR, IX_ICUERR] = rho_icu_err
        self["transition"] = T

        c_vec = np.zeros(N_STATES, dtype=dt)
        c_vec[IX_USTAR] = lambda_u * s.u_bar
        c_vec[IX_ICUSTAR] = lambda_icu * s.icu_bar
        self["state_intercept"] = c_vec

        Z = np.zeros((5, N_STATES), dtype=dt)
        Z[0, IX_USTAR] = 1.0; Z[0, IX_L] = -1.0                 # u = u* - l  (beta_u fijo=-1)
        Z[1, IX_TGPSTAR] = 1.0; Z[1, IX_L] = beta_p              # TGP = TGP* + beta_p*l
        Z[2, IX_ICUSTAR] = 1.0; Z[2, IX_L] = beta_c; Z[2, IX_ICUERR] = 1.0   # ICU (error como estado)
        if self.wage_holgura == "gap":
            Z[3, IX_USTAR] = -kappa_w
        else:
            Z[3, IX_L] = kappa_w
        Z[4, IX_USTAR] = -kappa                                  # Phillips precios (gap)
        if self.espec.covid_forma == "faltante_ciclo":
            # matriz de diseno variable en el tiempo: en la ventana, kappa*(u - u*) = -kappa*l
            Zt = np.repeat(Z[:, :, None], self.nobs, axis=2)
            w = self._ventana_covid
            Zt[4, IX_USTAR, w] = 0.0; Zt[4, IX_L, w] = -kappa
            if self.wage_holgura == "gap":
                Zt[3, IX_USTAR, w] = 0.0; Zt[3, IX_L, w] = -kappa_w
            self["design"] = Zt
        else:
            self["design"] = Z

        if self.espec.covid_forma == "decae":
            meses = self._meses_covid
            covid = np.where(meses >= 0, np.exp(-params[ix["covid_decaimiento"]] * np.maximum(meses, 0.0)), 0.0)
        else:
            covid = s.covid_dummy
        d = np.zeros((5, self.nobs), dtype=dt)
        d[0, :] = delta_u * covid
        d[1, :] = d_geih * s.tgp_empalme_geih + d_tgp_covid * covid
        d[2, :] = d_icu_covid * covid
        d[3, :] = (c_w + gamma_w * np.nan_to_num(s.wage_rel_lag1, nan=0.0)
                   + gamma_we * np.nan_to_num(self.x_salarios, nan=0.0)
                   + params[ix["gamma_wp"]] * np.nan_to_num(s.pasada_menos_nucleo, nan=0.0))
        u_holgura = s.unemployment
        if self.espec.covid_forma == "faltante_ciclo":
            u_holgura = np.where(self._ventana_covid, 0.0, s.unemployment)   # la holgura sale del estado l
        if self.wage_holgura == "gap":
            d[3, :] = d[3, :] + kappa_w * u_holgura
        d[4, :] = (a0 + rho1p * np.nan_to_num(s.y_phillips_lag1, nan=0.0)
                   + rho2p * np.nan_to_num(s.y_phillips_lag2, nan=0.0)
                   + gamma_pe * np.nan_to_num(s.eme_gap_meta, nan=0.0)
                   + theta_oil * s.oil_shock + kappa * u_holgura)
        self["obs_intercept"] = d

        Q = np.diag(np.array([np.exp(log_sig_ustar) ** 2, np.exp(log_sig_icustar) ** 2,
                               np.exp(log_sig_l) ** 2, np.exp(log_sig_tgpstar) ** 2,
                               np.exp(log_sig_icu) ** 2], dtype=dt))
        self["state_cov"] = Q
        H = np.diag(np.array([np.exp(log_sig_u) ** 2, np.exp(log_sig_p) ** 2,
                               0.0, np.exp(log_sig_w) ** 2,
                               np.exp(log_sig_pi) ** 2], dtype=dt))
        self["obs_cov"] = H


# =========================================================================== #
#  2bis. Priors de suavidad de las tendencias                                 #
# =========================================================================== #
# Prior inverso-gamma sobre la VARIANZA de la innovacion de cada tendencia, con
# moda m^2 (m = moda de sigma) y forma a: -log p = (a+1)*log(s2) + b/s2, b = m^2(a+1).
# El termino b/s2 impide el pile-up (sigma -> 0, tendencia constante); el termino
# logaritmico castiga tendencias que persiguen al dato. Historia: v6 heredo
# (moda 0.05, a=8) para u* y recalibro ICU* a a=25 con el chequeo MA36; la v7
# evalua ese esquema contra uno simetrico derivado de creencias economicas
# (`priors_economicos`) en el gauntlet (gauntlet/RESULTADOS.md).
_PRIOR_PARAM = {"ustar": "log_sigma_ustar_trend", "icustar": "log_sigma_icustar_trend",
                "tgpstar": "log_sigma_tgpstar_trend"}


def _ig_penalty_a(var: float, b: float, a: float) -> float:
    """-log IG(var; a, b) (+const)."""
    var = max(var, 1e-12)
    return (a + 1.0) * math.log(var) + b / var


def trend_prior_penalty(params: np.ndarray, priors: Dict) -> float:
    pen = 0.0
    for estado, spec in priors.items():
        if spec is None:
            continue
        moda, a = spec
        sig = math.exp(params[IX[_PRIOR_PARAM[estado]]])
        pen += _ig_penalty_a(sig ** 2, (moda ** 2) * (a + 1.0), a)
    return pen


def intervalo_prior(moda: float, a: float, prob: float = 0.95) -> Tuple[float, float]:
    """Intervalo central de sigma implicado por el prior IG sobre la varianza."""
    from scipy import stats
    b = moda ** 2 * (a + 1.0)
    lo, hi = stats.invgamma(a, scale=b).ppf([(1 - prob) / 2, (1 + prob) / 2])
    return math.sqrt(lo), math.sqrt(hi)


def stationary_penalty(params: np.ndarray, espec: Optional[EspecV7] = None) -> float:
    pen = 0.0
    rho1, rho2 = params[IX["rho_l1"]], params[IX["rho_l2"]]
    if not _stationary_ok(rho1, rho2):
        pen += 1e6 * (abs(rho1) + abs(rho2))
    if espec is not None and espec.precios_forma != "libre":
        cp = coef_precios(params, espec)   # s < 1 lo garantizan las cotas; aqui solo rho2 - rho1 < 1
        if not _stationary_ok(cp["rho1"], cp["rho2"]):
            pen += 1e6 * (abs(cp["rho1"]) + abs(cp["rho2"]))
    return pen


def safe_loglike(mod: NairuV7Model, params: np.ndarray) -> float:
    pen = stationary_penalty(params, mod.espec)
    if pen > 0:
        return -1e8 - pen
    try:
        return float(mod.loglike(params))
    except Exception:
        return -1e8


def neg_loglike(params: np.ndarray, mod: NairuV7Model) -> float:
    ll = safe_loglike(mod, params)
    if ll <= -1e7:
        return 1e8
    return -ll


def neg_log_posterior(params: np.ndarray, mod: NairuV7Model) -> float:
    nll = neg_loglike(params, mod)
    if nll >= 1e7:
        return nll
    return nll + trend_prior_penalty(params, mod.espec.priors)


def extraer_smooth(mod: NairuV7Model, params: np.ndarray) -> Dict[str, np.ndarray]:
    res = mod.smooth(params)
    ss = res.smoothed_state; ssv = res.smoothed_state_cov
    return {"ustar": ss[IX_USTAR], "icustar": ss[IX_ICUSTAR], "l": ss[IX_L],
            "tgpstar": ss[IX_TGPSTAR], "tgpslope": ss[IX_TGPSLOPE],
            # estados FILTRADOS (solo informacion hasta t): estabilidad en tiempo real
            "ustar_filt": res.filtered_state[IX_USTAR], "icustar_filt": res.filtered_state[IX_ICUSTAR],
            # TGP observada sin el salto del empalme ECH->GEIH (comparable con TGP*)
            "tgp_ajustada": mod.s.tgp - params[IX["delta_tgp_geih"]] * mod.s.tgp_empalme_geih,
            "ustar_var": ssv[IX_USTAR, IX_USTAR], "icustar_var": ssv[IX_ICUSTAR, IX_ICUSTAR],
            "tgpstar_var": ssv[IX_TGPSTAR, IX_TGPSTAR], "res": res}


# =========================================================================== #
#  3. Multistart + fit                                                        #
# =========================================================================== #
def estimar(mod: NairuV7Model, n_starts: int, seed: int = 20260923,
            maxiter: int = 150, maxfun: int = 1500, verbose: bool = True) -> Dict:
    rng = np.random.default_rng(seed)
    lo, hi = [b[0] for b in mod.bounds], [b[1] for b in mod.bounds]
    base = np.clip(mod.start_params, lo, hi)
    starts = [base.copy()]
    grid = []
    for kw in (-0.15, -0.3, -0.6):
        for rl1 in (0.7, 1.1, 1.5):
            grid.append((kw, rl1))
    rng.shuffle(grid)
    for kw, rl1 in grid[: max(0, n_starts - 1)]:
        st = base.copy(); st[IX["kappa_w"]] = kw; st[IX["rho_l1"]] = rl1
        st[IX["rho_l2"]] = -0.3 if rl1 > 1.0 else 0.1
        starts.append(np.clip(st, lo, hi))

    best = None
    t0 = time.time()
    for i, st in enumerate(starts):
        opt = minimize(neg_log_posterior, x0=st, args=(mod,), method="L-BFGS-B", bounds=mod.bounds,
                       options={"maxiter": maxiter, "maxfun": maxfun, "ftol": 1e-9, "gtol": 1e-5})
        if verbose:
            print(f"    [start {i+1}/{len(starts)}] obj={opt.fun:.4f} success={opt.success} "
                  f"({time.time()-t0:.1f}s acum.)")
        if best is None or opt.fun < best.fun:
            best = opt
    return {"result": best, "n_starts": len(starts), "elapsed": time.time() - t0}


def polish(mod: NairuV7Model, x0: np.ndarray, maxiter=20000, maxfun=60000) -> object:
    """Refinamiento final con tolerancias estrictas. `success` de scipy no
    distingue una solucion de esquina (parametro en su cota) de una falla
    numerica: el diagnostico de convergencia se hace aparte (gradiente)."""
    return minimize(neg_log_posterior, x0=x0, args=(mod,), method="L-BFGS-B", bounds=mod.bounds,
                    options={"maxiter": maxiter, "maxfun": maxfun, "ftol": 1e-14, "gtol": 1e-8})


def errores_estandar(mod: NairuV7Model, params: np.ndarray) -> np.ndarray:
    """EE del hessiano numerico de la verosimilitud pura (sin prior), solo sobre
    los parametros LIBRES; los fijados por la especificacion quedan en NaN."""
    libres = [i for i, (lo, hi) in enumerate(mod.bounds) if lo < hi]
    se = np.full(N_PARAMS, np.nan)

    def f(x):
        p = params.copy(); p[libres] = x
        return float(mod.loglike(p))
    try:
        from statsmodels.tools.numdiff import approx_hess3
        H = approx_hess3(params[libres], f)
        cov = np.linalg.pinv(-H)
        var = np.diag(cov)
        se_l = np.sqrt(np.where(var > 0, var, np.nan))
        # varianza <= 0: el hessiano no es definido negativo en esa direccion (la
        # verosimilitud es plana o tiene la curvatura equivocada) -> EE no definido.
        # Sin curvatura alguna (la pseudo-inversa devolveria 0): EE infinito.
        se_l[np.diag(-H) < 1e-6] = np.inf
        se[libres] = se_l
    except Exception:
        pass
    return se


def ajustar(s: SampleV7, wage_holgura: str, n_starts: int,
            disable_cols: Tuple[int, ...] = (),
            polish_maxiter: int = 20000, polish_maxfun: int = 60000,
            espec: Optional[EspecV7] = None, verbose: bool = True) -> Tuple[NairuV7Model, np.ndarray, Dict]:
    mod = NairuV7Model(s, wage_holgura=wage_holgura, disable_cols=disable_cols, espec=espec)
    tag = f"{mod.espec.nombre}"
    if verbose:
        print(f"[{tag}] multistart n_starts={n_starts}...")
    fit = estimar(mod, n_starts=n_starts, verbose=verbose)
    opt0 = fit["result"]
    opt = polish(mod, opt0.x, maxiter=polish_maxiter, maxfun=polish_maxfun)
    if verbose:
        print(f"[{tag}] multistart -> obj={opt0.fun:.4f}; polish -> obj={opt.fun:.4f} "
              f"success={opt.success} nit={opt.nit} nfev={opt.nfev}")
    params = np.array(opt.x, dtype=float)
    se_all = errores_estandar(mod, params)
    info = {"neg_log_post_multistart": float(opt0.fun), "neg_log_post_final": float(opt.fun),
            "loglike_puro": float(mod.loglike(params)), "success": bool(opt.success),
            "nit": int(opt.nit), "nfev": int(opt.nfev), "message": str(opt.message),
            "n_starts": fit["n_starts"], "elapsed": fit["elapsed"], "se": se_all,
            "n_libres": int(sum(lo < hi for lo, hi in mod.bounds)),
            "sigma_ustar_trend": float(math.exp(params[IX["log_sigma_ustar_trend"]])),
            "sigma_icustar_trend": float(math.exp(params[IX["log_sigma_icustar_trend"]])),
            "sigma_tgpstar_trend": float(math.exp(params[IX["log_sigma_tgpstar_trend"]])),
            "tgp_tendencia": mod.tgp_tendencia,
            "lambda_u": float(params[IX["lambda_u"]]), "lambda_icu": float(params[IX["lambda_icu"]]),
            "se_lambda_u": float(se_all[IX["lambda_u"]]), "se_lambda_icu": float(se_all[IX["lambda_icu"]]),
            "u_bar": float(s.u_bar), "icu_bar": float(s.icu_bar)}
    return mod, params, info


# =========================================================================== #
#  5. Decomposicion: cual observable identifica el NAIRU (RMSD por ecuacion)  #
# =========================================================================== #
def ma36_sd(x: np.ndarray) -> float:
    return float(pd.Series(x).rolling(window=36, min_periods=24).mean().std())


def chequeo_ma36(smooth: Dict[str, np.ndarray], s: SampleV7) -> pd.DataFrame:
    """Condicion de ACEPTACION adoptada: la sd de la
    MA36 del ESTADO (componente de baja frecuencia) debe ser MENOR que la del
    OBSERVADO correspondiente -- una tendencia no puede oscilar mas que el
    propio dato. Se aplica a u*, a ICU* y (v7) a TGP*; para TGP* el observado se
    corrige por el salto del empalme ECH->GEIH (delta_tgp_geih), que TGP* no absorbe."""
    filas = []
    for nombre, estado, obs in [("u*", smooth["ustar"], s.unemployment),
                                 ("ICU*", smooth["icustar"], s.icu),
                                 ("TGP*", smooth["tgpstar"], smooth["tgp_ajustada"])]:
        sd_e, sd_o = ma36_sd(estado), ma36_sd(obs)
        filas.append({"estado": nombre, "sd_ma36_estado": sd_e, "sd_ma36_observado": sd_o,
                     "cumple_sd_estado_menor": bool(sd_e < sd_o)})
    return pd.DataFrame(filas)

# ##########################################################################
# ###  MODULO C: ORQUESTACION (PASOS 0 A 3)                                  ###
# ##########################################################################

OUT_DIR = Path(OUTPUT_DIR)   # OUT_DIR (str) viene del modulo de PIB
V7_NAIRU_CSV = OUT_DIR / "nairu_estimates_v7.csv"
N_STARTS_PRODUCCION = 6   # arranques del multistart en la estimacion de produccion

# La tendencia de la PTF se obtiene SIEMPRE con la regresion por tramos estilo CBO
# (`estimar_ptf_tendencia_cbo`): tendencia lineal a trozos sobre el residuo de Solow,
# con quiebres de pendiente en los picos del ciclo fechados por Bry-Boschan y en el
# quiebre exogeno del fin del auge de materias primas (2014Q3). Es el unico metodo
# de produccion; no hay alternativa seleccionable.


# =========================================================================== #
#  PASO 1: bloque de tasas naturales (NAIRU / NAICU)                        #
# =========================================================================== #
def paso1_nairu_naicu(n_starts: int = N_STARTS_PRODUCCION):
    print("\n############################################################")
    print("### v7 PASO 1/3: NAIRU/NAICU (bloque laboral y de precios)  ###")
    print("###   ancla por regla objetiva + priors de suavidad        ###")
    print("############################################################")
    s = cargar_datos()
    print(f"[v7] especificacion: {ESPEC_PRODUCCION}")
    print(f"[v7] ancla objetiva: u_bar={s.u_bar:.4f}%  icu_bar={s.icu_bar:.4f}  "
          f"({len(s.episodios_ancla)} episodios de inflacion nucleo estable)")

    mod, params, fit = ajustar(s, "gap", n_starts)
    print(f"[v7] ajuste principal: success(scipy)={fit['success']} nit={fit['nit']} nfev={fit['nfev']}  "
          f"lambda_u={fit['lambda_u']:.4f} (SE={fit['se_lambda_u']:.4f})  "
          f"sigma(u*)={fit['sigma_ustar_trend']:.4f}  sigma(ICU*)={fit['sigma_icustar_trend']:.4f}  "
          f"sigma(TGP*)={fit['sigma_tgpstar_trend']:.5f} ({fit['tgp_tendencia']})")

    smooth = extraer_smooth(mod, params)

    ma36 = chequeo_ma36(smooth, s)
    print("[v7] Chequeo MA36 (condicion de aceptacion del prior:")
    print(ma36.to_string(index=False))
    if not bool(ma36["cumple_sd_estado_menor"].all()):
        print("[v7][AVISO] El chequeo MA36 NO se cumple para algun estado en esta corrida -- revisar "
              "la seccion de convergencia de la nota metodologica. Se continua (es un diagnostico "
              "de esta integracion, no una compuerta), pero debe reportarse al orquestador.")

    nairu_se = np.sqrt(np.maximum(smooth["ustar_var"], 1e-10))
    naicu_se = np.sqrt(np.maximum(smooth["icustar_var"], 1e-10))
    out = pd.DataFrame({
        "Date": pd.to_datetime(s.dates),
        "unemployment": s.unemployment,
        "icu": s.icu,
        "nairu_smooth": np.clip(smooth["ustar"], MIN_NAIRU_LEVEL, MAX_NAIRU_LEVEL),
        "naicu_smooth": np.clip(smooth["icustar"], MIN_NAICU_LEVEL, MAX_NAICU_LEVEL),
    })
    out["nairu_smooth_lo95"] = out["nairu_smooth"] - Z_95 * nairu_se
    out["nairu_smooth_hi95"] = out["nairu_smooth"] + Z_95 * nairu_se
    out["naicu_smooth_lo95"] = out["naicu_smooth"] - Z_95 * naicu_se
    out["naicu_smooth_hi95"] = out["naicu_smooth"] + Z_95 * naicu_se

    # Mismo escritor que la version anterior usaba (generico sobre el esquema de `out`) -- garantiza
    # el mismo esquema de columnas que consume `leer_nairu_csv`.
    _escribir_nairu_csv(out, V7_NAIRU_CSV)
    print(f"[v7] NAIRU/NAICU -> {V7_NAIRU_CSV}  "
          f"(NAIRU_fin_muestra={float(out['nairu_smooth'].iloc[-1]):.2f}%, "
          f"NAICU_fin_muestra={float(out['naicu_smooth'].iloc[-1]):.2f})")

    # v7: estados adicionales (TGP*, ciclo laboral) y coeficientes con errores estandar
    tgp_se = np.sqrt(np.maximum(smooth["tgpstar_var"], 1e-10))
    d_geih = float(params[IX["delta_tgp_geih"]])
    estados = pd.DataFrame({
        # TGP* y la TGP homogenea se expresan en unidades de la GEIH (medicion vigente):
        # el estado esta en unidades ECH, de modo que se le suma delta_tgp_geih.
        "Date": pd.to_datetime(s.dates), "tgp": s.tgp,
        "tgp_homogenea_geih": smooth["tgp_ajustada"] + d_geih,
        "tgp_star": smooth["tgpstar"] + d_geih, "tgp_star_lo95": smooth["tgpstar"] + d_geih - Z_95 * tgp_se,
        "tgp_star_hi95": smooth["tgpstar"] + d_geih + Z_95 * tgp_se,
        "tgp_star_pendiente_anual": 12.0 * smooth["tgpslope"],
        "ciclo_laboral_l": smooth["l"],
        "y_phillips_precios": s.y_phillips, "y_phillips_salarios": s.wage_rel,
    })
    estados.to_csv(OUT_DIR / "estados_v7.csv", index=False)
    coef = pd.DataFrame({"parametro": PARAM_NAMES, "valor": params, "se": fit["se"]})
    coef["t"] = coef["valor"] / coef["se"]
    coef["fijo"] = [lo == hi for lo, hi in mod.bounds]
    coef["en_cota"] = [bool(lo < hi and min(abs(v - lo), abs(v - hi)) < 1e-4)
                       for v, (lo, hi) in zip(params, mod.bounds)]
    # coeficientes EFECTIVOS de la Phillips de precios (en las formas 'estacionaria' e
    # 'hibrida' el parametro "rho1" es la persistencia s = rho1 + rho2)
    cp = coef_precios(params, mod.espec)
    coef = pd.concat([coef, pd.DataFrame([
        {"parametro": f"efectivo_{k}", "valor": v, "se": np.nan, "t": np.nan, "fijo": False, "en_cota": False}
        for k, v in cp.items()])], ignore_index=True)
    coef.to_csv(OUT_DIR / "nairu_mle_coefficients_v7.csv", index=False)
    fit["loglike_puro"] = float(mod.loglike(params))
    print("[v7] Coeficientes:")
    print(coef.to_string(index=False, float_format=lambda v: f"{v:.4f}"))

    png = OUT_DIR / "pib_potencial_integrado_v7_nairu_naicu.png"
    _graficar_nairu_naicu(out, s, fit, png, estados=estados)
    print(f"[v7] Figura NAIRU/NAICU -> {png}")
    return s, mod, params, fit, smooth, out, ma36


def _graficar_nairu_naicu(out: pd.DataFrame, s, fit: Dict, path: Path,
                         estados: Optional[pd.DataFrame] = None) -> None:
    dates = pd.to_datetime(out["Date"])
    fig, ax = plt.subplots(1, 3, figsize=(22, 6))
    ax[0].fill_between(dates, out["nairu_smooth_lo95"], out["nairu_smooth_hi95"],
                        color="#4EA72E", alpha=0.20, label="IC 95%")
    ax[0].plot(dates, out["unemployment"], color="black", lw=0.9, alpha=0.45, label="Desempleo (obs)")
    ax[0].plot(dates, out["nairu_smooth"], color="#196B24", lw=2.2, label="NAIRU v7 (suavizado)")
    ax[0].axhline(s.u_bar, color="#196B24", lw=0.9, ls=":", label=f"u_bar={s.u_bar:.2f}% (ancla objetiva)")
    ax[0].set_title(f"NAIRU v7 (lambda_u={fit['lambda_u']:.3f}, sigma(u*)={fit['sigma_ustar_trend']:.3f})")
    ax[0].set_ylabel("%"); ax[0].legend(fontsize=9)
    ax[1].fill_between(dates, out["naicu_smooth_lo95"], out["naicu_smooth_hi95"],
                        color="#156082", alpha=0.18, label="IC 95%")
    ax[1].plot(dates, out["icu"], color="black", lw=0.9, alpha=0.45, label="ICU (obs)")
    ax[1].plot(dates, out["naicu_smooth"], color="#0B3D5C", lw=2.2, label="NAICU v7 (suavizado)")
    ax[1].axhline(s.icu_bar, color="#0B3D5C", lw=0.9, ls=":", label=f"icu_bar={s.icu_bar:.2f} (ancla objetiva)")
    ax[1].set_title(f"NAICU v7 (lambda_icu={fit['lambda_icu']:.3f}, sigma(ICU*)={fit['sigma_icustar_trend']:.3f})")
    ax[1].legend(fontsize=9)
    if estados is not None:
        ax[2].fill_between(dates, estados["tgp_star_lo95"], estados["tgp_star_hi95"],
                            color="#A02B93", alpha=0.18, label="IC 95%")
        ax[2].plot(dates, estados["tgp"], color="black", lw=0.9, alpha=0.30, label="TGP (obs)")
        ax[2].plot(dates, estados["tgp_homogenea_geih"], color="black", lw=0.9, alpha=0.6,
                   label="TGP homogenea (unidades GEIH)")
        ax[2].plot(dates, estados["tgp_star"], color="#A02B93", lw=2.2, label="TGP* v7 (suavizado, unidades GEIH)")
        ax[2].set_title(f"TGP* v7 ({fit['tgp_tendencia']}, sigma={fit['sigma_tgpstar_trend']:.4f})")
        ax[2].legend(fontsize=9)
    fig.suptitle("Estimacion de NAIRU, NAICU y TGP* -- ancla de nivel por regla objetiva\n(estimadas sin usar el PIB en ninguna ecuacion del bloque)",
                 fontsize=14)
    fig.tight_layout(rect=(0.0, 0.0, 1.0, 0.96))
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)


# =========================================================================== #
#  PASO 2: modulo PIB (la version anterior, sin cambios de logica) alimentado con NAIRU v7    #
# =========================================================================== #
def paso2_pib_potencial(out_nairu: pd.DataFrame):
    print("\n############################################################")
    print("### v7 PASO 2/3: PIB POTENCIAL (funcion de produccion)     ###")
    print("############################################################")
    try:
        picos = detectar_picos_bbq()
    except Exception as exc:
        print(f"[v7][AVISO] No se detectaron picos BBQ: {exc!r}")
        picos = []
    CONFIG_PRODUCCION.picos_bbq = list(picos)

    # Bug ya visto en la (una version anterior): el PIB lee la NAIRU de config.nairu_csv,
    # NO de un global -- se fija EXPLICITAMENTE al CSV de v7 y se verifica.
    CONFIG_PRODUCCION.nairu_csv = str(V7_NAIRU_CSV)
    assert CONFIG_PRODUCCION.nairu_csv == str(V7_NAIRU_CSV), \
        "CONFIG_V2.nairu_csv no quedo fijado al CSV de v7 -- no continuar."
    print(f"[v7] CONFIG_V2.nairu_csv = {CONFIG_PRODUCCION.nairu_csv}")

    df, reg, alpha, T, reg_ptf = ejecutar_pipeline(CONFIG_PRODUCCION)
    print(f"[v7] T (ultimo trimestre completo) = {T.date()}   alpha = {float(alpha):.6g}")

    od = str(OUT_DIR)
    excel_out = os.path.join(od, "pib_potencial_integrado_v7.xlsx")
    escribir_excel_salida(df, reg, alpha, T, excel_out, config=CONFIG_PRODUCCION, reg_ptf=reg_ptf)
    df.to_csv(os.path.join(od, "pib_potencial_integrado_v7_series.csv"), index=False)

    png_niveles = os.path.join(od, "pib_potencial_integrado_v7_niveles.png")
    png_brechas = os.path.join(od, "pib_potencial_integrado_v7_brechas.png")
    graficar_niveles(df, T, png_niveles)
    graficar_brechas(df, T, png_brechas)

    print("[v7] Banda 95% del PIB potencial (delta method, 4 corridas adicionales)...")
    try:
        ci = _ci_pib_potencial(out_nairu, df)
    except Exception as exc:
        print(f"[v7][AVISO] No se pudo calcular la banda del PIB potencial: {exc!r}")
        ci = None
    png_paneles = os.path.join(od, "pib_potencial_integrado_v7_paneles.png")
    _graficar_pib_paneles(df, T, ci, png_paneles)
    png_ptf = os.path.join(od, "pib_potencial_integrado_v7_ptf_vs_dane.png")
    _graficar_ptf_vs_dane(df, T, png_ptf)
    png_ptf_ten = os.path.join(od, "pib_potencial_integrado_v7_ptf_tendencia.png")
    _graficar_ptf_tendencia(df, T, png_ptf_ten)

    return df, reg, alpha, T, reg_ptf, excel_out, png_niveles, png_brechas, png_paneles, png_ptf


# =========================================================================== #
#  PASO 3: contraste EX POST vs VIOG/1-VIOG -- DIAGNOSTICO, no calibracion   #
# =========================================================================== #
def paso3_diagnostico_filtros(df: pd.DataFrame):
    print("\n############################################################")
    print("### v7 PASO 3/3: contraste EX POST vs VIOG/1-VIOG          ###")
    print("###   (diagnostico -- NO se usa para elegir hiperparametros)###")
    print("############################################################")
    ref = leer_compuestos_filtros()
    met = metricas_vs_compuestos(df, ref)
    print(f"[v7] Niveles (ventana {CALIB_WINDOW[0]}..{CALIB_WINDOW[1]}, sin COVID): "
          f"RMSE medio={met['rmse_mean']:.4f}pp  corr medio={met['corr_mean']:+.3f}  "
          f"(n={met['n_obs']})")

    # Descomposicion por banda de frecuencia (Christiano-Fitzgerald, dos colas),
    # replicando diagnostico/07_desacople_laboral_producto.py, la revision metodologica).
    # IMPORTANTE: el filtro CF se aplica sobre la serie TRIMESTRAL COMPLETA y
    # CONTIGUA (sin cortar la ventana ni excluir COVID) -- cortar meses ANTES de
    # filtrar introduciria una discontinuidad artificial (un salto donde antes
    # habia COVID) que el filtro de dos colas interpretaria como ciclo espurio.
    # La ventana y la exclusion de COVID se aplican DESPUES, solo al calcular la
    # correlacion, igual que en el diagnostico original.
    m_full = _brechas_comparables(df, ref).set_index("q").sort_index()
    ours_full, viog_full, invviog_full = m_full["gap_ours"], m_full["gap4_viog"], m_full["gap4_inv_viog"]
    en_ventana_covid = m_full["en_ventana"] & ~m_full["covid"]

    def _bandas(x: pd.Series):
        x = x.dropna()
        alta, _ = cffilter(x, low=8, high=32, drift=True)
        baja, _ = cffilter(x, low=32, high=80, drift=True)
        return alta, baja

    def _corr_banda(o: pd.Series, v: pd.Series, iv: pd.Series, mask: pd.Series):
        d = pd.concat([o.rename("o"), v.rename("v"), iv.rename("iv")], axis=1)
        d = d[mask.reindex(d.index, fill_value=False)].dropna()
        if len(d) < 3:
            return float("nan"), len(d)
        return float(0.5 * (d["o"].corr(d["v"]) + d["o"].corr(d["iv"]))), int(len(d))

    ours_alta, ours_baja = _bandas(ours_full)
    viog_alta, viog_baja = _bandas(viog_full)
    invviog_alta, invviog_baja = _bandas(invviog_full)
    corr_alta, n_alta = _corr_banda(ours_alta, viog_alta, invviog_alta, en_ventana_covid)
    corr_baja, n_baja = _corr_banda(ours_baja, viog_baja, invviog_baja, en_ventana_covid)
    print(f"[v7] Banda alta (2-8 anhos, ciclo de negocios): corr medio={corr_alta:+.3f}  (n={n_alta})")
    print(f"[v7] Banda baja (8-20 anhos):                    corr medio={corr_baja:+.3f}  (n={n_baja})")

    png_filtros = OUT_DIR / "pib_potencial_integrado_v7_vs_filtros.png"
    _graficar_filtros(df, ref, met, corr_alta, corr_baja, png_filtros)
    return met, corr_alta, corr_baja, n_alta, n_baja, png_filtros


def _graficar_filtros(df, ref, met, corr_alta, corr_baja, path: Path) -> None:
    m = _brechas_comparables(df, ref)
    m = m[m["en_ventana"]]
    x = m["q"].dt.to_timestamp(how="end")
    fig, ax = plt.subplots(figsize=(14, 6.5))
    ax.axhline(0, color="black", lw=0.8)
    ax.plot(x, m["gap4_viog"], color="#7f7f7f", lw=1.8, label="Compuesto VIOG (filtros, base 4T)")
    ax.plot(x, m["gap4_inv_viog"], color="#7f7f7f", lw=1.8, ls="--", label="Compuesto 1/VIOG (filtros, base 4T)")
    ax.plot(x, m["gap_ours"], color="#196B24", lw=2.4,
            label="Modelo v7 (NAIRU/NAICU estimadas sin el PIB; ancla por regla objetiva)")
    c0 = CALIB_COVID_EXCL[0].to_timestamp(how="start"); c1 = CALIB_COVID_EXCL[1].to_timestamp(how="end")
    ax.axvspan(c0, c1, color="grey", alpha=0.25, hatch="//", label="COVID (excluido de la metrica)")
    ax.set_title("Brecha del producto del modelo v7 vs compuestos de filtros estadisticos\n"
                 f"DIAGNOSTICO EX POST (no criterio de seleccion): RMSE medio={met['rmse_mean']:.2f}pp, "
                 f"corr nivel={met['corr_mean']:+.2f}, corr banda 2-8a={corr_alta:+.2f}, "
                 f"corr banda 8-20a={corr_baja:+.2f}", fontsize=11)
    ax.set_ylabel("% del PIB potencial (base suma movil 4T)")
    ax.legend(fontsize=9, loc="lower left", frameon=False); ax.grid(alpha=0.3)
    ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(False)
    fig.tight_layout(); fig.savefig(path, dpi=200, bbox_inches="tight"); plt.close(fig)


# =========================================================================== #
#  MAIN                                                                       #
# =========================================================================== #
def main() -> None:
    t0 = time.time()
    os.makedirs(str(OUT_DIR), exist_ok=True)

    s, mod, params, fit, smooth, out_nairu, ma36 = paso1_nairu_naicu()
    (df, reg, alpha, T, reg_ptf, excel_out, png_niveles, png_brechas,
     png_paneles, png_ptf) = paso2_pib_potencial(out_nairu)
    met, corr_alta, corr_baja, n_alta, n_baja, png_filtros = paso3_diagnostico_filtros(df)

    _, corr_dane = construir_resumen_ptf_anual_dane(df, T)
    cp = coef_precios(params, ESPEC_PRODUCCION)
    lineas = list(construir_reporte(df, reg, alpha, T, reg_ptf, corr_dane, CONFIG_PRODUCCION))
    lineas += [
        "",
        "BLOQUE NAIRU/NAICU (MODULO B de este archivo):",
        f"  u_bar (ancla objetiva) = {s.u_bar:.4f}%   icu_bar = {s.icu_bar:.4f}   "
        f"({len(s.episodios_ancla)} episodios de inflacion nucleo estable)",
        f"  lambda_u = {fit['lambda_u']:.4f} (SE={fit['se_lambda_u']:.4f})   "
        f"lambda_icu = {fit['lambda_icu']:.4f} (SE={fit['se_lambda_icu']:.4f})",
        f"  sigma(u*) = {fit['sigma_ustar_trend']:.4f}   sigma(ICU*) = {fit['sigma_icustar_trend']:.4f}   "
        f"sigma(TGP*) = {fit['sigma_tgpstar_trend']:.5f} ({fit['tgp_tendencia']})",
        f"  log-verosimilitud = {fit['loglike_puro']:.2f}",
        f"  beta_p (TGP sobre ciclo) = {params[IX['beta_p']]:.4f}   delta empalme GEIH = "
        f"{params[IX['delta_tgp_geih']]:.4f}   TGP* fin de muestra = {float(smooth['tgpstar'][-1]):.4f}",
        f"  Phillips precios ({ESPEC_PRODUCCION.precios_forma}): gamma_pe = {cp['gamma_pe']:.4f}   "
        f"kappa = {params[IX['kappa']]:.4f}   rho1 = {cp['rho1']:.4f}   rho2 = {cp['rho2']:.4f}   "
        f"persistencia = {cp['persistencia']:.4f}   a0 = {cp['a0']:.4f}",
        f"  Phillips salarios: gamma_w = {params[IX['gamma_w']]:.4f}   gamma_we = {params[IX['gamma_we']]:.4f}   "
        f"kappa_w = {params[IX['kappa_w']]:.4f}",
        f"  Chequeo MA36: {ma36.to_dict(orient='records')}",
        f"  NAIRU fin de muestra = {float(out_nairu['nairu_smooth'].iloc[-1]):.4f}%   "
        f"NAICU fin de muestra = {float(out_nairu['naicu_smooth'].iloc[-1]):.4f}",
        "",
        "Contraste EX POST vs filtros estadisticos (DIAGNOSTICO, NO criterio de seleccion):",
        f"  RMSE medio = {met['rmse_mean']:.4f} pp   corr medio (niveles) = {met['corr_mean']:+.4f}   "
        f"(n={met['n_obs']})",
        f"  corr banda alta (2-8 anhos, ciclo de negocios) = {corr_alta:+.4f}  (n={n_alta})",
        f"  corr banda baja (8-20 anhos)                    = {corr_baja:+.4f}  (n={n_baja})",
    ]
    if T in set(df["date"]):
        brecha_T = float(df.loc[df["date"] == T, "brecha_pot"].iloc[0]) * 100.0
        lineas.append(f"  Brecha del producto fin de muestra ({T.date()}) = {brecha_T:+.4f}%")

    val_out = os.path.join(str(OUT_DIR), "pib_potencial_integrado_v7_validacion.txt")
    with open(val_out, "w", encoding="utf-8") as f:
        f.write("\n".join(str(l) for l in lineas))
    print("\n=== RESUMEN v7 ===")
    for ln in lineas:
        print(ln)

    print(f"\n[v7] tiempo total = {time.time() - t0:.1f}s")
    print("[v7] Archivos escritos:")
    for p in (V7_NAIRU_CSV, OUT_DIR / "pib_potencial_integrado_v7_nairu_naicu.png",
              excel_out, png_niveles, png_brechas, png_paneles, png_ptf, png_filtros, val_out):
        print(f"     {p}")


if __name__ == "__main__":
    main()
