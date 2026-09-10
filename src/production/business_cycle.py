"""Fechado del ciclo económico (algoritmo Bry-Boschan trimestral, "BBQ") y
variables de tendencia por tramos ("rampas-meseta" estilo CBO/NBER).

Portado, fórmula por fórmula, de ``legacy/pib_potencial_integrado_v3.py``
(Módulo 2). Se usa en dos puntos del motor de PIB potencial:

    1. ``factors.estimar_tgp_star``     — tendencia de TGP* por tramos.
    2. ``tfp.estimar_ptf_tendencia_cbo`` — tendencia de PTF* por tramos.

Algoritmo BBQ (Harding & Pagan, 2002; Bry & Boschan, 1971)
------------------------------------------------------------
Sobre ``log(PIB)`` trimestral:
    1. Extremos locales en una ventana de ±k trimestres.
    2. Censura de los k trimestres iniciales/finales (sin margen para
       confirmar el extremo).
    3. Alternancia obligatoria pico/valle (si dos picos quedan seguidos, se
       conserva el más alto; si dos valles, el más bajo).
    4. Duración mínima de fase (``BBQ_MIN_PHASE`` trimestres entre giros).
    5. Duración mínima de ciclo completo (``BBQ_MIN_CYCLE`` trimestres entre
       picos consecutivos o valles consecutivos).

Variables de ciclo ("rampas-meseta")
-------------------------------------
Para cada pico detectado, una variable que crece 0.25/trimestre desde el
pico ANTERIOR hasta este pico y luego se congela (meseta) — la base de
tendencia por tramos que usan tanto TGP* como PTF* en la especificación v2
(Cambios 1 y 7 de SPEC_V2.md).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# ── Parámetros del algoritmo BBQ (Harding & Pagan, 2002) ──────────────────────
BBQ_K_WINDOW: int = 2       # ventana de extremos locales
BBQ_MIN_PHASE: int = 2      # duración mínima de fase (trimestres)
BBQ_MIN_CYCLE: int = 5      # duración mínima de ciclo completo (trimestres)
BBQ_INCREMENTO: float = 0.25  # incremento por trimestre de la rampa de ciclo


# ---------------------------------------------------------------------------
# Algoritmo BBQ
# ---------------------------------------------------------------------------

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


def bbq_turning_points(y: np.ndarray) -> list[tuple[int, str]]:
    """Algoritmo BBQ completo sobre una serie ``y`` (típicamente log-PIB).

    Returns
    -------
    list[tuple[int, str]]
        Lista de ``(t, tipo)`` con ``t`` = índice posicional en ``y`` y
        ``tipo`` = ``"P"`` (pico) o ``"T"`` (valle/trough).
    """
    n = len(y)
    cands = _bbq_extremos_locales(y, BBQ_K_WINDOW)
    cands = _bbq_censurar(cands, n, BBQ_K_WINDOW)
    turns = _bbq_alternar(cands, y)
    turns = _bbq_fase_minima(turns, y, BBQ_MIN_PHASE)
    turns = _bbq_ciclo_minimo(turns, y, BBQ_MIN_CYCLE)
    turns = _bbq_fase_minima(turns, y, BBQ_MIN_PHASE)
    turns = _bbq_alternar(turns, y)
    return turns


def detect_bbq_peaks(quarterly: pd.DataFrame, value_col: str = "V_pib",
                      date_col: str = "date") -> list[pd.Timestamp]:
    """Detecta las fechas de PICO del ciclo económico sobre ``log(value_col)``.

    Reemplaza a ``detectar_picos_bbq`` de v3 (que leía el PIB del Excel
    boceto): aquí el PIB ya viene en el DataFrame trimestral del repo.

    Parameters
    ----------
    quarterly : pd.DataFrame
        Requiere columnas ``date`` y ``value_col`` (por defecto ``V_pib``,
        el PIB real desestacionalizado). Se ordena por fecha internamente.
    value_col : str
        Columna sobre la que se detectan los picos (en niveles; se aplica
        log internamente).

    Returns
    -------
    list[pd.Timestamp]
        Fechas de pico, ordenadas ascendentemente.
    """
    d = quarterly.dropna(subset=[value_col]).sort_values(date_col)
    y = np.log(d[value_col].to_numpy(dtype=float))
    turns = bbq_turning_points(y)
    fechas = d[date_col].tolist()
    picos = [fechas[t] for t, tipo in turns if tipo == "P"]
    return sorted(picos)


# ---------------------------------------------------------------------------
# Variables de ciclo ("rampas-meseta")
# ---------------------------------------------------------------------------

def _quarters_elapsed(date: pd.Timestamp, ref: pd.Timestamp) -> float:
    """Trimestres (float) transcurridos de ``ref`` a ``date``."""
    return ((date.year - ref.year) * 12 + (date.month - ref.month)) / 3.0


def construir_variables_ciclo(dates, picos, incremento: float = BBQ_INCREMENTO) -> pd.DataFrame:
    """Construye las variables de ciclo (rampas-meseta) al estilo CBO.

    Para cada pico ``j`` (ordenados ascendentemente), la variable de ciclo
    vale 0 antes del pico, crece ``incremento`` por trimestre desde el pico
    hasta el pico SIGUIENTE (donde se congela en meseta), y para el ÚLTIMO
    pico crece sin cota superior (rampa en curso, el ciclo aún no cerró).

    Parameters
    ----------
    dates : Iterable[pd.Timestamp]
        Fechas (trimestrales) sobre las que evaluar las rampas.
    picos : Iterable[pd.Timestamp]
        Fechas de pico (no necesariamente ordenadas).

    Returns
    -------
    pd.DataFrame
        Indexado por ``dates``, una columna ``ciclo_j`` por cada pico.
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
        out[f"ciclo_{j + 1}"] = ramp
    return out
