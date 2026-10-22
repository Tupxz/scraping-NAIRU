"""Ejercicio de revisión en pseudo-tiempo-real de los filtros del VIOG.

Pregunta: al añadir UNA observación nueva, ¿cuánto se mueven las estimaciones
de brecha ya publicadas, y a qué distancia del borde derecho?

Los filtros se replican exactamente como los llama src/sources/viog/viog.py
(sobre el NIVEL de Y; la brecha es ln Y − ln tendencia). El Kalman/UCM queda
fuera: necesita el MLE de statsmodels, que no está disponible aquí.
"""
import numpy as np
import pandas as pd
from scipy.signal import butter, filtfilt, fftconvolve
from scipy import sparse
from scipy.sparse.linalg import spsolve

UP = "/mnt/user-data/uploads/scraping-NAIRU"

# ── Puertos literales de statsmodels / del repo ─────────────────────────────

def bk_trend(y, low=6, high=32, K=12):
    """statsmodels.tsa.filters.bkfilter + el envoltorio de viog.py (NaN en extremos)."""
    w1, w2 = 2*np.pi/high, 2*np.pi/low
    bw = np.zeros(2*K+1)
    bw[K] = (w2 - w1)/np.pi
    j = np.arange(1, K+1)
    wj = 1/(np.pi*j) * (np.sin(w2*j) - np.sin(w1*j))
    bw[K+j] = wj
    bw[:K] = wj[::-1]
    bw -= bw.mean()
    cycle = fftconvolve(y, bw, mode="valid")
    trend = np.full(len(y), np.nan)
    trend[K:len(y)-K] = y[K:len(y)-K] - cycle
    return trend


def cf_trend(y, low=6, high=32, drift=False):
    """statsmodels.tsa.filters.cffilter (dos colas), drift=False."""
    x = np.asarray(y, dtype=float).reshape(-1, 1)
    nobs = len(x)
    a, b = 2*np.pi/high, 2*np.pi/low
    if drift:
        x = x - np.arange(nobs)[:, None] * (x[-1] - x[0]) / (nobs - 1)
    J = np.arange(1, nobs+1)
    Bj = (np.sin(b*J) - np.sin(a*J)) / (np.pi*J)
    B0 = (b - a)/np.pi
    Bj = np.r_[B0, Bj][:, None]
    out = np.zeros((nobs, 1))
    for i in range(nobs):
        B = -.5*Bj[0] - np.sum(Bj[1:-i-2])
        A = -Bj[0] - np.sum(Bj[1:-i-2]) - np.sum(Bj[1:i]) - B
        out[i] = (Bj[0]*x[i] + np.dot(Bj[1:-i-2].T, x[i+1:-1])
                  + B*x[-1] + np.dot(Bj[1:i].T, x[1:i][::-1]) + A*x[0])
    return (x.squeeze() - out.squeeze())


def hp_trend(x, lamb=1600.0):
    """statsmodels.tsa.filters.hpfilter."""
    n = len(x)
    I = sparse.eye(n, n)
    K = sparse.dia_matrix((np.repeat([[1.0], [-2.0], [1.0]], n, axis=1),
                           np.array([0, 1, 2])), shape=(n-2, n))
    return spsolve((I + lamb*K.T.dot(K)).tocsc(), x)


def bhp_trend(y, lamb=1600.0, iterations=3):
    """src/production/tfp.py::boosted_hp_filter — HP iterado sobre el ciclo residual."""
    c = np.asarray(y, dtype=float).copy()
    for _ in range(iterations):
        c = c - hp_trend(c, lamb)
    return np.asarray(y, dtype=float) - c


def bw_trend(y, order=8, cutoff=1/16):
    b, a = butter(N=order, Wn=cutoff, btype="low")
    return filtfilt(b, a, np.asarray(y, dtype=float))


FILTROS = {"bk": bk_trend, "cf": cf_trend, "bw": bw_trend, "bhp": bhp_trend}


def gaps(y):
    """Brechas en pp para cada filtro, sobre la muestra y (nivel)."""
    ln = np.log(np.asarray(y, dtype=float))
    return {k: 100*(ln - np.log(f(y))) for k, f in FILTROS.items()}


# ── 1. Datos y validación contra lo publicado ───────────────────────────────

pib = pd.read_excel(f"{UP}/data/inputs/PIB_CO.xlsx")
Y = pib["Value(Billions)"].to_numpy(float)
pub = pd.read_csv(f"{UP}/data/processed/viog_colombia.csv", parse_dates=["date"])
fechas = pub["date"]
assert len(Y) == len(pub) == 129

print("VALIDACIÓN — réplica vs. data/processed/viog_colombia.csv (pp)")
g_full = gaps(Y)
for k in FILTROS:
    d = np.abs(g_full[k] - 100*pub[f"gap_{k}"].to_numpy())
    print(f"  gap_{k:<4} error máx = {np.nanmax(d):.2e} pp")
print()

# ── 2. Añadir UNA observación: ¿cuánto se revisa lo ya estimado? ─────────────
# Para cada vintage T (últimos 40), comparamos la estimación en t hecha con
# y[:T] contra la hecha con y[:T+1], en las T fechas comunes. El "rezago" es
# la distancia desde el final del vintage viejo: rezago 0 = último dato.

VINTAGES = range(len(Y)-40, len(Y))
acc = {k: {} for k in FILTROS}
for T in VINTAGES:
    gv, gw = gaps(Y[:T]), gaps(Y[:T+1])          # viejo (T obs) vs nuevo (T+1)
    for k in FILTROS:
        d = np.abs(gw[k][:T] - gv[k])            # revisión en las T fechas comunes
        for lag in range(T):
            acc[k].setdefault(T-1-lag, []).append(d[lag])   # rezago desde el borde

print("REVISIÓN AL AÑADIR UN TRIMESTRE — media de |Δ brecha| en pp")
print("(promedio sobre los últimos 40 vintages; rezago = trimestres desde el borde)")
print("BK no publica estimación en los últimos 12 trimestres → '(s/e)' = sin estimación\n")
lags = [0, 1, 2, 4, 8, 12, 20, 40]
print(f"{'Filtro':<8}" + "".join(f"{('t-'+str(l)) if l else 't':>9}" for l in lags)
      + f"{'máx':>10}{'p99':>9}")
for k in ["bk", "cf", "bw", "bhp"]:
    fila = ""
    for l in lags:
        vs = np.asarray(acc[k].get(l, [np.nan]), dtype=float)
        v = np.nan if np.all(np.isnan(vs)) else np.nanmean(vs)
        fila += f"{v:>9.3f}" if np.isfinite(v) else f"{'(s/e)':>9}"
    todos = np.concatenate([np.asarray(v, dtype=float) for v in acc[k].values()])
    todos = todos[np.isfinite(todos)]
    print(f"{k:<8}{fila}{todos.max():>10.3f}{np.percentile(todos,99):>9.3f}")
    if k == "bk":
        print(f"{'':8}  → revisión máxima de BK en todo el ejercicio: {todos.max():.2e} pp "
              f"(cero salvo error de redondeo)")

# ── 3. Revisión ex-post acumulada ───────────────────────────────────────────
# Primera estimación publicable para la fecha t (para BK llega 12 trimestres
# tarde; para los demás, en el propio t) frente a la estimación de hoy.
print("\nREVISIÓN EX-POST ACUMULADA — |estimación de hoy − primera estimación| (pp)")
print("(BK: su primera estimación de t sale en t+12; los demás publican en t)\n")
print(f"{'Filtro':<8}{'espera':>8}{'media':>9}{'p90':>9}{'máx':>9}{'2020Q2':>10}")
i_2020 = int(np.where(fechas == pd.Timestamp("2020-04-01"))[0][0])
for k, espera in [("bk", 12), ("cf", 0), ("bw", 0), ("bhp", 0)]:
    difs, d2020 = [], np.nan
    for i in range(48, len(Y)):                 # fecha objetivo i
        T = i + 1 + espera                      # vintage en que se publica por 1ª vez
        if T > len(Y):
            break
        gv = gaps(Y[:T])[k]
        if not np.isfinite(gv[i]):
            continue
        d = abs(g_full[k][i] - gv[i])
        difs.append(d)
        if i == i_2020:
            d2020 = d
    difs = np.asarray(difs)
    s2020 = f"{d2020:>10.2f}" if np.isfinite(d2020) else f"{'(s/e)':>10}"
    print(f"{k:<8}{('t+'+str(espera)) if espera else 't':>8}"
          f"{difs.mean():>9.2f}{np.percentile(difs,90):>9.2f}{difs.max():>9.2f}{s2020}")

# ── 4. Sensibilidad del BHP al número de iteraciones ────────────────────────
# El repo fija iterations=3. Phillips & Shi (2021) proponen elegirlo con un
# criterio de parada (ADF sobre el residuo, o un BIC modificado con los grados
# de libertad efectivos = traza del suavizador). Aquí se muestra qué está en
# juego: el bHP tras m pasadas es S_m = I − (I − S)^m con S = (I + λK'K)^{-1}.
print("\nSENSIBILIDAD DEL BHP AL NÚMERO DE ITERACIONES")
print("(gl efectivos = traza del suavizador acumulado S_m)\n")
n = len(Y)
lnY = np.log(Y)
Ident = np.eye(n)
Kmat = np.zeros((n-2, n))
for i in range(n-2):
    Kmat[i, i], Kmat[i, i+1], Kmat[i, i+2] = 1.0, -2.0, 1.0
S = np.linalg.inv(Ident + 1600.0 * Kmat.T @ Kmat)
P = Ident.copy()
print(f"{'m':>3}{'gl efect.':>11}{'2020Q2':>10}{'2026Q1':>10}{'sd':>8}")
for m in range(1, 11):
    P = P @ (Ident - S)
    g = 100*(lnY - np.log((Ident - P) @ Y))
    if m in (1, 2, 3, 4, 6, 8, 10):
        tag = "  <- iterations=3 del repo" if m == 3 else ""
        print(f"{m:>3}{np.trace(Ident-P):>11.1f}{g[i_2020]:>10.2f}{g[-1]:>10.2f}"
              f"{g.std(ddof=1):>8.2f}{tag}")
