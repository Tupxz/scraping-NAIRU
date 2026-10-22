"""Comparación del filtro C-F de UNA COLA (causal) vs DOS COLAS sobre el PIB de Colombia.

Contexto: apply_filters() pasó a usar por defecto la versión de una cola del
filtro Christiano-Fitzgerald (cfg.cf_one_sided=True en VIOGConfig): trend_cf en
cada t depende SOLO de y_1..y_t, de modo que la brecha es la que un analista
habría visto en tiempo real y no se revisa retroactivamente (crítica de
Orphanides 2001 AER; 2003 JME). Este script cuantifica el costo/beneficio del
cambio sobre PIB_CO.xlsx (VIOG_CO_CONFIG):

  1. Valida que la versión de dos colas reproducida aquí coincide con el
     gap_cf publicado en data/processed/viog_colombia.csv (misma serie, mismo
     statsmodels) — ancla la comparación a lo que hoy muestra la página web.
  2. Grafica tendencia y brecha bajo ambas versiones: el retraso de fase y la
     mayor persistencia del ciclo de una cola cerca del final de la muestra
     son el costo esperado; la ausencia de revisiones ex-post es el beneficio.
  3. Mide la revisión ex-post implícita del filtro de dos colas:
     r(t) = gap_2colas(t) − gap_1cola(t)  (qué tanto reescribe la historia el
     filtro al conocer el futuro; la brecha de una cola en t es, por
     construcción, el valor de borde que el propio filtro de dos colas habría
     entregado en el trimestre t).
  4. Recalcula los compuestos VIOG y 1/VIOG intercambiando SOLO gap_cf
     (usando compute_viog_weights/compute_weighted_gap reales del pipeline y
     los gaps publicados de los otros 4 filtros) para ver el efecto del
     cambio en la brecha compuesta.

Uso:  python scripts/compare_cf_one_sided.py
Salida: outputs/diagnostico_cf/cf_una_cola_vs_dos_colas.png + stats en consola.
No modifica nada del pipeline.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from statsmodels.tsa.filters.cf_filter import cffilter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config import VIOG_CO_CONFIG  # noqa: E402
from src.sources.viog.viog import (  # noqa: E402
    cf_filter_one_sided,
    compute_viog_weights,
    compute_weighted_gap,
)

OUT = ROOT / "outputs" / "diagnostico_cf"

# ── Paleta (dataviz: categórica validada, superficie clara) ───────────────────
C_1COLA = "#2a78d6"   # slot 1 (azul)  — una cola (nueva, protagonista)
C_2COLA = "#eb6834"   # slot 2 (naranja) — dos colas (anterior)
C_SERIE = "#898781"   # tinta muted — PIB observado (contexto)
INK, INK2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, BASE, SURF = "#e1e0d9", "#c3c2b7", "#fcfcfb"


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    cfg = VIOG_CO_CONFIG

    # ── Serie: PIB Colombia (mismo input del pipeline) ────────────────────────
    df = pd.read_excel(ROOT / "data" / "inputs" / cfg.input_filename)
    per = pd.PeriodIndex.from_fields(year=df["Year"], quarter=df["Quarter"], freq="Q")
    y = pd.Series(pd.to_numeric(df["Value(Billions)"]).values, index=per).sort_index()
    ln_y = np.log(y)
    x = y.index.to_timestamp()

    # ── Dos colas (versión anterior del pipeline: niveles, drift=False) ──────
    _, trend_2s = cffilter(y, low=cfg.cf_low, high=cfg.cf_high, drift=False)
    trend_2s = pd.Series(np.asarray(trend_2s), index=y.index)
    gap_2s = (ln_y - np.log(trend_2s)) * 100

    # ── Una cola (nueva versión por defecto) ─────────────────────────────────
    _, trend_1s = cf_filter_one_sided(
        y.to_numpy(), low=cfg.cf_low, high=cfg.cf_high, min_obs=cfg.cf_min_obs
    )
    trend_1s = pd.Series(trend_1s, index=y.index)
    gap_1s = (ln_y - np.log(trend_1s)) * 100
    warmup = cfg.cf_min_obs or 2 * cfg.cf_high

    # ── 1. Validación contra el CSV publicado ────────────────────────────────
    pub = pd.read_csv(ROOT / "data" / "processed" / cfg.processed_filename,
                      parse_dates=["date"])
    pub.index = pd.PeriodIndex(pub["date"], freq="Q")
    diff_pub = (gap_2s / 100 - pub["gap_cf"]).abs().max()
    print(f"1) |gap_cf 2 colas recalculado − publicado| máx = {diff_pub:.3e}"
          f"  ({'OK: misma serie que la web' if diff_pub < 1e-10 else 'REVISAR'})")

    # ── 3. Revisión ex-post implícita del filtro de dos colas ────────────────
    valid = gap_1s.notna()
    rev = (gap_2s - gap_1s)[valid]
    corr = gap_2s[valid].corr(gap_1s[valid])
    # Rezago de fase: lag que maximiza la correlación cruzada (1s vs 2s)
    lags = range(0, 9)
    xcorr = [gap_1s[valid].corr(gap_2s[valid].shift(k)) for k in lags]
    lag_star = int(np.nanargmax(xcorr))
    print(f"2) corr(1 cola, 2 colas) contemporánea = {corr:.3f}; "
          f"máxima = {np.nanmax(xcorr):.3f} con la de 2 colas rezagada "
          f"{lag_star} trimestre(s) → retraso de fase ≈ {lag_star}T")
    print(f"3) revisión ex-post del filtro de 2 colas (gap_2s − gap_1s), "
          f"{valid.sum()} trimestres:")
    print(f"   media |r| = {rev.abs().mean():.2f} pp | máx |r| = "
          f"{rev.abs().max():.2f} pp en {rev.abs().idxmax()} | "
          f"sd(gap_2s) = {gap_2s[valid].std():.2f} pp")
    print(f"4) brecha en el último dato ({y.index[-1]}): "
          f"una cola = {gap_1s.iloc[-1]:+.2f} pp | dos colas = {gap_2s.iloc[-1]:+.2f} pp"
          f"  (en el borde ambos coinciden por construcción: "
          f"diff = {abs(gap_1s.iloc[-1] - gap_2s.iloc[-1]):.2e})")

    # ── 4. Efecto sobre los compuestos VIOG (intercambiando solo gap_cf) ─────
    tags = ["bk", "cf", "bw", "bhp", "kalman"]
    base = pd.DataFrame(index=pub.index)
    base["ln_Y"] = ln_y
    for t in tags:
        base[f"gap_{t}"] = pub[f"gap_{t}"]
        base[f"ln_trend_{t}"] = base["ln_Y"] - base[f"gap_{t}"]

    def _compuesto(df_in: pd.DataFrame) -> pd.DataFrame:
        d = compute_viog_weights(df_in.copy(), gap_vars=tags)
        return compute_weighted_gap(d)

    old = _compuesto(base)
    new_df = base.copy()
    new_df["gap_cf"] = gap_1s / 100
    new_df["ln_trend_cf"] = new_df["ln_Y"] - new_df["gap_cf"]
    new = _compuesto(new_df)

    w_old = old["weight_inv_rev_cf"].iloc[-1]
    w_new = new["weight_inv_rev_cf"].iloc[-1]
    d_viog = ((new["gap_viog"] - old["gap_viog"]).abs() * 100)
    d_inv = ((new["gap_inv_viog"] - old["gap_inv_viog"]).abs() * 100)
    print(f"5) peso 1/VIOG del CF en el último dato: {w_old:.3f} → {w_new:.3f}")
    print(f"   |Δ gap_viog| medio = {d_viog.mean():.3f} pp (máx {d_viog.max():.3f}); "
          f"|Δ gap_inv_viog| medio = {d_inv.mean():.3f} pp (máx {d_inv.max():.3f})")

    # ── Gráfica ──────────────────────────────────────────────────────────────
    plt.rcParams.update({
        "font.family": "sans-serif", "text.color": INK,
        "axes.edgecolor": BASE, "axes.labelcolor": MUTED,
        "xtick.color": MUTED, "ytick.color": MUTED,
        "axes.facecolor": SURF, "figure.facecolor": SURF,
    })
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(10.5, 8.2), sharex=True,
        gridspec_kw={"height_ratios": [1.0, 1.15], "hspace": 0.16},
    )
    covid = pd.Period("2020Q2", freq="Q").to_timestamp()

    # Panel 1 — ln niveles
    ax1.plot(x, ln_y, color=C_SERIE, lw=1.2, label="ln PIB observado")
    ax1.plot(x, np.log(trend_2s), color=C_2COLA, lw=1.8,
             label="Tendencia C-F dos colas (anterior)")
    ax1.plot(x, np.log(trend_1s), color=C_1COLA, lw=1.8,
             label="Tendencia C-F una cola (causal, nueva)")
    ax1.set_title("Filtro Christiano-Fitzgerald: una cola (tiempo real) vs dos colas\n",
                  loc="left", fontsize=13, fontweight="bold", color=INK)
    ax1.text(0, 1.02, "PIB trimestral de Colombia 1994Q1–2026Q1 (DANE, empalme; "
             f"banda {cfg.cf_low}–{cfg.cf_high}T). La versión de una cola arranca tras "
             f"un warm-up de {warmup} trimestres.",
             transform=ax1.transAxes, fontsize=8.5, color=INK2)
    ax1.set_ylabel("ln PIB")

    # Panel 2 — brechas
    ax2.axhline(0, color=BASE, lw=1.0)
    ax2.plot(x, gap_2s, color=C_2COLA, lw=1.8,
             label="Brecha C-F dos colas (vista ex-post, hoy)")
    ax2.plot(x, gap_1s, color=C_1COLA, lw=1.8,
             label="Brecha C-F una cola (vista en tiempo real)")
    ax2.set_ylabel("Brecha del producto (pp, log×100)")
    ax2.set_title("Brecha implícita: la línea azul en t solo usa datos hasta t",
                  loc="left", fontsize=10, color=INK2)

    # Etiquetas directas selectivas (los valores finales coinciden por
    # construcción: el borde del filtro de dos colas ES el de una cola)
    ax2.annotate(f"{gap_1s.iloc[-1]:+.1f} (ambas)", xy=(x[-1], gap_1s.iloc[-1]),
                 xytext=(6, 0), textcoords="offset points",
                 fontsize=8.5, color=C_1COLA, fontweight="bold", va="center")
    # 2019: brecha "descubierta" retroactivamente por el filtro de dos colas
    i19 = y.index.get_loc(pd.Period("2019Q2", freq="Q"))
    ax2.annotate(
        f"2019: ex-post {gap_2s.iloc[i19]:+.1f} pp,\n"
        f"en tiempo real {gap_1s.iloc[i19]:+.1f} pp",
        xy=(x[i19], gap_2s.iloc[i19]), xytext=(-118, 6), textcoords="offset points",
        fontsize=8, color=INK2,
        arrowprops=dict(arrowstyle="-", color=BASE, lw=0.8),
    )
    # 2020Q2: en la crisis, el filtro causal rebaja la tendencia (no ve el rebote)
    i20 = y.index.get_loc(pd.Period("2020Q2", freq="Q"))
    ax2.annotate(
        f"2020Q2: ex-post {gap_2s.iloc[i20]:+.1f} pp,\n"
        f"en tiempo real {gap_1s.iloc[i20]:+.1f} pp",
        xy=(x[i20], gap_2s.iloc[i20]), xytext=(-150, -2), textcoords="offset points",
        fontsize=8, color=INK2,
        arrowprops=dict(arrowstyle="-", color=BASE, lw=0.8),
    )

    for ax in (ax1, ax2):
        ax.grid(axis="y", color=GRID, lw=0.6)
        ax.spines[["top", "right"]].set_visible(False)
        ax.spines[["left", "bottom"]].set_color(BASE)
        ax.axvline(covid, color=GRID, lw=1.0, zorder=0)
        ax.legend(loc="lower left" if ax is ax2 else "upper left",
                  frameon=False, fontsize=8.5)
        ax.margins(x=0.01)
    ax1.annotate("COVID\n2020Q2", xy=(covid, 0.985), xycoords=("data", "axes fraction"),
                 fontsize=7.5, color=MUTED, ha="center", va="top")
    ax2.xaxis.set_major_locator(mdates.YearLocator(4))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    fig.savefig(OUT / "cf_una_cola_vs_dos_colas.png", dpi=150,
                bbox_inches="tight", facecolor=SURF)
    print(f"\nGráfica: {OUT / 'cf_una_cola_vs_dos_colas.png'}")

    # Gemela de datos (accesibilidad / inspección numérica)
    pd.DataFrame({
        "date": x, "ln_pib": ln_y.values,
        "trend_cf_dos_colas": trend_2s.values, "trend_cf_una_cola": trend_1s.values,
        "gap_cf_dos_colas_pp": gap_2s.values, "gap_cf_una_cola_pp": gap_1s.values,
        "revision_expost_pp": (gap_2s - gap_1s).values,
    }).to_csv(OUT / "cf_una_cola_vs_dos_colas.csv", index=False)
    print(f"Datos:   {OUT / 'cf_una_cola_vs_dos_colas.csv'}")


if __name__ == "__main__":
    main()
