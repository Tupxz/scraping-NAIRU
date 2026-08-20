"""Diagnóstico del gap C-F en Colombia: ¿por qué se ve "senoidal"?

Compara 4 variantes del filtro Christiano-Fitzgerald sobre PIB_CO.xlsx:

  A. Notebook exacto ....... niveles, drift=False (lo que corre el pipeline hoy)
  B. drift=True ............ remueve el drift lineal antes de filtrar
  C. En logaritmos ......... filtra ln(PIB); el choque COVID pesa igual en
                             toda la muestra (en niveles pesa más al final)
  D. COVID-ajustado ........ filtra ln(PIB) con 2020Q2-Q4 interpolados
                             (práctica estándar post-2020 en bancos centrales);
                             la brecha se calcula contra el PIB observado

La hipótesis: el "senoidal" es ringing del pasa-banda alrededor del −16.3%
de 2020Q2 (joroba 2019 amplificada, contra-hundimiento 2021, ondas 2022+).
La variante D debería eliminarlo; A-C deberían compartirlo.

Uso:  python scripts/diagnose_cf.py
Salida: outputs/diagnostico_cf/cf_variantes.png + stats en consola.
No modifica nada del pipeline.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from statsmodels.tsa.filters.cf_filter import cffilter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

OUT = ROOT / "outputs" / "diagnostico_cf"
LOW, HIGH = 6, 32  # banda del notebook


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    df = pd.read_excel(ROOT / "data" / "inputs" / "PIB_CO.xlsx")
    per = pd.PeriodIndex.from_fields(year=df["Year"], quarter=df["Quarter"], freq="Q")
    y = pd.Series(pd.to_numeric(df["Value(Billions)"]).values, index=per).sort_index()
    ln_y = np.log(y)
    x = y.index.to_timestamp()

    gaps: dict[str, pd.Series] = {}

    # A. Notebook exacto: niveles, drift=False
    _, trend_a = cffilter(y, low=LOW, high=HIGH, drift=False)
    gaps["A notebook (niveles, drift=False)"] = ln_y - np.log(trend_a)

    # B. Niveles, drift=True
    _, trend_b = cffilter(y, low=LOW, high=HIGH, drift=True)
    gaps["B drift=True"] = ln_y - np.log(trend_b)

    # C. En logaritmos
    cycle_c, _ = cffilter(ln_y, low=LOW, high=HIGH, drift=False)
    gaps["C ln(PIB)"] = pd.Series(np.asarray(cycle_c), index=y.index)

    # D. ln(PIB) con COVID interpolado (2020Q2-2020Q4)
    ln_adj = ln_y.copy()
    covid = (y.index >= pd.Period("2020Q2")) & (y.index <= pd.Period("2020Q4"))
    ln_adj[covid] = np.nan
    ln_adj = ln_adj.interpolate()  # lineal en logs = geométrica en niveles
    _, trend_d = cffilter(ln_adj, low=LOW, high=HIGH, drift=False)
    gaps["D COVID-ajustado"] = ln_y - pd.Series(np.asarray(trend_d), index=y.index)

    # Referencia BHP publicada
    ref = pd.read_csv(ROOT / "data" / "processed" / "viog_colombia.csv",
                      parse_dates=["date"])

    fig, ax = plt.subplots(figsize=(12, 6))
    for label, g in gaps.items():
        ax.plot(x, g, lw=1.8 if label.startswith("A") else 1.2, label=label)
    ax.plot(ref["date"], ref["gap_bhp"], color="gray", ls=":", label="gap_bhp (referencia)")
    ax.axhline(0, color="black", lw=0.8, ls="--")
    ax.axvline(pd.Timestamp("2020-04-01"), color="red", lw=0.8, alpha=0.5)
    ax.set_title("gap C-F Colombia — 4 variantes (el eco alrededor de 2020 es el 'senoidal')")
    ax.set_ylabel("Brecha (log)")
    ax.legend(fontsize="small")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(OUT / "cf_variantes.png", dpi=150)
    plt.close(fig)

    print(f"\n{'variante':<38} {'std 16-19':>9} {'max|.| 16-19':>12} {'std 21-24':>9} {'max|.| 21-24':>12}")
    for label, g in gaps.items():
        s1 = g[(g.index >= pd.Period("2016Q1")) & (g.index <= pd.Period("2019Q4"))]
        s2 = g[(g.index >= pd.Period("2021Q1")) & (g.index <= pd.Period("2024Q4"))]
        print(f"{label:<38} {s1.std():>9.4f} {s1.abs().max():>12.4f} {s2.std():>9.4f} {s2.abs().max():>12.4f}")
    print(f"\nFigura: {OUT / 'cf_variantes.png'}")
    print("Si D aplana las ondas 2017-2024 y A-C las comparten → es ringing por COVID,")
    print("no un error de programación (A es idéntica al notebook y al pipeline).")


if __name__ == "__main__":
    main()
