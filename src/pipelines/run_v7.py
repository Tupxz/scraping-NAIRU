"""Corre la especificación v7 completa (NAIRU/NAICU + PIB potencial + contraste).

Uso:
    python -m src.pipelines.run_v7                 # insumos desde el repo
    python -m src.pipelines.run_v7 --referencia    # insumos de la línea base (verificación)

Pasos:
1. ``src.v7.insumos.preparar_raiz`` arma ``data/interim/v7_<modo>/`` con los
   insumos que lee el motor y registra de dónde salió cada uno.
2. Se carga ``src/v7/motor.py`` (copia literal del script de referencia) con
   ``V7_ROOT`` apuntando a esa carpeta y se corren sus pasos 1, 2 y 3 (el 3,
   contraste con VIOG, solo si está el Excel de filtros).
3. Las salidas se copian a ``outputs/v7/`` (o ``outputs/v7_referencia/``) junto
   con ``fuentes_insumos.json`` y ``resumen_v7.json``.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

from src.config import DATA_DIR, OUTPUTS_DIR, PROJECT_ROOT
from src.v7 import insumos

logger = logging.getLogger("nairu_pipeline.v7")

MOTOR_PATH = PROJECT_ROOT / "src" / "v7" / "motor.py"


def cargar_motor(raiz: Path):
    """Importa una instancia NUEVA del motor con la raíz dada (el motor guarda
    estado en variables de módulo, así que no se reutiliza entre corridas)."""
    os.environ["V7_ROOT"] = str(Path(raiz).resolve())
    nombre = f"_motor_v7_{int(time.time() * 1e6)}"
    spec = importlib.util.spec_from_file_location(nombre, MOTOR_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[nombre] = mod
    spec.loader.exec_module(mod)
    return mod


def curva_phillips_parcial(m, s, params, fit, smooth) -> tuple[pd.DataFrame, dict]:
    """Curva de Phillips de precios PARCIAL de v7 (ec. 14 del paper).

    g_t = ρ1 g_{t-1} + ρ2 g_{t-2} + γpe (E_{t-12}π − meta) + θ oil_t + κ h_t + ε_t,
    con g = inflación básica − meta y h = u − u* (en la ventana COVID, h = −ℓ).
    El "parcial" quita de g_t todo salvo el bloque de holgura:
        parcial_t = g_t − ρ1 g_{t-1} − ρ2 g_{t-2} − γpe (E−meta) − θ oil_t − a0 = κ h_t + ε_t,
    y se grafica contra h_t. Es la misma idea de la curva parcial del tablero v3,
    adaptada a la ecuación de v7 (la dependiente ya no es Δg sino g en nivel).
    """
    cp = m.coef_precios(params, m.ESPEC_PRODUCCION)
    ix = m.IX
    kappa = float(params[ix["kappa"]])
    theta = float(params[ix["theta_oil"]])
    se_k = float(fit["se"][ix["kappa"]])
    fechas = pd.to_datetime(s.dates)
    covid = (fechas >= m.COVID_START) & (fechas <= m.COVID_END)
    ustar = np.asarray(smooth["ustar"], dtype=float)
    h = np.where(covid, -np.asarray(smooth["l"], dtype=float), s.unemployment - ustar)
    y = np.asarray(s.y_phillips, dtype=float)
    parcial = (y - cp["a0"] - cp["rho1"] * s.y_phillips_lag1 - cp["rho2"] * s.y_phillips_lag2
               - cp["gamma_pe"] * s.eme_gap_meta - theta * s.oil_shock)
    df = pd.DataFrame({"Date": fechas, "brecha_inflacion": y, "holgura": h,
                       "phillips_parcial": parcial, "en_covid": covid})
    p = float(2 * stats.norm.sf(abs(kappa / se_k))) if se_k > 0 else None
    return df, {"kappa": kappa, "kappa_se": se_k, "kappa_p": p, **{k: float(v) for k, v in cp.items()},
                "theta_oil": theta}


def contraste_viog(df: pd.DataFrame) -> dict | None:
    """Contraste ex post de la brecha de v7 contra los compuestos VIOG y 1/VIOG.

    Misma métrica que el paso 3 del motor (ventana 2006T1–2024T4 sin 2020T2–2021T2;
    RMSE y correlación promedio frente a los dos compuestos; bandas de frecuencia con
    Christiano-Fitzgerald sobre la serie común completa), pero leyendo directamente
    data/processed/viog_colombia.csv. Desde la decisión de 2026-11-10 el VIOG-CO se
    calcula sobre el PIB anualizado (suma de 4 trimestres) y en logaritmos, así que
    sus brechas ya están en la misma base que la de la función de producción.
    """
    from statsmodels.tsa.filters.cf_filter import cffilter
    from src.config import PROCESSED_DIR

    p = PROCESSED_DIR / "viog_colombia.csv"
    if not p.exists():
        return None
    v = pd.read_csv(p, parse_dates=["date"])
    ref = pd.DataFrame({"q": v["date"].dt.to_period("Q"),
                        "viog": 100 * v["gap_viog"], "inv_viog": 100 * v["gap_inv_viog"]})
    ours = pd.DataFrame({"q": pd.to_datetime(df["date"]).dt.to_period("Q"),
                         "propia": 100 * df["brecha_pot"].astype(float)}).dropna()
    m = ours.merge(ref, on="q", how="inner").dropna().set_index("q").sort_index()
    ventana = (m.index >= pd.Period("2006Q1", "Q")) & (m.index <= pd.Period("2024Q4", "Q"))
    covid = (m.index >= pd.Period("2020Q2", "Q")) & (m.index <= pd.Period("2021Q2", "Q"))
    w = m[ventana & ~covid]
    res = {"n": int(len(w)), "muestra_comun": f"{m.index[0]}–{m.index[-1]}"}
    for c in ("viog", "inv_viog"):
        d = w["propia"] - w[c]
        res[f"rmse_{c}"] = float(np.sqrt(np.mean(d ** 2)))
        res[f"corr_{c}"] = float(w["propia"].corr(w[c]))
        res[f"sesgo_{c}"] = float(d.mean())
    res["rmse_medio_pp"] = 0.5 * (res["rmse_viog"] + res["rmse_inv_viog"])
    res["corr_niveles"] = 0.5 * (res["corr_viog"] + res["corr_inv_viog"])
    for nombre, (lo, hi) in (("corr_ciclo_2_8", (8, 32)), ("corr_8_20", (32, 80))):
        b = {c: pd.Series(cffilter(m[c], low=lo, high=hi, drift=True)[0], index=m.index)
             for c in ("propia", "viog", "inv_viog")}
        bb = pd.DataFrame(b)[ventana & ~covid]
        res[nombre] = float(0.5 * (bb["propia"].corr(bb["viog"]) + bb["propia"].corr(bb["inv_viog"])))
    full = m
    res["corr_todo_viog"] = float(full["propia"].corr(full["viog"]))
    res["corr_todo_inv_viog"] = float(full["propia"].corr(full["inv_viog"]))
    return res


def run(modo: str = "repo", con_contraste: bool = True) -> dict:
    t0 = time.time()
    raiz = DATA_DIR / "interim" / f"v7_{modo}"
    destino = OUTPUTS_DIR / ("v7" if modo == "repo" else f"v7_{modo}")

    fuentes = insumos.preparar_raiz(raiz, modo=modo)
    for k, v in fuentes.items():
        (logger.warning if "REFERENCIA" in v or "NO DISPONIBLE" in v else logger.info)(
            "insumo %-60s <- %s", k, v)

    m = cargar_motor(raiz)
    m.OUT_DIR.mkdir(parents=True, exist_ok=True)
    s, mod, params, fit, smooth, out_nairu, ma36 = m.paso1_nairu_naicu()
    phil, phil_coef = curva_phillips_parcial(m, s, params, fit, smooth)
    phil.to_csv(m.OUT_DIR / "phillips_parcial_v7.csv", index=False)
    df, reg, alpha, T, reg_ptf, *_ = m.paso2_pib_potencial(out_nairu)

    resumen = {
        "modo": modo,
        "u_bar": float(s.u_bar),
        "loglike": float(fit["loglike_puro"]),
        "nairu_fin": float(out_nairu["nairu_smooth"].iloc[-1]),
        "naicu_fin": float(out_nairu["naicu_smooth"].iloc[-1]),
        "mes_fin": str(out_nairu["Date"].iloc[-1])[:10],
        "T": str(T.date()),
        "alpha": float(alpha),
        # Picos del ciclo fechados por Bry-Boschan sobre el PIB vigente. Según el
        # paper (sec. 4.2.2) se re-fechan con cada corrida; si el DANE revisa la
        # serie desestacionalizada pueden aparecer o desaparecer picos (p. ej.
        # 2008T3 con la descarga del 2026-11-01). Se registran para notarlo.
        "phillips_precios": phil_coef,
        "picos_bbq": [str(pd.Timestamp(p).date()) for p in m.CONFIG_PRODUCCION.picos_bbq],
    }
    d_T = df.loc[df["date"] == T, "brecha_pot"]
    resumen["brecha_T_pct"] = float(d_T.iloc[0]) * 100.0 if len(d_T) and d_T.notna().all() else None
    bp = df.dropna(subset=["brecha_pot"])
    resumen["ultima_brecha"] = {"fecha": str(bp["date"].iloc[-1].date()),
                                "pct": float(bp["brecha_pot"].iloc[-1]) * 100.0}

    if not con_contraste:
        resumen["contraste_filtros"] = None
    elif modo == "referencia":
        # Réplica del paper: el paso 3 del motor con el Excel de filtros de agosto de 2026.
        if (raiz / m.FILTROS_XLSX.split(os.sep)[-1]).exists():
            met, corr_alta, corr_baja, *_ = m.paso3_diagnostico_filtros(df)
            resumen["contraste_filtros"] = {"rmse_medio_pp": met["rmse_mean"], "corr_niveles": met["corr_mean"],
                                            "corr_ciclo_2_8": corr_alta, "corr_8_20": corr_baja, "n": met["n_obs"]}
        else:
            resumen["contraste_filtros"] = None
            logger.warning("Paso 3 (contraste con VIOG) omitido: no está el Excel de filtros.")
    else:
        resumen["contraste_filtros"] = contraste_viog(df)
        if resumen["contraste_filtros"] is None:
            logger.warning("Contraste con VIOG omitido: falta data/processed/viog_colombia.csv.")
        else:
            c = resumen["contraste_filtros"]
            logger.info("Contraste vs VIOG/1-VIOG: RMSE %.2f pp, corr %.2f, ciclo 2-8 años %.2f, 8-20 años %.2f (n=%d)",
                        c["rmse_medio_pp"], c["corr_niveles"], c["corr_ciclo_2_8"], c["corr_8_20"], c["n"])

    shutil.copytree(m.OUT_DIR, destino, dirs_exist_ok=True)
    resumen["segundos"] = round(time.time() - t0, 1)
    (destino / "fuentes_insumos.json").write_text(json.dumps(fuentes, indent=2, ensure_ascii=False))
    (destino / "resumen_v7.json").write_text(json.dumps(resumen, indent=2, ensure_ascii=False))
    logger.info("Picos Bry-Boschan: %s", ", ".join(resumen["picos_bbq"]) or "ninguno")
    logger.info("v7 listo en %.1fs -> %s | NAIRU %s = %.4f%%, NAICU = %.4f, brecha %s = %s",
                resumen["segundos"], destino, resumen["mes_fin"], resumen["nairu_fin"],
                resumen["naicu_fin"], resumen["ultima_brecha"]["fecha"],
                f"{resumen['ultima_brecha']['pct']:+.3f}%")
    return resumen


def main() -> None:
    ap = argparse.ArgumentParser(description="Especificación v7: NAIRU/NAICU + PIB potencial")
    ap.add_argument("--referencia", action="store_true",
                    help="usar los insumos exactos de la línea base (verificación)")
    ap.add_argument("--sin-contraste", action="store_true", help="omitir el paso 3 (VIOG)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(name)s | %(levelname)s | %(message)s")
    r = run("referencia" if a.referencia else "repo", con_contraste=not a.sin_contraste)
    print(json.dumps(r, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
