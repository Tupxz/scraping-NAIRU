"""Diagnóstico paper vs tablero (v7): de dónde sale la diferencia en el PIB potencial.

Uso:
    python scripts/diagnostico_paper_vs_tablero.py [--workers 6]

Corre el motor v7 (``src/v7/motor.py``, sin tocarlo) sobre raíces de insumos
armadas en ``data/interim/v7_atribucion/`` (gitignored), combinando insumo por
insumo la versión del paper (modo ``--referencia``) con la del repo (modo repo).
No escribe en ``outputs/`` ni en ``tests/fixtures/``.

Cada insumo tiene tres versiones:
    ref   la del paper (legacy/v7/datos + Data_NAIRU_2026-05-11_icu2001.xlsx)
    rev   la del repo recortada a la cobertura del paper (misma última fecha):
          aísla revisiones del DANE y cambios de fuente
    repo  la del repo completa (revisiones + datos nuevos)

Escenarios:
    1. Uno a la vez: el paper con un insumo en versión ``repo``.
    2. Shapley en dos etapas sobre 5 bloques (mensuales, PIB, ingreso, GEIH
       trimestral, capital humano): ref -> rev (revisiones) y rev -> repo
       (datos nuevos). La suma de las contribuciones es exactamente la brecha.
    3. Contrafactual ``picos_paper:*``: mismos insumos con los picos Bry-Boschan
       del paper fijos (sin el pico 2008T3 que aparece con el PIB revisado).
    4. ``C:<mes>``: el repo completo con Data_NAIRU cortado en cada mes nuevo.

Resultados en ``data/interim/v7_atribucion/resultados.csv`` y ``shapley.csv``;
el informe es ``docs/diagnostico_paper_vs_tablero.md``.
"""
from __future__ import annotations

import argparse
import contextlib
import itertools
import json
import math
import shutil
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
if str(RAIZ_PROYECTO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROYECTO))

from src.config import DATA_DIR  # noqa: E402

ATR = DATA_DIR / "interim" / "v7_atribucion"
FUENTE = {"ref": ATR / "_fuente_referencia", "repo": ATR / "_fuente_repo"}
TRIMESTRES = ["2025-03-01", "2025-06-01", "2025-09-01", "2025-12-01"]
PICOS_PAPER = ["2019-12-01", "2023-03-01"]   # picos Bry-Boschan con el PIB de 2026T1
MESES_NUEVOS = ["2025-12-01", "2026-01-01", "2026-02-01", "2026-03-01"]  # cortes del bloque mensual

# --------------------------------------------------------------------------- #
#  Insumos: archivo relativo a la raíz y columnas (None = archivo completo)    #
# --------------------------------------------------------------------------- #
DN = "Data_NAIRU.xlsx"
TRIM = "datos/insumos_pib_trimestral.csv"
MENSUALES = {
    "td": (DN, ["Unemp_Desest"]),
    "icu": (DN, ["ICU"]),
    "inflacion": (DN, ["Core_Inf", "Inf_Rate", "Inf_Goal"]),
    "brent": (DN, ["Brent_Oil_Price"]),
    "tes": (DN, ["TES_Rate_1yr_COP", "TES_Rate_1yr_UVR"]),
    "trm": ("trm_cop_usd_promedio_mensual.csv", None),
    "eme": ("datos/eme_expectativas_inflacion_total_mensual_promedio.csv", None),
    "emmet": ("datos/emmet_salarios_v7.csv", None),
    "subempleo": ("datos/geih_subempleo_bruto_mensual.csv", None),
    "tgp": ("datos/geih_tgp_to_td_desest_mensual.csv", None),
    "vacantes": ("datos/banrep_vacantes_prensa_mensual.csv", None),
}
TRIMESTRALES = {
    "pib": (TRIM, ["V_pib", "X_vapg", "BE_inv"]),
    "ingreso": (TRIM, ["BQ_ra", "BS_ebe", "BT_im", "BR_tms"]),
    "geih_trim": ("datos/insumos_pib_mensual.csv", None),
    "hc": ("datos/insumos_pib_hc_anual.csv", None),
}
INSUMOS = {**MENSUALES, **TRIMESTRALES}
# Bloques del Shapley: "mensuales" mueve los 11 insumos mensuales juntos.
BLOQUES = ["mensuales", "pib", "ingreso", "geih_trim", "hc"]


def _expandir(v: dict) -> dict:
    """{bloque|insumo: version} -> {insumo: version} (por defecto 'ref')."""
    out = {k: "ref" for k in INSUMOS}
    for k, ver in v.items():
        for ins in (MENSUALES if k == "mensuales" else [k]):
            out[ins] = ver
    return out


def _firma(v: dict, claves) -> str:
    return "-".join(f"{k}={v[k]}" for k in claves if v[k] != "ref") or "ref"


# --------------------------------------------------------------------------- #
#  Lectura / escritura de insumos                                             #
# --------------------------------------------------------------------------- #
def _leer(raiz: Path, rel: str) -> pd.DataFrame:
    p = raiz / rel
    if rel == DN:
        d = pd.read_excel(p)
        d["date"] = pd.to_datetime(dict(year=d["Year"], month=d["Month"], day=1))
        return d
    d = pd.read_csv(p)
    if "date" in d.columns:
        d["date"] = pd.to_datetime(d["date"])
    return d


def _clave(d: pd.DataFrame) -> str:
    return "date" if "date" in d.columns else "year"


def _version(repo: pd.DataFrame, ref: pd.DataFrame, cols, ver: str) -> pd.DataFrame:
    """Datos del repo en la versión ``ver``:
    'repo' completa; 'rev' sin observaciones posteriores a la última del paper
    (por columna); 'hasta:AAAA-MM-DD' sin observaciones posteriores a esa fecha."""
    if ver == "repo":
        return repo
    k = _clave(ref)
    repo = repo.copy()
    for c in (cols or [None]):
        if ver == "rev":
            ult = ref[k].max() if c is None else ref.loc[ref[c].notna(), k].max()
        elif ver.startswith("hasta:"):
            ult = pd.Timestamp(ver.split(":", 1)[1])
        else:
            raise ValueError(f"versión desconocida: {ver!r}")
        if c is None:
            return repo[repo[k] <= ult]
        repo.loc[repo[k] > ult, c] = np.nan
    return repo


def _combinar(rel: str, cols_por_version: dict) -> pd.DataFrame:
    """Archivo con columnas tomadas de versiones distintas (unión de fechas)."""
    ref = _leer(FUENTE["ref"], rel)
    repo = _leer(FUENTE["repo"], rel)
    k = _clave(ref)
    base = ref.set_index(k)
    fechas = base.index.union(repo.set_index(k).index)
    base = base.reindex(fechas)
    for ver, cols in cols_por_version.items():
        if ver == "ref" or not cols:
            continue
        src = _version(repo, ref, cols, ver)
        base[cols] = src.set_index(k)[cols].reindex(fechas)
    base.index.name = k
    return base.reset_index()


def _escribir(d: pd.DataFrame, raiz: Path, rel: str) -> None:
    p = raiz / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    if rel == DN:
        d = d.dropna(subset=[c for c in d.columns if c not in ("date", "Year", "Month", "Date")], how="all")
        d = d.assign(Year=d["date"].dt.year, Month=d["date"].dt.month, Date=d["date"])
        d.drop(columns="date").to_excel(p, index=False)
    else:
        d = d.copy()
        if "date" in d.columns:
            d["date"] = d["date"].dt.strftime("%Y-%m-%d")
        d.to_csv(p, index=False)


def armar_raiz(raiz: Path, v: dict) -> None:
    """Raíz completa de insumos con la versión ``v[insumo]`` de cada insumo."""
    if raiz.exists():
        shutil.rmtree(raiz)
    shutil.copytree(FUENTE["ref"], raiz, ignore=shutil.ignore_patterns("outputs"))
    (raiz / "outputs").mkdir()
    por_archivo: dict = {}
    for ins, (rel, cols) in INSUMOS.items():
        por_archivo.setdefault(rel, []).append((ins, cols))
    for rel, items in por_archivo.items():
        vers = {v[ins] for ins, _ in items}
        if vers == {"ref"}:
            continue
        if items[0][1] is None:                    # archivo completo
            src = _version(_leer(FUENTE["repo"], rel), _leer(FUENTE["ref"], rel), None, v[items[0][0]])
            _escribir(src, raiz, rel)
        else:                                      # columnas de un archivo compartido
            cols_por_version: dict = {}
            for ins, cols in items:
                cols_por_version.setdefault(v[ins], []).extend(cols)
            _escribir(_combinar(rel, cols_por_version), raiz, rel)


# --------------------------------------------------------------------------- #
#  Corridas del motor (cada una en su proceso)                                #
# --------------------------------------------------------------------------- #
def _motor(raiz: Path):
    from src.pipelines.run_v7 import cargar_motor
    return cargar_motor(raiz)


def correr_mensual(raiz_str: str) -> str:
    """Paso 1 (NAIRU/NAICU). Deja outputs/nairu_estimates_v7.csv en la raíz."""
    raiz = Path(raiz_str)
    with open(raiz / "motor.log", "w") as log, contextlib.redirect_stdout(log):
        m = _motor(raiz)
        m.OUT_DIR.mkdir(parents=True, exist_ok=True)
        s, mod, params, fit, smooth, out, _ = m.paso1_nairu_naicu()
        info = {"u_bar": float(s.u_bar), "loglike": float(fit["loglike_puro"]),
                "mes_fin": str(out["Date"].iloc[-1])[:10]}
    (raiz / "outputs" / "paso1.json").write_text(json.dumps(info))
    return raiz_str


def correr_produccion(raiz_str: str) -> dict:
    """Paso 2 (función de producción) igual que ``motor.paso2_pib_potencial`` hasta
    ``ejecutar_pipeline`` (sin Excel, gráficas ni banda de confianza)."""
    raiz = Path(raiz_str)
    fijos = raiz / "picos_fijos.json"
    with open(raiz / "motor.log", "a") as log, contextlib.redirect_stdout(log):
        m = _motor(raiz)
        if fijos.exists():   # contrafactual: picos dados en vez de re-fechados
            m.CONFIG_PRODUCCION.picos_bbq = [pd.Timestamp(x) for x in json.loads(fijos.read_text())]
        else:
            m.CONFIG_PRODUCCION.picos_bbq = list(m.detectar_picos_bbq())
        m.CONFIG_PRODUCCION.nairu_csv = str(m.V7_NAIRU_CSV)
        df, _, alpha, T, _ = m.ejecutar_pipeline(m.CONFIG_PRODUCCION)
    df.to_csv(raiz / "outputs" / "pib_potencial_integrado_v7_series.csv", index=False)
    return metricas(raiz, df, T, m.CONFIG_PRODUCCION.picos_bbq)


def metricas(raiz: Path, df: pd.DataFrame, T, picos) -> dict:
    d = df.set_index("date")
    g = d["pib_pot"].pct_change(4, fill_method=None) * 100
    n = pd.read_csv(raiz / "outputs" / "nairu_estimates_v7.csv", parse_dates=["Date"]).set_index("Date")
    r = {f"g_{t[:7]}": float(g.get(pd.Timestamp(t), np.nan)) for t in TRIMESTRES}
    r["g_2025"] = float(np.mean([r[f"g_{t[:7]}"] for t in TRIMESTRES]))
    r["nairu_2025-12"] = float(n["nairu_estimate"].get(pd.Timestamp("2025-12-01"), np.nan))
    q4 = pd.Timestamp("2025-12-01")
    for c in ("nairu", "idx_pib", "pib_pot", "tgp_star", "brecha_pot", "alpha"):
        r[f"{c}_2025T4"] = float(d[c].get(q4, np.nan))
    # Contribuciones al crecimiento potencial (log, 4 trimestres, promedio 2025)
    a = float(d["alpha"].dropna().iloc[0])
    lg = np.log(d[["ptf_star", "idx_K_star", "idx_LH_star", "pib_pot"]]).diff(4) * 100
    sel = lg.loc[pd.to_datetime(TRIMESTRES)]
    r["c_ptf"] = float(sel["ptf_star"].mean())
    r["c_capital"] = float((a * sel["idx_K_star"]).mean())
    r["c_trabajo"] = float(((1 - a) * sel["idx_LH_star"]).mean())
    r["T"] = str(pd.Timestamp(T).date())
    r["picos_bbq"] = ",".join(str(pd.Timestamp(p).date())[:7] for p in picos)
    return r


# --------------------------------------------------------------------------- #
#  Orquestación                                                               #
# --------------------------------------------------------------------------- #
def escenarios() -> dict:
    """{nombre: {insumo: version}}."""
    esc = {"paper": _expandir({}), "repo": _expandir({b: "repo" for b in BLOQUES}),
           "repo_rev": _expandir({b: "rev" for b in BLOQUES})}
    for ins in INSUMOS:
        esc[f"uno:{ins}"] = _expandir({ins: "repo"})
    esc["uno:mensuales"] = _expandir({"mensuales": "repo"})
    # Contrafactual: mismos insumos, picos Bry-Boschan del paper (sin 2008T3).
    for k in ("uno:pib", "repo_rev", "repo"):
        esc[f"picos_paper:{k}"] = {**esc[k], "_picos": PICOS_PAPER}
    # Datos nuevos mes a mes: todo el repo, bloque mensual (Data_NAIRU) cortado en cada mes.
    for mes in MESES_NUEVOS:
        esc[f"C:{mes[:7]}"] = {**esc["repo"], **{ins: f"hasta:{mes}" for ins, (rel, _) in MENSUALES.items()
                                                  if rel == DN}}
    # Shapley etapa A (revisiones): bloques en S pasan de ref a rev.
    # Etapa B (datos nuevos): todos en rev; los de S pasan a repo.
    for r in range(len(BLOQUES) + 1):
        for S in itertools.combinations(BLOQUES, r):
            esc[f"A:{'+'.join(S) or '-'}"] = _expandir({b: "rev" for b in S})
            esc[f"B:{'+'.join(S) or '-'}"] = _expandir(
                {b: ("repo" if b in S else "rev") for b in BLOQUES})
    return esc


def shapley(res: pd.DataFrame, etapa: str, col: str) -> dict:
    n = len(BLOQUES)
    val = {frozenset(k.split(":")[1].split("+")) - {"-"}: float(res.loc[k, col])
           for k in res.index if k.startswith(f"{etapa}:")}
    phi = {}
    for b in BLOQUES:
        otros = [x for x in BLOQUES if x != b]
        tot = 0.0
        for r in range(n):
            for S in itertools.combinations(otros, r):
                w = math.factorial(r) * math.factorial(n - r - 1) / math.factorial(n)
                tot += w * (val[frozenset(S) | {b}] - val[frozenset(S)])
        phi[b] = tot
    return phi


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--workers", type=int, default=6)
    a = ap.parse_args()

    from src.v7 import insumos
    ATR.mkdir(parents=True, exist_ok=True)
    for modo, p in (("referencia", FUENTE["ref"]), ("repo", FUENTE["repo"])):
        if p.exists():
            shutil.rmtree(p)
        insumos.preparar_raiz(p, modo=modo)

    esc = escenarios()
    claves_m = list(MENSUALES)
    # Paso 1: una corrida por combinación distinta de insumos mensuales.
    mens = {}
    for v in esc.values():
        f = _firma(v, claves_m).replace(":", "_")
        mens.setdefault(f, {k: v[k] for k in claves_m})
    print(f"{len(esc)} escenarios; {len(mens)} corridas del bloque mensual")
    raices_m = {}
    for f, vm in mens.items():
        raiz = ATR / "mensual" / f
        armar_raiz(raiz, _expandir(vm))
        raices_m[f] = raiz
    with ProcessPoolExecutor(a.workers) as ex:
        list(ex.map(correr_mensual, [str(r) for r in raices_m.values()]))

    # Paso 2: una corrida de producción por escenario, con la NAIRU de su firma mensual.
    raices = {}
    for nombre, v in esc.items():
        raiz = ATR / "escenarios" / nombre.replace(":", "__").replace("+", "_")
        armar_raiz(raiz, v)
        src = raices_m[_firma(v, claves_m).replace(":", "_")] / "outputs"
        for f in ("nairu_estimates_v7.csv", "paso1.json"):
            shutil.copy2(src / f, raiz / "outputs" / f)
        if "_picos" in v:
            (raiz / "picos_fijos.json").write_text(json.dumps(v["_picos"]))
        raices[nombre] = raiz
    with ProcessPoolExecutor(a.workers) as ex:
        res = dict(zip(raices, ex.map(correr_produccion, [str(r) for r in raices.values()])))
    res = pd.DataFrame(res).T
    res.index.name = "escenario"
    res.to_csv(ATR / "resultados.csv")

    filas = []
    for col in ("g_2025-03", "g_2025-06", "g_2025-09", "g_2025-12", "g_2025", "nairu_2025-12"):
        for etapa in ("A", "B"):
            for b, x in shapley(res, etapa, col).items():
                filas.append({"metrica": col, "etapa": etapa, "bloque": b, "contribucion": x})
    pd.DataFrame(filas).to_csv(ATR / "shapley.csv", index=False)
    print(res[["g_2025-03", "g_2025-06", "g_2025-09", "g_2025-12", "nairu_2025-12", "T", "picos_bbq"]]
          .loc[[k for k in res.index if not k.startswith(("A:", "B:"))]].to_string())


if __name__ == "__main__":
    main()
