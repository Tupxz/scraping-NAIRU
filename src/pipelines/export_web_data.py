"""Exporta las salidas del modelo v7 a docs/data/ para el tablero (GitHub Pages).

Lee ``outputs/v7/`` (lo escribe ``python -m src.pipelines.run_v7``) y
``data/processed/viog_colombia.csv``, y escribe en ``docs/data/``:

- ``nairu_monthly.csv``  — NAIRU/NAICU mensual con bandas, brechas y la curva
  de Phillips parcial de v7.
- ``pib_trimestral.csv`` — PIB observado y potencial (función de producción),
  factores, PTF y contribuciones al crecimiento potencial.
- ``viog_trimestral.csv`` — brechas de los 5 filtros estadísticos y compuestos.
- ``meta.json``          — últimos valores para las tarjetas del tablero.

Se conservan los NOMBRES de columna que ya usa ``docs/index.html`` para no
romper el tablero; lo que cambió de significado con v7 se anota abajo.

Cambios frente a la exportación de v3 (2026-11-09):
- Fuente: ``outputs/v7/`` en vez de ``outputs/nairu/`` y ``outputs/pib_potencial/``.
- ``uci_obs`` es el ICU DESESTACIONALIZADO (STL), que es el que usa v7.
- v7 publica bandas al 95 %; las de 90 % se derivan de ellas (misma desviación
  estándar del suavizador, z = 1,645 en vez de 1,960).
- ``pib_bhp_idx``/``brecha_bhp`` pasan a ser la tendencia Hodrick-Prescott simple
  del producto que calcula v7 (referencia estadística), no Boosted-HP.
- Curva de Phillips: v7 estima la brecha de inflación (básica − meta) en NIVEL
  con dos rezagos, expectativas y choque petrolero (ec. 14 del paper). Se exporta
  como ``phillips_v7_parcial`` y ``holgura_v7`` (columnas nuevas, para no mezclar
  con la gráfica de v3, que usaba Δ de la brecha); meta.json lleva κ y su p-valor.
- Fechas trimestrales en convención de INICIO de trimestre (2026-01-01 = 2026T1),
  igual que el resto del tablero; v7 fecha por el mes de cierre.
"""
from __future__ import annotations

import json
import logging
import warnings
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import OUTPUTS_DIR, PROCESSED_DIR

logger = logging.getLogger("nairu_pipeline.export_web")

DOCS_DATA_DIR = Path(__file__).resolve().parents[2] / "docs" / "data"

V7_DIR      = OUTPUTS_DIR / "v7"
NAIRU_CSV   = V7_DIR / "nairu_estimates_v7.csv"
ESTADOS_CSV = V7_DIR / "estados_v7.csv"
PHILLIPS_CSV = V7_DIR / "phillips_parcial_v7.csv"
PIB_CSV     = V7_DIR / "pib_potencial_integrado_v7_series.csv"
RESUMEN_JSON = V7_DIR / "resumen_v7.json"
VIOG_CSV    = PROCESSED_DIR / "viog_colombia.csv"

Z90_SOBRE_Z95 = 1.6448536269514722 / 1.959963984540054

# Serie trimestral de v7 -> nombre en docs/data/pib_trimestral.csv
PIB_EXPORT_COLS = {
    "V_pib":       "pib_cop",      # PIB real desestacionalizado, miles de millones de 2015
    "idx_pib":     "pib_idx",      # PIB anualizado (suma 4T), índice 2005T1 = 100
    "pib_pot":     "pib_pot_idx",  # PIB potencial, mismo índice
    "idx_trend":   "pib_bhp_idx",  # tendencia HP del producto (referencia estadística)
    "brecha_pot":  "brecha_cd",    # fracción -> se pasa a %
    "brecha_hp":   "brecha_bhp",   # fracción -> se pasa a %
    "alpha":       "alpha",
    "ptf":         "ptf_obs",
    "ptf_star":    "ptf_pot",
    "idx_K":       "k_idx",
    "idx_K_star":  "k_pot_idx",
    "idx_L":       "l_idx",
    "idx_L_star":  "l_pot_idx",
    "idx_LH":      "lh_idx",
    "idx_LH_star": "lh_pot_idx",
    "icu":         "uci",
    "naicu":       "naicu_q",
    "td":          "td",
    "nairu":       "nairu_q",
    "tgp":         "tgp",
    "tgp_star":    "tgp_star",
    "pet":         "pet",
}

VIOG_EXPORT_COLS = {
    "date": "fecha", "year": "anio", "quarter": "trimestre",
    "gap_viog": "viog", "gap_inv_viog": "viog_inv", "gap_bhp": "bhp", "gap_cf": "cf",
    "gap_bk": "bk", "gap_bw": "bw", "gap_kalman": "kalman",
}
VIOG_GAP_COLS = ["viog", "viog_inv", "bhp", "cf", "bk", "bw", "kalman"]
VIOG_START = "1994-01-01"


# ── Helpers ───────────────────────────────────────────────────────────────────
def _exigir(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(
            f"Archivo no encontrado: {path}\n"
            "Corre primero el modelo: python -m src.pipelines.run_v7"
        )
    return path


def _round_floats(df: pd.DataFrame, decimals: int = 4) -> pd.DataFrame:
    for col in df.select_dtypes("float64").columns:
        df[col] = df[col].round(decimals)
    return df


def _last_valid(df: pd.DataFrame | None, key: str) -> float | None:
    if df is None or key not in df.columns:
        return None
    s = pd.to_numeric(df[key], errors="coerce").dropna()
    return round(float(s.iloc[-1]), 2) if len(s) else None


def _banda90(centro: pd.Series, hi95: pd.Series) -> tuple[pd.Series, pd.Series]:
    medio = (hi95 - centro) * Z90_SOBRE_Z95
    return centro - medio, centro + medio


# ── Bloques ───────────────────────────────────────────────────────────────────
def construir_mensual() -> pd.DataFrame:
    n = pd.read_csv(_exigir(NAIRU_CSV), parse_dates=["Date"])
    e = pd.read_csv(_exigir(ESTADOS_CSV), parse_dates=["Date"])
    out = pd.DataFrame({
        "fecha": n["Date"],
        "td_obs": n["unemployment_current"],
        "nairu": n["nairu_estimate"],
    })
    out["nairu_lo90"], out["nairu_hi90"] = _banda90(n["nairu_estimate"], n["nairu_ci_upper_95"])
    out["nairu_lo95"] = n["nairu_ci_lower_95"]
    out["nairu_hi95"] = n["nairu_ci_upper_95"]
    out["uci_obs"] = n["icu_current"]
    out["naicu"] = n["naicu_estimate"]
    out["naicu_lo90"], out["naicu_hi90"] = _banda90(n["naicu_estimate"], n["naicu_ci_upper_95"])
    out = out.merge(e[["Date", "y_phillips_precios", "tgp", "tgp_star", "ciclo_laboral_l"]]
                    .rename(columns={"Date": "fecha", "y_phillips_precios": "brecha_inf",
                                     "tgp": "tgp_obs", "ciclo_laboral_l": "ciclo_laboral"}),
                    on="fecha", how="left")
    out["brecha_laboral"] = out["td_obs"] - out["nairu"]
    out["brecha_uci"] = out["uci_obs"] - out["naicu"]
    if PHILLIPS_CSV.exists():
        p = pd.read_csv(PHILLIPS_CSV, parse_dates=["Date"])
        out = out.merge(p[["Date", "phillips_parcial", "holgura"]]
                        .rename(columns={"Date": "fecha", "phillips_parcial": "phillips_v7_parcial",
                                         "holgura": "holgura_v7"}), on="fecha", how="left")
    else:
        warnings.warn(f"{PHILLIPS_CSV} no existe: no se exporta la curva de Phillips parcial.", stacklevel=2)
    out["fecha"] = out["fecha"].dt.strftime("%Y-%m-%d")
    return _round_floats(out)


def construir_trimestral() -> pd.DataFrame:
    d = pd.read_csv(_exigir(PIB_CSV), parse_dates=["date"])
    cols = [c for c in PIB_EXPORT_COLS if c in d.columns]
    faltan = sorted(set(PIB_EXPORT_COLS) - set(cols))
    if faltan:
        logger.warning("[export-web] columnas ausentes en la serie v7, se omiten: %s", faltan)
    pib = d[["date"] + cols].rename(columns=PIB_EXPORT_COLS)
    trimestre = pib["date"].dt.quarter
    pib.insert(0, "fecha", pd.to_datetime({"year": pib["date"].dt.year,
                                          "month": (trimestre - 1) * 3 + 1, "day": 1}).dt.strftime("%Y-%m-%d"))
    pib.insert(1, "anio", pib["date"].dt.year)
    pib.insert(2, "trimestre", trimestre)
    pib = pib.drop(columns="date")
    for c in ("brecha_cd", "brecha_bhp"):
        if c in pib:
            pib[c] = pib[c] * 100.0

    # Contribuciones al crecimiento potencial interanual (pp). crecimiento_potencial
    # se calcula aparte, directo del índice, para poder verificar la identidad.
    if {"k_pot_idx", "lh_pot_idx", "ptf_pot", "alpha", "pib_pot_idx"} <= set(pib.columns):
        alpha = pib["alpha"].astype(float)
        pib["contrib_capital"] = alpha * np.log(pib["k_pot_idx"]).diff(4) * 100
        pib["contrib_trabajo"] = (1 - alpha) * np.log(pib["lh_pot_idx"]).diff(4) * 100
        pib["contrib_ptf"] = np.log(pib["ptf_pot"]).diff(4) * 100
        pib["crecimiento_potencial"] = np.log(pib["pib_pot_idx"]).diff(4) * 100

    # Descomposición exacta de la brecha (ec. 22 del paper), en puntos logarítmicos.
    if {"k_idx", "k_pot_idx", "lh_idx", "lh_pot_idx", "ptf_obs", "ptf_pot", "alpha"} <= set(pib.columns):
        a = pib["alpha"].astype(float)
        pib["brecha_capital"] = a * 100 * np.log(pib["k_idx"] / pib["k_pot_idx"])
        pib["brecha_trabajo"] = (1 - a) * 100 * np.log(pib["lh_idx"] / pib["lh_pot_idx"])
        pib["brecha_ptf"] = 100 * np.log(pib["ptf_obs"] / pib["ptf_pot"])
    return _round_floats(pib)


def construir_viog() -> pd.DataFrame | None:
    if not VIOG_CSV.exists():
        warnings.warn(f"{VIOG_CSV} no encontrado — ejecuta --viog-co primero.", stacklevel=2)
        return None
    viog = pd.read_csv(VIOG_CSV)
    viog = viog[[c for c in VIOG_EXPORT_COLS if c in viog.columns]].rename(columns=VIOG_EXPORT_COLS)
    viog["fecha"] = pd.to_datetime(viog["fecha"])
    viog = viog[viog["fecha"] >= VIOG_START].reset_index(drop=True)
    for col in VIOG_GAP_COLS:
        if col in viog.columns:
            viog[col] = viog[col] * 100.0
    viog["fecha"] = viog["fecha"].dt.strftime("%Y-%m-%d")
    return _round_floats(viog)


# ── Pipeline principal ────────────────────────────────────────────────────────
def export_web_data(docs_data_dir: Path = DOCS_DATA_DIR) -> dict:
    docs_data_dir.mkdir(parents=True, exist_ok=True)
    logger.info("[export-web] Exportando salidas v7 a %s …", docs_data_dir)

    nairu = construir_mensual()
    nairu.to_csv(docs_data_dir / "nairu_monthly.csv", index=False)
    pib = construir_trimestral()
    pib.to_csv(docs_data_dir / "pib_trimestral.csv", index=False)
    viog = construir_viog()
    if viog is not None:
        viog.to_csv(docs_data_dir / "viog_trimestral.csv", index=False)

    resumen = json.loads(RESUMEN_JSON.read_text()) if RESUMEN_JSON.exists() else {}

    def _num(v):
        try:
            v = float(v)
        except (TypeError, ValueError):
            return None
        return round(v, 2) if pd.notna(v) else None

    ult = nairu.iloc[-1]
    meta = {
        "modelo": "v7",
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "last_obs_nairu": str(ult["fecha"])[:10],
        "n_obs_monthly": len(nairu),
        "n_obs_quarterly": len(pib),
        "latest_nairu": _num(ult["nairu"]),
        "latest_td": _num(ult["td_obs"]),
        "latest_naicu": _num(ult["naicu"]),
        "latest_uci": _num(ult["uci_obs"]),
        "latest_brecha_laboral": _num(ult["brecha_laboral"]),
        "latest_brecha_cd": _last_valid(pib, "brecha_cd"),
        "latest_brecha_bhp": _last_valid(pib, "brecha_bhp"),
        "latest_pib_idx": _last_valid(pib, "pib_idx"),
        "latest_pib_pot_idx": _last_valid(pib, "pib_pot_idx"),
        "last_obs_pib": str(pib.loc[pib["brecha_cd"].notna(), "fecha"].max())[:10],
        "picos_bbq": resumen.get("picos_bbq"),
        "u_bar": _num(resumen.get("u_bar")),
    }
    ph = resumen.get("phillips_precios") or {}
    if ph:
        meta["phillips_kappa"] = round(ph["kappa"], 4)
        meta["phillips_kappa_p"] = round(ph["kappa_p"], 4) if ph.get("kappa_p") is not None else None
    if viog is not None and len(viog):
        meta["latest_brecha_viog"] = _last_valid(viog, "viog")
        meta["last_obs_viog"] = str(viog["fecha"].max())[:10]

    (docs_data_dir / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[export-web] meta.json → %s", meta)
    return meta


def run() -> None:
    export_web_data()
