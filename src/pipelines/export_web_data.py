"""Exporta los outputs del pipeline a docs/data/ para GitHub Pages.

Lee los CSV de outputs/nairu/ y outputs/pib_potencial/ y genera
versiones limpias (columnas renombradas, sin NaN internos del modelo)
en docs/data/, junto con un meta.json de última actualización.

Uso
---
    python -m src.main --export-web
    # o directamente:
    from src.pipelines.export_web_data import run; run()

Nota (2026-09-10, alineación v3 — ver CHANGELOG.md [0.5.6] y
docs/integracion_v3.md): el motor de PIB potencial pasó de niveles
(PIB/K_usado/K_pot/L_obs/L_pot en pesos o personas) a ÍNDICES base=100
(idx_pib/idx_K/idx_K_star/idx_L/idx_L_star). PIB_EXPORT_COLS se actualizó
para leer los nombres de columna nuevos — las columnas viejas (PIB,
K_usado, K_pot, L_obs, L_pot, UCI, NAICU_q, TD, NAIRU_q) ya no existen en
outputs/pib_potencial/pib_potencial_colombia.csv, así que con el mapa
anterior _read_and_rename las descartaba en silencio (no fallaba, porque
solo selecciona las columnas presentes) y el CSV exportado quedaba con
menos de la mitad de las columnas esperadas, sin avisar.

También se corrigió la fecha trimestral: la columna "date" del CSV del
pipeline usa la convención de v3 (mes de CIERRE del trimestre — p. ej.
"2026-03-01" para 2026-Q1), mientras el resto del repo (NAIRU mensual,
VIOG trimestral) usa el mes de INICIO (convención QS de pandas — p. ej.
"2026-01-01" para 2026-Q1). Usar "date" tal cual habría desalineado el
eje de tiempo del PIB potencial 2 meses respecto a las demás series en
cualquier gráfica combinada. Aquí "fecha" se reconstruye siempre desde
year/quarter (ambas columnas SÍ son confiables) en convención QS-inicio,
igual que el resto del repo — no se toca la columna "date" original del
pipeline (eso es un cambio de superficie más amplio, fuera de alcance de
esta exportación).
"""

from __future__ import annotations

import json
import logging
import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.config import OUTPUTS_DIR, PROCESSED_DIR

logger = logging.getLogger("nairu_pipeline.export_web")

DOCS_DATA_DIR = Path(__file__).resolve().parents[2] / "docs" / "data"

NAIRU_CSV   = OUTPUTS_DIR / "nairu" / "nairu_colombia.csv"
PIB_CSV     = OUTPUTS_DIR / "pib_potencial" / "pib_potencial_colombia.csv"
VIOG_CSV    = PROCESSED_DIR / "viog_colombia.csv"


# ── Columnas exportadas ───────────────────────────────────────────────────────

NAIRU_EXPORT_COLS = {
    "Date":              "fecha",
    "unemployment_current": "td_obs",
    "nairu_estimate":    "nairu",
    "nairu_ci_lower_90": "nairu_lo90",
    "nairu_ci_upper_90": "nairu_hi90",
    "nairu_ci_lower_95": "nairu_lo95",
    "nairu_ci_upper_95": "nairu_hi95",
    "icu_current":       "uci_obs",
    "naicu_estimate":    "naicu",
    "naicu_ci_lower_90": "naicu_lo90",
    "naicu_ci_upper_90": "naicu_hi90",
    "inflation_gap":     "brecha_inf",
    "unemployment_gap":  "brecha_laboral",
    "icu_gap":           "brecha_uci",
}

# Nombres vigentes en outputs/pib_potencial/pib_potencial_colombia.csv desde
# la realineación v3 (2026-09-10). "year"/"quarter" se leen aparte para
# reconstruir "fecha" (ver docstring del módulo) y no se listan aquí.
PIB_EXPORT_COLS = {
    "V_pib":         "pib_cop",       # PIB observado, millones de pesos (serie "original", sin transformar)
    "idx_pib":       "pib_idx",       # PIB observado, índice base=100 (2007-Q3)
    "PIB_pot":       "pib_pot_idx",   # PIB potencial, índice base=100
    "PIB_tend_BHP":  "pib_bhp_idx",   # PIB tendencial (Boosted-HP), índice base=100
    "Brecha_CD":     "brecha_cd",     # % — Cobb-Douglas estructural (idx_pib/PIB_pot - 1)
    "Brecha_BHP":    "brecha_bhp",    # % — estadística (HP), referencia secundaria
    "alpha":         "alpha",         # participación del capital, estilo CBO (EBE/(RA+EBE))
    "A_obs":         "ptf_obs",       # productividad total de los factores, observada
    "A_pot":         "ptf_pot",       # PTF, tendencia estructural (BBQ + OLS)
    "idx_K":         "k_idx",         # capital observado, índice (stock DANE)
    "idx_K_star":    "k_pot_idx",     # capital potencial, índice
    "idx_L":         "l_idx",         # trabajo observado, índice de horas
    "idx_L_star":    "l_pot_idx",     # trabajo potencial, índice de horas (con TGP*)
    "idx_LH":        "lh_idx",        # trabajo observado x capital humano (insumo real de la función Cobb-Douglas)
    "idx_LH_star":   "lh_pot_idx",    # trabajo potencial x capital humano (insumo real de la función Cobb-Douglas)
    "icu":           "uci",           # utilización de capacidad instalada (ANDI)
    "naicu":         "naicu_q",       # NAICU trimestral (promedio del NAICU mensual)
    "td":            "td",            # tasa de desempleo observada
    "nairu":         "nairu_q",       # NAIRU trimestral
    "tgp":           "tgp",           # tasa global de participación observada
    "tgp_star":      "tgp_star",      # TGP potencial/tendencial
    "pet":           "pet",           # población en edad de trabajar, miles
}

# VIOG: brecha del producto por filtros (las brechas vienen en fracción log → ×100 = %)
VIOG_EXPORT_COLS = {
    "date":         "fecha",
    "year":         "anio",
    "quarter":      "trimestre",
    "gap_viog":     "viog",       # compuesto (pesos por varianza de revisión)
    "gap_inv_viog": "viog_inv",   # compuesto (pesos inversos de revisión)
    "gap_bhp":      "bhp",        # Boosted Hodrick-Prescott
    "gap_cf":       "cf",         # Christiano-Fitzgerald
    "gap_bk":       "bk",         # Baxter-King
    "gap_bw":       "bw",         # Butterworth
    "gap_kalman":   "kalman",     # Kalman (UCM)
}
VIOG_GAP_COLS = ["viog", "viog_inv", "bhp", "cf", "bk", "bw", "kalman"]
VIOG_START = "1994-01-01"  # ventana completa del empalme (antes 2005)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _read_and_rename(path: Path, col_map: dict, extra_cols: tuple[str, ...] = ()) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Archivo no encontrado: {path}\n"
            "Ejecuta el pipeline correspondiente antes de --export-web."
        )
    df = pd.read_csv(path)
    wanted = list(extra_cols) + list(col_map.keys())
    cols_present = [c for c in wanted if c in df.columns]
    missing = [c for c in wanted if c not in df.columns]
    if missing:
        logger.warning("[export-web] %s: columnas ausentes, se omiten: %s", path.name, missing)
    df = df[cols_present].rename(columns=col_map)
    return df


def _round_floats(df: pd.DataFrame, decimals: int = 4) -> pd.DataFrame:
    for col in df.select_dtypes("float64").columns:
        df[col] = df[col].round(decimals)
    return df


def _last_valid(df: pd.DataFrame | None, key: str) -> float | None:
    """Último valor no-NaN de la columna (None si no hay ninguno).

    El último trimestre puede tener PIB observado pero aún no potencial
    (p. ej. FBKF rezagado) → ``iloc[-1]`` daría NaN aunque exista un dato
    válido un trimestre atrás. None se serializa como ``null`` (JSON válido).
    """
    if df is None or key not in df.columns:
        return None
    s = pd.to_numeric(df[key], errors="coerce").dropna()
    return round(float(s.iloc[-1]), 2) if len(s) else None


# ── Pipeline principal ────────────────────────────────────────────────────────

def export_web_data(docs_data_dir: Path = DOCS_DATA_DIR) -> None:
    """Lee los outputs y escribe los CSVs y meta.json en docs/data/."""
    docs_data_dir.mkdir(parents=True, exist_ok=True)
    logger.info("[export-web] Exportando a %s …", docs_data_dir)

    # ── 1. NAIRU mensual ──────────────────────────────────────────────
    nairu = _read_and_rename(NAIRU_CSV, NAIRU_EXPORT_COLS)
    nairu = _round_floats(nairu)
    out_nairu = docs_data_dir / "nairu_monthly.csv"
    nairu.to_csv(out_nairu, index=False)
    logger.info("[export-web] %s (%d filas)", out_nairu.name, len(nairu))

    # ── 2. PIB Potencial trimestral ───────────────────────────────────
    if PIB_CSV.exists():
        pib = _read_and_rename(PIB_CSV, PIB_EXPORT_COLS, extra_cols=("year", "quarter"))
        # "fecha" reconstruida desde year/quarter en convención QS-inicio
        # (ver docstring del módulo) — NO se usa la columna "date" original.
        pib.insert(
            0,
            "fecha",
            pd.to_datetime({
                "year": pib["year"].astype(int),
                "month": (pib["quarter"].astype(int) - 1) * 3 + 1,
                "day": 1,
            }).dt.strftime("%Y-%m-%d"),
        )
        pib = pib.rename(columns={"year": "anio", "quarter": "trimestre"})

        # Contribuciones al crecimiento potencial interanual (4 trimestres), en pp.
        # Identidad contable de crecimiento: si contrib_capital+contrib_trabajo+contrib_ptf
        # no suma ~crecimiento_potencial, algo en la exportación está mal — por eso se deja
        # crecimiento_potencial calculado de forma INDEPENDIENTE (directo de pib_pot_idx, no
        # como suma de las 3 contribuciones) para poder comparar ambos.
        import numpy as np
        if {"k_pot_idx", "lh_pot_idx", "ptf_pot", "alpha", "pib_pot_idx"}.issubset(pib.columns):
            ln_k  = np.log(pib["k_pot_idx"].astype(float))
            ln_lh = np.log(pib["lh_pot_idx"].astype(float))
            ln_a  = np.log(pib["ptf_pot"].astype(float))
            ln_pot = np.log(pib["pib_pot_idx"].astype(float))
            alpha = pib["alpha"].astype(float)
            pib["contrib_capital"] = alpha * ln_k.diff(4) * 100
            pib["contrib_trabajo"] = (1 - alpha) * ln_lh.diff(4) * 100
            pib["contrib_ptf"]     = ln_a.diff(4) * 100
            pib["crecimiento_potencial"] = ln_pot.diff(4) * 100

        pib = _round_floats(pib)
        out_pib = docs_data_dir / "pib_trimestral.csv"
        pib.to_csv(out_pib, index=False)
        logger.info("[export-web] %s (%d filas)", out_pib.name, len(pib))
    else:
        warnings.warn(
            f"{PIB_CSV} no encontrado — ejecuta --pib-potencial primero.",
            stacklevel=2,
        )
        pib = None

    # ── 3. VIOG trimestral (5 filtros + compuesto) ────────────────────
    viog = None
    if VIOG_CSV.exists():
        viog = _read_and_rename(VIOG_CSV, VIOG_EXPORT_COLS)
        viog["fecha"] = pd.to_datetime(viog["fecha"])
        viog = viog[viog["fecha"] >= VIOG_START].reset_index(drop=True)
        # Brechas en fracción log → porcentaje
        for col in VIOG_GAP_COLS:
            if col in viog.columns:
                viog[col] = viog[col] * 100.0
        viog["fecha"] = viog["fecha"].dt.strftime("%Y-%m-%d")
        viog = _round_floats(viog)
        out_viog = docs_data_dir / "viog_trimestral.csv"
        viog.to_csv(out_viog, index=False)
        logger.info("[export-web] %s (%d filas)", out_viog.name, len(viog))
    else:
        warnings.warn(
            f"{VIOG_CSV} no encontrado — ejecuta --viog-co primero.",
            stacklevel=2,
        )

    # ── 4. meta.json ──────────────────────────────────────────────────
    def _num(row, key):
        """Valor numérico seguro para JSON: NaN o ausente → None (→ ``null``).

        Evita que ``json.dumps`` escriba el literal ``NaN`` (JSON inválido), que
        rompería el ``JSON.parse`` del tablero por completo.
        """
        try:
            v = float(row.get(key))
        except (TypeError, ValueError):
            return None
        return round(v, 2) if pd.notna(v) else None

    last_nairu = nairu["fecha"].max() if len(nairu) else "—"
    latest = nairu.iloc[-1] if len(nairu) else {}
    meta = {
        "last_updated":    datetime.now().strftime("%Y-%m-%d %H:%M"),
        "last_obs_nairu":  str(last_nairu)[:10],
        "n_obs_monthly":   len(nairu),
        "n_obs_quarterly": len(pib) if pib is not None else 0,
        "latest_nairu":    _num(latest, "nairu"),
        "latest_td":       _num(latest, "td_obs"),
        "latest_naicu":    _num(latest, "naicu"),
        "latest_uci":      _num(latest, "uci_obs"),
        "latest_brecha_laboral": _num(latest, "brecha_laboral"),
    }
    if pib is not None and len(pib):
        meta["latest_brecha_cd"]  = _last_valid(pib, "brecha_cd")
        meta["latest_brecha_bhp"] = _last_valid(pib, "brecha_bhp")
        meta["latest_pib_idx"]      = _last_valid(pib, "pib_idx")
        meta["latest_pib_pot_idx"]  = _last_valid(pib, "pib_pot_idx")
        meta["last_obs_pib"]      = str(pib["fecha"].max())[:10]
    if viog is not None and len(viog):
        meta["latest_brecha_viog"] = _last_valid(viog, "viog")
        meta["last_obs_viog"]      = str(viog["fecha"].max())[:10]

    out_meta = docs_data_dir / "meta.json"
    out_meta.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[export-web] meta.json → %s", meta)

    logger.info("[export-web] ✓ Exportación completa en %s", docs_data_dir)


def run() -> None:
    """Entry-point para el pipeline principal."""
    export_web_data()
