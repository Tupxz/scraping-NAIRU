"""Exporta los outputs del pipeline a docs/data/ para GitHub Pages.

Lee los CSV de outputs/nairu/ y outputs/pib_potencial/ y genera
versiones limpias (columnas renombradas, sin NaN internos del modelo)
en docs/data/, junto con un meta.json de última actualización.

Uso
---
    python -m src.main --export-web
    # o directamente:
    from src.pipelines.export_web_data import run; run()
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

PIB_EXPORT_COLS = {
    "date":          "fecha",
    "year":          "anio",
    "quarter":       "trimestre",
    "PIB":           "pib_obs",
    "PIB_pot":       "pib_pot",
    "PIB_tend_BHP":  "pib_bhp",
    "Brecha_CD":     "brecha_cd",
    "Brecha_BHP":    "brecha_bhp",
    "alpha":         "alpha",
    "A_obs":         "ptf_obs",
    "A_pot":         "ptf_pot",
    "K_usado":       "k_usado",
    "K_pot":         "k_pot",
    "L_obs":         "l_obs",
    "L_pot":         "l_pot",
    "UCI":           "uci",
    "NAICU_q":       "naicu_q",
    "TD":            "td",
    "NAIRU_q":       "nairu_q",
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
    "gap_ref":      "ref",        # Referencia: PIB potencial C-D (NaN pre-2005)
}
VIOG_GAP_COLS = ["viog", "viog_inv", "bhp", "cf", "bk", "bw", "kalman", "ref"]
VIOG_START = "1994-01-01"  # ventana completa del empalme (antes 2005)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _read_and_rename(path: Path, col_map: dict) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Archivo no encontrado: {path}\n"
            "Ejecuta el pipeline correspondiente antes de --export-web."
        )
    df = pd.read_csv(path)
    cols_present = {k: v for k, v in col_map.items() if k in df.columns}
    df = df[list(cols_present.keys())].rename(columns=cols_present)
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
        pib = _read_and_rename(PIB_CSV, PIB_EXPORT_COLS)
        # Índice 2005=100 para el gráfico de niveles
        base = pib.loc[pib["anio"] == 2005, "pib_obs"].mean()
        if base and base > 0:
            for col in ["pib_obs", "pib_pot", "pib_bhp"]:
                if col in pib.columns:
                    pib[f"{col}_idx"] = (pib[col] / base * 100).round(2)
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
