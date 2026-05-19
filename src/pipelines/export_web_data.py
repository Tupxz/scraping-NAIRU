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

from src.config import OUTPUTS_DIR
from src.pipelines.run_pib_potencial import _build_monthly  # reutiliza builder

logger = logging.getLogger("nairu_pipeline.export_web")

DOCS_DATA_DIR = Path(__file__).resolve().parents[2] / "docs" / "data"

NAIRU_CSV   = OUTPUTS_DIR / "nairu" / "nairu_colombia.csv"
PIB_CSV     = OUTPUTS_DIR / "pib_potencial" / "pib_potencial_colombia.csv"
MONTHLY_CSV = OUTPUTS_DIR / "pib_potencial" / "mensual_web.csv"  # generado aquí


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
    "PIB_tend_HP":   "pib_hp",
    "Brecha_CD":     "brecha_cd",
    "Brecha_HP":     "brecha_hp",
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
            for col in ["pib_obs", "pib_pot", "pib_hp"]:
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

    # ── 3. meta.json ──────────────────────────────────────────────────
    last_nairu = nairu["fecha"].max() if len(nairu) else "—"
    latest = nairu.iloc[-1] if len(nairu) else {}
    meta = {
        "last_updated":    datetime.now().strftime("%Y-%m-%d %H:%M"),
        "last_obs_nairu":  str(last_nairu)[:10],
        "n_obs_monthly":   len(nairu),
        "n_obs_quarterly": len(pib) if pib is not None else 0,
        "latest_nairu":    round(float(latest.get("nairu", 0)), 2),
        "latest_td":       round(float(latest.get("td_obs", 0)), 2),
        "latest_naicu":    round(float(latest.get("naicu", 0)), 2),
        "latest_uci":      round(float(latest.get("uci_obs", 0)), 2),
        "latest_brecha_laboral": round(float(latest.get("brecha_laboral", 0)), 2),
    }
    if pib is not None and len(pib):
        last_pib = pib.iloc[-1]
        meta["latest_brecha_cd"] = round(float(last_pib.get("brecha_cd", 0)), 2)
        meta["latest_brecha_hp"] = round(float(last_pib.get("brecha_hp", 0)), 2)
        meta["last_obs_pib"]     = str(pib["fecha"].max())[:10]

    out_meta = docs_data_dir / "meta.json"
    out_meta.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[export-web] meta.json → %s", meta)

    logger.info("[export-web] ✓ Exportación completa en %s", docs_data_dir)


def run() -> None:
    """Entry-point para el pipeline principal."""
    export_web_data()
