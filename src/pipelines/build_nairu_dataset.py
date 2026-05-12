"""Construye Data_NAIRU.xlsx desde las fuentes procesadas del repo.

Mapeo de columnas
-----------------
Data_NAIRU.xlsx      ← fuente en data/processed/
──────────────────────────────────────────────────────
Unemp_Desest         ← dane_labor_colombia.csv        [unemployment_rate]
Inf_Goal             ← inflation_banrep_colombia.csv  [Inf_Goal]
Inf_Rate             ← inflation_banrep_colombia.csv  [Inf_Rate]
Core_Inf             ← inflation_banrep_colombia.csv  [Core_Inf]
Brent_Oil_Price      ← brent_colombia.csv             [brent_usd_per_barrel]
TES_Rate_1yr_COP     ← tes_banrep_colombia.csv        [TES_PESOS_1Y]
TES_Rate_1yr_UVR     ← tes_banrep_colombia.csv        [TES_UVR_1Y]
ICU                  ← andi_capacidad_instalada.csv   [capacity_utilization]

El rango temporal del dataset resultante se define por la intersección de
las fuentes con datos completos (todas las filas con al menos Unemp_Desest,
Inf_Rate e ICU no nulas).  El archivo final se guarda en
``data/inputs/Data_NAIRU.xlsx`` para ser leído por ``src/nairu/estimation.py``.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.config import INPUTS_DIR, PROCESSED_DIR

logger = logging.getLogger("nairu_pipeline.build_nairu")

# ── Columnas obligatorias (sin ellas se descarta la fila) ─────────────
REQUIRED_COLS = ["Unemp_Desest", "Inf_Rate", "ICU"]


# ═══════════════════════════════════════════════════════════════════════
# Helpers de carga
# ═══════════════════════════════════════════════════════════════════════

def _load(filename: str, date_col: str = "date") -> pd.DataFrame:
    path = PROCESSED_DIR / filename
    df = pd.read_csv(path)
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col).drop_duplicates(subset=[date_col])
    return df.set_index(date_col)


# ═══════════════════════════════════════════════════════════════════════
# Pipeline principal
# ═══════════════════════════════════════════════════════════════════════

def build_nairu_dataset(
    output_path: Path | None = None,
) -> pd.DataFrame:
    """Construye y guarda ``Data_NAIRU.xlsx``.

    Parameters
    ----------
    output_path:
        Ruta de salida.  Por defecto ``data/inputs/Data_NAIRU.xlsx``.

    Returns
    -------
    pd.DataFrame
        Dataset en formato largo con columnas Year, Month, Date y las
        8 series.
    """
    out = output_path or (INPUTS_DIR / "Data_NAIRU.xlsx")
    logger.info("[NAIRU-dataset] Construyendo Data_NAIRU.xlsx …")

    # ── Cargar cada fuente ────────────────────────────────────────────
    labor = _load("dane_labor_colombia.csv")[["unemployment_rate"]]
    labor.columns = ["Unemp_Desest"]

    inflation = _load("inflation_banrep_colombia.csv")[["Inf_Goal", "Inf_Rate", "Core_Inf"]]

    brent = _load("brent_colombia.csv")[["brent_usd_per_barrel"]]
    brent.columns = ["Brent_Oil_Price"]

    tes = _load("tes_banrep_colombia.csv")[["TES_PESOS_1Y", "TES_UVR_1Y"]]
    tes.columns = ["TES_Rate_1yr_COP", "TES_Rate_1yr_UVR"]

    icu = _load("andi_capacidad_instalada.csv")[["capacity_utilization"]]
    icu.columns = ["ICU"]
    # La EOIC (ANDI) solo tiene datos confiables desde enero 2004.
    # Cualquier fila anterior se descarta para evitar que fechas mal
    # parseadas en el CSV procesado contaminen Data_NAIRU.xlsx.
    icu_start = pd.Timestamp("2004-01-01")
    pre2004 = (icu.index < icu_start).sum()
    if pre2004 > 0:
        logger.warning(
            "[NAIRU-dataset] ICU: descartando %d filas anteriores a 2004-01-01",
            pre2004,
        )
        icu = icu[icu.index >= icu_start]

    # ── Merge por fecha (outer para no perder ningún mes) ─────────────
    df = (
        labor
        .join(inflation, how="outer")
        .join(brent, how="outer")
        .join(tes, how="outer")
        .join(icu, how="outer")
    )
    df = df.sort_index()

    # ── Añadir columnas Year / Month / Date ───────────────────────────
    df.index.name = "Date"
    df = df.reset_index()
    df.insert(0, "Year", df["Date"].dt.year)
    df.insert(1, "Month", df["Date"].dt.month)

    # ── Filtrar rango: mantener solo filas donde las series clave existen
    before = len(df)
    mask = df[REQUIRED_COLS].notna().all(axis=1)
    df = df[mask].reset_index(drop=True)
    logger.info(
        "[NAIRU-dataset] Filas antes/después de filtrar nulos clave: %d → %d",
        before, len(df),
    )

    # ── Guard de fecha mínima: nunca antes de 2004-01-01 ─────────────
    # ICU (ANDI EOIC) es la serie más antigua del modelo y arranca en 2004.
    before2 = len(df)
    df = df[df["Date"] >= "2004-01-01"].reset_index(drop=True)
    if len(df) < before2:
        logger.warning(
            "[NAIRU-dataset] Descartadas %d filas anteriores a 2004-01-01",
            before2 - len(df),
        )

    # ── Guardar ───────────────────────────────────────────────────────
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(out, index=False, sheet_name="Sheet1")
    logger.info(
        "[NAIRU-dataset] Guardado: %s  (%d filas, %s → %s)",
        out.name, len(df),
        df["Date"].iloc[0].strftime("%Y-%m"),
        df["Date"].iloc[-1].strftime("%Y-%m"),
    )
    return df


def run() -> pd.DataFrame:
    """Entry-point para el pipeline principal."""
    return build_nairu_dataset()
