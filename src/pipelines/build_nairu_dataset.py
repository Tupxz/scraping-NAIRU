"""Construye Data_NAIRU.xlsx desde las fuentes procesadas del repo.

Mapeo de columnas
-----------------
Data_NAIRU.xlsx      ← fuente en data/processed/
──────────────────────────────────────────────────────
Unemp_Desest         ← dane_labor_colombia.csv        [unemployment_rate]  (TD desest. PUBLICADA, 1 decimal)
Inf_Goal             ← inflation_banrep_colombia.csv  [Inf_Goal]
Inf_Rate             ← inflation_banrep_colombia.csv  [Inf_Rate]
Core_Inf             ← inflation_banrep_colombia.csv  [Core_Inf]
Brent_Oil_Price      ← brent_colombia.csv             [brent_usd_per_barrel]
TES_Rate_1yr_COP     ← tes_banrep_colombia.csv        [TES_PESOS_1Y]
TES_Rate_1yr_UVR     ← tes_banrep_colombia.csv        [TES_UVR_1Y]
ICU                  ← andi_capacidad_instalada.csv   [capacity_utilization]

El dataset empieza en ``DATA_START`` (2001-01, como el ``Data_NAIRU.xlsx``
del commit a30fb4b que usó la línea base v7: el ICU se desestacionaliza con
STL desde 2001) y conserva las filas con Unemp_Desest, Inf_Rate e ICU no
nulas.  Las demás columnas pueden venir vacías al inicio (TES desde 2002-12):
cada modelo arma su propia muestra con ``dropna``.  El archivo final se guarda en
``data/inputs/Data_NAIRU.xlsx``, que lee el modelo v7 (``src/v7/insumos.py``).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.config import INPUTS_DIR, PROCESSED_DIR

logger = logging.getLogger("nairu_pipeline.build_nairu")

# ── Columnas obligatorias (sin ellas se descarta la fila) ─────────────
REQUIRED_COLS = ["Unemp_Desest", "Inf_Rate", "ICU"]

# Primer mes del dataset.
DATA_START = pd.Timestamp("2001-01-01")


# ═══════════════════════════════════════════════════════════════════════
# Helpers de carga
# ═══════════════════════════════════════════════════════════════════════

def _load(filename: str, date_col: str = "date") -> pd.DataFrame:
    path = PROCESSED_DIR / filename
    df = pd.read_csv(path)
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col).drop_duplicates(subset=[date_col])
    return df.set_index(date_col)


def truncar_en_hueco(icu: pd.DataFrame) -> pd.DataFrame:
    """Deja el ICU solo hasta el último mes contiguo desde su inicio.

    El STL de v7 (``desestacionalizar_icu``) falla si el ICU tiene huecos
    internos, y la serie de la ANDI puede tenerlos al final (p. ej. octubre y
    noviembre, que se cargan a mano, mientras ya entró diciembre).  Los meses
    posteriores al primer hueco no entran a ``Data_NAIRU.xlsx`` hasta que el
    hueco se complete; se emite un warning con los meses faltantes y los
    descartados.
    """
    serie = icu["ICU"].dropna().sort_index()
    if serie.empty:
        return icu.iloc[0:0]
    meses = serie.index.to_period("M")
    esperado = pd.period_range(meses[0], periods=len(meses), freq="M")
    rotos = meses != esperado
    if not rotos.any():
        return icu.loc[serie.index]
    corte = int(rotos.argmax())                  # primera posición fuera de secuencia
    ultimo = meses[corte - 1]
    faltan = [str(p) for p in pd.period_range(ultimo + 1, meses[corte] - 1, freq="M")]
    descartados = [str(p) for p in meses[corte:]]
    logger.warning(
        "[NAIRU-dataset] ICU con hueco: faltan %s. Se trunca en %s y no entran %s "
        "(completar el hueco en data/inputs/icu_historical.csv).",
        faltan, ultimo, descartados,
    )
    return icu.loc[serie.index[:corte]]


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
    icu = truncar_en_hueco(icu)

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

    # ── Filtrar rango: desde DATA_START, con las series clave completas ──
    # Hasta 2026-11 el núcleo empezaba en 2004-01 (más 12 meses "semilla"
    # de TES 2003) porque el ICU del repo empezaba en 2004.  Con el
    # histórico ICU de la ANDI desde 2001 y la TD publicada desde 2001,
    # el dataset arranca en 2001-01 como el de la línea base v7; las filas
    # de 2003 ya traen TES, así que la semilla no hace falta.
    before = len(df)
    keep = df[REQUIRED_COLS].notna().all(axis=1) & (df["Date"] >= DATA_START)
    df = df[keep].sort_values("Date").reset_index(drop=True)
    logger.info(
        "[NAIRU-dataset] Filas antes/después de filtrar (desde %s, con %s): %d → %d",
        DATA_START.strftime("%Y-%m"), "/".join(REQUIRED_COLS), before, len(df),
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
