"""Unión de todas las bases procesadas en un dataset mensual único.

Realiza outer-merge secuencial por ``date`` de los datasets procesados
y genera un CSV consolidado sin columnas ``source`` ni ``download_date``.

Columnas de salida:
    date, year, month,
    unemployment_rate, tgp_rate, pet_thousands,   ← GEIH (mensual)
    ipc_index,                                     ← IPC DANE (mensual)
    Inf_Goal, Inf_Rate, Core_Inf,                  ← Inflación BANREP (mensual)
    brent_usd_per_barrel,                          ← Brent FRED/EIA (mensual)
    capacity_utilization,                          ← ANDI EOIC (desde 2004-01)
    TES_UVR_1Y, TES_PESOS_1Y,                     ← TES BANREP (mensual)
    capital_stock_real, depreciation_rate,         ← PWT 11.0 (anual → NaN meses)
    human_capital                                  ← PWT 11.0 (anual → NaN meses)
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.config import FINAL_DIR, PROCESSED_DIR
from src.io_utils import load_csv, save_csv

logger = logging.getLogger("nairu_pipeline.merge")

# Mapeo: nombre lógico → (filename, columnas a conservar).
# Las columnas pueden definirse como:
#   - list[str]            : conservar tal cual (ej. ["ipc_index"])
#   - dict[str, str]       : conservar y renombrar {"gap_viog": "gap_viog_us"}
#                            Útil para distinguir series del mismo concepto
#                            entre dos países (VIOG-USA vs VIOG-Colombia).
ColumnSpec = list[str] | dict[str, str]

_SOURCES: dict[str, tuple[str, ColumnSpec]] = {
    # ── Mensuales ─────────────────────────────────────────────────
    "unemployment": (
        "dane_labor_colombia.csv",          # TD + TGP + PET (Fase 2)
        ["unemployment_rate", "tgp_rate", "pet_thousands"],
    ),
    "ipc": (
        "ipc_colombia.csv",
        ["ipc_index"],
    ),
    "inflation": (
        "inflation_banrep_colombia.csv",
        ["Inf_Goal", "Inf_Rate", "Core_Inf"],
    ),
    "brent": (
        "brent_colombia.csv",
        ["brent_usd_per_barrel"],
    ),
    "andi": (
        "andi_capacidad_instalada.csv",
        ["capacity_utilization"],
    ),
    "tes": (
        "tes_banrep_colombia.csv",
        ["TES_UVR_1Y", "TES_PESOS_1Y"],
    ),
    # ── Anuales (PWT 11.0 — aparecerán con NaN en meses sin dato) ─
    "pwt": (
        "pwt_colombia.csv",
        ["capital_stock_real", "depreciation_rate", "human_capital"],
    ),
    # ── Trimestrales (VIOG — brecha del producto, dos países) ─────
    # Renombramos para distinguir USA de Colombia en el dataset final.
    "viog_us": (
        "viog_usa.csv",
        {"gap_viog": "gap_viog_us", "gap_inv_viog": "gap_inv_viog_us"},
    ),
    "viog_co": (
        "viog_colombia.csv",
        {"gap_viog": "gap_viog_co", "gap_inv_viog": "gap_inv_viog_co"},
    ),
}

MERGED_FILENAME = "nairu_dataset.csv"

MERGED_COLUMNS: list[str] = [
    # Identificadores temporales
    "date",
    "year",
    "month",
    # ── Mensuales ─────────────────────────────────────────
    "unemployment_rate",        # GEIH - TD
    "tgp_rate",                 # GEIH - TGP
    "pet_thousands",            # GEIH - PET (calculada)
    "ipc_index",                # IPC DANE
    "Inf_Goal",                 # Inflación meta BANREP
    "Inf_Rate",                 # Inflación observada BANREP
    "Core_Inf",                 # Inflación núcleo BANREP
    "brent_usd_per_barrel",     # Brent FRED/EIA
    "capacity_utilization",     # ANDI EOIC
    "TES_UVR_1Y",               # TES UVR 1Y BANREP
    "TES_PESOS_1Y",             # TES Pesos 1Y BANREP
    # ── Anuales (NaN en meses sin observación) ────────────
    "capital_stock_real",       # PWT 11.0 - rnna (precios nac. const. 2017)
    "depreciation_rate",        # PWT 11.0 - delta (tasa de depreciación)
    "human_capital",            # PWT 11.0 - hc (índice)
    # ── Trimestrales VIOG-USA (NaN en meses sin observación) ──────
    "gap_viog_us",              # VIOG-USA - Brecha del producto ponderada (VIOG)
    "gap_inv_viog_us",          # VIOG-USA - Brecha ponderada (1/VIOG)
    # ── Trimestrales VIOG-Colombia ─────────────────────────────────
    "gap_viog_co",              # VIOG-CO - Brecha del producto ponderada (VIOG)
    "gap_inv_viog_co",          # VIOG-CO - Brecha ponderada (1/VIOG)
]


def _load_source(
    name: str,
    filename: str,
    data_cols: ColumnSpec,
    processed_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame | None:
    """Carga un CSV procesado y selecciona/renombra columnas de datos.

    ``data_cols`` puede ser:
      - list[str]      : columnas a conservar tal cual.
      - dict[str, str] : ``{nombre_origen: nombre_destino}`` — selecciona
                         las columnas y las renombra (útil para distinguir
                         VIOG-USA de VIOG-Colombia).
    """
    path = processed_dir / filename
    if not path.exists():
        logger.warning("Archivo no encontrado (se omite): %s", path)
        return None

    df = load_csv(path)

    if isinstance(data_cols, dict):
        source_cols = list(data_cols.keys())
        rename_map = data_cols
    else:
        source_cols = list(data_cols)
        rename_map = {}

    keep = ["date"] + [c for c in source_cols if c in df.columns]
    df = df[keep].copy()
    if rename_map:
        df = df.rename(columns=rename_map)
    df["date"] = pd.to_datetime(df["date"])

    output_cols = list(rename_map.values()) if rename_map else source_cols
    logger.info(
        "%-15s cargado: %d filas, cols=%s",
        name, len(df), output_cols,
    )
    return df


def merge_all_sources(
    processed_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Carga y une todas las bases procesadas por fecha.

    Realiza outer-merge secuencial por ``date``.  Las columnas
    ``source`` y ``download_date`` se excluyen; solo se conservan
    las variables de datos + ``date``, ``year``, ``month``.

    Parameters
    ----------
    processed_dir : Path
        Directorio donde están los CSVs procesados.

    Returns
    -------
    pd.DataFrame
        Dataset unificado con todas las variables.

    Raises
    ------
    ValueError
        Si no se encuentra ningún archivo procesado.
    """
    dfs: list[pd.DataFrame] = []

    for name, (filename, data_cols) in _SOURCES.items():
        df = _load_source(name, filename, data_cols, processed_dir)
        if df is not None:
            # Safeguard: ICU (ANDI) solo tiene datos confiables desde 2004-01-01
            if name == "andi":
                before = len(df)
                df = df[df["date"] >= "2004-01-01"].reset_index(drop=True)
                if len(df) < before:
                    logger.warning(
                        "[merge] ICU: %d filas pre-2004 descartadas", before - len(df)
                    )
            dfs.append(df)

    if not dfs:
        raise ValueError(
            "No se encontró ningún archivo procesado en "
            f"{processed_dir}. Ejecute los pipelines primero."
        )

    # Merge secuencial por date
    merged = dfs[0]
    for df_next in dfs[1:]:
        merged = pd.merge(merged, df_next, on="date", how="outer")

    # Ordenar y reconstruir year/month
    merged = merged.sort_values("date").reset_index(drop=True)
    merged["year"] = merged["date"].dt.year
    merged["month"] = merged["date"].dt.month

    # Recortar: conservar solo desde 2004-01-01 en adelante
    # (inicio de ICU/ANDI, la serie más restrictiva del modelo NAIRU)
    merged = merged[merged["date"] >= "2004-01-01"].reset_index(drop=True)

    # Ordenar columnas: date, year, month, luego datos en orden definido
    present_cols = [c for c in MERGED_COLUMNS if c in merged.columns]
    merged = merged[present_cols]

    logger.info(
        "Dataset unificado: %d filas × %d columnas, "
        "rango: %s → %s",
        len(merged), len(merged.columns),
        merged["date"].min(), merged["date"].max(),
    )
    return merged


def save_merged_dataset(
    df: pd.DataFrame,
    output_dir: Path = FINAL_DIR,
) -> Path:
    """Guarda el dataset unificado como CSV en ``data/final/`` por defecto."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / MERGED_FILENAME
    save_csv(df, path)
    logger.info("Dataset unificado guardado: %s", path)
    return path


def run_merge_pipeline(
    processed_dir: Path = PROCESSED_DIR,
    output_dir: Path = FINAL_DIR,
) -> pd.DataFrame:
    """Pipeline completo: carga → merge → guardado.

    Lee los CSVs por fuente desde ``processed_dir`` y escribe el dataset
    consolidado en ``output_dir`` (``data/final/`` por defecto).
    """
    df = merge_all_sources(processed_dir)
    save_merged_dataset(df, output_dir)
    return df
