"""Unión de todas las bases procesadas en un dataset mensual único.

Realiza outer-merge secuencial por ``date`` de los datasets procesados
y genera un CSV consolidado sin columnas ``source`` ni ``download_date``.

Columnas de salida:
    date, year, month,
    unemployment_rate, tgp_rate, pet_thousands,   ← GEIH (mensual)
    ipc_index,                                     ← IPC DANE (mensual)
    Inf_Goal, Inf_Rate, Core_Inf,                  ← Inflación BANREP (mensual)
    brent_usd_per_barrel,                          ← Brent FRED/EIA (mensual)
    capacity_utilization,                          ← ANDI EOIC (mensual)
    TES_UVR_1Y, TES_PESOS_1Y,                     ← TES BANREP (mensual)
    capital_stock_ck, capital_stock_cn,            ← PWT 11.0 (anual → NaN meses)
    human_capital                                  ← PWT 11.0 (anual → NaN meses)
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.config import PROCESSED_DIR
from src.io_utils import load_csv, save_csv

logger = logging.getLogger("nairu_pipeline.merge")

# Mapeo: nombre lógico → (filename, columnas de datos a conservar)
_SOURCES: dict[str, tuple[str, list[str]]] = {
    # ── Mensuales ─────────────────────────────────────────────────
    "unemployment": (
        "dane_labor_colombia.csv",          # TD + TGP + PET (Fase 2)
        ["unemployment_rate", "tgp_rate", "pet_thousands"],
    ),
    "informality": (
        "dane_informality_colombia.csv",    # Tasa informalidad 13 ciudades (trim. móvil)
        ["informality_rate_13c"],
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
        ["capital_stock_ck", "capital_stock_cn", "human_capital"],
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
    "informality_rate_13c",     # GEIH-EISS - Informalidad 13 ciudades (trim. móvil)
    "ipc_index",                # IPC DANE
    "Inf_Goal",                 # Inflación meta BANREP
    "Inf_Rate",                 # Inflación observada BANREP
    "Core_Inf",                 # Inflación núcleo BANREP
    "brent_usd_per_barrel",     # Brent FRED/EIA
    "capacity_utilization",     # ANDI EOIC
    "TES_UVR_1Y",               # TES UVR 1Y BANREP
    "TES_PESOS_1Y",             # TES Pesos 1Y BANREP
    # ── Anuales (NaN en meses sin observación) ────────────
    "capital_stock_ck",         # PWT 11.0 - Stock capital PPP corrientes
    "capital_stock_cn",         # PWT 11.0 - Stock capital precios nac. ctes.
    "human_capital",            # PWT 11.0 - Índice Capital Humano
]


def _load_source(
    name: str,
    filename: str,
    data_cols: list[str],
    processed_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame | None:
    """Carga un CSV procesado y selecciona solo date + columnas de datos."""
    path = processed_dir / filename
    if not path.exists():
        logger.warning("Archivo no encontrado (se omite): %s", path)
        return None

    df = load_csv(path)
    keep = ["date"] + [c for c in data_cols if c in df.columns]
    df = df[keep].copy()
    df["date"] = pd.to_datetime(df["date"])
    logger.info(
        "%-15s cargado: %d filas, cols=%s",
        name, len(df), data_cols,
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
    output_dir: Path = PROCESSED_DIR,
) -> Path:
    """Guarda el dataset unificado como CSV."""
    path = output_dir / MERGED_FILENAME
    save_csv(df, path)
    logger.info("Dataset unificado guardado: %s", path)
    return path


def run_merge_pipeline(
    processed_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Pipeline completo: carga → merge → guardado."""
    df = merge_all_sources(processed_dir)
    save_merged_dataset(df, processed_dir)
    return df
