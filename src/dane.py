"""Módulo de extracción y limpieza de datos del DANE.

Contiene funciones para descargar la tasa de desempleo desde el DANE
(o fuente placeholder) y transformarla al formato estándar del pipeline.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from src.config import (
    DANE_PROCESSED_FILENAME,
    DANE_RAW_FILENAME,
    DANE_UNEMPLOYMENT_URL,
    PROCESSED_COLUMNS,
    PROCESSED_DIR,
    RAW_DANE_DIR,
)
from src.io_utils import save_csv

logger = logging.getLogger("nairu_pipeline.dane")


# ── Extracción ────────────────────────────────────────────────────────


def download_raw_data(
    url: str = DANE_UNEMPLOYMENT_URL,
    output_dir: Path = RAW_DANE_DIR,
    filename: str = DANE_RAW_FILENAME,
    timeout: int = 60,
) -> Path:
    """Descarga datos crudos desde la URL configurada.

    Parameters
    ----------
    url : str
        URL del archivo a descargar.
    output_dir : Path
        Directorio de destino para el archivo raw.
    filename : str
        Nombre del archivo de salida.
    timeout : int
        Timeout en segundos para la petición HTTP.

    Returns
    -------
    Path
        Ruta al archivo descargado.

    Raises
    ------
    requests.HTTPError
        Si la descarga falla.
    """
    logger.info("Descargando datos desde: %s", url)

    response = requests.get(url, timeout=timeout)
    response.raise_for_status()

    output_path = output_dir / filename
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(response.content)

    size_kb = len(response.content) / 1024
    logger.info(
        "Archivo descargado: %s (%.1f KB)", output_path.name, size_kb
    )
    return output_path


# ── Transformación ────────────────────────────────────────────────────


def clean_unemployment_data(raw_path: Path) -> pd.DataFrame:
    """Limpia y estandariza los datos de desempleo descargados.

    Esta función transforma los datos crudos al formato estándar del
    pipeline. Actualmente trabaja con el dataset placeholder (BLS).
    Cuando se integre la fuente real del DANE, la lógica de parsing
    debe adaptarse al formato Excel del GEIH.

    Parameters
    ----------
    raw_path : Path
        Ruta al archivo CSV crudo.

    Returns
    -------
    pd.DataFrame
        DataFrame limpio con las columnas estándar del pipeline.
    """
    logger.info("Cargando datos crudos desde: %s", raw_path)
    df_raw = pd.read_csv(raw_path)

    logger.info(
        "Datos crudos: %d filas, %d columnas", len(df_raw), len(df_raw.columns)
    )
    logger.info("Columnas encontradas: %s", list(df_raw.columns))

    # ── Transformación del dataset placeholder (BLS annual data) ──
    # El CSV tiene columnas: year, unemployed_percent, etc.
    # Son datos anuales de empleo de EE.UU. usados como placeholder.
    # Adaptamos al formato estándar del pipeline.

    df = df_raw.copy()

    # Normalizar nombres de columna
    df.columns = df.columns.str.strip().str.lower()

    # Verificar que tenemos las columnas mínimas necesarias
    required_raw = {"year", "unemployed_percent"}
    available = set(df.columns)
    if not required_raw.issubset(available):
        missing = required_raw - available
        raise ValueError(
            f"Columnas requeridas faltantes en datos crudos: {missing}. "
            f"Columnas disponibles: {list(df.columns)}"
        )

    # Datos anuales: asignar mes = 1 (enero) como referencia
    df["month"] = 1

    # Construir fecha (1 de enero de cada año)
    df["date"] = pd.to_datetime(
        df["year"].astype(int).astype(str) + "-01-01"
    )

    # Renombrar y seleccionar columnas
    df = df.rename(columns={"unemployed_percent": "unemployment_rate"})
    df["source"] = "DANE"  # Se mantiene DANE como fuente objetivo
    df["download_date"] = date.today().isoformat()

    # Asegurar tipos
    df["unemployment_rate"] = pd.to_numeric(
        df["unemployment_rate"], errors="coerce"
    )
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)

    # Eliminar filas con tasa nula
    rows_before = len(df)
    df = df.dropna(subset=["unemployment_rate", "date"]).copy()
    rows_dropped = rows_before - len(df)
    if rows_dropped > 0:
        logger.warning("Se eliminaron %d filas con valores nulos", rows_dropped)

    # Eliminar duplicados por fecha (conservar el primero)
    dupes_before = len(df)
    df = df.drop_duplicates(subset=["date"], keep="first").copy()
    dupes_dropped = dupes_before - len(df)
    if dupes_dropped > 0:
        logger.warning("Se eliminaron %d filas duplicadas por fecha", dupes_dropped)

    # Seleccionar y ordenar columnas finales
    df = df[PROCESSED_COLUMNS].sort_values("date").reset_index(drop=True)

    logger.info("Dataset limpio: %d filas, columnas: %s", len(df), list(df.columns))
    return df


# ── Carga ─────────────────────────────────────────────────────────────


def save_processed_data(
    df: pd.DataFrame,
    output_dir: Path = PROCESSED_DIR,
    filename: str = DANE_PROCESSED_FILENAME,
) -> Path:
    """Guarda el dataset procesado en disco.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame procesado.
    output_dir : Path
        Directorio de destino.
    filename : str
        Nombre del archivo de salida.

    Returns
    -------
    Path
        Ruta al archivo guardado.
    """
    output_path = output_dir / filename
    save_csv(df, output_path)
    logger.info("Dataset procesado guardado en: %s", output_path)
    return output_path
