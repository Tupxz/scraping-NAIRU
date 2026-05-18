"""Utilidades de entrada/salida para el pipeline.

Funciones para crear directorios, configurar logging y operaciones
genéricas de lectura/escritura de datos.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from src.config import (
    LOGS_DIR,
    LOG_DATE_FORMAT,
    LOG_FILENAME,
    LOG_FORMAT,
    OUTPUTS_DIR,
    PROCESSED_DIR,
    RAW_ANDI_DIR,
    RAW_BANREP_DIR,
    RAW_DANE_DIR,
    RAW_FRED_DIR,
    RAW_PWT_DIR,
)


def ensure_directories() -> None:
    """Crea los directorios necesarios si no existen."""
    for directory in (
        RAW_DANE_DIR, RAW_BANREP_DIR, RAW_FRED_DIR, RAW_ANDI_DIR, RAW_PWT_DIR,
        PROCESSED_DIR, LOGS_DIR, OUTPUTS_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Configura logging para el pipeline.

    Escribe en archivo (logs/pipeline.log) y en consola simultáneamente.

    Parameters
    ----------
    level : int
        Nivel de logging (default: INFO).

    Returns
    -------
    logging.Logger
        Logger raíz configurado.
    """
    ensure_directories()

    logger = logging.getLogger("nairu_pipeline")
    logger.setLevel(level)

    # Evitar duplicar handlers si se llama más de una vez
    if logger.handlers:
        return logger

    # Handler de archivo
    file_handler = logging.FileHandler(
        LOGS_DIR / LOG_FILENAME, encoding="utf-8"
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))

    # Handler de consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def save_csv(df: pd.DataFrame, path: Path) -> Path:
    """Guarda un DataFrame como CSV.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a guardar.
    path : Path
        Ruta de destino.

    Returns
    -------
    Path
        Ruta donde se guardó el archivo.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path


def load_csv(
    path: Path,
    *,
    parse_dates: bool = True,
) -> pd.DataFrame:
    """Lee un CSV procesado y devuelve un DataFrame.

    Por defecto parsea la columna ``date`` a ``datetime64[ns]`` si
    existe. Esto evita que cada consumidor (``merge.py``, modelos,
    notebooks) repita la conversión manualmente.

    Parameters
    ----------
    path : Path
        Ruta al archivo CSV.
    parse_dates : bool
        Si ``True`` (default), intenta parsear la columna ``date``
        como ``datetime64`` durante la carga.

    Returns
    -------
    pd.DataFrame
        DataFrame cargado.

    Raises
    ------
    FileNotFoundError
        Si el archivo no existe.
    """
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {path}")

    if parse_dates:
        # Leer solo la cabecera primero para decidir si "date" existe
        # (más barato que cargar todo el CSV y reintentar).
        header = pd.read_csv(path, nrows=0).columns
        date_cols = ["date"] if "date" in header else None
        return pd.read_csv(path, parse_dates=date_cols)

    return pd.read_csv(path)
