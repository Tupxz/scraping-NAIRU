"""Extracción de precios del crudo Brent desde FRED/EIA.

Pipeline de 3 capas:

1. **DESCARGA** — Obtiene el CSV de FRED (serie POILBREUSDM) vía urllib
   con fallback a curl si la conexión falla.
2. **PARSING**  — Lee el CSV crudo, filtra valores faltantes (``"."``),
   convierte a float y genera DataFrame con esquema estándar.
3. **AGREGACIÓN** — Produce dataset mensual (promedio por mes) compatible
   con el resto de los datasets del proyecto.

Fuente
------
FRED Graph CSV endpoint:
    ``https://fred.stlouisfed.org/graph/fredgraph.csv?id=POILBREUSDM&cosd=...&coed=...``

El CSV tiene columnas ``DATE`` (o ``observation_date``) y ``POILBREUSDM``.
"""

from __future__ import annotations

import csv
import io
import logging
import shutil
import subprocess
from datetime import date, datetime
from http.client import RemoteDisconnected
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from src.config import (
    BRENT_CONFIG,
    BRENT_PROCESSED_COLUMNS,
    BrentConfig,
    PROCESSED_DIR,
    RAW_FRED_DIR,
)
from src.io_utils import save_csv

logger = logging.getLogger("nairu_pipeline.fred.brent")


# ═══════════════════════════════════════════════════════════════════════
# 1. DESCARGA
# ═══════════════════════════════════════════════════════════════════════


def build_source_url(
    config: BrentConfig = BRENT_CONFIG,
    end_date: date | None = None,
) -> str:
    """Construye la URL de descarga del CSV de FRED.

    Parameters
    ----------
    config : BrentConfig
        Configuración de la fuente Brent.
    end_date : date | None
        Fecha de fin; si ``None``, usa ``date.today()``.

    Returns
    -------
    str
        URL completa con query-string.
    """
    if end_date is None:
        end_date = date.today()
    params = {
        "id": config.series_id,
        "cosd": config.start_date,
        "coed": end_date.isoformat(),
    }
    return f"{config.source_base_url}?{urlencode(params)}"


def fetch_csv_text(
    source_url: str,
    config: BrentConfig = BRENT_CONFIG,
) -> str:
    """Descarga el CSV como texto.

    Intenta 3 estrategias en orden:
    1. ``curl`` con ``--http1.1`` (FRED presenta problemas con HTTP/2).
    2. ``requests`` (usa los certificados del sistema vía certifi).
    3. ``urllib`` (stdlib, puede fallar por SSL en macOS).

    Parameters
    ----------
    source_url : str
        URL del CSV a descargar.
    config : BrentConfig
        Configuración para headers y timeout.

    Returns
    -------
    str
        Contenido del CSV en texto plano.
    """
    # 1. Intentar con curl (HTTP/1.1 — FRED puede colgar en HTTP/2)
    curl_path = shutil.which("curl")
    if curl_path:
        try:
            result = subprocess.run(
                [
                    curl_path,
                    "--silent",
                    "--show-error",
                    "--location",
                    "--fail",
                    "--http1.1",
                    "--max-time", str(config.timeout),
                    source_url,
                ],
                capture_output=True,
                check=True,
                encoding="utf-8",
                text=True,
            )
            logger.info("CSV descargado vía curl: %d bytes", len(result.stdout))
            return result.stdout
        except subprocess.CalledProcessError as exc:
            logger.warning(
                "curl falló (exit %d: %s), intentando requests...",
                exc.returncode, exc.stderr.strip() or "sin detalle",
            )
    else:
        logger.info("curl no disponible, intentando requests...")

    # 2. Intentar con requests
    try:
        import requests as req_lib
        resp = req_lib.get(
            source_url,
            headers=config.http_headers,
            timeout=config.timeout,
        )
        resp.raise_for_status()
        logger.info("CSV descargado vía requests: %d bytes", len(resp.text))
        return resp.text
    except Exception as req_exc:
        logger.warning(
            "requests falló (%s), intentando urllib...", req_exc
        )

    # 3. Intentar con urllib (última opción)
    request = Request(
        source_url,
        headers=config.http_headers,
    )

    try:
        with urlopen(request, timeout=config.timeout) as response:
            text = response.read().decode("utf-8")
            logger.info("CSV descargado vía urllib: %d bytes", len(text))
            return text
    except (HTTPError, URLError, RemoteDisconnected, ConnectionError) as exc:
        raise RuntimeError(
            "No se pudo descargar la serie Brent con ningún método "
            f"(curl, requests, urllib). Último error: {exc}"
        ) from exc


# ═══════════════════════════════════════════════════════════════════════
# 2. PARSING
# ═══════════════════════════════════════════════════════════════════════


def parse_fred_csv(
    csv_text: str,
    config: BrentConfig = BRENT_CONFIG,
) -> pd.DataFrame:
    """Parsea el CSV de FRED y devuelve un DataFrame limpio.

    El CSV tiene dos columnas: ``DATE`` (o ``observation_date``) y
    ``{SERIES_ID}``.  Valores faltantes se representan como ``"."``.

    Parameters
    ----------
    csv_text : str
        Contenido CSV descargado de FRED.
    config : BrentConfig
        Configuración (series_id para identificar la columna de valores).

    Returns
    -------
    pd.DataFrame
        Columnas: ``date`` (datetime), ``brent_usd_per_barrel`` (float).

    Raises
    ------
    ValueError
        Si el esquema del CSV no es el esperado.
    """
    reader = csv.DictReader(csv_text.strip().splitlines())
    fieldnames = reader.fieldnames or []

    # Detectar campo de fecha (FRED usa "DATE" o "observation_date")
    if "DATE" in fieldnames:
        date_field = "DATE"
    elif "observation_date" in fieldnames:
        date_field = "observation_date"
    else:
        raise ValueError(
            f"Esquema CSV inesperado: no se encontró campo de fecha. "
            f"Campos: {fieldnames}"
        )

    if config.series_id not in fieldnames:
        raise ValueError(
            f"Esquema CSV inesperado: no se encontró columna "
            f"'{config.series_id}'. Campos: {fieldnames}"
        )

    records: list[dict] = []
    for row in reader:
        value_str = row[config.series_id].strip()
        # FRED usa "." para valores faltantes
        if not value_str or value_str == ".":
            continue
        try:
            value = float(value_str)
        except ValueError:
            logger.warning(
                "Valor no numérico ignorado: '%s' en fecha %s",
                value_str, row.get(date_field, "?"),
            )
            continue

        obs_date = datetime.strptime(row[date_field], "%Y-%m-%d").date()
        records.append({
            "date": obs_date,
            "brent_usd_per_barrel": value,
        })

    if not records:
        raise ValueError("No se obtuvieron observaciones válidas del CSV de FRED.")

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    logger.info(
        "CSV parseado: %d observaciones, rango: %s → %s",
        len(df), df["date"].min().date(), df["date"].max().date(),
    )
    return df


def filter_by_date(
    df: pd.DataFrame,
    start_date: str | date = "2001-01-01",
    end_date: date | None = None,
) -> pd.DataFrame:
    """Filtra observaciones por rango de fechas.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columna ``date``.
    start_date : str | date
        Fecha de inicio (inclusive).
    end_date : date | None
        Fecha de fin (inclusive); ``None`` = sin límite.

    Returns
    -------
    pd.DataFrame
        DataFrame filtrado.
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    mask = df["date"].dt.date >= start_date
    if end_date is not None:
        mask &= df["date"].dt.date <= end_date
    return df[mask].copy().reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════
# 3. AGREGACIÓN MENSUAL
# ═══════════════════════════════════════════════════════════════════════


def aggregate_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega datos (potencialmente diarios) a promedio mensual.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columnas ``date`` y ``brent_usd_per_barrel``.

    Returns
    -------
    pd.DataFrame
        DataFrame con una fila por mes, columnas:
        ``date, year, month, brent_usd_per_barrel``.
    """
    df = df.copy()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    monthly = (
        df.groupby(["year", "month"], as_index=False)["brent_usd_per_barrel"]
        .mean()
        .round(2)
    )

    # Reconstruir date como primer día del mes
    monthly["date"] = pd.to_datetime(
        monthly[["year", "month"]].assign(day=1)
    )

    monthly = monthly[["date", "year", "month", "brent_usd_per_barrel"]]
    monthly = monthly.sort_values("date").reset_index(drop=True)

    logger.info(
        "Agregación mensual: %d meses, rango: %s → %s",
        len(monthly), monthly["date"].min().date(), monthly["date"].max().date(),
    )
    return monthly


# ═══════════════════════════════════════════════════════════════════════
# TRANSFORMACIÓN
# ═══════════════════════════════════════════════════════════════════════


def clean_brent_data(
    df_monthly: pd.DataFrame,
) -> pd.DataFrame:
    """Agrega metadatos y ajusta esquema final.

    Parameters
    ----------
    df_monthly : pd.DataFrame
        DataFrame mensual con ``date, year, month, brent_usd_per_barrel``.

    Returns
    -------
    pd.DataFrame
        DataFrame con columnas estándar del proyecto.
    """
    df = df_monthly.copy()
    df["source"] = "FRED_EIA"
    df["download_date"] = date.today().isoformat()

    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)

    # Eliminar duplicados por fecha (precaución)
    dupes_before = len(df)
    df = df.drop_duplicates(subset=["date"], keep="first").copy()
    dupes_dropped = dupes_before - len(df)
    if dupes_dropped > 0:
        logger.warning("Se eliminaron %d filas duplicadas por fecha", dupes_dropped)

    # Ordenar columnas según estándar
    df = df[BRENT_PROCESSED_COLUMNS].sort_values("date").reset_index(drop=True)

    logger.info(
        "Brent limpio: %d filas, rango: %s → %s",
        len(df), df["date"].min(), df["date"].max(),
    )
    return df


# ═══════════════════════════════════════════════════════════════════════
# CARGA
# ═══════════════════════════════════════════════════════════════════════


def save_raw_csv(
    csv_text: str,
    output_dir: Path = RAW_FRED_DIR,
    config: BrentConfig = BRENT_CONFIG,
) -> Path:
    """Guarda el CSV crudo descargado para auditoría.

    Parameters
    ----------
    csv_text : str
        Texto CSV tal como se descargó de FRED.
    output_dir : Path
        Directorio de destino.
    config : BrentConfig
        Configuración (nombre del archivo).

    Returns
    -------
    Path
        Ruta del archivo guardado.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / config.raw_csv_filename
    path.write_text(csv_text, encoding="utf-8")
    logger.info("CSV crudo guardado: %s", path)
    return path


def save_processed_data(
    df: pd.DataFrame,
    output_dir: Path = PROCESSED_DIR,
    config: BrentConfig = BRENT_CONFIG,
) -> Path:
    """Guarda el dataset Brent procesado en disco.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame procesado.
    output_dir : Path
        Directorio de destino.
    config : BrentConfig
        Configuración (nombre del archivo).

    Returns
    -------
    Path
        Ruta del archivo guardado.
    """
    output_path = output_dir / config.processed_filename
    save_csv(df, output_path)
    logger.info("Dataset procesado guardado: %s", output_path)
    return output_path


# ═══════════════════════════════════════════════════════════════════════
# ORQUESTACIÓN
# ═══════════════════════════════════════════════════════════════════════


def run_brent_pipeline(
    config: BrentConfig = BRENT_CONFIG,
    output_dir: Path = PROCESSED_DIR,
    raw_dir: Path = RAW_FRED_DIR,
) -> pd.DataFrame:
    """Pipeline completo: descarga → parsing → agregación → guardado.

    Parameters
    ----------
    config : BrentConfig
        Configuración de la fuente.
    output_dir : Path
        Directorio para el CSV procesado.
    raw_dir : Path
        Directorio para el CSV crudo.

    Returns
    -------
    pd.DataFrame
        Dataset Brent procesado (mensual).
    """
    # 1. Construir URL y descargar
    source_url = build_source_url(config)
    logger.info("Descargando Brent desde: %s", source_url)
    csv_text = fetch_csv_text(source_url, config)

    # 2. Guardar CSV crudo
    save_raw_csv(csv_text, output_dir=raw_dir, config=config)

    # 3. Parsear
    df_raw = parse_fred_csv(csv_text, config)

    # 4. Filtrar por fechas
    df_filtered = filter_by_date(df_raw, start_date=config.start_date)

    # 5. Agregar a mensual
    df_monthly = aggregate_monthly(df_filtered)

    # 6. Limpiar y estandarizar
    df_clean = clean_brent_data(df_monthly)

    # 7. Guardar
    save_processed_data(df_clean, output_dir=output_dir, config=config)

    return df_clean
