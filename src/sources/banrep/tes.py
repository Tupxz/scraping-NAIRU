"""Extracción de tasas de interés TES Cero Cupón desde BANREP/SUAMECA.

Pipeline de 4 capas:

1. **SESIÓN**      — Warm-up HTTP para establecer cookies con SUAMECA.
2. **EXTRACCIÓN**  — Consulta diaria (tipoDato=1) por cada serie TES.
3. **PARSING**     — Convierte epoch_ms → fecha real (diaria).
4. **AGREGACIÓN**  — Agrega a frecuencia mensual tomando el **último**
   valor disponible por mes (proxy de cierre de mes).

Series:
- **TES_UVR_1Y** (idSerie 15275): Tasa Cero Cupón TES UVR — 1 año
- **TES_PESOS_1Y** (idSerie 15272): Tasa Cero Cupón TES pesos — 1 año

Formato de respuesta SUAMECA::

    [
      {
        "id": 15275,
        "nombre": "Tasa de interés Cero Cupón ...",
        ...
        "data": [[epoch_ms, valor], [epoch_ms, valor], ...]
      }
    ]
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from src.config import (
    BANREP_TES_CONFIG,
    BANREP_TES_PROCESSED_COLUMNS,
    BanrepTESConfig,
    PROCESSED_DIR,
    RAW_BANREP_DIR,
)
from src.io_utils import save_csv

logger = logging.getLogger("nairu_pipeline.banrep.tes")


# ═══════════════════════════════════════════════════════════════════════
# 1. SESIÓN  (warm-up)
# ═══════════════════════════════════════════════════════════════════════


def create_session(
    config: BanrepTESConfig = BANREP_TES_CONFIG,
) -> requests.Session:
    """Crea una sesión HTTP con warm-up para SUAMECA.

    El backend de SUAMECA requiere una visita previa a la página
    pública para establecer las cookies de sesión.  Sin este paso,
    las llamadas al API devuelven 403.

    Returns
    -------
    requests.Session
        Sesión autenticada lista para consultar el API.
    """
    session = requests.Session()
    session.headers.update(config.http_headers)

    logger.info("Warm-up SUAMECA (TES): %s", config.warmup_url)
    resp = session.get(config.warmup_url, timeout=config.timeout)
    resp.raise_for_status()
    logger.info("Warm-up OK: %d bytes", len(resp.content))

    return session


# ═══════════════════════════════════════════════════════════════════════
# 2. EXTRACCIÓN  (API REST)
# ═══════════════════════════════════════════════════════════════════════


def fetch_series(
    session: requests.Session,
    id_serie: int,
    tipo_dato: int,
    cant_datos: int = 8000,
    config: BanrepTESConfig = BANREP_TES_CONFIG,
) -> list[list]:
    """Consulta una serie individual del API de SUAMECA.

    Parameters
    ----------
    session : requests.Session
        Sesión con cookies de warm-up.
    id_serie : int
        Identificador de la serie en SUAMECA.
    tipo_dato : int
        Tipo de dato (1 = diario).
    cant_datos : int
        Cantidad máxima de observaciones a solicitar.

    Returns
    -------
    list[list]
        Lista de ``[epoch_ms, valor]`` tal como la devuelve el API.

    Raises
    ------
    ValueError
        Si la respuesta no tiene la estructura esperada.
    """
    params = {
        "idSerie": id_serie,
        "tipoDato": tipo_dato,
        "cantDatos": cant_datos,
    }

    logger.info(
        "Consultando serie TES %d (tipoDato=%d, cantDatos=%d)",
        id_serie, tipo_dato, cant_datos,
    )

    resp = session.get(
        config.endpoint_url,
        params=params,
        headers={"Referer": config.warmup_url},
        timeout=config.timeout,
    )
    resp.raise_for_status()

    payload = resp.json()

    # Validar estructura: lista con al menos un elemento que tenga "data"
    if not isinstance(payload, list) or len(payload) == 0:
        raise ValueError(
            f"Respuesta inesperada para serie TES {id_serie}: "
            f"se esperaba lista no vacía, se obtuvo {type(payload)}"
        )

    serie_obj = payload[0]
    if "data" not in serie_obj:
        raise ValueError(
            f"Respuesta para serie TES {id_serie} no contiene campo 'data'. "
            f"Claves disponibles: {list(serie_obj.keys())}"
        )

    data = serie_obj["data"]
    logger.info(
        "Serie TES %d: %d observaciones, nombre='%s'",
        id_serie, len(data), serie_obj.get("nombre", "?"),
    )
    return data


def fetch_all_series(
    session: requests.Session,
    config: BanrepTESConfig = BANREP_TES_CONFIG,
) -> dict[str, list[list]]:
    """Descarga todas las series TES definidas en ``config.series_map``.

    Returns
    -------
    dict[str, list[list]]
        Mapa ``{nombre_columna: [[epoch_ms, valor], ...]}``
    """
    result: dict[str, list[list]] = {}
    for col_name, id_serie in config.series_map.items():
        data = fetch_series(
            session,
            id_serie=id_serie,
            tipo_dato=config.tipo_dato,
            cant_datos=config.cant_datos,
            config=config,
        )
        result[col_name] = data
    return result


# ═══════════════════════════════════════════════════════════════════════
# 3. PARSING  (epoch → date diaria)
# ═══════════════════════════════════════════════════════════════════════


def epoch_ms_to_date(epoch_ms: int | float) -> date:
    """Convierte epoch en milisegundos a ``date`` (fecha real, NO normalizada).

    A diferencia del pipeline de inflación, aquí conservamos la fecha
    exacta porque los datos son diarios y necesitamos saber qué día
    corresponde cada observación para la agregación mensual posterior.

    Se usa UTC para evitar desfases por zona horaria local.
    """
    dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return date(dt.year, dt.month, dt.day)


def parse_series_data(
    raw_data: list[list],
    col_name: str,
) -> pd.DataFrame:
    """Convierte la lista ``[[epoch_ms, valor], ...]`` en DataFrame diario.

    Returns
    -------
    pd.DataFrame
        Columnas: ``date``, ``{col_name}``
    """
    records = []
    for item in raw_data:
        if len(item) < 2:
            continue
        epoch_ms, valor = item[0], item[1]
        if valor is None:
            continue
        d = epoch_ms_to_date(epoch_ms)
        records.append({"date": d, col_name: float(valor)})

    df = pd.DataFrame(records)
    if df.empty:
        return df

    # Eliminar duplicados de fecha (quedarse con el último valor)
    df = df.drop_duplicates(subset=["date"], keep="last")
    return df.sort_values("date").reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════
# 4. AGREGACIÓN  (diario → mensual, último valor del mes)
# ═══════════════════════════════════════════════════════════════════════


def aggregate_daily_to_monthly(
    all_raw: dict[str, list[list]],
) -> pd.DataFrame:
    """Parsea las series diarias, hace merge y agrega a mensual.

    La agregación toma el **último valor disponible** de cada mes
    para cada serie (proxy de cierre de mes / month-end).

    Returns
    -------
    pd.DataFrame
        Columnas: ``date``, ``year``, ``month`` + una columna por serie.
    """
    dfs: list[pd.DataFrame] = []
    for col_name, raw_data in all_raw.items():
        df_serie = parse_series_data(raw_data, col_name)
        if not df_serie.empty:
            dfs.append(df_serie)

    if not dfs:
        raise ValueError("No se obtuvieron datos de ninguna serie TES.")

    # Merge secuencial por fecha (diaria)
    merged = dfs[0]
    for df_next in dfs[1:]:
        merged = pd.merge(merged, df_next, on="date", how="outer")

    merged = merged.sort_values("date").reset_index(drop=True)
    merged["date"] = pd.to_datetime(merged["date"])

    # Extraer year y month para agrupación
    merged["year"] = merged["date"].dt.year
    merged["month"] = merged["date"].dt.month

    logger.info(
        "Datos diarios unidos: %d filas, rango: %s → %s",
        len(merged), merged["date"].min(), merged["date"].max(),
    )

    # ── Agregar a mensual: último valor del mes por serie ─────────
    series_cols = [c for c in merged.columns if c not in ("date", "year", "month")]
    agg_dict = {col: "last" for col in series_cols}
    monthly = (
        merged.sort_values("date")
        .groupby(["year", "month"], sort=True)
        .agg(agg_dict)
        .reset_index()
    )

    # Construir date como primer día del mes (consistencia con el proyecto)
    monthly["date"] = pd.to_datetime(
        monthly["year"].astype(str) + "-"
        + monthly["month"].astype(str).str.zfill(2) + "-01"
    )

    # Reordenar columnas
    monthly = monthly[["date", "year", "month"] + series_cols]
    monthly = monthly.sort_values("date").reset_index(drop=True)

    logger.info(
        "Agregación mensual (último valor): %d meses, rango: %s → %s",
        len(monthly), monthly["date"].min(), monthly["date"].max(),
    )
    return monthly


# ═══════════════════════════════════════════════════════════════════════
# TRANSFORMACIÓN
# ═══════════════════════════════════════════════════════════════════════


def clean_banrep_tes_data(
    all_raw: dict[str, list[list]],
    config: BanrepTESConfig = BANREP_TES_CONFIG,
) -> pd.DataFrame:
    """Pipeline: parsea JSON diario → merge → agrega mensual → esquema final."""
    df = aggregate_daily_to_monthly(all_raw)

    df["source"] = "BANREP_SUAMECA"
    df["download_date"] = date.today().isoformat()

    # Asegurar tipos
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)

    # Eliminar duplicados por fecha
    dupes_before = len(df)
    df = df.drop_duplicates(subset=["date"], keep="first").copy()
    dupes_dropped = dupes_before - len(df)
    if dupes_dropped > 0:
        logger.warning("Se eliminaron %d filas duplicadas por fecha", dupes_dropped)

    # Asegurar que todas las columnas de serie existan
    series_cols = list(config.series_map.keys())
    for col in series_cols:
        if col not in df.columns:
            df[col] = None

    # Seleccionar y ordenar columnas según esquema
    output_cols = ["date", "year", "month"] + series_cols + ["source", "download_date"]
    df = df[output_cols].sort_values("date").reset_index(drop=True)

    logger.info(
        "TES limpio: %d filas, rango: %s → %s",
        len(df), df["date"].min(), df["date"].max(),
    )
    return df


# ═══════════════════════════════════════════════════════════════════════
# CARGA
# ═══════════════════════════════════════════════════════════════════════


def save_raw_json(
    all_raw: dict[str, list[list]],
    output_dir: Path = RAW_BANREP_DIR,
    config: BanrepTESConfig = BANREP_TES_CONFIG,
) -> Path:
    """Guarda el JSON crudo para auditoría."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / config.raw_json_filename
    path.write_text(json.dumps(all_raw, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("JSON crudo TES guardado: %s", path)
    return path


def save_processed_data(
    df: pd.DataFrame,
    output_dir: Path = PROCESSED_DIR,
    config: BanrepTESConfig = BANREP_TES_CONFIG,
) -> Path:
    """Guarda el dataset TES procesado en disco."""
    output_path = output_dir / config.processed_filename
    save_csv(df, output_path)
    logger.info("Dataset TES procesado guardado: %s", output_path)
    return output_path


# ═══════════════════════════════════════════════════════════════════════
# ORQUESTACIÓN
# ═══════════════════════════════════════════════════════════════════════


def run_banrep_tes_pipeline(
    config: BanrepTESConfig = BANREP_TES_CONFIG,
    output_dir: Path = PROCESSED_DIR,
    raw_dir: Path = RAW_BANREP_DIR,
) -> pd.DataFrame:
    """Pipeline completo: sesión → extracción diaria → agregación mensual → guardado."""
    # 1. Crear sesión
    session = create_session(config)

    # 2. Descargar todas las series (diarias)
    all_raw = fetch_all_series(session, config)

    # 3. Guardar JSON crudo
    save_raw_json(all_raw, output_dir=raw_dir, config=config)

    # 4. Parsear, agregar y limpiar
    df = clean_banrep_tes_data(all_raw, config)

    # 5. Guardar
    save_processed_data(df, output_dir=output_dir, config=config)

    return df
