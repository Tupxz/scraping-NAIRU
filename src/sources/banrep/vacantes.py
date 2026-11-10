"""Vacantes según anuncios de prensa impresa (Banrep, SUAMECA) para v7.

Siete series mensuales (ids 16570–16576: Manizales, Cali, Bucaramanga,
Medellín, Barranquilla, Pasto, Bogotá), desde 1976 (Barranquilla desde 1978,
Pasto desde 2000).  Se consultan con la misma API y sesión que la inflación
(``inflation.fetch_series``; warm-up y arreglo TLS).

Salida: ``data/processed/banrep_vacantes_prensa_mensual.csv`` con **solo**
``date`` (primer día del mes) y una columna por ciudad con el nombre de la
serie en SUAMECA ("Vacantes, Cali, Diario El País"…), como el CSV de la línea
base.  No lleva ``source``/``download_date``: v7 suma todas las columnas que
no son ``date``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd
import requests

from src.config import (
    BANREP_VACANTES_CONFIG,
    PROCESSED_DIR,
    RAW_BANREP_DIR,
    BanrepVacantesConfig,
)
from src.io_utils import save_csv

logger = logging.getLogger("nairu_pipeline.banrep.vacantes")


def descargar_vacantes(config: BanrepVacantesConfig = BANREP_VACANTES_CONFIG,
                       session: requests.Session | None = None) -> list[dict]:
    """Lista de ``{"id", "nombre", "data"}`` (data = ``[[epoch_ms, valor], ...]``)."""
    from src.config import BANREP_INFLATION_CONFIG
    from src.sources.banrep.inflation import create_session

    session = session or create_session()
    series = []
    for sid in config.series_ids:
        r = session.get(BANREP_INFLATION_CONFIG.endpoint_url,
                        params={"idSerie": sid, "tipoDato": config.tipo_dato, "cantDatos": config.cant_datos},
                        headers={"Referer": BANREP_INFLATION_CONFIG.warmup_url},
                        timeout=BANREP_INFLATION_CONFIG.timeout)
        r.raise_for_status()
        p = r.json()
        if not isinstance(p, list) or not p or not p[0].get("data"):
            raise ValueError(f"SUAMECA no devolvió datos para la serie de vacantes {sid}.")
        series.append({"id": sid, "nombre": p[0].get("nombre", str(sid)), "data": p[0]["data"]})
    return series


def armar_vacantes(series: list[dict]) -> pd.DataFrame:
    """Tabla ancha mensual: ``date`` + una columna por ciudad (nombre SUAMECA)."""
    columnas = {}
    for s in series:
        if not str(s["nombre"]).startswith("Vacantes"):
            raise ValueError(f"La serie {s['id']} no es de vacantes: {s['nombre']!r}")
        d = pd.DataFrame(s["data"], columns=["ms", "valor"])
        fechas = pd.to_datetime(d["ms"], unit="ms").dt.to_period("M").dt.to_timestamp()
        if fechas.duplicated().any():
            raise ValueError(f"Meses duplicados en la serie de vacantes {s['id']}.")
        columnas[s["nombre"]] = pd.Series(pd.to_numeric(d["valor"], errors="coerce").to_numpy(),
                                          index=fechas.to_numpy())
    df = pd.DataFrame(columnas).sort_index()
    df.index.name = "date"
    return df.reset_index()


def construir_vacantes(config: BanrepVacantesConfig = BANREP_VACANTES_CONFIG,
                       raw_dir: Path | None = RAW_BANREP_DIR,
                       session: requests.Session | None = None) -> pd.DataFrame:
    series = descargar_vacantes(config, session)
    if raw_dir is not None:
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / config.raw_filename).write_text(json.dumps(series), encoding="utf-8")
    df = armar_vacantes(series)
    logger.info("Vacantes: %d meses × %d ciudades, %s → %s", len(df), df.shape[1] - 1,
                df["date"].min().date(), df["date"].max().date())
    return df


def guardar_vacantes(df: pd.DataFrame, output_dir: Path = PROCESSED_DIR,
                     config: BanrepVacantesConfig = BANREP_VACANTES_CONFIG) -> Path:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    path = output_dir / config.processed_filename
    save_csv(out, path)
    logger.info("Vacantes guardadas: %s (%d filas)", path, len(out))
    return path
