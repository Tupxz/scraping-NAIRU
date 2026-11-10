"""Encuesta Mensual de Expectativas de Analistas Económicos (EME, Banrep).

1. **DESCARGA** — Excel de series históricas desde SUAMECA.  La URL se toma
   del menú ``ENCUESTAS`` (campo ``nombreAdjunto`` que termina en
   ``series_historicas.xlsx``); si no aparece, se usa ``archivo_url``.  La
   sesión es la de SUAMECA (warm-up y arreglo TLS de ``tls.py``).
2. **PARSING** — Hoja ``INFLACION_TOTAL``: una fila de horizontes, otra con
   PROMEDIO / DESVIACIÓN ESTÁNDAR / MEDIANA y una fila por encuesta.  Se toma
   el **promedio** de cada horizonte, en porcentaje (el Excel trae
   fracciones: 0,054293 → 5,4293) redondeado a 4 decimales.  Frente al CSV
   de la línea base solo hay diferencias de 0,0001 pp en mitades exactas
   del quinto decimal (redondeo binario).  La fecha de la encuesta (p. ej. 2026-10-15) se lleva
   al primer día del mes.

Salida: ``data/processed/eme_expectativas_inflacion_total_mensual_promedio.csv``
(``date``, ``mes_actual``, ``fin_ano_presente``, ``doce_meses``,
``fin_ano_siguiente``, ``veinticuatro_meses``, ``sesenta_meses`` + metadatos).
v7 usa ``doce_meses`` (expectativa de inflación total a 12 meses).
"""

from __future__ import annotations

import io
import json
import logging
import re
import unicodedata
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from src.config import (
    BANREP_EME_CONFIG,
    EME_HORIZONTES,
    EME_PROCESSED_COLUMNS,
    PROCESSED_DIR,
    RAW_BANREP_DIR,
    BanrepEMEConfig,
)
from src.io_utils import save_csv

logger = logging.getLogger("nairu_pipeline.banrep.eme")

FUENTE = "BANREP_SUAMECA_EME"

# Encabezado de horizonte → columna de salida.
_HORIZONTES: list[tuple[str, str]] = [
    (r"mensual", "mes_actual"),
    (r"diciembre\s+presente", "fin_ano_presente"),
    (r"doce\s+meses", "doce_meses"),
    (r"diciembre\s+del\s+siguiente", "fin_ano_siguiente"),
    (r"veinticuatro\s+meses", "veinticuatro_meses"),
    (r"sesenta\s+meses", "sesenta_meses"),
]


def _norm(x: object) -> str:
    t = unicodedata.normalize("NFKD", str(x))
    return re.sub(r"\s+", " ", "".join(c for c in t if not unicodedata.combining(c))).strip().lower()


# ═══════════════════════════════════════════════════════════════════════
# Descarga
# ═══════════════════════════════════════════════════════════════════════


def url_series_historicas(menu_json: str, respaldo: str = BANREP_EME_CONFIG.archivo_url) -> str:
    """URL del Excel de series históricas de la EME según el menú de SUAMECA."""
    try:
        texto = json.dumps(json.loads(menu_json))
    except (ValueError, TypeError):
        return respaldo
    urls = re.findall(r'https?://[^"\\]+/encuesta_mensual_analistas_economicos/[^"\\]*series_historicas[^"\\]*\.xlsx',
                      texto)
    return urls[0] if urls else respaldo


def descargar_eme(config: BanrepEMEConfig = BANREP_EME_CONFIG,
                  session: requests.Session | None = None) -> bytes:
    """Bytes del Excel de series históricas (error si no es un .xlsx)."""
    if session is None:
        from src.sources.banrep.inflation import create_session
        session = create_session()
    url = config.archivo_url
    try:
        r = session.get(config.menu_url, timeout=config.timeout)
        r.raise_for_status()
        url = url_series_historicas(r.text, config.archivo_url)
    except requests.RequestException as exc:
        logger.warning("Menú ENCUESTAS de SUAMECA no disponible (%s); se usa la URL fija.", exc)
    logger.info("Descargando EME: %s", url)
    r = session.get(url, timeout=config.timeout)
    r.raise_for_status()
    if r.content[:2] != b"PK":
        raise RuntimeError(f"La descarga de la EME no es un .xlsx ({r.headers.get('content-type')}): {url}")
    return r.content


# ═══════════════════════════════════════════════════════════════════════
# Parsing
# ═══════════════════════════════════════════════════════════════════════


def parsear_eme(contenido: bytes | Path, config: BanrepEMEConfig = BANREP_EME_CONFIG) -> pd.DataFrame:
    """Promedio de la inflación total esperada por horizonte, en %, mensual.

    Raises
    ------
    ValueError
        Si falta la hoja, la fila de encabezados o algún horizonte, o si hay
        meses duplicados.
    """
    fuente = io.BytesIO(contenido) if isinstance(contenido, bytes) else contenido
    try:
        crudo = pd.read_excel(fuente, sheet_name=config.hoja, header=None)
    except ValueError as exc:
        raise ValueError(f"El Excel de la EME no tiene la hoja '{config.hoja}'.") from exc

    fila_enc = next((i for i in range(min(15, len(crudo)))
                     if _norm(crudo.iat[i, 0]).startswith("fecha encuesta")), None)
    if fila_enc is None or fila_enc == 0:
        raise ValueError("No se encontró la fila 'FECHA ENCUESTA' en la hoja de la EME.")
    horizontes = crudo.iloc[fila_enc - 1].ffill()
    medidas = crudo.iloc[fila_enc]

    columnas: dict[str, int] = {}
    for j in range(1, crudo.shape[1]):
        if _norm(medidas.iat[j]) != "promedio":
            continue
        h = _norm(horizontes.iat[j])
        for patron, nombre in _HORIZONTES:
            if re.search(patron, h) and nombre not in columnas:
                columnas[nombre] = j
                break
    faltan = [h for h in EME_HORIZONTES if h not in columnas]
    if faltan:
        raise ValueError(f"Horizontes de la EME no encontrados: {faltan}")

    datos = crudo.iloc[fila_enc + 1:]
    fechas = pd.to_datetime(datos.iloc[:, 0], errors="coerce")
    datos = datos[fechas.notna()]
    fechas = fechas[fechas.notna()]
    out = pd.DataFrame({"date": fechas.dt.to_period("M").dt.to_timestamp().to_numpy()})
    for nombre in EME_HORIZONTES:
        vals = pd.to_numeric(datos.iloc[:, columnas[nombre]], errors="coerce")
        out[nombre] = (vals * 100.0).round(4).to_numpy()
    if out["date"].duplicated().any():
        raise ValueError(f"Meses duplicados en la EME: {out.loc[out['date'].duplicated(), 'date'].tolist()[:5]}")
    out = out.sort_values("date").reset_index(drop=True)
    logger.info("EME parseada: %d meses, %s → %s", len(out),
                out["date"].min().date(), out["date"].max().date())
    return out


def limpiar_eme(df: pd.DataFrame, config: BanrepEMEConfig = BANREP_EME_CONFIG,
                hoy: date | None = None) -> pd.DataFrame:
    df = df[df["date"] >= pd.Timestamp(config.start_date)].copy()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["source"] = FUENTE
    df["download_date"] = (hoy or date.today()).isoformat()
    return df[EME_PROCESSED_COLUMNS].reset_index(drop=True)


def construir_eme(config: BanrepEMEConfig = BANREP_EME_CONFIG,
                  raw_dir: Path | None = RAW_BANREP_DIR,
                  session: requests.Session | None = None,
                  hoy: date | None = None) -> pd.DataFrame:
    """Descarga → crudo → parsing → limpieza (sin escribir el procesado)."""
    contenido = descargar_eme(config, session)
    if raw_dir is not None:
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / config.raw_filename).write_bytes(contenido)
    return limpiar_eme(parsear_eme(contenido, config), config, hoy)


def guardar_eme(df: pd.DataFrame, output_dir: Path = PROCESSED_DIR,
                config: BanrepEMEConfig = BANREP_EME_CONFIG) -> Path:
    path = output_dir / config.processed_filename
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    save_csv(out, path)
    logger.info("EME guardada: %s (%d filas)", path, len(out))
    return path
