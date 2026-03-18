"""Utilidades compartidas para parsear archivos del DANE.

Funciones reutilizables para descargar datos, detectar hojas,
encabezados y mapear columnas en archivos Excel del DANE.
Usadas tanto por el módulo de desempleo como por el de IPC.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger("nairu_pipeline.dane.common")


# ═══════════════════════════════════════════════════════════════════════
# Descarga HTTP genérica
# ═══════════════════════════════════════════════════════════════════════


def download_file(
    url: str,
    output_path: Path,
    *,
    timeout: int = 120,
    headers: dict[str, str] | None = None,
) -> Path:
    """Descarga un archivo desde una URL y lo guarda en disco.

    Parameters
    ----------
    url : str
        URL del recurso a descargar.
    output_path : Path
        Ruta completa de destino (incluyendo nombre de archivo).
    timeout : int
        Timeout HTTP en segundos.
    headers : dict, optional
        Headers HTTP personalizados.

    Returns
    -------
    Path
        Ruta al archivo descargado.
    """
    logger.info("Descargando: %s", url)
    response = requests.get(url, timeout=timeout, headers=headers or {})
    response.raise_for_status()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(response.content)

    size_kb = len(response.content) / 1024
    logger.info("Descargado: %s (%.1f KB)", output_path.name, size_kb)
    return output_path


# ═══════════════════════════════════════════════════════════════════════
# Detección robusta de estructura Excel
# ═══════════════════════════════════════════════════════════════════════


def detect_relevant_sheet(
    xlsx_path: Path,
    keywords: list[str] | None = None,
) -> str:
    """Detecta la hoja más relevante en un archivo Excel multi-hoja.

    Puntúa cada hoja según cuántas keywords aparecen en sus primeras
    filas. La hoja con mayor puntaje gana.
    """
    if keywords is None:
        keywords = ["desempleo", "tasa", "td", "unemployment"]

    xlsx = pd.ExcelFile(xlsx_path, engine="openpyxl")
    sheet_names = xlsx.sheet_names
    logger.info("Hojas encontradas: %s", sheet_names)

    if len(sheet_names) == 1:
        logger.info("Única hoja disponible: '%s'", sheet_names[0])
        return sheet_names[0]

    best_sheet = sheet_names[0]
    best_score = -1

    for sheet in sheet_names:
        df_peek = pd.read_excel(
            xlsx_path, sheet_name=sheet, header=None, nrows=40,
            engine="openpyxl", dtype=str,
        )
        text_blob = " ".join(
            df_peek.fillna("").astype(str).values.flatten()
        ).lower()

        score = sum(1 for kw in keywords if kw.lower() in text_blob)
        sheet_lower = sheet.lower()
        score += sum(2 for kw in keywords if kw.lower() in sheet_lower)

        logger.debug("Hoja '%s': score=%d", sheet, score)
        if score > best_score:
            best_score = score
            best_sheet = sheet

    logger.info("Hoja seleccionada: '%s' (score=%d)", best_sheet, best_score)
    return best_sheet


def detect_header_row(
    df_raw: pd.DataFrame,
    keywords: list[str] | None = None,
    max_scan: int = 30,
) -> int:
    """Detecta la fila que contiene los encabezados reales.

    Escanea las primeras N filas y devuelve la que más keywords contiene.
    """
    if keywords is None:
        keywords = ["año", "mes", "tasa", "total", "trimestre", "desempleo",
                     "year", "month", "rate", "unemployment", "date"]

    best_row = 0
    best_score = -1
    scan_limit = min(max_scan, len(df_raw))

    for i in range(scan_limit):
        row_text = " ".join(
            str(v).strip().lower() for v in df_raw.iloc[i] if pd.notna(v)
        )
        score = sum(1 for kw in keywords if kw.lower() in row_text)
        if score > best_score:
            best_score = score
            best_row = i

    logger.info(
        "Fila de encabezado detectada: %d (score=%d, contenido: %s)",
        best_row, best_score,
        [str(v).strip() for v in df_raw.iloc[best_row] if pd.notna(v)][:8],
    )
    return best_row


def match_column(col_name: str, patterns: list[str]) -> bool:
    """Verifica si un nombre de columna coincide con algún patrón regex."""
    for pattern in patterns:
        if re.search(pattern, col_name, re.IGNORECASE):
            return True
    return False


def auto_map_columns(
    columns: list[str],
    column_patterns: dict[str, list[str]],
) -> dict[str, str]:
    """Mapea columnas del archivo crudo a nombres del pipeline.

    Usa patrones regex para encontrar la columna que corresponde a
    cada campo del pipeline.

    Returns
    -------
    dict[str, str]
        Diccionario {nombre_columna_cruda: nombre_pipeline}.
    """
    mapping: dict[str, str] = {}

    for pipeline_field, patterns in column_patterns.items():
        for col in columns:
            if match_column(col, patterns) and col not in mapping:
                mapping[col] = pipeline_field
                logger.info("Columna mapeada: '%s' → '%s'", col, pipeline_field)
                break
        else:
            logger.warning(
                "No se encontró columna para '%s' (patrones: %s)",
                pipeline_field, patterns,
            )

    return mapping
