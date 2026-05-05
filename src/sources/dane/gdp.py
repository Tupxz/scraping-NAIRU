"""Extracción y limpieza del PIB trimestral desestacionalizado del DANE.

Pipeline de 3 capas:

1. **SCRAPING**  — Descarga la página *PIB - Información técnica* de
   Cuentas Nacionales Trimestrales y extrae el enlace al anexo
   ``anex-ProduccionConstantes-{trim}{YYYY}.xlsx`` más reciente.
2. **DESCARGA**  — Descarga el Excel seleccionado.
3. **PARSING**   — Lee la hoja *Cuadro 4* (PIB desestacionalizado, 12
   agrupaciones), localiza la fila *Producto Interno Bruto* y
   reconstruye un DataFrame trimestral en formato largo.

Notas:
  - Frecuencia: **trimestral**. Se asigna al primer mes del trimestre
    (Q1 → enero, Q2 → abril, Q3 → julio, Q4 → octubre).
  - Unidad: miles de millones de pesos a precios constantes (la base
    cambia con cada revisión metodológica del DANE).
  - El Cuadro 4 se elige sobre Cuadros 5/6 (25 y 61 agrupaciones)
    porque la serie agregada del PIB total es idéntica pero con menos
    columnas auxiliares.  Esto reduce la superficie de cambio si el
    DANE reordena los desagregados.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.config import (
    DANE_GDP_CONFIG,
    DANEGDPConfig,
    PROCESSED_DIR,
    RAW_DANE_DIR,
)
from src.io_utils import save_csv

logger = logging.getLogger("nairu_pipeline.dane.gdp")


# ═══════════════════════════════════════════════════════════════════════
# 1. SCRAPING
# ═══════════════════════════════════════════════════════════════════════


def fetch_gdp_page(config: DANEGDPConfig = DANE_GDP_CONFIG) -> str:
    """Descarga el HTML de la página *PIB - Información técnica* del DANE."""
    logger.info("Descargando página PIB DANE: %s", config.page_url)
    response = requests.get(
        config.page_url,
        headers=config.http_headers,
        timeout=config.timeout,
        verify=False,
    )
    response.raise_for_status()
    logger.info("Página descargada: %d bytes", len(response.content))
    return response.text


def extract_gdp_xlsx_link(
    html: str,
    config: DANEGDPConfig = DANE_GDP_CONFIG,
) -> str:
    """Extrae la URL del anexo ProduccionConstantes más reciente.

    El DANE publica varios trimestres en la misma página; tomamos el
    enlace que aparece primero en el HTML (suele ser el más reciente).
    Si hay varios candidatos, se ordenan por trimestre/año extraídos
    de la URL para garantizar el más nuevo.
    """
    soup = BeautifulSoup(html, "html.parser")
    pattern = re.compile(config.link_pattern, re.IGNORECASE)

    links: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if pattern.search(href):
            absolute = urljoin(config.base_url, href)
            if absolute not in links:
                links.append(absolute)
            logger.debug("Enlace ProduccionConstantes encontrado: %s", absolute)

    if not links:
        raise ValueError(
            "No se encontró ningún anexo ProduccionConstantes .xlsx en la "
            f"página del DANE. URL: {config.page_url}"
        )

    # Ordena por (año, trimestre romano) para tomar el más reciente
    roman = {"I": 1, "II": 2, "III": 3, "IV": 4}
    extract = re.compile(r"-(I{1,3}|IV)trim(\d{4})\.xlsx$")

    def _key(url: str) -> tuple[int, int]:
        m = extract.search(url)
        if not m:
            return (0, 0)
        return (int(m.group(2)), roman.get(m.group(1), 0))

    links.sort(key=_key, reverse=True)
    selected = links[0]
    logger.info("Anexo ProduccionConstantes seleccionado: %s", selected)
    return selected


# ═══════════════════════════════════════════════════════════════════════
# 2. DESCARGA
# ═══════════════════════════════════════════════════════════════════════


def download_gdp_excel(
    url: str,
    output_dir: Path = RAW_DANE_DIR,
    config: DANEGDPConfig = DANE_GDP_CONFIG,
) -> Path:
    """Descarga el Excel del PIB y lo guarda en data/raw/dane/."""
    logger.info("Descargando Excel PIB: %s", url)
    response = requests.get(
        url,
        headers=config.http_headers,
        timeout=config.timeout,
        verify=False,
    )
    response.raise_for_status()

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / config.raw_xlsx_filename
    path.write_bytes(response.content)

    size_kb = len(response.content) / 1024
    logger.info("Excel descargado: %s (%.1f KB)", path.name, size_kb)
    return path


# ═══════════════════════════════════════════════════════════════════════
# 3. PARSING
# ═══════════════════════════════════════════════════════════════════════


_ROMAN_TO_QUARTER = {"I": 1, "II": 2, "III": 3, "IV": 4}
_QUARTER_TO_MONTH = {1: 1, 2: 4, 3: 7, 4: 10}


def _parse_quarter(roman_str: str) -> int | None:
    """Convierte 'I', 'II', 'III', 'IV' (case-insensitive) → 1..4."""
    if not isinstance(roman_str, str):
        return None
    return _ROMAN_TO_QUARTER.get(roman_str.strip().upper())


def parse_gdp_excel(
    path: Path,
    config: DANEGDPConfig = DANE_GDP_CONFIG,
) -> pd.DataFrame:
    """Parsea la hoja 'Cuadro 4' y devuelve el formato largo estándar.

    Columnas de salida:
        date, year, quarter, gdp_observed, source, download_date
    """
    logger.info("Parseando Excel PIB DANE: %s", path.name)

    df_raw = pd.read_excel(path, sheet_name=config.sheet_name, header=None)

    # ── Filas de años y trimestres ────────────────────────────────────
    years_raw = df_raw.iloc[config.year_row].ffill()      # años repetidos hacia adelante
    quarters_raw = df_raw.iloc[config.quarter_row]

    # ── Localizar la fila del PIB total (primer match = bloque niveles) ─
    concept_col_series = df_raw.iloc[:, config.concept_col].astype(str).str.strip()
    target = config.concept_label.strip().lower()
    matches = concept_col_series.str.lower() == target
    if not matches.any():
        raise ValueError(
            f"No se encontró la fila '{config.concept_label}' en la columna "
            f"{config.concept_col} de la hoja '{config.sheet_name}'."
        )
    pib_row_idx = int(matches.idxmax())   # primer True
    logger.info("Fila '%s' detectada en índice %d", config.concept_label, pib_row_idx)

    pib_row = df_raw.iloc[pib_row_idx]

    # ── Reconstruir fechas y extraer valores ──────────────────────────
    records: list[dict] = []
    today_str = date.today().isoformat()

    for col_idx in range(config.data_start_col, len(pib_row)):
        year_val = years_raw.iloc[col_idx]
        quarter_str = quarters_raw.iloc[col_idx]
        value = pib_row.iloc[col_idx]

        if pd.isna(year_val) or pd.isna(quarter_str) or pd.isna(value):
            continue

        try:
            year_int = int(float(str(year_val)))
        except (ValueError, TypeError):
            continue

        quarter_int = _parse_quarter(str(quarter_str))
        if quarter_int is None:
            logger.debug("Trimestre no reconocido: '%s'", quarter_str)
            continue

        try:
            value_float = float(value)
        except (ValueError, TypeError):
            continue

        month = _QUARTER_TO_MONTH[quarter_int]
        records.append({
            "date": pd.Timestamp(year=year_int, month=month, day=1),
            "year": year_int,
            "quarter": quarter_int,
            "gdp_observed": round(value_float, 4),
            "source": config.source_label,
            "download_date": today_str,
        })

    if not records:
        raise ValueError(
            "No se extrajeron registros del PIB. "
            "Verifica la estructura del Excel."
        )

    df = pd.DataFrame(records)
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)

    logger.info(
        "PIB DANE parseado: %d trimestres (%s – %s)",
        len(df),
        df["date"].min().strftime("%Y-Q%q") if False else df["date"].min().strftime("%Y-%m"),
        df["date"].max().strftime("%Y-%m"),
    )
    return df


# ═══════════════════════════════════════════════════════════════════════
# Pipeline orquestador
# ═══════════════════════════════════════════════════════════════════════


def run_dane_gdp_pipeline(
    config: DANEGDPConfig = DANE_GDP_CONFIG,
    raw_dir: Path = RAW_DANE_DIR,
    processed_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Ejecuta el pipeline completo: scraping → descarga → parsing → guardado."""
    logger.info("── Iniciando pipeline PIB DANE ──")

    html = fetch_gdp_page(config)
    url = extract_gdp_xlsx_link(html, config)
    xlsx_path = download_gdp_excel(url, raw_dir, config)
    df = parse_gdp_excel(xlsx_path, config)

    output_path = processed_dir / config.processed_filename
    save_csv(df, output_path)
    logger.info("Guardado: %s (%d filas)", output_path.name, len(df))
    return df
