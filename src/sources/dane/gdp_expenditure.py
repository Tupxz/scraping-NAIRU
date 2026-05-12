"""Extracción y limpieza de la Inversión trimestral desestacionalizada del DANE.

Pipeline de 3 capas:

1. **SCRAPING**  — Descarga la página *PIB - Información técnica* de
   Cuentas Nacionales Trimestrales y extrae el enlace al anexo
   ``anex-GastoConstantes-{trim}{YYYY}.xlsx`` más reciente.
2. **DESCARGA**  — Descarga el Excel seleccionado.
3. **PARSING**   — Lee la hoja *Cuadro 2* (PIB gasto desestacionalizado),
   localiza la fila *Formación bruta de capital fijo* y reconstruye
   un DataFrame trimestral en formato largo.

Notas:
  - Frecuencia: **trimestral**. Se asigna al primer mes del trimestre.
  - Unidad: miles de millones de pesos a precios constantes (base 2015).
  - El Cuadro 2 contiene los datos ajustados por efecto estacional y de
    calendario, que son los comparables con el PIB desestacionalizado
    ya presente en el pipeline.
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
    DANE_GDP_EXPENDITURE_CONFIG,
    DANEGDPExpenditureConfig,
    PROCESSED_DIR,
    RAW_DANE_DIR,
)
from src.io_utils import save_csv

logger = logging.getLogger("nairu_pipeline.dane.gdp_expenditure")

_ROMAN_TO_QUARTER = {"I": 1, "II": 2, "III": 3, "IV": 4}
_QUARTER_TO_MONTH = {1: 1, 2: 4, 3: 7, 4: 10}


# ═══════════════════════════════════════════════════════════════════════
# 1. SCRAPING
# ═══════════════════════════════════════════════════════════════════════


def fetch_expenditure_page(
    config: DANEGDPExpenditureConfig = DANE_GDP_EXPENDITURE_CONFIG,
) -> str:
    """Descarga el HTML de la página PIB información técnica del DANE."""
    logger.info("Descargando página PIB gasto DANE: %s", config.page_url)
    response = requests.get(
        config.page_url,
        headers=config.http_headers,
        timeout=config.timeout,
        verify=False,
    )
    response.raise_for_status()
    return response.text


def extract_expenditure_xlsx_link(
    html: str,
    config: DANEGDPExpenditureConfig = DANE_GDP_EXPENDITURE_CONFIG,
) -> str:
    """Extrae la URL del anexo GastoConstantes más reciente."""
    soup = BeautifulSoup(html, "html.parser")
    pattern = re.compile(config.link_pattern, re.IGNORECASE)

    links: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if pattern.search(href):
            absolute = urljoin(config.base_url, href)
            if absolute not in links:
                links.append(absolute)
            logger.debug("Enlace GastoConstantes encontrado: %s", absolute)

    if not links:
        raise ValueError(
            "No se encontró ningún anexo GastoConstantes .xlsx en la "
            f"página del DANE. URL: {config.page_url}"
        )

    roman = {"I": 1, "II": 2, "III": 3, "IV": 4}
    extract = re.compile(r"-(I{1,3}|IV)trim(\d{4})\.xlsx$")

    def _key(url: str) -> tuple[int, int]:
        m = extract.search(url)
        if not m:
            return (0, 0)
        return (int(m.group(2)), roman.get(m.group(1), 0))

    links.sort(key=_key, reverse=True)
    selected = links[0]
    logger.info("Anexo GastoConstantes seleccionado: %s", selected)
    return selected


# ═══════════════════════════════════════════════════════════════════════
# 2. DESCARGA
# ═══════════════════════════════════════════════════════════════════════


def download_expenditure_excel(
    url: str,
    output_dir: Path = RAW_DANE_DIR,
    config: DANEGDPExpenditureConfig = DANE_GDP_EXPENDITURE_CONFIG,
) -> Path:
    """Descarga el Excel de gasto y lo guarda en data/raw/dane/."""
    logger.info("Descargando Excel PIB gasto: %s", url)
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


def _parse_quarter(roman_str: str) -> int | None:
    if not isinstance(roman_str, str):
        return None
    return _ROMAN_TO_QUARTER.get(roman_str.strip().upper())


def parse_expenditure_excel(
    path: Path,
    config: DANEGDPExpenditureConfig = DANE_GDP_EXPENDITURE_CONFIG,
) -> pd.DataFrame:
    """Parsea el Cuadro 2 del anexo GastoConstantes.

    Columnas de salida:
        date, year, quarter, investment, source, download_date
    """
    logger.info("Parseando Excel PIB gasto DANE: %s", path.name)

    df_raw = pd.read_excel(path, sheet_name=config.sheet_name, header=None)

    years_row = df_raw.iloc[config.year_row]
    quarters_raw = df_raw.iloc[config.quarter_row]

    def _clean_year(v) -> int | None:
        if pd.isna(v):
            return None
        s = re.sub(r"[^\d]", "", str(v).strip())
        return int(s) if s else None

    # Inferir años secuencialmente (mismo mecanismo que gdp.py)
    inferred_years: dict[int, int] = {}
    current_year: int | None = None
    prev_quarter: int | None = None
    for col_idx in range(config.data_start_col, len(years_row)):
        raw_year = _clean_year(years_row.iloc[col_idx])
        q = _parse_quarter(str(quarters_raw.iloc[col_idx]))
        if q is None:
            continue
        if raw_year is not None:
            if current_year is None or raw_year >= current_year:
                current_year = raw_year
        elif current_year is not None and prev_quarter == 4 and q == 1:
            current_year += 1
        if current_year is not None:
            inferred_years[col_idx] = current_year
        prev_quarter = q

    # Localizar la fila de inversión
    concept_col_series = df_raw.iloc[:, config.concept_col].astype(str).str.strip()
    target = config.concept_label.strip().lower()
    matches = concept_col_series.str.lower() == target
    if not matches.any():
        raise ValueError(
            f"No se encontró la fila '{config.concept_label}' en la columna "
            f"{config.concept_col} de la hoja '{config.sheet_name}'."
        )
    inv_row_idx = int(matches.idxmax())
    logger.info("Fila '%s' detectada en índice %d", config.concept_label, inv_row_idx)

    inv_row = df_raw.iloc[inv_row_idx]

    records: list[dict] = []
    today_str = date.today().isoformat()

    for col_idx in range(config.data_start_col, len(inv_row)):
        year_int = inferred_years.get(col_idx)
        quarter_str = quarters_raw.iloc[col_idx]
        value = inv_row.iloc[col_idx]

        if year_int is None or pd.isna(quarter_str) or pd.isna(value):
            continue

        quarter_int = _parse_quarter(str(quarter_str))
        if quarter_int is None:
            continue

        try:
            value_float = float(value)
        except (ValueError, TypeError):
            continue

        month = _QUARTER_TO_MONTH[quarter_int]
        records.append({
            "date":       pd.Timestamp(year=year_int, month=month, day=1),
            "year":       year_int,
            "quarter":    quarter_int,
            "investment": round(value_float, 4),
            "source":     config.source_label,
            "download_date": today_str,
        })

    if not records:
        raise ValueError(
            "No se extrajeron registros de inversión. "
            "Verifica la estructura del Excel."
        )

    df = pd.DataFrame(records)
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)

    logger.info(
        "Inversión DANE parseada: %d trimestres (%s – %s)",
        len(df),
        df["date"].min().strftime("%Y-%m"),
        df["date"].max().strftime("%Y-%m"),
    )
    return df


# ═══════════════════════════════════════════════════════════════════════
# Pipeline orquestador
# ═══════════════════════════════════════════════════════════════════════


def run_dane_gdp_expenditure_pipeline(
    config: DANEGDPExpenditureConfig = DANE_GDP_EXPENDITURE_CONFIG,
    raw_dir: Path = RAW_DANE_DIR,
    processed_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Ejecuta el pipeline completo: scraping → descarga → parsing → guardado."""
    logger.info("── Iniciando pipeline Inversión DANE ──")

    html = fetch_expenditure_page(config)
    url = extract_expenditure_xlsx_link(html, config)
    xlsx_path = download_expenditure_excel(url, raw_dir, config)
    df = parse_expenditure_excel(xlsx_path, config)

    output_path = processed_dir / config.processed_filename
    save_csv(df, output_path)
    logger.info("Guardado: %s (%d filas)", output_path.name, len(df))
    return df
