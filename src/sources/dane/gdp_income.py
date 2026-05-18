"""Extracción del PIB trimestral enfoque del ingreso del DANE.

Pipeline de 3 capas:

1. **SCRAPING**  — Página *PIB - Información técnica*, anexo
   ``anex-PIB-EnfoqueCorriente-{trim}{YYYY}.xlsx``.
2. **DESCARGA**  — Guarda el Excel en data/raw/dane/.
3. **PARSING**   — Hoja *PIB_Ingreso*, bloque de niveles a precios
   corrientes (filas 13-41):
     - D1  → Remuneración de los asalariados
     - B2  → Excedente Bruto de Explotación
     - B3  → Ingreso Mixto

Notas:
  - Solo disponible desde 2016-Q1 (nueva base metodológica del DANE).
  - Valores en **miles de millones de pesos corrientes** (no desest.).
  - Frecuencia: trimestral (Q1→Ene, Q2→Abr, Q3→Jul, Q4→Oct).
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
    DANE_GDP_INCOME_CONFIG,
    DANEGDPIncomeConfig,
    PROCESSED_DIR,
    RAW_DANE_DIR,
)
from src.io_utils import save_csv
from src.sources.dane.common import dane_request_kwargs

logger = logging.getLogger("nairu_pipeline.dane.gdp_income")

_ROMAN_TO_QUARTER = {"I": 1, "II": 2, "III": 3, "IV": 4}
_QUARTER_TO_MONTH = {1: 1, 2: 4, 3: 7, 4: 10}


# ═══════════════════════════════════════════════════════════════════════
# 1. SCRAPING
# ═══════════════════════════════════════════════════════════════════════

def fetch_income_page(
    config: DANEGDPIncomeConfig = DANE_GDP_INCOME_CONFIG,
) -> str:
    logger.info("Descargando página PIB ingreso DANE: %s", config.page_url)
    r = requests.get(config.page_url, headers=config.http_headers,
                     **dane_request_kwargs(timeout=config.timeout))
    r.raise_for_status()
    return r.text


def extract_income_xlsx_link(
    html: str,
    config: DANEGDPIncomeConfig = DANE_GDP_INCOME_CONFIG,
) -> str:
    soup = BeautifulSoup(html, "html.parser")
    pattern = re.compile(config.link_pattern, re.IGNORECASE)
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if pattern.search(href):
            abs_url = urljoin(config.base_url, href)
            if abs_url not in links:
                links.append(abs_url)

    if not links:
        raise ValueError(
            f"No se encontró el anexo PIB-EnfoqueCorriente en {config.page_url}"
        )

    roman = {"I": 1, "II": 2, "III": 3, "IV": 4}
    ext = re.compile(r"-(I{1,3}|IV)trim(\d{4})\.xlsx$")

    def _key(u: str) -> tuple[int, int]:
        m = ext.search(u)
        return (int(m.group(2)), roman.get(m.group(1), 0)) if m else (0, 0)

    links.sort(key=_key, reverse=True)
    logger.info("Anexo PIB Ingreso seleccionado: %s", links[0])
    return links[0]


# ═══════════════════════════════════════════════════════════════════════
# 2. DESCARGA
# ═══════════════════════════════════════════════════════════════════════

def download_income_excel(
    url: str,
    output_dir: Path = RAW_DANE_DIR,
    config: DANEGDPIncomeConfig = DANE_GDP_INCOME_CONFIG,
) -> Path:
    logger.info("Descargando Excel PIB ingreso: %s", url)
    r = requests.get(url, headers=config.http_headers,
                     **dane_request_kwargs(timeout=config.timeout))
    r.raise_for_status()
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / config.raw_xlsx_filename
    path.write_bytes(r.content)
    logger.info("Excel descargado: %s (%.1f KB)", path.name, len(r.content) / 1024)
    return path


# ═══════════════════════════════════════════════════════════════════════
# 3. PARSING
# ═══════════════════════════════════════════════════════════════════════

def _parse_quarter(s: str) -> int | None:
    return _ROMAN_TO_QUARTER.get(str(s).strip().upper())


def _clean_year(v) -> int | None:
    if pd.isna(v):
        return None
    s = re.sub(r"[^\d]", "", str(v).strip())
    return int(s) if s else None


def parse_income_excel(
    path: Path,
    config: DANEGDPIncomeConfig = DANE_GDP_INCOME_CONFIG,
) -> pd.DataFrame:
    """Parsea la hoja PIB_Ingreso y devuelve Remuneración, EBE e Ingreso Mixto.

    Columnas de salida:
        date, year, quarter, compensation_employees,
        gross_operating_surplus, mixed_income, source, download_date
    """
    logger.info("Parseando Excel PIB ingreso DANE: %s", path.name)
    df_raw = pd.read_excel(path, sheet_name=config.sheet_name, header=None)

    years_row = df_raw.iloc[config.year_row]
    quarters_raw = df_raw.iloc[config.quarter_row]

    # Inferir años secuencialmente
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

    concept_col = df_raw.iloc[:, config.concept_col].astype(str).str.strip()

    def _get_row(label: str) -> pd.Series:
        matches = concept_col.str.lower() == label.lower()
        if not matches.any():
            raise ValueError(f"No se encontró fila '{label}' en la columna {config.concept_col}")
        idx = int(matches.idxmax())
        logger.info("Fila '%s' en índice %d", label, idx)
        return df_raw.iloc[idx]

    row_comp   = _get_row(config.label_compensation)
    row_ebt    = _get_row(config.label_gross_surplus)
    row_mixed  = _get_row(config.label_mixed_income)

    records: list[dict] = []
    today_str = date.today().isoformat()

    for col_idx in range(config.data_start_col, df_raw.shape[1]):
        year_int = inferred_years.get(col_idx)
        q_str = quarters_raw.iloc[col_idx]
        if year_int is None or pd.isna(q_str):
            continue
        q_int = _parse_quarter(str(q_str))
        if q_int is None:
            continue

        def _val(row: pd.Series) -> float | None:
            v = row.iloc[col_idx]
            try:
                return round(float(v), 4) if not pd.isna(v) else None
            except (ValueError, TypeError):
                return None

        v_comp  = _val(row_comp)
        v_ebt   = _val(row_ebt)
        v_mixed = _val(row_mixed)

        if v_comp is None and v_ebt is None and v_mixed is None:
            continue

        month = _QUARTER_TO_MONTH[q_int]
        records.append({
            "date":                    pd.Timestamp(year=year_int, month=month, day=1),
            "year":                    year_int,
            "quarter":                 q_int,
            "compensation_employees":  v_comp,
            "gross_operating_surplus": v_ebt,
            "mixed_income":            v_mixed,
            "source":                  config.source_label,
            "download_date":           today_str,
        })

    if not records:
        raise ValueError("No se extrajeron registros del enfoque de ingreso.")

    df = (
        pd.DataFrame(records)
        .sort_values("date")
        .drop_duplicates(subset=["date"])
        .reset_index(drop=True)
    )
    logger.info(
        "PIB ingreso DANE parseado: %d trimestres (%s – %s)",
        len(df),
        df["date"].min().strftime("%Y-%m"),
        df["date"].max().strftime("%Y-%m"),
    )
    return df


# ═══════════════════════════════════════════════════════════════════════
# Pipeline orquestador
# ═══════════════════════════════════════════════════════════════════════

def run_dane_gdp_income_pipeline(
    config: DANEGDPIncomeConfig = DANE_GDP_INCOME_CONFIG,
    raw_dir: Path = RAW_DANE_DIR,
    processed_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    logger.info("── Iniciando pipeline PIB Ingreso DANE ──")
    html = fetch_income_page(config)
    url = extract_income_xlsx_link(html, config)
    xlsx_path = download_income_excel(url, raw_dir, config)
    df = parse_income_excel(xlsx_path, config)
    output_path = processed_dir / config.processed_filename
    save_csv(df, output_path)
    logger.info("Guardado: %s (%d filas)", output_path.name, len(df))
    return df
