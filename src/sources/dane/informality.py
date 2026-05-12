"""Extracción y limpieza de datos de informalidad laboral del DANE (GEIH-EISS).

Pipeline de 3 capas:

1. **SCRAPING**  — Descarga la página de empleo informal y seguridad social
   del DANE, extrae el enlace al Excel GEIHEISS más reciente.
2. **DESCARGA**  — Descarga el Excel seleccionado.
3. **PARSING**   — Lee la hoja *Prop informalidad*, extrae la fila
   *13 Ciudades y A.M.*, reconstruye las fechas de los trimestres
   móviles y produce el formato largo estándar.

Notas de frecuencia:
  - La serie es **trimestre móvil** (ene-mar, feb-abr, …).
  - La fecha se asigna al **último mes** del trimestre móvil, que
    es la convención habitual en análisis macro colombiano.
  - Cobertura: 2021-01 en adelante (inicio de la publicación EISS).
"""

from __future__ import annotations

import logging
import re
from datetime import date
from io import BytesIO
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from src.config import (
    GEIH_INFORMALITY_CONFIG,
    GEIHInformalityConfig,
    PROCESSED_DIR,
    RAW_DANE_DIR,
)
from src.io_utils import save_csv
from src.sources.dane.common import MONTH_ABBR_ES, make_dane_session

logger = logging.getLogger("nairu_pipeline.dane.informality")


# ═══════════════════════════════════════════════════════════════════════
# 1. SCRAPING
# ═══════════════════════════════════════════════════════════════════════


def fetch_informality_page(
    config: GEIHInformalityConfig = GEIH_INFORMALITY_CONFIG,
    session: "requests.Session | None" = None,  # type: ignore[name-defined]
) -> str:
    """Descarga el HTML de la página de empleo informal del DANE.

    Si ``session`` no se proporciona, se crea una efímera con retries y
    keep-alive. Para encadenar la descarga del Excel después, conviene
    pasar la misma sesión y reutilizar la conexión TLS.
    """
    logger.info("Descargando página informalidad DANE: %s", config.page_url)
    sess = session or make_dane_session(headers=config.http_headers)
    response = sess.get(config.page_url, timeout=config.timeout)
    response.raise_for_status()
    logger.info("Página descargada: %d bytes", len(response.content))
    return response.text


def extract_informality_xlsx_link(
    html: str,
    config: GEIHInformalityConfig = GEIH_INFORMALITY_CONFIG,
) -> str:
    """Extrae la URL del anexo GEIHEISS más reciente desde el HTML."""
    soup = BeautifulSoup(html, "html.parser")
    pattern = re.compile(config.link_pattern, re.IGNORECASE)

    links: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if pattern.search(href):
            absolute = urljoin(config.base_url, href)
            links.append(absolute)
            logger.debug("Enlace GEIHEISS encontrado: %s", absolute)

    if not links:
        raise ValueError(
            "No se encontró ningún enlace GEIHEISS .xlsx en la página del DANE. "
            f"URL: {config.page_url}"
        )

    # El más reciente suele ser el primero (posición en la página)
    selected = links[0]
    logger.info("Anexo GEIHEISS seleccionado: %s", selected)
    return selected


# ═══════════════════════════════════════════════════════════════════════
# 2. DESCARGA
# ═══════════════════════════════════════════════════════════════════════


def download_informality_excel(
    url: str,
    output_dir: Path = RAW_DANE_DIR,
    config: GEIHInformalityConfig = GEIH_INFORMALITY_CONFIG,
    session: "requests.Session | None" = None,  # type: ignore[name-defined]
) -> Path:
    """Descarga el Excel GEIHEISS y lo guarda en data/raw/dane/."""
    logger.info("Descargando Excel GEIHEISS: %s", url)
    sess = session or make_dane_session(headers=config.http_headers)
    response = sess.get(url, timeout=config.timeout)
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


def _parse_trimestre_date(trimestre_str: str, year: int) -> date | None:
    """Convierte un string de trimestre móvil a fecha (último mes del período).

    Formatos soportados:
      "Ene - mar"           + year=2021 → date(2021, 3, 1)
      "Oct - dic"           + year=2021 → date(2021, 12, 1)
      "Nov 21 - ene 22"     + any       → date(2022, 1, 1)
      "Dic 21 - feb 22"     + any       → date(2022, 2, 1)
    """
    s = str(trimestre_str).strip().lower()
    if s in ("nan", ""):
        return None

    # Patrón inter-año: "nov 21 - ene 22"
    cross = re.search(r"\w+\s+\d+\s*-\s*(\w+)\s+(\d{2})", s)
    if cross:
        last_month = MONTH_ABBR_ES.get(cross.group(1))
        last_year = 2000 + int(cross.group(2))
        if last_month:
            return date(last_year, last_month, 1)

    # Patrón normal: "ene - mar" o "abr - jun "
    normal = re.search(r"\w+\s*-\s*(\w+)", s)
    if normal:
        last_month = MONTH_ABBR_ES.get(normal.group(1).strip())
        if last_month and year:
            return date(int(year), last_month, 1)

    return None


def parse_informality_excel(
    path: Path,
    config: GEIHInformalityConfig = GEIH_INFORMALITY_CONFIG,
) -> pd.DataFrame:
    """Parsea la hoja 'Prop informalidad' y devuelve el formato largo estándar.

    Columnas de salida:
        date, year, month, informality_rate_13c, source, download_date
    """
    logger.info("Parseando Excel informalidad: %s", path.name)

    df_raw = pd.read_excel(path, sheet_name=config.sheet_name, header=None)

    # ── Localizar filas de años y trimestres ──────────────────────────
    year_row_idx = config.year_row          # fila con 2021, 2022, …
    trimestre_row_idx = config.trimestre_row  # fila con "Ene - mar", etc.

    years_raw = df_raw.iloc[year_row_idx]
    trimestres_raw = df_raw.iloc[trimestre_row_idx]

    # Forward-fill los años (están en la primera columna de cada grupo)
    years_filled = years_raw.ffill()

    # ── Localizar fila de 13 ciudades ────────────────────────────────
    target_pattern = re.compile(config.city_label_pattern, re.IGNORECASE)
    data_row_idx: int | None = None
    for i, row in df_raw.iterrows():
        first_cell = str(row.iloc[0]).strip()
        if target_pattern.search(first_cell):
            data_row_idx = i
            break

    if data_row_idx is None:
        raise ValueError(
            f"No se encontró la fila '{config.city_label_pattern}' "
            f"en la hoja '{config.sheet_name}' del Excel."
        )
    logger.info("Fila '13 Ciudades y A.M.' detectada en índice %d", data_row_idx)

    data_row = df_raw.iloc[data_row_idx]

    # ── Reconstruir fechas y extraer valores ─────────────────────────
    records: list[dict] = []
    today_str = date.today().isoformat()

    for col_idx in range(1, len(data_row)):
        trimestre_str = trimestres_raw.iloc[col_idx]
        year_val = years_filled.iloc[col_idx]
        value = data_row.iloc[col_idx]

        if pd.isna(trimestre_str) or pd.isna(value):
            continue

        try:
            year_int = int(float(str(year_val)))
        except (ValueError, TypeError):
            continue

        parsed_date = _parse_trimestre_date(str(trimestre_str), year_int)
        if parsed_date is None:
            logger.debug("No se pudo parsear trimestre: '%s'", trimestre_str)
            continue

        records.append({
            "date": pd.Timestamp(parsed_date),
            "year": parsed_date.year,
            "month": parsed_date.month,
            "informality_rate_13c": round(float(value), 4),
            "source": config.source_label,
            "download_date": today_str,
        })

    if not records:
        raise ValueError(
            "No se extrajeron registros de informalidad. "
            "Verifica la estructura del Excel."
        )

    df = pd.DataFrame(records)
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)

    logger.info(
        "Informalidad parseada: %d observaciones (%s – %s)",
        len(df),
        df["date"].min().strftime("%Y-%m"),
        df["date"].max().strftime("%Y-%m"),
    )
    return df


# ═══════════════════════════════════════════════════════════════════════
# Pipeline orquestador
# ═══════════════════════════════════════════════════════════════════════


def run_informality_pipeline(
    config: GEIHInformalityConfig = GEIH_INFORMALITY_CONFIG,
    raw_dir: Path = RAW_DANE_DIR,
    processed_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Ejecuta el pipeline completo: scraping → descarga → parsing → guardado.

    Reutiliza una sola ``requests.Session`` con keep-alive para los dos
    GETs (página índice + descarga del Excel) — evita renegociar TLS
    contra ``www.dane.gov.co``.
    """
    logger.info("── Iniciando pipeline informalidad DANE ──")

    session = make_dane_session(headers=config.http_headers)
    html = fetch_informality_page(config, session=session)
    url = extract_informality_xlsx_link(html, config)
    xlsx_path = download_informality_excel(url, raw_dir, config, session=session)
    df = parse_informality_excel(xlsx_path, config)

    output_path = processed_dir / config.processed_filename
    save_csv(df, output_path)
    logger.info("Guardado: %s (%d filas)", output_path.name, len(df))
    return df
