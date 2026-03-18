"""Extracción y limpieza de datos de desempleo del DANE (GEIH).

Pipeline de 3 capas (análogo a ``ipc.py``):

1. **SCRAPING**  — Descarga la página temática de empleo del DANE,
   extrae enlaces a archivos .xlsx de la GEIH y selecciona el
   anexo más reciente.
2. **DESCARGA**  — Descarga el Excel seleccionado + guarda HTML.
3. **PARSING**   — Lee la hoja *Total nacional* (formato pivoteado:
   conceptos × año·mes), extrae la fila *Tasa de Desocupación (TD)*,
   reconstruye fechas y genera el formato largo estándar.

También conserva soporte para el perfil **placeholder** CSV (BLS)
usado en los tests unitarios.
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
    ACTIVE_PROFILE,
    GEIH_CONFIG,
    GEIHConfig,
    PROCESSED_COLUMNS,
    PROCESSED_DIR,
    RAW_DANE_DIR,
    SourceProfile,
)
from src.io_utils import save_csv

logger = logging.getLogger("nairu_pipeline.dane.unemployment")


# ═══════════════════════════════════════════════════════════════════════
# 1. SCRAPING  (fuente real DANE)
# ═══════════════════════════════════════════════════════════════════════


def fetch_geih_page(config: GEIHConfig = GEIH_CONFIG) -> str:
    """Descarga el HTML de la página temática de empleo del DANE."""
    logger.info("Descargando página GEIH: %s", config.page_url)
    response = requests.get(
        config.page_url, headers=config.http_headers, timeout=config.timeout,
    )
    response.raise_for_status()
    logger.info("Página descargada: %d bytes", len(response.content))
    return response.text


def save_html_snapshot(
    html: str,
    output_dir: Path = RAW_DANE_DIR,
    filename: str | None = None,
    config: GEIHConfig = GEIH_CONFIG,
) -> Path:
    """Guarda copia del HTML para auditoría."""
    output_dir.mkdir(parents=True, exist_ok=True)
    fname = filename or config.raw_html_filename
    path = output_dir / fname
    path.write_text(html, encoding="utf-8")
    logger.info("HTML guardado: %s (%.1f KB)", path.name, len(html) / 1024)
    return path


def extract_geih_xlsx_links(
    html: str,
    config: GEIHConfig = GEIH_CONFIG,
) -> list[dict[str, str]]:
    """Extrae enlaces a archivos .xlsx del anexo GEIH desde el HTML."""
    soup = BeautifulSoup(html, "html.parser")
    pattern = re.compile(config.link_pattern, re.IGNORECASE)

    links: list[dict[str, str]] = []
    for anchor in soup.find_all("a", href=pattern):
        href = anchor["href"]
        absolute_url = urljoin(config.base_url, href)
        text = anchor.get_text(strip=True)
        links.append({"url": absolute_url, "text": text, "href": href})
        logger.debug("Enlace GEIH encontrado: %s → %s", text, absolute_url)

    logger.info("Total enlaces GEIH .xlsx encontrados: %d", len(links))
    return links


def select_geih_link(
    links: list[dict[str, str]],
) -> dict[str, str]:
    """Selecciona el enlace al anexo GEIH más reciente."""
    if not links:
        raise ValueError(
            "No se encontraron enlaces GEIH .xlsx en la página del DANE."
        )

    if len(links) > 1:
        links = _sort_geih_by_period(links)

    selected = links[0]
    logger.info("Anexo GEIH seleccionado: %s", selected["url"])
    return selected


def parse_period_from_geih_href(href: str) -> tuple[int, int] | None:
    """Extrae (year, month) del nombre del archivo GEIH.

    Ejemplo: ``anex-GEIH-ene2026.xlsx`` → ``(2026, 1)``
    """
    month_map = {
        "ene": 1, "feb": 2, "mar": 3, "abr": 4,
        "may": 5, "jun": 6, "jul": 7, "ago": 8,
        "sep": 9, "oct": 10, "nov": 11, "dic": 12,
    }
    m = re.search(r"([a-z]{3})(\d{4})\.xlsx$", href, re.IGNORECASE)
    if m:
        month_num = month_map.get(m.group(1).lower())
        year = int(m.group(2))
        if month_num:
            return (year, month_num)
    return None


def _sort_geih_by_period(
    links: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Ordena enlaces GEIH por periodo (más reciente primero)."""

    def sort_key(lnk: dict[str, str]) -> tuple[int, int]:
        parsed = parse_period_from_geih_href(lnk["href"])
        return parsed if parsed else (0, 0)

    return sorted(links, key=sort_key, reverse=True)


# ═══════════════════════════════════════════════════════════════════════
# 2. DESCARGA
# ═══════════════════════════════════════════════════════════════════════


def download_geih_excel(
    url: str,
    output_dir: Path = RAW_DANE_DIR,
    filename: str | None = None,
    config: GEIHConfig = GEIH_CONFIG,
) -> Path:
    """Descarga el archivo Excel del GEIH."""
    logger.info("Descargando Excel GEIH: %s", url)
    response = requests.get(
        url, headers=config.http_headers, timeout=config.timeout,
    )
    response.raise_for_status()

    output_dir.mkdir(parents=True, exist_ok=True)
    fname = filename or config.raw_xlsx_filename
    path = output_dir / fname
    path.write_bytes(response.content)

    size_kb = len(response.content) / 1024
    logger.info("Excel descargado: %s (%.1f KB)", path.name, size_kb)
    return path


# ═══════════════════════════════════════════════════════════════════════
# 3. PARSING  (Excel pivoteado GEIH)
# ═══════════════════════════════════════════════════════════════════════


def _detect_year_row(
    df_raw: pd.DataFrame,
    max_scan: int = 30,
) -> int:
    """Detecta la fila que contiene los años (2001, 2002, ...).

    Busca la fila con más celdas que parezcan un año de 4 dígitos.
    """
    best_row = 0
    best_count = 0
    scan_limit = min(max_scan, len(df_raw))

    for i in range(scan_limit):
        year_count = sum(
            1 for v in df_raw.iloc[i]
            if pd.notna(v) and re.match(r"^\d{4}(\.0)?$", str(v).strip())
        )
        if year_count > best_count:
            best_count = year_count
            best_row = i

    logger.info("Fila de años detectada: %d (%d años encontrados)", best_row, best_count)
    return best_row


def _detect_month_row(
    df_raw: pd.DataFrame,
    year_row: int,
    month_abbrevs: set[str] | None = None,
    max_scan: int = 5,
) -> int:
    """Detecta la fila de abreviaturas de mes (Ene, Feb, ...).

    Busca en las filas inmediatamente después de ``year_row``.
    """
    if month_abbrevs is None:
        month_abbrevs = {"ene", "feb", "mar", "abr", "may", "jun",
                         "jul", "ago", "sep", "oct", "nov", "dic"}

    for offset in range(1, max_scan + 1):
        idx = year_row + offset
        if idx >= len(df_raw):
            break
        row_vals = {
            str(v).strip().lower()
            for v in df_raw.iloc[idx]
            if pd.notna(v)
        }
        matches = row_vals & month_abbrevs
        if len(matches) >= 6:  # al menos mitad de los meses
            logger.info("Fila de meses detectada: %d (%d coincidencias)", idx, len(matches))
            return idx

    # Fallback: asumir fila siguiente a year_row
    fallback = year_row + 1
    logger.warning("No se detectó fila de meses; usando fallback: %d", fallback)
    return fallback


def _detect_td_row(
    df_raw: pd.DataFrame,
    label_pattern: str,
    start_row: int = 0,
) -> int:
    """Detecta la fila que contiene la Tasa de Desocupación (TD)."""
    pat = re.compile(label_pattern, re.IGNORECASE)
    for i in range(start_row, len(df_raw)):
        first_cell = str(df_raw.iloc[i, 0]).strip()
        if pat.search(first_cell):
            logger.info("Fila TD detectada: %d ('%s')", i, first_cell[:60])
            return i

    raise ValueError(
        f"No se encontró fila con patrón '{label_pattern}' en la hoja. "
        f"Filas escaneadas: {start_row}–{len(df_raw) - 1}"
    )


def _build_date_columns(
    df_raw: pd.DataFrame,
    year_row: int,
    month_row: int,
    month_map: dict[str, int],
) -> list[dict]:
    """Reconstruye la lista de (col_index, year, month) desde las cabeceras.

    En el Excel GEIH, la fila de años tiene el año solo en la primera
    columna de cada grupo de 12 (las demás son None), y la fila de meses
    tiene abreviaturas en TODAS las columnas de datos.

    Estrategia: iterar columnas, propagar (forward-fill) el último año
    visto, y combinar con el mes de cada columna.
    """
    year_values = list(df_raw.iloc[year_row])
    month_values = list(df_raw.iloc[month_row])
    num_cols = len(year_values)

    result: list[dict] = []
    current_year: int | None = None

    for col_idx in range(1, num_cols):  # columna 0 = "Concepto"
        # Actualizar año si la celda tiene un valor
        raw_year = year_values[col_idx]
        if pd.notna(raw_year):
            try:
                current_year = int(float(str(raw_year).strip()))
            except (ValueError, TypeError):
                pass

        if current_year is None:
            continue

        # Leer mes
        raw_month = str(month_values[col_idx]).strip().lower() if pd.notna(month_values[col_idx]) else ""
        month_num = month_map.get(raw_month)
        if month_num is None:
            continue

        result.append({
            "col_idx": col_idx,
            "year": current_year,
            "month": month_num,
        })

    logger.info(
        "Columnas fecha reconstruidas: %d (de %d → %d-%02d a %d-%02d)",
        len(result),
        num_cols - 1,
        result[0]["year"] if result else 0,
        result[0]["month"] if result else 0,
        result[-1]["year"] if result else 0,
        result[-1]["month"] if result else 0,
    )
    return result


def load_geih_excel(
    xlsx_path: Path,
    config: GEIHConfig = GEIH_CONFIG,
) -> pd.DataFrame:
    """Lee el Excel pivoteado del GEIH y extrae la TD en formato largo.

    Pasos:
    1. Leer la hoja sin header (todo como strings para robustez).
    2. Detectar filas de años, meses y TD.
    3. Reconstruir las fechas desde las cabeceras.
    4. Extraer los valores de TD y generar DataFrame largo.
    """
    logger.info("Cargando Excel GEIH: %s", xlsx_path)

    df_raw = pd.read_excel(
        xlsx_path,
        sheet_name=config.sheet_name,
        header=None,
        engine="openpyxl",
    )

    logger.info(
        "Excel leído: %d filas × %d cols, hoja='%s'",
        len(df_raw), len(df_raw.columns), config.sheet_name,
    )

    # Detectar filas clave
    year_row = config.year_row
    month_row = config.month_row

    if year_row is None:
        year_row = _detect_year_row(df_raw)
    if month_row is None:
        month_row = _detect_month_row(df_raw, year_row, set(config.month_map.keys()))

    td_row = _detect_td_row(df_raw, config.td_label_pattern, start_row=month_row)

    # Reconstruir fechas
    date_cols = _build_date_columns(df_raw, year_row, month_row, config.month_map)

    if not date_cols:
        raise ValueError("No se pudieron reconstruir columnas fecha del Excel GEIH.")

    # Extraer valores de TD
    td_values = df_raw.iloc[td_row]
    records: list[dict] = []
    for dc in date_cols:
        raw_val = td_values.iloc[dc["col_idx"]]
        val = pd.to_numeric(raw_val, errors="coerce")
        if pd.notna(val):
            records.append({
                "year": dc["year"],
                "month": dc["month"],
                "unemployment_rate": float(val),
            })

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(
        df["year"].astype(str) + "-"
        + df["month"].astype(str).str.zfill(2) + "-01"
    )
    df = df.sort_values("date").reset_index(drop=True)

    logger.info(
        "GEIH parseado: %d observaciones, %s → %s",
        len(df), df["date"].min(), df["date"].max(),
    )
    return df[["date", "year", "month", "unemployment_rate"]]


# ═══════════════════════════════════════════════════════════════════════
# TRANSFORMACIÓN (normalización común)
# ═══════════════════════════════════════════════════════════════════════


def clean_geih_data(
    xlsx_path: Path,
    config: GEIHConfig = GEIH_CONFIG,
) -> pd.DataFrame:
    """Pipeline: carga Excel → formato largo → esquema final."""
    df = load_geih_excel(xlsx_path, config)

    df["source"] = "DANE"
    df["download_date"] = date.today().isoformat()
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)

    # Eliminar duplicados
    dupes_before = len(df)
    df = df.drop_duplicates(subset=["date"], keep="first").copy()
    dupes_dropped = dupes_before - len(df)
    if dupes_dropped > 0:
        logger.warning("Se eliminaron %d filas duplicadas por fecha", dupes_dropped)

    df = df[PROCESSED_COLUMNS].sort_values("date").reset_index(drop=True)

    logger.info(
        "Desempleo limpio: %d filas, rango: %s → %s",
        len(df), df["date"].min(), df["date"].max(),
    )
    return df


# ═══════════════════════════════════════════════════════════════════════
# CARGA
# ═══════════════════════════════════════════════════════════════════════


def save_processed_data(
    df: pd.DataFrame,
    output_dir: Path = PROCESSED_DIR,
    filename: str | None = None,
    config: GEIHConfig = GEIH_CONFIG,
) -> Path:
    """Guarda el dataset de desempleo procesado en disco."""
    fname = filename or config.processed_filename
    output_path = output_dir / fname
    save_csv(df, output_path)
    logger.info("Dataset procesado guardado en: %s", output_path)
    return output_path


# ═══════════════════════════════════════════════════════════════════════
# ORQUESTACIÓN  (fuente real)
# ═══════════════════════════════════════════════════════════════════════


def run_geih_pipeline(
    config: GEIHConfig = GEIH_CONFIG,
    output_dir: Path = PROCESSED_DIR,
    raw_dir: Path = RAW_DANE_DIR,
) -> pd.DataFrame:
    """Pipeline completo: scraping → descarga → parse → validate → save."""
    # 1. Scraping
    html = fetch_geih_page(config)
    save_html_snapshot(html, output_dir=raw_dir, config=config)

    # 2. Seleccionar enlace
    links = extract_geih_xlsx_links(html, config)
    target = select_geih_link(links)

    # 3. Descargar Excel
    xlsx_path = download_geih_excel(
        url=target["url"], output_dir=raw_dir, config=config,
    )

    # 4. Parsear y limpiar
    df = clean_geih_data(xlsx_path, config)

    # 5. Guardar
    save_processed_data(df, output_dir=output_dir, config=config)

    return df


# ═══════════════════════════════════════════════════════════════════════
# SOPORTE LEGACY  (placeholder CSV para tests)
# ═══════════════════════════════════════════════════════════════════════


def download_raw_data(
    profile: SourceProfile = ACTIVE_PROFILE,
    output_dir: Path = RAW_DANE_DIR,
) -> Path:
    """Descarga datos crudos (placeholder CSV). Legacy."""
    from src.sources.dane.common import download_file

    logger.info("Fuente activa: %s", profile.name)
    output_path = output_dir / profile.raw_filename
    download_file(
        url=profile.url,
        output_path=output_path,
        timeout=profile.timeout,
        headers=profile.http_headers,
    )
    logger.info("Formato: %s", profile.file_format)
    return output_path


def clean_placeholder_data(raw_path: Path) -> pd.DataFrame:
    """Limpia el dataset placeholder (BLS CSV)."""
    logger.info("Parsing CSV placeholder desde: %s", raw_path)
    df = pd.read_csv(raw_path)
    df.columns = df.columns.str.strip().str.lower()

    required_raw = {"year", "unemployed_percent"}
    available = set(df.columns)
    if not required_raw.issubset(available):
        missing = required_raw - available
        raise ValueError(
            f"Columnas requeridas faltantes en CSV placeholder: {missing}. "
            f"Columnas disponibles: {list(df.columns)}"
        )

    df["month"] = 1
    df["date"] = pd.to_datetime(df["year"].astype(int).astype(str) + "-01-01")
    df = df.rename(columns={"unemployed_percent": "unemployment_rate"})
    return df


def clean_dane_excel_data(
    raw_path: Path,
    profile: SourceProfile,
) -> pd.DataFrame:
    """Limpia un archivo Excel en formato SourceProfile (legacy)."""
    from src.sources.dane.common import (
        auto_map_columns,
        detect_header_row,
        detect_relevant_sheet,
    )

    logger.info("Parsing Excel DANE (legacy) desde: %s", raw_path)

    # Seleccionar hoja
    if profile.sheet_name is not None:
        sheet = profile.sheet_name
    else:
        sheet = detect_relevant_sheet(raw_path, keywords=profile.header_keywords)

    # Detectar encabezado
    df_no_header = pd.read_excel(
        raw_path, sheet_name=sheet, header=None,
        engine="openpyxl", dtype=str,
    )
    if profile.header_row is not None:
        header_idx = profile.header_row
    else:
        header_idx = detect_header_row(
            df_no_header,
            keywords=profile.header_keywords,
            max_scan=profile.header_scan_rows,
        )

    df = pd.read_excel(
        raw_path, sheet_name=sheet, header=header_idx, engine="openpyxl",
    )
    unnamed_cols = [c for c in df.columns if str(c).startswith("Unnamed")]
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)
    df = df.dropna(how="all").reset_index(drop=True)
    df.columns = (
        df.columns.astype(str).str.strip().str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^\w]", "", regex=True)
    )

    # Mapear columnas
    if profile.column_mapping is not None:
        col_map = profile.column_mapping
    else:
        col_map = auto_map_columns(list(df.columns), profile.column_patterns)
    if not col_map:
        raise ValueError(
            "No se pudo mapear ninguna columna. "
            f"Columnas disponibles: {list(df.columns)}"
        )
    df = df.rename(columns=col_map)

    if "date" not in df.columns and "year" in df.columns:
        month_col = df["month"] if "month" in df.columns else 1
        df["date"] = pd.to_datetime(
            df["year"].astype(int).astype(str) + "-"
            + pd.Series(month_col).astype(int).astype(str).str.zfill(2)
            + "-01"
        )
    if "year" not in df.columns and "date" in df.columns:
        df["year"] = pd.to_datetime(df["date"]).dt.year
    if "month" not in df.columns and "date" in df.columns:
        df["month"] = pd.to_datetime(df["date"]).dt.month
    elif "month" not in df.columns:
        df["month"] = 1

    return df


def clean_unemployment_data(
    raw_path: Path,
    profile: SourceProfile = ACTIVE_PROFILE,
) -> pd.DataFrame:
    """Limpia y estandariza datos de desempleo (placeholder / legacy).

    Para la fuente DANE real, usar ``run_geih_pipeline()`` en su lugar.
    """
    logger.info("Cargando datos crudos desde: %s", raw_path)

    if profile.file_format == "csv":
        df = clean_placeholder_data(raw_path)
    elif profile.file_format == "xlsx":
        df = clean_dane_excel_data(raw_path, profile)
    else:
        raise ValueError(f"Formato no soportado: {profile.file_format}")

    df["source"] = "DANE"
    df["download_date"] = date.today().isoformat()
    df["unemployment_rate"] = pd.to_numeric(df["unemployment_rate"], errors="coerce")
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    if "month" in df.columns:
        df["month"] = pd.to_numeric(df["month"], errors="coerce").astype("Int64")

    rows_before = len(df)
    df = df.dropna(subset=["unemployment_rate", "date"]).copy()
    rows_dropped = rows_before - len(df)
    if rows_dropped > 0:
        logger.warning("Se eliminaron %d filas con valores nulos", rows_dropped)

    dupes_before = len(df)
    df = df.drop_duplicates(subset=["date"], keep="first").copy()
    dupes_dropped = dupes_before - len(df)
    if dupes_dropped > 0:
        logger.warning("Se eliminaron %d filas duplicadas por fecha", dupes_dropped)

    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    df = df[PROCESSED_COLUMNS].sort_values("date").reset_index(drop=True)

    logger.info("Dataset limpio: %d filas, columnas: %s", len(df), list(df.columns))
    return df
