"""Extracción, parsing y limpieza del IPC desde el DANE.

Estrategia de 3 capas:
1. SCRAPING   — Descarga la página HTML del IPC, extrae enlaces .xlsx,
                selecciona el archivo de índices (serie de empalme).
2. DESCARGA   — Descarga el Excel seleccionado + guarda HTML de respaldo.
3. PARSING    — Lee el Excel pivoteado (meses × años), lo convierte
                a formato largo (date, year, month, ipc_index) y limpia.
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
    IPC_CONFIG,
    IPC_PROCESSED_COLUMNS,
    IPCConfig,
    PROCESSED_DIR,
    RAW_DANE_DIR,
)
from src.io_utils import save_csv

logger = logging.getLogger("nairu_pipeline.dane.ipc")


# ═══════════════════════════════════════════════════════════════════════
# 1. SCRAPING
# ═══════════════════════════════════════════════════════════════════════


def fetch_ipc_page(config: IPCConfig = IPC_CONFIG) -> str:
    """Descarga el HTML de la página del IPC del DANE."""
    logger.info("Descargando página IPC: %s", config.page_url)
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
    config: IPCConfig = IPC_CONFIG,
) -> Path:
    """Guarda una copia del HTML para auditoría/debugging."""
    output_dir.mkdir(parents=True, exist_ok=True)
    fname = filename or config.raw_html_filename
    path = output_dir / fname
    path.write_text(html, encoding="utf-8")
    logger.info("HTML guardado: %s (%.1f KB)", path.name, len(html) / 1024)
    return path


def extract_ipc_xlsx_links(
    html: str,
    config: IPCConfig = IPC_CONFIG,
) -> list[dict[str, str]]:
    """Extrae enlaces a archivos .xlsx del IPC desde el HTML."""
    soup = BeautifulSoup(html, "html.parser")
    pattern = re.compile(config.link_pattern, re.IGNORECASE)

    links = []
    for anchor in soup.find_all("a", href=pattern):
        href = anchor["href"]
        absolute_url = urljoin(config.base_url, href)
        text = anchor.get_text(strip=True)
        links.append({"url": absolute_url, "text": text, "href": href})
        logger.debug("Enlace IPC encontrado: %s → %s", text, absolute_url)

    logger.info("Total enlaces IPC .xlsx encontrados: %d", len(links))
    return links


def select_target_link(
    links: list[dict[str, str]],
    config: IPCConfig = IPC_CONFIG,
) -> dict[str, str]:
    """Selecciona el enlace al archivo Excel de índices (serie de empalme)."""
    target_re = re.compile(config.target_file_pattern, re.IGNORECASE)
    candidates = [lnk for lnk in links if target_re.search(lnk["href"])]

    if not candidates:
        available = [lnk["href"] for lnk in links]
        raise ValueError(
            f"No se encontró enlace IPC con patrón '{config.target_file_pattern}'. "
            f"Enlaces disponibles: {available}"
        )

    if len(candidates) > 1:
        candidates = _sort_by_period(candidates)

    selected = candidates[0]
    logger.info("Archivo IPC seleccionado: %s", selected["url"])
    return selected


def _sort_by_period(links: list[dict[str, str]]) -> list[dict[str, str]]:
    """Ordena enlaces por periodo (más reciente primero)."""
    month_map = {
        "ene": 1, "feb": 2, "mar": 3, "abr": 4,
        "may": 5, "jun": 6, "jul": 7, "ago": 8,
        "sep": 9, "oct": 10, "nov": 11, "dic": 12,
    }
    period_re = re.compile(r"([a-z]{3})(\d{4})\.xlsx$", re.IGNORECASE)

    def sort_key(lnk: dict[str, str]) -> tuple[int, int]:
        m = period_re.search(lnk["href"])
        if m:
            month_num = month_map.get(m.group(1).lower(), 0)
            return (int(m.group(2)), month_num)
        return (0, 0)

    return sorted(links, key=sort_key, reverse=True)


def parse_period_from_href(href: str) -> tuple[int, int] | None:
    """Extrae (year, month) del nombre de archivo del DANE.

    Ejemplo: 'anex-IPC-Indices-feb2026.xlsx' → (2026, 2)
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


# ═══════════════════════════════════════════════════════════════════════
# 2. DESCARGA
# ═══════════════════════════════════════════════════════════════════════


def download_ipc_excel(
    url: str,
    output_dir: Path = RAW_DANE_DIR,
    filename: str | None = None,
    config: IPCConfig = IPC_CONFIG,
) -> Path:
    """Descarga el archivo Excel del IPC desde una URL directa."""
    logger.info("Descargando Excel IPC: %s", url)
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
# 3. PARSING
# ═══════════════════════════════════════════════════════════════════════


def detect_header_row_ipc(
    df_raw: pd.DataFrame,
    month_column: str = "Mes",
    max_scan: int = 30,
) -> int:
    """Detecta la fila de encabezado en el Excel IPC.

    Busca la fila que contenga "Mes" o una secuencia de años.
    """
    scan_limit = min(max_scan, len(df_raw))

    for i in range(scan_limit):
        row_values = [
            str(v).strip().lower() for v in df_raw.iloc[i] if pd.notna(v)
        ]
        if month_column.lower() in row_values:
            logger.info("Encabezado IPC detectado en fila %d", i)
            return i
        year_like = sum(
            1 for v in row_values if re.match(r"^\d{4}(\.0)?$", v)
        )
        if year_like >= 5:
            logger.info("Encabezado IPC detectado en fila %d (por años)", i)
            return i

    logger.warning("No se detectó encabezado IPC; usando fila 0 como fallback")
    return 0


def load_ipc_excel(
    xlsx_path: Path,
    config: IPCConfig = IPC_CONFIG,
) -> pd.DataFrame:
    """Lee el Excel pivoteado del IPC y lo convierte a formato largo."""
    logger.info("Cargando Excel IPC: %s", xlsx_path)

    # Paso 1: Seleccionar hoja
    if config.sheet_name is not None:
        sheet = config.sheet_name
    else:
        xlsx = pd.ExcelFile(xlsx_path, engine="openpyxl")
        sheet = xlsx.sheet_names[0]
        logger.info("Auto-seleccionada primera hoja: '%s'", sheet)

    # Paso 2: Detectar encabezado
    if config.header_row is not None:
        header_idx = config.header_row
    else:
        df_peek = pd.read_excel(
            xlsx_path, sheet_name=sheet, header=None,
            engine="openpyxl", dtype=str,
        )
        header_idx = detect_header_row_ipc(df_peek, month_column=config.month_column)

    # Paso 3: Leer con encabezado correcto
    df = pd.read_excel(
        xlsx_path, sheet_name=sheet, header=header_idx, engine="openpyxl",
    )

    logger.info(
        "Excel leído: %d filas × %d columnas, hoja='%s', header_row=%d",
        len(df), len(df.columns), sheet, header_idx,
    )
    logger.info("Columnas: %s", list(df.columns)[:10])

    # Paso 4: Limpiar columnas sin nombre
    df.columns = df.columns.astype(str).str.strip()
    unnamed = [c for c in df.columns if c.startswith("Unnamed")]
    if unnamed:
        df = df.drop(columns=unnamed)

    # Identificar columna de meses
    month_col = _find_month_column(df, config.month_column)

    # Filtrar filas con mes válido
    valid_months = set(config.month_map.keys())
    df[month_col] = df[month_col].astype(str).str.strip().str.lower()
    df = df[df[month_col].isin(valid_months)].copy()

    if df.empty:
        raise ValueError(
            f"No se encontraron filas con meses válidos en columna '{month_col}'. "
            f"Meses esperados: {list(valid_months)}"
        )

    # Paso 5: Identificar columnas-año
    year_columns = [
        c for c in df.columns
        if c != month_col and re.match(r"^\d{4}(\.0)?$", str(c))
    ]
    rename_years = {c: str(int(float(c))) for c in year_columns}
    df = df.rename(columns=rename_years)
    year_columns = list(rename_years.values())

    logger.info(
        "Años detectados: %d (%s ... %s)",
        len(year_columns),
        year_columns[0] if year_columns else "?",
        year_columns[-1] if year_columns else "?",
    )

    # Paso 6: Melt
    df_long = df.melt(
        id_vars=[month_col], value_vars=year_columns,
        var_name="year", value_name="ipc_index",
    )

    # Paso 7: Convertir tipos
    df_long["month"] = df_long[month_col].map(config.month_map)
    df_long["year"] = df_long["year"].astype(int)
    df_long["ipc_index"] = pd.to_numeric(df_long["ipc_index"], errors="coerce")

    df_long = df_long.dropna(subset=["ipc_index"]).copy()

    df_long["date"] = pd.to_datetime(
        df_long["year"].astype(str) + "-"
        + df_long["month"].astype(str).str.zfill(2) + "-01"
    )

    df_long = (
        df_long[["date", "year", "month", "ipc_index"]]
        .sort_values("date").reset_index(drop=True)
    )

    logger.info("Dataset IPC en formato largo: %d filas", len(df_long))
    return df_long


def _find_month_column(df: pd.DataFrame, expected: str = "Mes") -> str:
    """Encuentra la columna de meses en el DataFrame."""
    for col in df.columns:
        if col.lower().strip() == expected.lower():
            return col
    for col in df.columns:
        if expected.lower() in col.lower():
            return col
    logger.warning(
        "No se encontró columna '%s'; usando primera columna: '%s'",
        expected, df.columns[0],
    )
    return df.columns[0]


# ═══════════════════════════════════════════════════════════════════════
# TRANSFORMACIÓN
# ═══════════════════════════════════════════════════════════════════════


def clean_ipc_data(
    xlsx_path: Path,
    config: IPCConfig = IPC_CONFIG,
) -> pd.DataFrame:
    """Pipeline completo: carga Excel → formato largo → esquema final."""
    df = load_ipc_excel(xlsx_path, config)

    df["source"] = "DANE"
    df["download_date"] = date.today().isoformat()
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)

    dupes_before = len(df)
    df = df.drop_duplicates(subset=["date"], keep="first").copy()
    dupes_dropped = dupes_before - len(df)
    if dupes_dropped > 0:
        logger.warning("Se eliminaron %d filas duplicadas por fecha", dupes_dropped)

    df = df[IPC_PROCESSED_COLUMNS].sort_values("date").reset_index(drop=True)

    logger.info(
        "IPC limpio: %d filas, rango: %s → %s",
        len(df), df["date"].min(), df["date"].max(),
    )
    return df


# ═══════════════════════════════════════════════════════════════════════
# CARGA
# ═══════════════════════════════════════════════════════════════════════


def save_ipc_data(
    df: pd.DataFrame,
    output_dir: Path = PROCESSED_DIR,
    filename: str | None = None,
    config: IPCConfig = IPC_CONFIG,
) -> Path:
    """Guarda el dataset IPC procesado como CSV."""
    fname = filename or config.processed_filename
    path = output_dir / fname
    save_csv(df, path)
    logger.info("IPC procesado guardado: %s", path)
    return path


# ═══════════════════════════════════════════════════════════════════════
# ORQUESTACIÓN
# ═══════════════════════════════════════════════════════════════════════


def run_ipc_pipeline(
    config: IPCConfig = IPC_CONFIG,
    output_dir: Path = PROCESSED_DIR,
    raw_dir: Path = RAW_DANE_DIR,
) -> pd.DataFrame:
    """Ejecuta el pipeline IPC completo: scraping → descarga → parse → save."""
    # 1. Scraping
    html = fetch_ipc_page(config)
    save_html_snapshot(html, output_dir=raw_dir, config=config)

    # 2. Seleccionar enlace
    links = extract_ipc_xlsx_links(html, config)
    target = select_target_link(links, config)

    # 3. Descargar Excel
    xlsx_path = download_ipc_excel(
        url=target["url"], output_dir=raw_dir, config=config,
    )

    # 4. Parsear y limpiar
    df = clean_ipc_data(xlsx_path, config)

    # 5. Guardar
    save_ipc_data(df, output_dir=output_dir, config=config)

    return df
