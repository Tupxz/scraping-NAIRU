"""Extracción y limpieza de datos de desempleo del DANE (GEIH desestacionalizado).

Pipeline de 3 capas:

1. **SCRAPING**  — Descarga la página temática de empleo del DANE,
   extrae enlaces a archivos .xlsx del anexo GEIH desestacionalizado
   y selecciona el más reciente.
2. **DESCARGA**  — Descarga el Excel seleccionado + guarda HTML.
3. **PARSING**   — Lee la hoja *Total nacional* (formato pivoteado:
   conceptos × año·mes), extrae las series indicadas en
   ``GEIHConfig.series_map`` (por defecto solo la TD), reconstruye
   fechas y genera el formato largo estándar.
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
    GEIH_CONFIG,
    GEIHConfig,
    PROCESSED_COLUMNS,
    PROCESSED_DIR,
    RAW_DANE_DIR,
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

    Soporta ambos formatos:
    - ``anex-GEIH-Desestacionalizado-ene2026.xlsx`` → ``(2026, 1)``
    - ``anex-GEIH-ene2026.xlsx`` → ``(2026, 1)``
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
    """Detecta la fila que contiene la Tasa de Desocupación (TD).

    También se usa de forma genérica para localizar cualquier serie
    cuya etiqueta (columna 0) coincida con *label_pattern*.
    """
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


def _detect_series_rows(
    df_raw: pd.DataFrame,
    series_map: dict[str, str],
    start_row: int = 0,
    optional_series: set[str] | None = None,
) -> dict[str, int]:
    """Detecta la fila de cada serie definida en *series_map*.

    Parameters
    ----------
    df_raw : pd.DataFrame
        DataFrame crudo de la hoja Excel.
    series_map : dict[str, str]
        Mapa ``{nombre_columna: patrón_regex}`` de series a buscar.
    start_row : int
        Fila desde la que empezar la búsqueda (0-indexed).
    optional_series : set[str] | None
        Nombres de series que pueden no estar en el Excel.  Si se
        especifica y la serie no se encuentra, se emite un warning
        en lugar de lanzar ``ValueError``.

    Returns
    -------
    dict[str, int]
        Mapa ``{nombre_columna: fila_0indexed}`` para cada serie
        encontrada en el Excel.  Las series opcionales ausentes no
        aparecen en el resultado.

    Raises
    ------
    ValueError
        Si una serie **no opcional** del mapa no se encuentra.
    """
    optional = optional_series or set()
    result: dict[str, int] = {}
    for col_name, label_pattern in series_map.items():
        pat = re.compile(label_pattern, re.IGNORECASE)
        found = False
        for i in range(start_row, len(df_raw)):
            first_cell = str(df_raw.iloc[i, 0]).strip()
            if pat.search(first_cell):
                result[col_name] = i
                logger.info(
                    "Serie '%s' detectada en fila %d ('%s')",
                    col_name, i, first_cell[:60],
                )
                found = True
                break
        if not found:
            if col_name in optional:
                logger.warning(
                    "Serie opcional '%s' no encontrada en el Excel "
                    "(patrón: '%s'); columna omitida.",
                    col_name, label_pattern,
                )
            else:
                raise ValueError(
                    f"No se encontró fila para serie '{col_name}' "
                    f"(patrón: '{label_pattern}'). "
                    f"Filas escaneadas: {start_row}–{len(df_raw) - 1}"
                )
    return result


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


def _resolve_sheet_name(xlsx_path: Path, preferred: str) -> str:
    """Devuelve *preferred* si existe; si no, busca hoja que contenga la cadena.

    Esto permite que el parser funcione si el DANE renombra la hoja
    (p. ej. ``"Total nacional "`` con espacio al final).
    """
    import openpyxl

    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    sheets = wb.sheetnames
    wb.close()

    if preferred in sheets:
        return preferred

    # Fallback: buscar hoja que contenga la cadena (case-insensitive)
    needle = preferred.lower()
    for sname in sheets:
        if needle in sname.lower():
            logger.warning(
                "Hoja '%s' no encontrada; usando fallback: '%s'", preferred, sname,
            )
            return sname

    raise ValueError(
        f"No se encontró hoja '{preferred}' ni alternativa similar. "
        f"Hojas disponibles: {sheets}"
    )


def load_geih_excel(
    xlsx_path: Path,
    config: GEIHConfig = GEIH_CONFIG,
) -> pd.DataFrame:
    """Lee el Excel pivoteado del GEIH y extrae series en formato largo.

    Pasos:
    1. Resolver la hoja (con fallback si cambió el nombre).
    2. Leer la hoja sin header (todo crudo para robustez).
    3. Detectar filas de años, meses y series (vía ``series_map``).
    4. Reconstruir las fechas desde las cabeceras.
    5. Extraer los valores de cada serie y generar DataFrame largo.
    """
    logger.info("Cargando Excel GEIH: %s", xlsx_path)

    sheet = _resolve_sheet_name(xlsx_path, config.sheet_name)

    df_raw = pd.read_excel(
        xlsx_path,
        sheet_name=sheet,
        header=None,
        engine="openpyxl",
    )

    logger.info(
        "Excel leído: %d filas × %d cols, hoja='%s'",
        len(df_raw), len(df_raw.columns), sheet,
    )

    # Detectar filas clave
    year_row = config.year_row
    month_row = config.month_row

    if year_row is None:
        year_row = _detect_year_row(df_raw)
    if month_row is None:
        month_row = _detect_month_row(df_raw, year_row, set(config.month_map.keys()))

    # Detectar filas de series usando series_map.
    # Los tres componentes auxiliares para calcular PET son opcionales:
    # pueden no estar en todos los Excels del DANE (versiones antiguas o
    # parciales). tgp_rate también es opcional por la misma razón.
    series_rows = _detect_series_rows(
        df_raw,
        config.series_map,
        start_row=month_row,
        optional_series={
            "tgp_rate",
            "_raw_pop_employed",
            "_raw_pop_unemployed",
            "_raw_pop_inactive",
        },
    )

    # Reconstruir fechas
    date_cols = _build_date_columns(df_raw, year_row, month_row, config.month_map)

    if not date_cols:
        raise ValueError("No se pudieron reconstruir columnas fecha del Excel GEIH.")

    # Extraer valores de cada serie
    records: list[dict] = []
    for dc in date_cols:
        record: dict = {
            "year": dc["year"],
            "month": dc["month"],
        }
        all_nan = True
        for col_name, row_idx in series_rows.items():
            raw_val = df_raw.iloc[row_idx].iloc[dc["col_idx"]]
            val = pd.to_numeric(raw_val, errors="coerce")
            record[col_name] = float(val) if pd.notna(val) else None
            if pd.notna(val):
                all_nan = False
        if not all_nan:
            records.append(record)

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(
        df["year"].astype(str) + "-"
        + df["month"].astype(str).str.zfill(2) + "-01"
    )
    df = df.sort_values("date").reset_index(drop=True)

    # Determinar columnas de salida dinámicamente.
    # Solo incluir series que realmente se encontraron en el Excel
    # (las opcionales ausentes no están en series_rows).
    found_series_cols = list(series_rows.keys())
    # Mantener el orden original del series_map
    series_cols = [k for k in config.series_map.keys() if k in found_series_cols]
    output_cols = ["date", "year", "month"] + series_cols

    logger.info(
        "GEIH parseado: %d observaciones, %s → %s, series: %s",
        len(df), df["date"].min(), df["date"].max(), series_cols,
    )
    return df[output_cols]


# ═══════════════════════════════════════════════════════════════════════
# TRANSFORMACIÓN (normalización común)
# ═══════════════════════════════════════════════════════════════════════


def clean_geih_data(
    xlsx_path: Path,
    config: GEIHConfig = GEIH_CONFIG,
) -> pd.DataFrame:
    """Pipeline: carga Excel → formato largo → cálculo PET → esquema final.

    PET se calcula como la suma de los tres componentes de la fuerza laboral
    publicados por el DANE (en miles de personas):

        PET = Población ocupada + Población desocupada
              + Población fuera de la fuerza de trabajo

    Los tres componentes se extraen con el prefijo ``_raw_`` en el
    ``series_map`` y se descartan del output final tras el cálculo.
    Si alguno de los tres componentes no está disponible, la columna
    ``pet_thousands`` no se genera (compatibilidad hacia atrás).
    """
    df = load_geih_excel(xlsx_path, config)

    df["source"] = "DANE"
    df["download_date"] = date.today().isoformat()
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)

    # ── Calcular PET a partir de los tres componentes auxiliares ──
    raw_cols = ("_raw_pop_employed", "_raw_pop_unemployed", "_raw_pop_inactive")
    if all(c in df.columns for c in raw_cols):
        df["pet_thousands"] = (
            df["_raw_pop_employed"]
            + df["_raw_pop_unemployed"]
            + df["_raw_pop_inactive"]
        ).round(1)
        df = df.drop(columns=list(raw_cols))
        logger.info(
            "PET calculada: min=%.0f k, max=%.0f k",
            df["pet_thousands"].min(), df["pet_thousands"].max(),
        )
    else:
        # Eliminar columnas _raw_ parciales si quedaron
        leftover_raw = [c for c in df.columns if c.startswith("_raw_")]
        if leftover_raw:
            logger.warning(
                "Componentes PET incompletos; columnas descartadas: %s",
                leftover_raw,
            )
            df = df.drop(columns=leftover_raw)

    # Eliminar duplicados
    dupes_before = len(df)
    df = df.drop_duplicates(subset=["date"], keep="first").copy()
    dupes_dropped = dupes_before - len(df)
    if dupes_dropped > 0:
        logger.warning("Se eliminaron %d filas duplicadas por fecha", dupes_dropped)

    # Columnas de salida: usar PROCESSED_COLUMNS filtrado por las columnas
    # realmente disponibles (series opcionales pueden estar ausentes).
    output_cols = [c for c in PROCESSED_COLUMNS if c in df.columns]
    df = df[output_cols].sort_values("date").reset_index(drop=True)

    logger.info(
        "Laborales GEIH limpio: %d filas, rango: %s → %s, columnas: %s",
        len(df), df["date"].min(), df["date"].max(), list(df.columns),
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
