"""Extracción de Stock de Capital y Capital Humano de Colombia desde PWT 11.0.

Pipeline de 3 capas:

1. **DESCARGA** — Obtiene el CSV completo de las Penn World Tables 11.0
   desde el Dataverse de la Universidad de Groningen vía urllib con
   fallback a curl si la conexión falla.
2. **PARSING**  — Lee el CSV crudo, filtra por ``countrycode == "COL"``,
   selecciona las columnas ``year``, ``rnna``, ``delta`` y ``hc``,
   descarta filas sin valor en ``rnna`` y genera el DataFrame con
   esquema estándar.
3. **ORQUESTACIÓN** — ``run_pwt_pipeline`` coordina descarga + parsing
   + guardado y retorna el DataFrame procesado.

Fuente
------
Penn World Tables 11.0 – Dataverse Universidad de Groningen:
    ``https://dataverse.nl/api/access/datafile/554105``

Columnas extraídas:
    - ``rnna``  : Stock de capital a precios nacionales constantes 2017
      (millones COP 2017). Apta para series de tiempo (no varía con PPP
      ni con tipo de cambio corriente).
    - ``delta`` : Tasa de depreciación promedio del stock de capital
      (fracción 0–1; Colombia ≈ 0.047–0.048). Imprescindible para el
      método de inventario permanente del modelo del profesor Álvaro.
    - ``hc``    : Índice de Capital Humano (escolaridad + retornos).

Cobertura: Colombia, 1950–2023 (anual).
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from datetime import date
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd

from src.config import (
    PROCESSED_DIR,
    PWT_CONFIG,
    PWT_PROCESSED_COLUMNS,
    PWTConfig,
    RAW_PWT_DIR,
)
from src.io_utils import save_csv

logger = logging.getLogger("nairu_pipeline.pwt.pwt")


# ═══════════════════════════════════════════════════════════════════════
# 1. DESCARGA
# ═══════════════════════════════════════════════════════════════════════


def _find_local_pwt_file(raw_dir: Path) -> Path | None:
    """Busca cualquier archivo PWT válido en ``raw_dir``.

    Acepta el archivo canónico ``pwt_raw.csv`` o cualquier ``.csv`` /
    ``.xlsx`` presente en el directorio (p. ej. exportaciones de la
    herramienta online con nombre tipo ``2026-04-14T12-33_export.csv``).

    Los CSV se validan leyendo solo las primeras 5 líneas para detectar
    archivos corrompidos (error de encoding o separador inesperado) antes
    de intentar parsearlos completamente.

    Returns
    -------
    Path | None
        El archivo más reciente válido encontrado, o ``None`` si no hay ninguno.
    """
    candidates = sorted(
        [p for p in raw_dir.glob("*.csv")] + [p for p in raw_dir.glob("*.xlsx")],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for candidate in candidates:
        # Validación rápida: leer el archivo completo (sin parsear como tabla)
        # para detectar problemas de encoding antes de usarlo.
        if candidate.suffix.lower() == ".csv":
            valid = False
            for enc in ("utf-8", "latin-1"):
                try:
                    candidate.read_text(encoding=enc)
                    valid = True
                    break
                except (UnicodeDecodeError, OSError):
                    continue
            if not valid:
                logger.warning(
                    "Archivo PWT local '%s' parece corrompido — se ignorará y "
                    "se intentará descargar de nuevo.",
                    candidate.name,
                )
                continue
        logger.info(
            "Archivo PWT encontrado localmente: %s (%d bytes)",
            candidate, candidate.stat().st_size,
        )
        return candidate
    return None


def download_pwt_csv(
    config: PWTConfig = PWT_CONFIG,
    raw_dir: Path = RAW_PWT_DIR,
) -> Path:
    """Localiza o descarga el CSV/Excel de PWT.

    Intenta en orden:

    1. **Archivo local:** cualquier ``.csv`` o ``.xlsx`` en ``raw_dir``
       (incluye exportaciones de https://pwt-data-tool.streamlit.app/).
    2. **curl** con ``--insecure`` (robusto ante restricciones TLS macOS).
    3. **urllib** con contexto SSL sin verificar.

    .. note::
        El Dataverse de Groningen (dataverse.nl) requiere autenticación
        para descargas programáticas.  Si la descarga automática falla,
        exporta los datos de Colombia desde la herramienta online y
        guarda el CSV en ``data/raw/pwt/``.

    Returns
    -------
    Path
        Ruta al archivo encontrado o descargado.

    Raises
    ------
    RuntimeError
        Si no hay archivo local y todos los métodos de descarga fallan.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    # 0. Buscar archivo pre-existente (cualquier CSV/XLSX en el directorio)
    local_file = _find_local_pwt_file(raw_dir)
    if local_file:
        return local_file

    logger.info("No hay archivo local PWT. Intentando descarga desde: %s", config.source_url)

    output_path = raw_dir / config.raw_csv_filename

    # 1. curl con --insecure
    curl_path = shutil.which("curl")
    if curl_path:
        try:
            subprocess.run(
                [
                    curl_path, "--silent", "--show-error",
                    "--location", "--fail", "--insecure",
                    "--max-time", str(config.timeout),
                    "--output", str(output_path),
                    config.source_url,
                ],
                capture_output=True, check=True,
                encoding="utf-8", text=True,
            )
            if output_path.exists() and output_path.stat().st_size > 10_000:
                logger.info(
                    "CSV descargado vía curl: %s (%d bytes)",
                    output_path, output_path.stat().st_size,
                )
                return output_path
            logger.warning("curl: archivo demasiado pequeño, probable 403.")
        except subprocess.CalledProcessError as exc:
            logger.warning("curl falló (exit %d: %s).", exc.returncode,
                           exc.stderr.strip() or "sin detalle")
    else:
        logger.info("curl no disponible, intentando urllib...")

    # 2. urllib con SSL sin verificar
    import ssl as _ssl
    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    request = Request(config.source_url, headers=config.http_headers)
    try:
        with urlopen(request, timeout=config.timeout, context=ctx) as response:
            raw_bytes = response.read()
            if len(raw_bytes) > 10_000:
                output_path.write_bytes(raw_bytes)
                logger.info(
                    "CSV descargado vía urllib: %s (%d bytes)",
                    output_path, len(raw_bytes),
                )
                return output_path
            logger.warning("urllib: respuesta muy pequeña (%d bytes), probable 403.", len(raw_bytes))
    except (HTTPError, URLError, ConnectionError) as exc:
        logger.warning("urllib falló: %s", exc)

    # 3. Sin éxito — instrucción clara al usuario
    raise RuntimeError(
        "No se encontró ningún archivo PWT en data/raw/pwt/ y la descarga "
        "automática falló (el Dataverse requiere autenticación).\n\n"
        "Solución: exporta los datos de Colombia desde la herramienta online:\n"
        "  https://pwt-data-tool.streamlit.app/\n"
        "Selecciona: Country=Colombia, Variables=rnna,delta,hc → Export CSV\n"
        f"Guarda el archivo en: {raw_dir}/\n"
        "Luego vuelve a ejecutar: python -m src.main --pwt"
    )


# ═══════════════════════════════════════════════════════════════════════
# 2. PARSING
# ═══════════════════════════════════════════════════════════════════════


def _is_wide_format(df: pd.DataFrame) -> bool:
    """Detecta si el CSV tiene el formato wide de la herramienta online PWT.

    El formato wide tiene columnas: ``ISO code``, ``Country``,
    ``Variable code``, ``Variable name``, ``1950``, ``1951``, ...
    El formato largo (Dataverse clásico) tiene una columna ``year``.
    Normaliza el nombre de columnas quitando BOM u otros caracteres invisibles.
    """
    # Normalizar nombres de columnas (quita BOM y espacios extra)
    cols_clean = {str(c).lstrip("\ufeff").strip() for c in df.columns}
    return "Variable code" in cols_clean and "ISO code" in cols_clean


def _parse_wide_format(df_raw: pd.DataFrame, country_code: str) -> pd.DataFrame:
    """Convierte el formato wide (herramienta online PWT) a formato largo.

    Filtra por ``ISO code == country_code``, pivota años como columnas
    a filas, y devuelve un DataFrame con columnas ``year``, ``rnna``,
    ``delta``, ``hc``.
    """
    # Normalizar nombres de columnas (quitar BOM)
    df_raw = df_raw.rename(columns=lambda c: str(c).lstrip("\ufeff").strip())

    df_country = df_raw[df_raw["ISO code"] == country_code].copy()
    if df_country.empty:
        raise ValueError(
            f"No se encontraron datos para ISO code == '{country_code}'."
        )

    # Columnas de año: todo lo que sea un número de 4 dígitos
    year_cols = [c for c in df_raw.columns if str(c).isdigit() and len(str(c)) == 4]
    vars_needed = {"rnna", "delta", "hc"}
    available = set(df_country["Variable code"].tolist())
    missing = vars_needed - available
    if missing:
        raise ValueError(
            f"Variables requeridas ausentes en el CSV: {missing}. "
            f"Variables disponibles: {sorted(available)}"
        )

    # Pivotear: filas=variable, cols=año → filas=año, cols=variable
    df_pivot = (
        df_country[df_country["Variable code"].isin(vars_needed)]
        .set_index("Variable code")[year_cols]
        .T
    )
    df_pivot.index.name = "year"
    df_pivot = df_pivot.reset_index()
    df_pivot["year"] = df_pivot["year"].astype(int)
    for col in vars_needed:
        df_pivot[col] = pd.to_numeric(df_pivot[col], errors="coerce")

    return df_pivot[["year", "rnna", "delta", "hc"]].copy()


def parse_pwt_csv(
    raw_path: Path,
    config: PWTConfig = PWT_CONFIG,
) -> pd.DataFrame:
    """Parsea el CSV crudo de PWT (formato largo o wide) a DataFrame limpio.

    Detecta automáticamente el formato:

    - **Formato largo** (Dataverse clásico, ``.csv`` o ``.xlsx``):
      columnas ``countrycode``, ``year``, ``rnna``, ``delta``, ``hc``.
    - **Formato wide** (herramienta online https://pwt-data-tool.streamlit.app/):
      columnas ``ISO code``, ``Variable code``, ``1950``, ``1951``, ...

    Returns
    -------
    pd.DataFrame
        Columnas: ``date, year, month, capital_stock_real,
        depreciation_rate, human_capital, source, download_date``.
    """
    if not raw_path.exists():
        raise FileNotFoundError(f"Archivo crudo no encontrado: {raw_path}")

    logger.info("Leyendo archivo crudo PWT: %s", raw_path)
    suffix = raw_path.suffix.lower()
    if suffix in (".xlsx", ".xls"):
        # Excel del Dataverse: hoja "Data"
        try:
            df_raw = pd.read_excel(raw_path, sheet_name="Data", engine="openpyxl")
        except Exception:
            df_raw = pd.read_excel(raw_path, engine="openpyxl")
    else:
        try:
            df_raw = pd.read_csv(
                raw_path,
                encoding="utf-8",
                sep=None,          # detección automática del separador
                engine="python",
                on_bad_lines="skip",
            )
        except UnicodeDecodeError:
            logger.warning("UTF-8 falló al leer CSV PWT — reintentando con latin-1")
            df_raw = pd.read_csv(
                raw_path,
                encoding="latin-1",
                sep=None,
                engine="python",
                on_bad_lines="skip",
            )

    logger.info("Archivo leído: %d filas × %d cols", len(df_raw), len(df_raw.columns))

    # Detectar formato y extraer las 3 variables para el país
    if _is_wide_format(df_raw):
        logger.info("Formato detectado: WIDE (herramienta online PWT)")
        df_col = _parse_wide_format(df_raw, config.country_code)
        pwt_version = "PWT 11.0"
    else:
        logger.info("Formato detectado: LARGO (Dataverse clásico)")
        required_cols = {"countrycode", "year", "rnna", "delta", "hc"}
        missing_cols = required_cols - set(df_raw.columns)
        if missing_cols:
            raise ValueError(
                f"Columnas requeridas ausentes: {missing_cols}. "
                f"Disponibles: {list(df_raw.columns)}"
            )
        df_col = df_raw[df_raw["countrycode"] == config.country_code][
            ["year", "rnna", "delta", "hc"]
        ].copy()
        if df_col.empty:
            raise ValueError(
                f"No hay datos para countrycode == '{config.country_code}'."
            )
        pwt_version = "PWT 11.0"

    # Descartar filas sin rnna (variable principal)
    rows_before = len(df_col)
    df_col = df_col.dropna(subset=["rnna"]).reset_index(drop=True)
    dropped = rows_before - len(df_col)
    if dropped > 0:
        logger.warning("Descartadas %d filas con rnna nulo.", dropped)
    if df_col.empty:
        raise ValueError(f"Sin datos válidos para '{config.country_code}' tras filtrar rnna nulo.")

    # Construir columnas estándar
    df_col["year"] = df_col["year"].astype(int)
    df_col["date"] = pd.to_datetime(df_col["year"].astype(str) + "-01-01", format="%Y-%m-%d")
    df_col["month"] = 1

    df_col = df_col.rename(columns={
        "rnna":  "capital_stock_real",
        "delta": "depreciation_rate",
        "hc":    "human_capital",
    })

    for col in ("capital_stock_real", "depreciation_rate", "human_capital"):
        if col in df_col.columns:
            df_col[col] = df_col[col].round(4)

    df_col["source"] = pwt_version
    df_col["download_date"] = date.today().isoformat()

    dupes = df_col.duplicated(subset=["date"]).sum()
    if dupes > 0:
        logger.warning("Eliminando %d duplicados por fecha.", dupes)
        df_col = df_col.drop_duplicates(subset=["date"], keep="first")

    df_col = df_col[PWT_PROCESSED_COLUMNS].sort_values("date").reset_index(drop=True)

    logger.info(
        "%s parseado: %d obs, %s → %s, source='%s'",
        pwt_version, len(df_col),
        df_col["date"].min().date(), df_col["date"].max().date(),
        pwt_version,
    )
    return df_col


# ═══════════════════════════════════════════════════════════════════════
# 3. ORQUESTACIÓN
# ═══════════════════════════════════════════════════════════════════════


def run_pwt_pipeline(
    config: PWTConfig = PWT_CONFIG,
    raw_dir: Path = RAW_PWT_DIR,
    processed_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Pipeline completo: descarga → parsing → guardado.

    Parameters
    ----------
    config : PWTConfig
        Configuración de la fuente PWT.
    raw_dir : Path
        Directorio para el CSV crudo.
    processed_dir : Path
        Directorio para el CSV procesado.

    Returns
    -------
    pd.DataFrame
        Dataset PWT procesado (anual, Colombia).
    """
    # 1. Descargar CSV crudo
    raw_path = download_pwt_csv(config=config, raw_dir=raw_dir)

    # 2. Parsear y transformar
    df = parse_pwt_csv(raw_path=raw_path, config=config)

    # 3. Guardar CSV procesado
    output_path = processed_dir / config.processed_filename
    save_csv(df, output_path)
    logger.info("Dataset PWT procesado guardado: %s", output_path)

    return df
