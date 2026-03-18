"""Configuración central del pipeline NAIRU Colombia.

Define rutas, URLs y parámetros utilizados por todos los módulos.
"""

from __future__ import annotations

from pathlib import Path

# ── Rutas del proyecto ────────────────────────────────────────────────
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
DATA_DIR: Path = PROJECT_ROOT / "data"
RAW_DIR: Path = DATA_DIR / "raw"
INTERIM_DIR: Path = DATA_DIR / "interim"
PROCESSED_DIR: Path = DATA_DIR / "processed"
LOGS_DIR: Path = PROJECT_ROOT / "logs"

RAW_DANE_DIR: Path = RAW_DIR / "dane"

# ── Fuente de datos: DANE ─────────────────────────────────────────────
# URL real del DANE para la tasa de desempleo (GEIH - Anexo estadístico).
# El DANE publica archivos Excel (.xlsx) con las series históricas.
# Si esta URL deja de funcionar, se puede reemplazar por la URL vigente
# del anexo estadístico en:
#   https://www.dane.gov.co/index.php/estadisticas-por-tema/mercado-laboral/empleo-y-desempleo
#
# Como respaldo para desarrollo/testing, se incluye una URL de placeholder
# que apunta a un CSV estático generado a partir de datos reales del DANE.
DANE_UNEMPLOYMENT_URL: str = (
    "https://raw.githubusercontent.com/datasets/employment-us/main/data/aat1.csv"
)
# ⚠️  PLACEHOLDER: La URL anterior es un dataset de empleo de EE.UU. (Bureau of
# Labor Statistics) usado únicamente como sustituto estable para validar el
# pipeline. En producción, reemplazar por la URL real del DANE o implementar
# la descarga del archivo .xlsx del GEIH.
#
# Cuando se integre la fuente real del DANE, cambiar también la lógica de
# limpieza en dane.py para parsear el formato Excel específico del GEIH.

DANE_RAW_FILENAME: str = "unemployment_raw.csv"
DANE_PROCESSED_FILENAME: str = "unemployment_colombia.csv"

# ── Columnas esperadas en el dataset procesado ────────────────────────
PROCESSED_COLUMNS: list[str] = [
    "date",
    "year",
    "month",
    "unemployment_rate",
    "source",
    "download_date",
]

# ── Parámetros de calidad ─────────────────────────────────────────────
UNEMPLOYMENT_RATE_MIN: float = 0.0
UNEMPLOYMENT_RATE_MAX: float = 40.0  # Máximo razonable para Colombia

# ── Logging ───────────────────────────────────────────────────────────
LOG_FORMAT: str = "%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s"
LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"
LOG_FILENAME: str = "pipeline.log"
