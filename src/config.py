"""Configuración central del pipeline NAIRU Colombia.

Define rutas, URLs y parámetros utilizados por todos los módulos.
Soporta múltiples fuentes: desempleo (placeholder/DANE) e IPC (DANE real).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

# ── Rutas del proyecto ────────────────────────────────────────────────
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
DATA_DIR: Path = PROJECT_ROOT / "data"
RAW_DIR: Path = DATA_DIR / "raw"
INTERIM_DIR: Path = DATA_DIR / "interim"
PROCESSED_DIR: Path = DATA_DIR / "processed"
LOGS_DIR: Path = PROJECT_ROOT / "logs"

RAW_DANE_DIR: Path = RAW_DIR / "dane"


# ── Perfil de fuente de datos ─────────────────────────────────────────
# Cada fuente (placeholder, DANE real, IPC) se describe con un dataclass
# que contiene toda la información necesaria para descargar y parsear.


@dataclass(frozen=True)
class SourceProfile:
    """Perfil de configuración para una fuente de datos."""

    name: str
    url: str
    raw_filename: str
    file_format: str  # "csv" | "xlsx"

    # ── Parámetros para Excel (ignorados si file_format == "csv") ──
    sheet_name: str | int | None = None
    header_row: int | None = None
    header_keywords: list[str] = field(
        default_factory=lambda: ["año", "mes", "trimestre", "tasa", "total"]
    )
    header_scan_rows: int = 30
    column_mapping: dict[str, str] | None = None
    column_patterns: dict[str, list[str]] = field(
        default_factory=lambda: {
            "unemployment_rate": [
                r"tasa.*desempleo",
                r"td\b",
                r"desempleo.*%",
                r"unemployment",
            ],
            "year": [r"^a[ñn]o$", r"^year$"],
            "month": [r"^mes$", r"^month$"],
            "date": [r"^fecha$", r"^date$", r"^periodo$"],
        }
    )

    # ── Parámetros de descarga HTTP ───────────────────────────────
    timeout: int = 120
    http_headers: dict[str, str] = field(
        default_factory=lambda: {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        }
    )


# ── Perfiles de desempleo ────────────────────────────────────────────

PLACEHOLDER_PROFILE = SourceProfile(
    name="placeholder_bls",
    url="https://raw.githubusercontent.com/datasets/employment-us/main/data/aat1.csv",
    raw_filename="unemployment_raw.csv",
    file_format="csv",
)

# Perfil activo para desempleo (placeholder – se ignora cuando se usa GEIHConfig)
ACTIVE_PROFILE: SourceProfile = PLACEHOLDER_PROFILE


# ── Configuración GEIH – Desempleo (DANE real) ───────────────────────

@dataclass(frozen=True)
class GEIHConfig:
    """Configuración para la fuente de desempleo de la GEIH del DANE.

    El DANE publica mensualmente el anexo de la Gran Encuesta Integrada
    de Hogares (GEIH) en su página de empleo y desempleo.  El Excel
    contiene una hoja 'Total nacional' con indicadores en formato
    pivoteado: filas = conceptos (TGP, TO, **TD**, etc.) y
    columnas = año × mes.

    Esta config define cómo hacer scraping de la página, seleccionar
    el archivo correcto, y parsear la tasa de desocupación (TD).
    """

    # ── Scraping ──────────────────────────────────────────────────
    # Página temática donde se publican los anexos GEIH.
    page_url: str = (
        "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
        "mercado-laboral/empleo-y-desempleo"
    )
    base_url: str = "https://www.dane.gov.co"

    # Patrón regex para filtrar enlaces al anexo GEIH (excluye PDFs,
    # desestacionalizados y archivos "acercade").
    link_pattern: str = r"/files/operaciones/GEIH/anex-GEIH-[a-z]{3}\d{4}\.xlsx$"

    # ── Parsing del Excel ─────────────────────────────────────────
    # Hoja con la serie mensual de Total Nacional.
    sheet_name: str = "Total nacional"

    # El Excel tiene una cabecera multi-fila:
    #   fila 12 (0-idx 11) → [Concepto, 2001, ..., 2026]  (años)
    #   fila 13 (0-idx 12) → [None, Ene, Feb, ..., Dic, Ene, ...]  (meses)
    #   fila 17 (0-idx 16) → [Tasa de Desocupación (TD), val, val, ...]
    #
    # year_row y month_row son las filas (0-indexed) donde están
    # los años y los meses respectivamente.  Ninguna de las dos es un
    # "header" clásico: se leen ambas para reconstruir las fechas.
    year_row: int = 11
    month_row: int = 12

    # Etiqueta exacta (o patrón) de la fila que contiene la TD.
    td_label_pattern: str = r"Tasa de Desocupaci[oó]n"

    # Mapeo de abreviatura de mes → número.
    month_map: dict[str, int] = field(default_factory=lambda: {
        "ene": 1, "feb": 2, "mar": 3, "abr": 4,
        "may": 5, "jun": 6, "jul": 7, "ago": 8,
        "sep": 9, "oct": 10, "nov": 11, "dic": 12,
    })

    # ── Archivos ──────────────────────────────────────────────────
    raw_html_filename: str = "geih_page.html"
    raw_xlsx_filename: str = "geih_raw.xlsx"
    processed_filename: str = "unemployment_colombia.csv"

    # ── HTTP ──────────────────────────────────────────────────────
    timeout: int = 120
    http_headers: dict[str, str] = field(
        default_factory=lambda: {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        }
    )


GEIH_CONFIG = GEIHConfig()


# ── Configuración IPC (DANE real) ────────────────────────────────────

@dataclass(frozen=True)
class IPCConfig:
    """Configuración para la fuente IPC del DANE.

    El DANE publica varios Excel de IPC mensualmente en su página.
    Esta config define cómo hacer scraping de la página y cuál archivo
    descargar automáticamente.
    """

    # Página donde se publican los enlaces a los Excel del IPC.
    page_url: str = (
        "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
        "precios-y-costos/indice-de-precios-al-consumidor-ipc"
    )
    # Base URL del DANE para construir URLs absolutas desde hrefs relativos.
    base_url: str = "https://www.dane.gov.co"

    # Patrón regex para filtrar enlaces relevantes del IPC dentro del HTML.
    # Los enlaces IPC están bajo /files/operaciones/IPC/ con extensión .xlsx.
    link_pattern: str = r"/files/operaciones/IPC/.*\.xlsx$"

    # Patrón del archivo específico que queremos (índices / serie de empalme).
    # "Indices" contiene la serie histórica del IPC base 2018.
    target_file_pattern: str = r"anex-IPC-Indices"

    # ── Parámetros de parsing del Excel ──────────────────────────
    # Hoja donde están los índices. None = auto-detectar.
    sheet_name: str | None = "IndicesIPC"
    # Fila del encabezado (0-indexed). En el Excel real: fila 8.
    # None = auto-detectar buscando la fila que contiene "Mes".
    header_row: int | None = 8
    # Nombre de la columna de meses (primera columna del Excel pivoteado).
    month_column: str = "Mes"

    # Mapeo de nombres de mes en español a número (para el melt).
    month_map: dict[str, int] = field(default_factory=lambda: {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
        "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
    })

    # ── Archivos de salida ────────────────────────────────────────
    raw_html_filename: str = "ipc_page.html"
    raw_xlsx_filename: str = "ipc_indices_raw.xlsx"
    processed_filename: str = "ipc_colombia.csv"

    # ── Parámetros de descarga HTTP ───────────────────────────────
    timeout: int = 120
    http_headers: dict[str, str] = field(
        default_factory=lambda: {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        }
    )


IPC_CONFIG = IPCConfig()


# ── Columnas procesadas por dataset ───────────────────────────────────

PROCESSED_COLUMNS: list[str] = [
    "date",
    "year",
    "month",
    "unemployment_rate",
    "source",
    "download_date",
]

IPC_PROCESSED_COLUMNS: list[str] = [
    "date",
    "year",
    "month",
    "ipc_index",
    "source",
    "download_date",
]

# ── Parámetros de calidad ─────────────────────────────────────────────
UNEMPLOYMENT_RATE_MIN: float = 0.0
UNEMPLOYMENT_RATE_MAX: float = 40.0

IPC_INDEX_MIN: float = 10.0    # Mínimo razonable (base 2018≈100, año 2003≈50)
IPC_INDEX_MAX: float = 300.0   # Máximo razonable para horizonte largo

# ── Logging ───────────────────────────────────────────────────────────
LOG_FORMAT: str = "%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s"
LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"
LOG_FILENAME: str = "pipeline.log"
