"""Configuración central del pipeline NAIRU Colombia.

Define rutas, URLs y parámetros utilizados por todos los módulos.
Fuentes: desempleo/TGP/PET GEIH (DANE), IPC (DANE), inflación (BANREP), Brent (FRED), PWT 11.0 (capital stock + capital humano).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ── Rutas del proyecto ────────────────────────────────────────────────
# Convención de capas (raw → inputs → processed → final):
#   raw/        Datos crudos descargados por scrapers (no editar a mano).
#   inputs/     Inputs manuales que NO provienen de un scraper
#               (ej. PIB_USA.xlsx para VIOG, benchmarks externos).
#   processed/  Outputs por fuente generados por los pipelines individuales.
#   final/      Dataset(s) consolidado(s) listos para modelar (output de merge).
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
DATA_DIR: Path = PROJECT_ROOT / "data"
RAW_DIR: Path = DATA_DIR / "raw"
INPUTS_DIR: Path = DATA_DIR / "inputs"
PROCESSED_DIR: Path = DATA_DIR / "processed"
FINAL_DIR: Path = DATA_DIR / "final"
LOGS_DIR: Path = PROJECT_ROOT / "logs"

RAW_DANE_DIR: Path = RAW_DIR / "dane"
RAW_BANREP_DIR: Path = RAW_DIR / "banrep"
RAW_FRED_DIR: Path = RAW_DIR / "fred"
RAW_ANDI_DIR: Path = RAW_DIR / "andi"
RAW_PWT_DIR: Path = RAW_DIR / "pwt"
OUTPUTS_DIR: Path = PROJECT_ROOT / "outputs"


# ── Configuración GEIH – Desempleo (DANE real) ───────────────────────

@dataclass(frozen=True)
class GEIHConfig:
    """Configuración para la fuente de desempleo desestacionalizado GEIH.

    El DANE publica mensualmente el anexo **desestacionalizado** de la
    Gran Encuesta Integrada de Hogares (GEIH) en su página de empleo y
    desempleo.  El Excel contiene una hoja 'Total nacional' con
    indicadores en formato pivoteado: filas = conceptos (TGP, TO,
    **TD**, etc.) y columnas = año × mes.

    Esta config define cómo hacer scraping de la página, seleccionar
    el archivo desestacionalizado correcto, y parsear las series
    indicadas en ``series_map``.
    """

    # ── Scraping ──────────────────────────────────────────────────
    # Página temática donde se publican los anexos GEIH.
    page_url: str = (
        "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
        "mercado-laboral/empleo-y-desempleo"
    )
    base_url: str = "https://www.dane.gov.co"

    # Patrón regex para filtrar enlaces al anexo GEIH desestacionalizado.
    link_pattern: str = (
        r"/files/operaciones/GEIH/"
        r"anex-GEIH-Desestacionalizado-[a-z]{3}\d{4}\.xlsx$"
    )

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

    # ── Mapa de series extraíbles ─────────────────────────────────
    # Cada entrada mapea un nombre de columna de salida al patrón
    # regex que identifica la fila correspondiente en el Excel.
    # Por defecto solo se extrae la TD; para agregar TGP, TO, etc.
    # basta ampliar este dict sin cambiar el parser.
    #
    # Nota PET: el DANE no publica "Población en Edad de Trabajar"
    # como fila directa.  Se calcula en clean_geih_data como:
    #   PET = pop_employed + pop_unemployed + pop_inactive
    # Las tres series auxiliares se extraen con el prefijo "_raw_" y
    # se descartan del output final tras el cálculo.
    series_map: dict[str, str] = field(default_factory=lambda: {
        "unemployment_rate":  r"Tasa de Desocupaci[oó]n",
        "tgp_rate":           r"Tasa Global de Participaci[oó]n",
        "_raw_pop_employed":  r"^Poblaci[oó]n ocupada",
        "_raw_pop_unemployed":r"^Poblaci[oó]n desocupada",
        "_raw_pop_inactive":  r"^Poblaci[oó]n fuera de la fuerza",
    })

    # Mapeo de abreviatura de mes → número.
    month_map: dict[str, int] = field(default_factory=lambda: {
        "ene": 1, "feb": 2, "mar": 3, "abr": 4,
        "may": 5, "jun": 6, "jul": 7, "ago": 8,
        "sep": 9, "oct": 10, "nov": 11, "dic": 12,
    })

    # ── Archivos ──────────────────────────────────────────────────
    raw_html_filename: str = "geih_page.html"
    raw_xlsx_filename: str = "geih_raw.xlsx"
    processed_filename: str = "dane_labor_colombia.csv"

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


# ── Configuración GEIH-EISS — Informalidad laboral (DANE real) ───────

@dataclass(frozen=True)
class GEIHInformalityConfig:
    """Configuración para la fuente de informalidad laboral GEIH-EISS.

    El DANE publica trimestralmente el anexo de **Empleo Informal y
    Seguridad Social (EISS)** en su página de mercado laboral.  El
    Excel contiene la hoja 'Prop informalidad' con la proporción de
    informalidad (% de ocupados informales) para Total Nacional,
    13 ciudades y A.M., 23 ciudades, y ciudades individuales.

    Series en trimestre móvil (no desestacionalizada):
      - "Ene - mar 2021" → asignado a 2021-03-01 (último mes)
      - "Nov 21 - ene 22"→ asignado a 2022-01-01 (último mes)

    Extraemos la fila **13 Ciudades y A.M.** (``city_label_pattern``),
    la más usada en análisis macro colombiano.
    """

    # ── Scraping ──────────────────────────────────────────────────
    page_url: str = (
        "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
        "mercado-laboral/empleo-informal-y-seguridad-social"
    )
    base_url: str = "https://www.dane.gov.co"

    # Patrón para filtrar el enlace al Excel GEIHEISS
    link_pattern: str = r"/files/operaciones/GEIH/anex-GEIHEISS-.+\.xlsx$"

    # ── Parsing del Excel ─────────────────────────────────────────
    sheet_name: str = "Prop informalidad"

    # Fila 10 (0-idx) = años: 2021, NaN, NaN, ..., 2022, NaN, ...
    year_row: int = 10

    # Fila 11 (0-idx) = trimestres: "Ene - mar", "Feb - abr", ...
    trimestre_row: int = 11

    # Patrón regex para la fila de 13 ciudades (fila 13 en el Excel)
    city_label_pattern: str = r"13\s+Ciudades?\s+y\s+A\.?M\.?"

    # Etiqueta de fuente para el CSV de salida
    source_label: str = "DANE GEIH-EISS"

    # ── Archivos ──────────────────────────────────────────────────
    raw_xlsx_filename: str = "geiheiss_raw.xlsx"
    processed_filename: str = "dane_informality_colombia.csv"

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


GEIH_INFORMALITY_CONFIG = GEIHInformalityConfig()


# ── Columnas procesadas — informalidad ───────────────────────────────

INFORMALITY_PROCESSED_COLUMNS: list[str] = [
    "date", "year", "month",
    "informality_rate_13c",
    "source", "download_date",
]

INFORMALITY_RATE_MIN: float = 0.0
INFORMALITY_RATE_MAX: float = 100.0


# ── Configuración Cuentas Nacionales — PIB Trimestral (DANE real) ─────

@dataclass(frozen=True)
class DANEGDPConfig:
    """Configuración para la fuente PIB trimestral desestacionalizado del DANE.

    El DANE publica trimestralmente el anexo **PIB enfoque producción a
    precios constantes** dentro de la página *Cuentas Nacionales
    Trimestrales — PIB información técnica*. El archivo ``anex-
    ProduccionConstantes-{trim}{YYYY}.xlsx`` contiene la serie a precios
    constantes en datos originales y **desestacionalizados** (los que
    interesan para el output gap del VIOG-Colombia).

    Estructura del Excel (hoja ``Cuadro 4``):
      - Fila 11 (0-idx 10) → años (en columnas D, H, L, ... cada año
        ocupa 4 columnas, una por trimestre).
      - Fila 12 (0-idx 11) → trimestres en romanos: I, II, III, IV.
      - Fila con ``col C == "Producto Interno Bruto"`` → serie agregada
        del PIB total (primer match = bloque de niveles; bloques
        posteriores son tasas de variación).

    La serie se asigna al primer mes del trimestre (Q1 → enero, Q2 →
    abril, Q3 → julio, Q4 → octubre), convención usual para empalmar
    series trimestrales con frecuencias mensuales en el merge.
    """

    # ── Scraping ──────────────────────────────────────────────────
    page_url: str = (
        "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
        "cuentas-nacionales/cuentas-nacionales-trimestrales/"
        "pib-informacion-tecnica"
    )
    base_url: str = "https://www.dane.gov.co"

    # Patrón regex para el anexo "Producción a precios constantes"
    # Ej.: /files/operaciones/PIB/anex-ProduccionConstantes-IVtrim2025.xlsx
    link_pattern: str = (
        r"/files/operaciones/PIB/anex-ProduccionConstantes-"
        r"(?:I|II|III|IV)trim\d{4}\.xlsx$"
    )

    # ── Parsing del Excel ─────────────────────────────────────────
    # Cuadro 4 = PIB desestacionalizado (12 agrupaciones — la serie más
    # estable y comparable internacionalmente).
    sheet_name: str = "Cuadro 4"

    # Fila 12 (0-idx 11) = años (col D=2005, H=2006, ...).
    # Fila 13 (0-idx 12) = trimestres romanos (I, II, III, IV).
    year_row: int = 11
    quarter_row: int = 12

    # Columna C (0-idx 2) contiene la etiqueta "Producto Interno Bruto"
    concept_col: int = 2
    concept_label: str = "Producto Interno Bruto"

    # Datos numéricos empiezan en columna D (0-idx 3)
    data_start_col: int = 3

    # Etiqueta para el campo source del CSV
    source_label: str = "DANE - Cuentas Nacionales Trimestrales"

    # ── Archivos ──────────────────────────────────────────────────
    raw_xlsx_filename: str = "dane_gdp_raw.xlsx"
    raw_html_filename: str = "dane_gdp_page.html"
    processed_filename: str = "dane_gdp_colombia.csv"

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


DANE_GDP_CONFIG = DANEGDPConfig()


# ── Columnas procesadas — PIB Colombia ───────────────────────────────

DANE_GDP_PROCESSED_COLUMNS: list[str] = [
    "date", "year", "quarter",
    "gdp_observed",
    "source", "download_date",
]


# ── Configuración PIB enfoque del gasto (Inversión) ──────────────────

@dataclass(frozen=True)
class DANEGDPExpenditureConfig:
    """Configuración para el anexo PIB enfoque del gasto del DANE.

    El DANE publica ``anex-GastoConstantes-{trim}{YYYY}.xlsx`` en la
    misma página de PIB información técnica.  Contiene datos originales
    y desestacionalizados.

    Estructura del Excel (hoja ``Cuadro 2`` — desestacionalizado):
      - Fila 9  (0-idx) col B='Concepto', col C en adelante = años
      - Fila 10 (0-idx) = trimestres romanos (I, II, III, IV) en col C+
      - Concepto en columna B (0-idx 1)
      - Datos numéricos empiezan en columna C (0-idx 2)
      - Fila "Formación bruta de capital fijo" = inversión desest.
    """

    # ── Scraping ──────────────────────────────────────────────────
    page_url: str = (
        "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
        "cuentas-nacionales/cuentas-nacionales-trimestrales/"
        "pib-informacion-tecnica"
    )
    base_url: str = "https://www.dane.gov.co"

    # Patrón regex para el anexo GastoConstantes
    link_pattern: str = (
        r"/files/operaciones/PIB/anex-GastoConstantes-"
        r"(?:I{1,3}|IV)trim\d{4}\.xlsx$"
    )

    # ── Parsing del Excel ─────────────────────────────────────────
    sheet_name: str = "Cuadro 2"
    year_row: int = 9
    quarter_row: int = 10
    concept_col: int = 1           # columna B
    concept_label: str = "Formación bruta de capital fijo"
    data_start_col: int = 2        # columna C

    source_label: str = "DANE - Cuentas Nacionales Trimestrales (Gasto)"

    # ── Archivos ──────────────────────────────────────────────────
    raw_xlsx_filename: str = "dane_gdp_gasto_raw.xlsx"
    processed_filename: str = "dane_gdp_expenditure_colombia.csv"

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


DANE_GDP_EXPENDITURE_CONFIG = DANEGDPExpenditureConfig()

DANE_GDP_EXPENDITURE_PROCESSED_COLUMNS: list[str] = [
    "date", "year", "quarter",
    "investment",
    "source", "download_date",
]

# Sanity bounds (PIB trimestral en miles de millones de pesos)
DANE_GDP_MIN: float = 0.0           # No puede ser negativo
DANE_GDP_MAX: float = 1_000_000.0   # Máximo defensivo (PIB CO ~270k bn COP en 2024)


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


# ── Configuración BANREP – Inflación (SUAMECA) ──────────────────────

@dataclass(frozen=True)
class BanrepInflationConfig:
    """Configuración para las series de inflación de BANREP/SUAMECA.

    La plataforma SUAMECA del Banco de la República expone un API REST
    que permite consultar series estadísticas.  Para acceder se necesita
    una sesión HTTP con warm-up previo (GET a la página pública) que
    establece las cookies necesarias.

    Series disponibles:
    - **Inf_Goal**: Meta de inflación (anual, expandida a meses por BANREP)
    - **Inf_Rate**: Inflación total anual (variación interanual, mensual)
    - **Core_Inf**: Inflación sin alimentos ni regulados (mensual)
    """

    # ── URLs ──────────────────────────────────────────────────────
    base_url: str = "https://suameca.banrep.gov.co"
    warmup_path: str = (
        "/estadisticas-economicas/informacionSerie"
        "/100001/inflacion_y_meta"
    )
    endpoint_path: str = (
        "/estadisticas-economicas-back/rest"
        "/estadisticaEconomicaRestService"
        "/consultaInformacionSerieXTipoDato"
    )

    # ── Mapa de series ────────────────────────────────────────────
    # Cada entrada: nombre_columna → {id, tipo_dato}
    series_map: dict[str, dict[str, int]] = field(default_factory=lambda: {
        "Inf_Goal": {"id": 853, "tipo_dato": 18},
        "Inf_Rate": {"id": 15270, "tipo_dato": 9},
        "Core_Inf": {"id": 15390, "tipo_dato": 9},
    })

    # Cantidad máxima de datos a solicitar por serie.
    cant_datos: int = 2400

    # ── Archivos ──────────────────────────────────────────────────
    raw_json_filename: str = "banrep_inflation_raw.json"
    processed_filename: str = "inflation_banrep_colombia.csv"

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

    @property
    def warmup_url(self) -> str:
        return self.base_url + self.warmup_path

    @property
    def endpoint_url(self) -> str:
        return self.base_url + self.endpoint_path


BANREP_INFLATION_CONFIG = BanrepInflationConfig()


# ── Configuración BANREP – TES Cero Cupón (SUAMECA) ─────────────────

@dataclass(frozen=True)
class BanrepTESConfig:
    """Configuración para las series de TES del Banco de la República.

    La plataforma SUAMECA expone las tasas de interés Cero Cupón de
    Títulos de Tesorería (TES) tanto en pesos como en UVR.  El pipeline
    descarga datos **diarios** (tipoDato=1) y los agrega a frecuencia
    mensual tomando el **último valor disponible** de cada mes (proxy
    de cierre de mes).

    Series:
    - **TES_UVR_1Y**: Tasa Cero Cupón TES UVR — 1 año (idSerie 15275)
    - **TES_PESOS_1Y**: Tasa Cero Cupón TES pesos — 1 año (idSerie 15272)
    """

    # ── URLs (misma plataforma SUAMECA) ──────────────────────────
    base_url: str = "https://suameca.banrep.gov.co"
    warmup_path: str = (
        "/estadisticas-economicas/informacionSerie"
        "/100001/inflacion_y_meta"
    )
    endpoint_path: str = (
        "/estadisticas-economicas-back/rest"
        "/estadisticaEconomicaRestService"
        "/consultaInformacionSerieXTipoDato"
    )

    # ── Mapa de series ────────────────────────────────────────────
    # Cada entrada: nombre_columna → idSerie en SUAMECA.
    series_map: dict[str, int] = field(default_factory=lambda: {
        "TES_UVR_1Y": 15275,
        "TES_PESOS_1Y": 15272,
    })

    # Tipo de dato: 1 = diario (se agrega a mensual en el pipeline).
    tipo_dato: int = 1

    # Cantidad de observaciones diarias a solicitar.
    cant_datos: int = 8000

    # ── Archivos ──────────────────────────────────────────────────
    raw_json_filename: str = "banrep_tes_raw.json"
    processed_filename: str = "tes_banrep_colombia.csv"

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

    @property
    def warmup_url(self) -> str:
        return self.base_url + self.warmup_path

    @property
    def endpoint_url(self) -> str:
        return self.base_url + self.endpoint_path


BANREP_TES_CONFIG = BanrepTESConfig()


# ── Configuración Brent (FRED / EIA) ────────────────────────────────

@dataclass(frozen=True)
class BrentConfig:
    """Configuración para la serie de precios del Brent (FRED/EIA).

    FRED publica la serie POILBREUSDM (precio mensual promedio del
    crudo Brent en USD/barril).  Se descarga como CSV vía URL directa
    (``fredgraph.csv``).  El pipeline también mantiene la serie diaria
    cruda y genera un dataset mensual (promedio) para compatibilidad
    con el resto del proyecto.

    Se soporta fallback a ``curl`` si ``urllib`` falla (restricciones
    de red corporativa, TLS, etc.).
    """

    # ── Fuente ────────────────────────────────────────────────────
    series_id: str = "POILBREUSDM"
    source_base_url: str = "https://fred.stlouisfed.org/graph/fredgraph.csv"
    start_date: str = "2001-01-01"  # ISO format

    # ── Archivos ──────────────────────────────────────────────────
    raw_csv_filename: str = "brent_raw.csv"
    processed_filename: str = "brent_colombia.csv"

    # ── HTTP ──────────────────────────────────────────────────────
    timeout: int = 60
    http_headers: dict[str, str] = field(
        default_factory=lambda: {
            "Accept": "text/csv,*/*;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36 "
                "NAIRUPipeline/1.0"
            ),
        }
    )


BRENT_CONFIG = BrentConfig()


# ── Configuración ANDI – Capacidad Instalada (EOIC) ─────────────────

@dataclass(frozen=True)
class AndiConfig:
    """Configuración para la Encuesta de Opinión Industrial Conjunta (EOIC).

    La ANDI publica mensualmente los resultados de la EOIC como PDF
    en su página de Desarrollo Económico y Competitividad.  El pipeline
    hace scraping de la página, descarga los PDFs y extrae el porcentaje
    de utilización de la capacidad instalada usando pdfplumber + fuzzy
    matching de frases clave.

    El indicador es un proxy de la brecha del producto, complementario
    al desempleo para estimar presiones inflacionarias.
    """

    # ── URLs de scraping ──────────────────────────────────────────
    base_url: str = "https://www.andi.com.co"
    eoic_page_url: str = (
        "https://www.andi.com.co/Home/Pagina/"
        "3-desarrollo-economico-y-competitividad"
    )

    # ── Regexes de identificación de enlaces ──────────────────────
    # Patrón obligatorio (sigla EOIC).
    eoic_required_pattern: str = r"\beoic\b"
    # Patrón amplio (nombre completo de la encuesta).
    eoic_broad_pattern: str = (
        r"encuesta\s+de\s+opini[oó]n\s+industrial\s+conjunta"
    )
    # Exclusiones (metodología, presentaciones, etc.).
    eoic_exclude_pattern: str = (
        r"metodolog[ií]a|presentaci[oó]n|resultados\s+generales|"
        r"cuestionario|formulario|ficha\s+t[eé]cnica"
    )

    # ── Extracción de datos del PDF ───────────────────────────────
    # Frases clave para localizar la utilización de capacidad instalada.
    capacity_phrases: list[str] = field(default_factory=lambda: [
        "utilizacion de la capacidad instalada",
        "utilización de la capacidad instalada",
        "uso de la capacidad instalada",
        "capacidad instalada utilizada",
        "porcentaje de utilizacion de capacidad",
        "porcentaje de utilización de capacidad",
        "capacidad instalada",
        "utilizacion capacidad",
        "utilización capacidad",
        "uso capacidad instalada",
        "utilizacion de capacidad",
        "utilización de capacidad",
        "aprovechamiento de la capacidad",
        "uso de capacidad",
    ])

    # Regex para capturar porcentajes (e.g. "78.5%", "78,5 %").
    percent_pattern: str = r"(\d{1,3}(?:[.,]\d{1,2})?)\s*%"

    # Umbral de similitud fuzzy para estrategias de extracción.
    text_similarity_threshold: float = 0.82
    table_similarity_threshold: float = 0.75

    # ── Archivos ──────────────────────────────────────────────────
    cache_filename: str = "processed_cache.json"
    processed_filename: str = "andi_capacidad_instalada.csv"
    report_filename: str = "andi_report.txt"

    # ── HTTP ──────────────────────────────────────────────────────
    timeout: int = 60
    max_retries: int = 3
    http_headers: dict[str, str] = field(
        default_factory=lambda: {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,"
                "application/xml;q=0.9,*/*;q=0.8"
            ),
        }
    )

    # Mapeo de meses en español → número.
    month_map: dict[str, int] = field(default_factory=lambda: {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
        "septiembre": 9, "octubre": 10, "noviembre": 11,
        "diciembre": 12,
    })


ANDI_CONFIG = AndiConfig()


# ── Columnas procesadas por dataset ───────────────────────────────────

PROCESSED_COLUMNS: list[str] = [
    "date",
    "year",
    "month",
    "unemployment_rate",
    "tgp_rate",
    "pet_thousands",
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

# GEIH – TGP y PET: rangos razonables
TGP_MIN: float = 40.0    # porcentaje (Colombia histórica ~55–70%)
TGP_MAX: float = 80.0
PET_MIN: float = 20_000.0   # miles de personas (Colombia ~40M PET)
PET_MAX: float = 60_000.0

IPC_INDEX_MIN: float = 10.0    # Mínimo razonable (base 2018≈100, año 2003≈50)
IPC_INDEX_MAX: float = 300.0   # Máximo razonable para horizonte largo

# BANREP inflación: rangos razonables (porcentaje)
INFLATION_RATE_MIN: float = -5.0   # deflación extrema teórica
INFLATION_RATE_MAX: float = 45.0   # inflación CO años 90 (~42%)
INFLATION_GOAL_MIN: float = 1.0
INFLATION_GOAL_MAX: float = 30.0

BANREP_PROCESSED_COLUMNS: list[str] = [
    "date",
    "year",
    "month",
    "Inf_Goal",
    "Inf_Rate",
    "Core_Inf",
    "source",
    "download_date",
]

BANREP_TES_PROCESSED_COLUMNS: list[str] = [
    "date",
    "year",
    "month",
    "TES_UVR_1Y",
    "TES_PESOS_1Y",
    "source",
    "download_date",
]

# BANREP TES: rangos razonables (tasa de interés, porcentaje)
TES_RATE_MIN: float = -5.0     # Tasa real negativa extrema (UVR)
TES_RATE_MAX: float = 40.0     # Tasa máxima defensiva (crisis años 90)

BRENT_PROCESSED_COLUMNS: list[str] = [
    "date",
    "year",
    "month",
    "brent_usd_per_barrel",
    "source",
    "download_date",
]

ANDI_PROCESSED_COLUMNS: list[str] = [
    "date",
    "year",
    "month",
    "capacity_utilization",
    "source",
    "download_date",
]

# Brent: rangos razonables (USD/barril)
BRENT_PRICE_MIN: float = 0.01   # Precio > 0 (incluye colapso 2020)
BRENT_PRICE_MAX: float = 200.0  # Máximo defensivo (pico histórico ~147)

# ANDI: rangos razonables (porcentaje de utilización de capacidad instalada)
CAPACITY_UTILIZATION_MIN: float = 30.0   # Mínimo plausible (crisis profunda)
CAPACITY_UTILIZATION_MAX: float = 100.0  # Máximo teórico
CAPACITY_UTILIZATION_MAX_CHANGE: float = 20.0  # Cambio mensual máximo (pp)

# ── Logging ───────────────────────────────────────────────────────────
LOG_FORMAT: str = "%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s"
LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"
LOG_FILENAME: str = "pipeline.log"


# ── Configuración PWT 10.01 – Stock de Capital y Capital Humano ──────

@dataclass(frozen=True)
class PWTConfig:
    """Configuración para la fuente Penn World Tables 10.01.

    Las Penn World Tables (PWT) publican datos de cuentas nacionales
    comparables internacionalmente.  El archivo CSV completo se descarga
    desde el Dataverse de la Universidad de Groningen y se filtra para
    Colombia (``countrycode == "COL"``).

    Series extraídas:
    - **ck**: Stock de capital a PPP corrientes (miles de millones USD).
    - **cn**: Stock de capital a precios nacionales constantes 2017
      (miles de millones USD).
    - **hc**: Índice de Capital Humano (basado en escolaridad y retornos
      a la educación).

    Cobertura temporal: 1950–2019 (anual).
    """

    # ── Fuente ────────────────────────────────────────────────────
    # PWT 11.0 (Oct 2025): Excel fileId=554105, Stata fileId=554030
    # Nota: el Dataverse requiere autenticación; usar herramienta online
    # https://pwt-data-tool.streamlit.app/ para exportar a data/raw/pwt/
    source_url: str = "https://dataverse.nl/api/access/datafile/554105"
    country_code: str = "COL"

    # ── Archivos ──────────────────────────────────────────────────
    raw_csv_filename: str = "pwt_raw.csv"
    processed_filename: str = "pwt_colombia.csv"

    # ── HTTP ──────────────────────────────────────────────────────
    timeout: int = 60
    http_headers: dict[str, str] = field(
        default_factory=lambda: {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        }
    )


PWT_CONFIG = PWTConfig()

PWT_PROCESSED_COLUMNS: list[str] = [
    "date",
    "year",
    "month",
    "capital_stock_real",   # ← rnna (precios nac. const. 2017, millones COP)
    "depreciation_rate",    # ← delta (fracción 0–1)
    "human_capital",        # ← hc (índice)
    "source",
    "download_date",
]

# PWT: rangos razonables
CAPITAL_STOCK_MIN: float = 0.0          # Stock de capital no puede ser negativo
CAPITAL_STOCK_MAX: float = 5_000_000.0  # millones COP 2017 (Colombia ≈ 0.9–2.7M)
DEPRECIATION_RATE_MIN: float = 0.01     # Tasa de depreciación mínima razonable
DEPRECIATION_RATE_MAX: float = 0.15     # Tasa de depreciación máxima defensiva
HUMAN_CAPITAL_MIN: float = 1.0          # Mínimo teórico del índice PWT
HUMAN_CAPITAL_MAX: float = 5.0          # Máximo teórico del índice PWT


# ── Configuración VIOG ───────────────────────────────────────────────

@dataclass(frozen=True)
class VIOGConfig:
    """Configuración para el pipeline VIOG (output gap USA ponderado por filtros).

    Aplica 5 filtros de tendencia (BK, CF, Butterworth, HP, Kalman) más el
    PIB potencial de función de producción (CBO), calcula ponderadores basados
    en el error acumulado (VIOG y 1/VIOG) y devuelve la brecha del producto
    compuesta.
    """

    input_filename: str = "PIB_USA.xlsx"
    processed_filename: str = "viog_usa.csv"
    source_label: str = "FRED/CBO"

    # Columnas del Excel de entrada.
    # ref_col=None → VIOG sin referencia externa (solo 5 filtros estadísticos).
    series_col: str = "Value(Billions)"
    ref_col: Optional[str] = "Potential Value(Billions)"

    # Baxter-King
    bk_low: int = 6
    bk_high: int = 32
    bk_K: int = 12

    # Christiano-Fitzgerald
    cf_low: int = 6
    cf_high: int = 32

    # Hodrick-Prescott
    hp_lambda: int = 1600

    # Butterworth
    bw_cutoff: float = 1.0 / 16.0
    bw_order: int = 8

    # Kalman UCM — bounds del período del ciclo en trimestres (igual que notebook original)
    kalman_cycle_period_bounds: tuple[float, float] = (0.3, 40.0)


VIOG_CONFIG = VIOGConfig()

# VIOG Colombia — replica del pipeline VIOG sobre el PIB colombiano.
# Input: data/inputs/PIB_CO.xlsx (combinación manual del usuario:
#   serie observada del scraper DANE + PIB potencial estimado por
#   función de producción provisto externamente).
# Mismos parámetros econométricos que el VIOG-USA (mismos filtros,
# mismos lambda/cutoff, mismos bounds del Kalman). Lo único que cambia
# es el nombre de archivo y la etiqueta de fuente.
VIOG_CO_CONFIG = VIOGConfig(
    input_filename="PIB_CO.xlsx",
    processed_filename="viog_colombia.csv",
    source_label="DANE Cuentas Nacionales",
    series_col="Value(Billions)",
    ref_col=None,  # Sin PIB potencial externo — solo 5 filtros estadísticos
)

VIOG_PROCESSED_COLUMNS: list[str] = [
    "date",
    "year",
    "quarter",
    "gap_viog",
    "gap_inv_viog",
    "gap_ref",
    "gap_hp",
    "gap_cf",
    "gap_bk",
    "gap_bw",
    "gap_kalman",
    "source",
]
