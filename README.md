# NAIRU Colombia — Pipeline de Datos

Pipeline de datos reproducible para construir la base empírica necesaria para
estimar la **NAIRU** (*Non-Accelerating Inflation Rate of Unemployment*) en
Colombia.  El proyecto descarga, limpia y valida series macroeconómicas
oficiales del **DANE** y el **Banco de la República** mediante scraping
automatizado y APIs REST, y las transforma en datasets analíticos listos
para modelamiento econométrico.

---

## Contexto económico

La **NAIRU** es la tasa de desempleo consistente con una inflación estable.
Por debajo de ella, la presión sobre el mercado laboral tiende a acelerar los
precios; por encima, la economía opera con holgura y la inflación tiende a
moderarse.

Estimar la NAIRU es fundamental para:

- Evaluar la **brecha del mercado laboral** y el ciclo económico.
- Informar decisiones de **política monetaria** (Banco de la República).
- Complementar el análisis del **PIB potencial** y la regla fiscal.

En Colombia, las estimaciones oficiales provienen principalmente del Banco de
la República y del Ministerio de Hacienda, pero no existe un repositorio
abierto y reproducible que integre las series de insumo y facilite la
replicación de los ejercicios econométricos.  Este proyecto busca llenar ese
vacío.

---

## Estado actual del proyecto

| Componente                                                 | Estado            |
| ---------------------------------------------------------- | ----------------- |
| Pipeline de desempleo/TGP/PET (DANE – GEIH desestacionalizado) | Funcional         |
| Pipeline de IPC (DANE – Índice de Precios al Consumidor)       | Funcional         |
| Pipeline de inflación (BANREP – SUAMECA)                       | Funcional         |
| Pipeline de Brent (FRED / EIA)                                  | Funcional         |
| Pipeline ANDI EOIC (Capacidad Instalada)                        | Funcional         |
| Pipeline TES Cero Cupón (BANREP – SUAMECA)                    | Funcional         |
| Pipeline PWT 11.0 (Capital Stock + Capital Humano)              | Funcional         |
| Base mensual unificada (nairu_dataset.csv — 17 columnas)        | Funcional         |
| Validaciones de calidad automatizadas                           | 371 tests pasando |
| Estimación econométrica de la NAIRU                           | Por implementar   |

**Datos generados actualmente:**

- **Desempleo, TGP y PET (GEIH desestacionalizado):** 300 observaciones mensuales (enero 2001 – diciembre 2025) con tasa de desocupación, tasa global de participación y población en edad de trabajar en miles de personas
- **Índice de Precios al Consumidor:** 278 observaciones mensuales (enero 2003 – febrero 2026)
- **Inflación BANREP:** 858 observaciones mensuales (julio 1955 – diciembre 2026) con meta de inflación, inflación total e inflación sin alimentos ni regulados
- **Precio del Brent:** 302 observaciones mensuales (enero 2001 – febrero 2026) en USD/barril
- **Capacidad instalada (ANDI EOIC):** Porcentaje de utilización de la capacidad instalada industrial (mensual)
- **Tasas TES Cero Cupón:** Tasas de interés a 1 año (pesos y UVR) agregadas a frecuencia mensual (último valor del mes)
- **Capital Stock y Capital Humano (PWT 11.0):** 70 observaciones anuales (1954–2023) con capital stock en PPP (ck), capital stock nominal (cn) y capital humano (hc)
- **Base unificada (nairu_dataset.csv):** 860 filas × 17 columnas (1954–2026), series mensuales + anuales PWT alineadas por fecha

---

## Fuentes de datos

### 1. Desempleo — DANE GEIH (desestacionalizado)

| Campo          | Detalle                                                                                                                      |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Fuente         | Gran Encuesta Integrada de Hogares (GEIH) — Anexo desestacionalizado                                                        |
| Indicador      | Tasa de Desocupación (TD) desestacionalizada — Total Nacional                                                              |
| Periodicidad   | Mensual                                                                                                                      |
| Cobertura      | Enero 2001 – presente                                                                                                       |
| Formato origen | Excel pivoteado (conceptos × año·mes)                                                                                     |
| Página        | [dane.gov.co — Empleo y desempleo](https://www.dane.gov.co/index.php/estadisticas-por-tema/mercado-laboral/empleo-y-desempleo) |

### 2. IPC — DANE

| Campo          | Detalle                                                                                                                         |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Fuente         | Índice de Precios al Consumidor (IPC)                                                                                          |
| Indicador      | Índice de empalme, base diciembre 2018 = 100                                                                                   |
| Periodicidad   | Mensual                                                                                                                         |
| Cobertura      | Enero 2003 – presente                                                                                                          |
| Formato origen | Excel pivoteado (meses × años)                                                                                                |
| Página        | [dane.gov.co — IPC](https://www.dane.gov.co/index.php/estadisticas-por-tema/precios-y-costos/indice-de-precios-al-consumidor-ipc) |

### 3. Inflación — Banco de la República (SUAMECA)

| Campo          | Detalle                                                                                                              |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| Fuente         | Plataforma SUAMECA del Banco de la República                                                                        |
| Indicadores    | Meta de inflación (Inf\_Goal), inflación total (Inf\_Rate), inflación sin alimentos ni regulados (Core\_Inf)      |
| Periodicidad   | Mensual                                                                                                              |
| Cobertura      | Inf\_Rate desde julio 1955, Core\_Inf desde marzo 1983, Inf\_Goal desde enero 1991                                   |
| Formato origen | API REST JSON (epoch\_ms + valor)                                                                                    |
| Página        | [suameca.banrep.gov.co](https://suameca.banrep.gov.co/estadisticas-economicas/informacionSerie/100001/inflacion_y_meta) |

### 4. Precio del petróleo Brent — FRED / EIA

| Campo          | Detalle                                                                       |
| -------------- | ----------------------------------------------------------------------------- |
| Fuente         | FRED (Federal Reserve Economic Data) / U.S. Energy Information Administration |
| Indicador      | Precio promedio mensual del crudo Brent (POILBREUSDM), USD/barril             |
| Periodicidad   | Mensual                                                                       |
| Cobertura      | Enero 2001 – presente                                                        |
| Formato origen | CSV vía URL directa (fredgraph.csv)                                          |
| Página        | [fred.stlouisfed.org](https://fred.stlouisfed.org/series/POILBREUSDM)            |

### 5. Capacidad instalada — ANDI EOIC

| Campo          | Detalle                                                                                               |
| -------------- | ----------------------------------------------------------------------------------------------------- |
| Fuente         | Encuesta de Opinión Industrial Conjunta (EOIC) — ANDI                                                |
| Indicador      | Porcentaje de utilización de la capacidad instalada industrial                                        |
| Periodicidad   | Mensual                                                                                               |
| Formato origen | PDF (extracción con pdfplumber + fuzzy matching)                                                     |
| Página        | [andi.com.co](https://www.andi.com.co/Home/Pagina/3-desarrollo-economico-y-competitividad)            |

El pipeline ANDI realiza scraping de la página de la ANDI, identifica los
PDFs de la EOIC, los descarga y extrae el porcentaje de utilización de la
capacidad instalada usando 3 estrategias de extracción (fuzzy text, tablas,
regex).  Soporta modos incremental (último PDF) y backfill (todos).

### 6. Tasas TES Cero Cupón — Banco de la República (SUAMECA)

| Campo          | Detalle                                                                                                              |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| Fuente         | Plataforma SUAMECA del Banco de la República                                                                        |
| Indicadores    | TES Cero Cupón pesos — 1 año (TES\_PESOS\_1Y), TES Cero Cupón UVR — 1 año (TES\_UVR\_1Y)                          |
| Periodicidad   | Diaria (agregada a mensual: último valor del mes)                                                                   |
| Formato origen | API REST JSON (epoch\_ms + valor), tipoDato=1 (diario)                                                              |
| Página        | [suameca.banrep.gov.co](https://suameca.banrep.gov.co/estadisticas-economicas/informacionSerie/100001/inflacion_y_meta) |

El pipeline TES descarga datos diarios de la curva Cero Cupón de TES
(pesos y UVR, plazo 1 año) desde SUAMECA y los agrega a frecuencia
mensual tomando el **último valor disponible** de cada mes como proxy
de cierre de mes.

### 7. Capital Stock y Capital Humano — PWT 11.0

| Campo          | Detalle                                                                                                              |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| Fuente         | Penn World Tables versión 11.0 (Feenstra, Inklaar & Timmer)                                                        |
| Indicadores    | Capital stock a precios PPP de 2017 (ck), capital stock a precios nacionales (cn), índice de capital humano (hc)   |
| Periodicidad   | Anual (referenciado a 1 de enero de cada año)                                                                       |
| Cobertura      | 1954–2023 (Colombia, ISO code: COL)                                                                                 |
| Formato origen | CSV wide exportado desde la herramienta online (ISO code, Variable code, año 1950…2023)                             |
| Página        | [PWT Data Tool](https://pwt-data-tool.streamlit.app/) / [Dataverse PWT 11.0](https://dataverse.nl/dataset.xhtml?persistentId=doi:10.34894/QK5VDO) |

El pipeline PWT busca primero un archivo local en `data/raw/pwt/` (exportado
manualmente desde la herramienta online); si no lo encuentra, intenta descarga
directa desde Dataverse.  Detecta automáticamente el formato wide (herramienta
online) vs. largo (Dataverse) y produce el esquema estándar del proyecto.

Ambos pipelines del DANE realizan **scraping automatizado** de la página,
detectan el anexo Excel más reciente, lo descargan, parsean la estructura
pivoteada y producen un CSV en formato largo estandarizado.  El pipeline
de BANREP consulta el API REST de SUAMECA con sesión HTTP autenticada.
El pipeline de Brent descarga el CSV directamente de FRED con fallback
curl → requests → urllib para máxima compatibilidad.

---

## Arquitectura del repositorio

```
scraping-NAIRU/
├── src/
│   ├── config.py                 # Configuración central (rutas, URLs, dataclasses)
│   ├── io_utils.py               # Utilidades de I/O (logging, CSV)
│   ├── quality_checks.py         # Validaciones de calidad (columnas, nulos, rangos)
│   ├── main.py                   # Punto de entrada CLI
│   ├── merge.py                  # Unificación de todas las fuentes → nairu_dataset.csv
│   ├── pipelines/
│   │   ├── run_unemployment.py     # Orquestación: desempleo + TGP + PET (GEIH)
│   │   ├── run_informality.py      # Orquestación: tasa de informalidad 13 ciudades
│   │   ├── run_ipc.py              # Orquestación: IPC
│   │   ├── run_banrep_inflation.py # Orquestación: inflación BANREP
│   │   ├── run_banrep_tes.py       # Orquestación: TES Cero Cupón BANREP
│   │   ├── run_brent.py            # Orquestación: Brent (FRED/EIA)
│   │   ├── run_andi.py             # Orquestación: ANDI EOIC (capacidad instalada)
│   │   ├── run_pwt.py              # Orquestación: PWT 11.0 (capital stock + cap. humano)
│   │   ├── run_viog.py             # Orquestación: VIOG (output gap USA ponderado)
│   │   └── run_all.py              # Ejecuta todos los pipelines + merge
│   └── sources/
│       ├── dane/
│       │   ├── common.py         # Helpers compartidos del DANE
│       │   ├── unemployment.py   # Scraping + parsing GEIH (TD, TGP, PET)
│       │   ├── informality.py    # Parsing GEIH-EISS (informalidad 13 ciudades)
│       │   └── ipc.py            # Scraping + parsing IPC
│       ├── banrep/
│       │   ├── inflation.py      # API REST SUAMECA: inflación + meta
│       │   └── tes.py            # API REST SUAMECA: TES Cero Cupón (diario → mensual)
│       ├── fred/
│       │   └── brent.py          # CSV FRED: precio Brent
│       ├── andi/
│       │   └── eoic.py           # Scraping PDFs + extracción capacidad instalada
│       ├── pwt/
│       │   └── pwt.py            # Descarga/parsing PWT 11.0 (wide y largo)
│       └── viog/
│           └── viog.py           # Output gap USA: BK, CF, BW, HP, Kalman + ponderación VIOG
├── tests/
│   ├── test_geih.py              # 44 tests — scraping, parsing y calidad GEIH
│   ├── test_informality.py       # 31 tests — parsing GEIH-EISS y calidad
│   ├── test_ipc.py               # 24 tests — scraping, parsing y calidad IPC
│   ├── test_banrep_inflation.py  # 30 tests — API BANREP, parsing y calidad
│   ├── test_banrep_tes.py        # 41 tests — TES diario→mensual, parsing y calidad
│   ├── test_brent.py             # 39 tests — CSV FRED, agregación y calidad
│   ├── test_andi.py              # 64 tests — scraping, PDF parsing y calidad ANDI
│   ├── test_pwt.py               # 34 tests — wide/largo format, parsing y calidad PWT
│   ├── test_viog.py              # 32 tests — filtros (BK/CF/BW/HP/Kalman) y VIOG
│   ├── test_merge.py             # 28 tests — integración merge, columnas y NaN anuales
│   └── test_pipeline.py          # 15 tests — estructura, calidad e I/O
├── data/
│   ├── raw/                      # Datos crudos descargados por scrapers
│   │   ├── dane/                 #   Archivos crudos del DANE (Excel, HTML)
│   │   ├── banrep/               #   Respuestas crudas del API SUAMECA (JSON)
│   │   ├── fred/                 #   CSV crudo de FRED (Brent, GDPC1, GDPPOT)
│   │   ├── andi/                 #   PDFs descargados de la ANDI (EOIC)
│   │   └── pwt/                  #   CSV exportado desde la herramienta PWT online
│   ├── inputs/                   # Inputs manuales (no provienen de scrapers)
│   │   ├── PIB_USA.xlsx          #   Insumo del VIOG (CBO + FRED, manual)
│   │   └── nairu_estimates_v6.csv  # Benchmark externo (estimaciones de NAIRU)
│   ├── processed/                # Outputs por fuente (alimentan el merge)
│   └── final/                    # Dataset(s) consolidado(s) — output del merge
├── docs/bib/                     # Bibliografía de referencia
├── logs/                         # Logs del pipeline
├── outputs/                      # Reportes generados (ANDI, etc.)
├── andi_agent/                   # ⚠ DEPRECADO — agente legacy del scraper ANDI EOIC
│                                 #   (sustituido por src/sources/andi/eoic.py)
└── requirements.txt
```

> **Nota sobre `andi_agent/`:** Es la primera versión standalone del
> scraper de la ANDI (con su propio `main.py`, `requirements.txt` y CSV
> auxiliar). La lógica equivalente —scraping + parsing de PDFs— está
> reescrita y testada en `src/sources/andi/eoic.py` y se invoca con
> `python -m src.main --andi`. La carpeta `andi_agent/` se conserva sólo
> como referencia histórica; **no debe modificarse** ni usarse en flujos
> nuevos.

**Principios de diseño:**

- **Separación de capas:** scraping → parsing → validación → guardado.
- **Configuración declarativa:** cada fuente se describe con un dataclass
  (`GEIHConfig`, `IPCConfig`, `BanrepInflationConfig`, `BanrepTESConfig`, `BrentConfig`, `AndiConfig`) que
  centraliza URLs, patrones y parámetros de parsing.
- **Detección robusta:** los parsers del DANE no asumen posiciones fijas de
  filas/columnas; usan heurísticas (regex, conteo de años, búsqueda de
  etiquetas) para adaptarse a cambios en el formato.  El módulo de BANREP
  normaliza timestamps epoch a fechas de inicio de mes y maneja sesiones HTTP
  con warmup automático.

---

## Cómo ejecutar el proyecto

### Requisitos previos

- Python ≥ 3.11
- Conexión a internet (para descargar datos de DANE, BANREP, FRED y ANDI)

### Instalación

**macOS / Linux (bash/zsh):**

```bash
git clone https://github.com/Tupxz/scraping-NAIRU.git
cd scraping-NAIRU
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**Windows (PowerShell):**

```powershell
git clone https://github.com/Tupxz/scraping-NAIRU.git
cd scraping-NAIRU
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

> **Nota (Windows):** si PowerShell bloquea la activación del entorno, ejecutar
> primero: `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`

### Ejecución

Cada comando descarga, parsea, valida y guarda los datos automáticamente en
`data/processed/` (CSVs por fuente).  El merge final se escribe en
`data/final/nairu_dataset.csv`.  No se requiere ningún paso manual adicional,
salvo para los inputs manuales en `data/inputs/` (ver "Arquitectura").

**Todos los pipelines a la vez:**

```bash
python -m src.main --all
```

**Pipelines individuales (se pueden combinar):**

```bash
# ── Mercado laboral colombiano ─────────────────────────────────
python -m src.main --unemployment          # Desempleo + TGP + PET (DANE GEIH desest.)
python -m src.main --informality           # Tasa de informalidad 13 ciudades (DANE GEIH-EISS)

# ── Precios e inflación ────────────────────────────────────────
python -m src.main --ipc                   # IPC (DANE)
python -m src.main --banrep                # Inflación + meta + núcleo (BANREP/SUAMECA)

# ── Sector financiero / energético ─────────────────────────────
python -m src.main --tes                   # TES Cero Cupón pesos+UVR 1Y (BANREP/SUAMECA)
python -m src.main --brent                 # Precio Brent (FRED/EIA)

# ── Industria ──────────────────────────────────────────────────
python -m src.main --andi                  # ANDI EOIC — último mes disponible (incremental)
python -m src.main --andi-backfill         # ANDI EOIC — todos los PDFs históricos
python -m src.main --andi-reprocess        # ANDI EOIC — reprocesar PDFs locales no incluidos

# ── Variables estructurales / externas ─────────────────────────
python -m src.main --pwt                   # Capital Stock + Capital Humano (PWT 11.0)
python -m src.main --dane-gdp              # PIB trimestral DANE (Cuentas Nacionales, desest.)
python -m src.main --viog                  # Brecha del producto USA (VIOG, 5 filtros + CBO)
python -m src.main --viog-co               # Brecha del producto Colombia (requiere PIB_CO.xlsx)

# ── Consolidación ──────────────────────────────────────────────
python -m src.main --merge                 # Unifica todas las fuentes → data/final/nairu_dataset.csv
```

**Combinar varios pipelines en una sola ejecución:**

```bash
python -m src.main --unemployment --ipc --andi
python -m src.main --viog --merge          # Recalcular VIOG y luego re-unir
```

**Notas sobre pipelines especiales:**

- `--viog` requiere que `data/inputs/PIB_USA.xlsx` exista (input manual,
  no descargado por scraper). Genera `data/processed/viog_usa.csv` y
  gráficas de los 5 filtros en `outputs/viog/`.
- `--viog-co` aplica el mismo VIOG sobre el PIB de Colombia. Requiere
  que el usuario combine en `data/inputs/PIB_CO.xlsx` la serie observada
  trimestral del DANE (descargada con `--dane-gdp`) con el PIB potencial
  estimado por función de producción (input externo). Si el archivo no
  existe, el pipeline omite el paso con un warning sin fallar.
- `--dane-gdp` descarga el anexo *anex-ProduccionConstantes* más reciente
  desde Cuentas Nacionales Trimestrales del DANE y produce
  `data/processed/dane_gdp_colombia.csv`.
- `--andi-backfill` vs `--andi-reprocess`: el primero descarga **todos** los
  PDFs históricos de la EOIC; el segundo solo procesa PDFs ya descargados
  en `data/raw/andi/` que no aparezcan en el CSV procesado.
- `--all` ejecuta los 11 pipelines de fuente + merge en orden lógico
  (insumos → consolidación). El VIOG-Colombia se omite con warning si
  todavía no existe `data/inputs/PIB_CO.xlsx`.

> Los mismos comandos funcionan idénticos en PowerShell; solo cambia la
> activación del entorno virtual (ver arriba).

### Tests

```bash
python -m pytest tests/ -v
```

---

## Salidas esperadas

Después de ejecutar `python -m src.main --all`, el pipeline genera:

| Archivo                                          | Contenido                                | Columnas                                                                   |
| ------------------------------------------------ | ---------------------------------------- | -------------------------------------------------------------------------- |
| `data/processed/dane_labor_colombia.csv`       | TD, TGP y PET mensuales (GEIH desest.)   | `date, year, month, unemployment_rate, tgp_rate, pet_thousands, source, download_date` |
| `data/processed/ipc_colombia.csv`              | Índice de precios al consumidor mensual | `date, year, month, ipc_index, source, download_date`                    |
| `data/processed/inflation_banrep_colombia.csv` | Inflación y meta del Banco de la Rep.   | `date, year, month, Inf_Goal, Inf_Rate, Core_Inf, source, download_date` |
| `data/processed/brent_colombia.csv`            | Precio mensual del Brent (USD/barril)    | `date, year, month, brent_usd_per_barrel, source, download_date`         |
| `data/processed/andi_capacidad_instalada.csv`  | Capacidad instalada industrial (EOIC)    | `date, year, month, capacity_utilization, source, download_date`         |
| `data/processed/tes_banrep_colombia.csv`       | Tasas TES Cero Cupón (pesos y UVR, 1Y) | `date, year, month, TES_UVR_1Y, TES_PESOS_1Y, source, download_date`    |
| `data/processed/pwt_colombia.csv`              | Capital stock y capital humano (anual)   | `date, year, month, capital_stock_ck, capital_stock_cn, human_capital, source, download_date` |
| `data/processed/dane_gdp_colombia.csv`         | PIB trimestral DANE (desestacionalizado) | `date, year, quarter, gdp_observed, source, download_date`              |
| `data/processed/viog_usa.csv`                  | Brecha del producto USA (VIOG, trim.)    | `date, year, quarter, gap_viog, gap_inv_viog, gap_ref, gap_hp, gap_cf, gap_bk, gap_bw, gap_kalman, source` |
| `data/processed/viog_colombia.csv` (opcional)  | Brecha del producto Colombia (VIOG)      | `date, year, quarter, gap_viog, gap_inv_viog, gap_ref, gap_hp, gap_cf, gap_bk, gap_bw, gap_kalman, source` |
| `data/final/nairu_dataset.csv`                 | Base unificada (≈884 filas × 22 cols)    | `date, year, month, unemployment_rate, tgp_rate, pet_thousands, informality_rate_13c, ipc_index, Inf_Goal, Inf_Rate, Core_Inf, brent_usd_per_barrel, capacity_utilization, TES_UVR_1Y, TES_PESOS_1Y, capital_stock_ck, capital_stock_cn, human_capital, gap_viog_us, gap_inv_viog_us, gap_viog_co, gap_inv_viog_co` |

### Cargar el dataset final en pandas

```python
import pandas as pd

# Carga base con índice temporal
df = pd.read_csv(
    "data/final/nairu_dataset.csv",
    parse_dates=["date"],
).set_index("date")

# Ejemplo: serie mensual de desempleo + inflación
df[["unemployment_rate", "Inf_Rate"]].plot(secondary_y="Inf_Rate")

# Ejemplo: filtrar período post-2010
df_recent = df.loc["2010":]

# Ejemplo: las columnas anuales (PWT) están con NaN en meses no-enero
pwt_annual = df[["capital_stock_ck", "human_capital"]].dropna()
```

> Las series anuales (PWT 11.0) sólo tienen valor en enero de cada año;
> los demás meses contienen NaN. Las trimestrales (VIOG) sólo en
> enero/abril/julio/octubre. Para análisis mensual puro, filtrar con
> ``df.dropna(subset=["unemployment_rate"])`` o las columnas que necesites.

Archivos intermedios (en `data/raw/`, ignorados por Git):

- `dane/geih_raw.xlsx` — Anexo GEIH desestacionalizado original
- `dane/geih_page.html` — Snapshot del HTML para auditoría
- `dane/ipc_indices_raw.xlsx` — Anexo IPC original
- `dane/ipc_page.html` — Snapshot del HTML
- `banrep/banrep_inflation_raw.json` — Respuestas crudas del API SUAMECA (inflación)
- `banrep/banrep_tes_raw.json` — Respuestas crudas del API SUAMECA (TES diario)
- `fred/brent_raw.csv` — CSV crudo descargado de FRED
- `andi/*.pdf` — PDFs descargados de la EOIC
- `andi/processed_cache.json` — Cache de PDFs procesados

Reportes generados (en `outputs/`):

- `andi_report.txt` — Resumen del pipeline ANDI: PDFs procesados, extracciones exitosas/fallidas, meses faltantes, valores sospechosos

---

## Dependencias principales

| Paquete            | Uso                                                 |
| ------------------ | --------------------------------------------------- |
| `pandas`         | Transformación de datos y manejo de DataFrames     |
| `requests`       | Descarga HTTP (DANE, ANDI), API REST (BANREP), CSV (FRED) |
| `beautifulsoup4` | Scraping de enlaces desde HTML (DANE, ANDI)         |
| `lxml`           | Parser HTML para BeautifulSoup (ANDI)               |
| `pdfplumber`     | Extracción de texto y tablas de PDFs (ANDI EOIC)   |
| `openpyxl`       | Lectura de archivos Excel (.xlsx)                   |
| `pytest`         | Suite de tests automatizados                        |

Versiones exactas en [`requirements.txt`](requirements.txt).

---

## Roadmap

- [X] Pipeline de desempleo conectado a fuente real del DANE (GEIH desestacionalizado)
- [X] Pipeline de IPC conectado a fuente real del DANE
- [X] Pipeline de inflación conectado al API SUAMECA de BANREP (Inf_Goal, Inf_Rate, Core_Inf)
- [X] Pipeline de Brent conectado a FRED/EIA (POILBREUSDM)
- [X] Pipeline ANDI EOIC — Capacidad instalada industrial (scraping PDF + pdfplumber)
- [X] Pipeline TES Cero Cupón (BANREP/SUAMECA) — tasas pesos y UVR a 1 año (diario → mensual)
- [X] Pipeline PWT 11.0 — Capital stock (ck, cn) y capital humano (hc) para Colombia (1954–2023)
- [X] Agregar TGP y PET al pipeline GEIH (Tasa Global de Participación + Población en Edad de Trabajar)
- [X] Validaciones de calidad automatizadas (371 tests)
- [X] Arquitectura modular (`sources/dane/` + `sources/banrep/` + `sources/fred/` + `sources/andi/` + `sources/pwt/` + `pipelines/`)
- [X] `series_map` configurable para extraer múltiples series (TGP, TO, TES, etc.)
- [X] Base mensual unificada (nairu_dataset.csv — 860 filas × 17 columnas)
- [ ] Calcular serie de inflación interanual a partir del IPC
- [ ] Análisis exploratorio conjunto (notebook)
- [ ] Implementar estimación de la NAIRU (filtro de Kalman / curva de Phillips)
- [ ] Incorporar estimaciones institucionales (Banco de la República, CARF)
- [ ] Agregar fuentes complementarias (expectativas de inflación, brecha del producto)

---

## Buenas prácticas del proyecto

- **Reproducibilidad:** los datos crudos se descargan desde la fuente original
  en cada ejecución; los archivos procesados son determinísticos dado el mismo
  insumo.
- **Tests offline:** los 371 tests usan fixtures sintéticas que simulan la
  estructura real de los datos del DANE, BANREP, FRED, ANDI y PWT, sin requerir conexión a internet.
- **Validaciones de calidad:** cada pipeline verifica columnas, nulos,
  duplicados, rangos y continuidad temporal antes de guardar.
- **Datos ignorados por Git:** los archivos `.xlsx`, `.csv` y `.html`
  descargados se regeneran con el pipeline y no se versionan.
- **Logging estructurado:** cada ejecución genera un log detallado en
  `logs/pipeline.log` con timestamps y trazabilidad completa.

---

## Licencia

[MIT](LICENSE)
