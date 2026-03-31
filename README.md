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
| Pipeline de desempleo (DANE – GEIH desestacionalizado)    | Funcional         |
| Pipeline de IPC (DANE – Índice de Precios al Consumidor) | Funcional         |
| Pipeline de inflación (BANREP – SUAMECA)                 | Funcional         |
| Pipeline de Brent (FRED / EIA)                             | Funcional         |
| Pipeline ANDI EOIC (Capacidad Instalada)                   | Funcional         |
| Pipeline TES Cero Cupón (BANREP – SUAMECA)               | Funcional         |
| Validaciones de calidad automatizadas                      | 243 tests pasando |
| Base mensual unificada (desempleo + IPC + inflación)      | Próximo paso     |
| Estimación econométrica de la NAIRU                      | Por implementar   |

**Datos generados actualmente:**

- **Tasa de desocupación (desestacionalizada):** 299 observaciones mensuales (enero 2001 – enero 2026)
- **Índice de Precios al Consumidor:** 278 observaciones mensuales (enero 2003 – febrero 2026)
- **Inflación BANREP:** 858 observaciones mensuales (julio 1955 – diciembre 2026) con meta de inflación, inflación total e inflación sin alimentos ni regulados
- **Precio del Brent:** 302 observaciones mensuales (enero 2001 – febrero 2026) en USD/barril
- **Capacidad instalada (ANDI EOIC):** Porcentaje de utilización de la capacidad instalada industrial (mensual)
- **Tasas TES Cero Cupón:** Tasas de interés a 1 año (pesos y UVR) agregadas a frecuencia mensual (último valor del mes)

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
│   ├── pipelines/
│   │   ├── run_unemployment.py   # Orquestación: desempleo
│   │   ├── run_ipc.py            # Orquestación: IPC
│   │   ├── run_banrep_inflation.py # Orquestación: inflación BANREP
│   │   ├── run_banrep_tes.py     # Orquestación: TES Cero Cupón BANREP
│   │   ├── run_brent.py          # Orquestación: Brent (FRED/EIA)
│   │   ├── run_andi.py           # Orquestación: ANDI EOIC (capacidad instalada)
│   │   └── run_all.py            # Ejecuta todos los pipelines
│   └── sources/
│       ├── dane/
│       │   ├── unemployment.py   # Scraping + parsing GEIH desestacionalizado
│       │   └── ipc.py            # Scraping + parsing IPC
│       ├── banrep/
│       │   ├── inflation.py      # API REST SUAMECA: inflación + meta
│       │   └── tes.py            # API REST SUAMECA: TES Cero Cupón (diario → mensual)
│       ├── fred/
│       │   └── brent.py          # CSV FRED: precio Brent
│       └── andi/
│           └── eoic.py           # Scraping PDFs + extracción capacidad instalada
├── tests/
│   ├── test_geih.py              # 36 tests — scraping, parsing y calidad GEIH
│   ├── test_ipc.py               # 23 tests — scraping, parsing y calidad IPC
│   ├── test_banrep_inflation.py  # 29 tests — API BANREP, parsing y calidad
│   ├── test_banrep_tes.py       # 40 tests — TES diario→mensual, parsing y calidad
│   ├── test_brent.py             # 38 tests — CSV FRED, agregación y calidad
│   ├── test_andi.py              # 63 tests — scraping, PDF parsing y calidad ANDI
│   └── test_pipeline.py          # 14 tests — estructura, calidad e I/O
├── data/
│   ├── raw/
│   │   ├── dane/                 # Archivos crudos descargados (Excel, HTML)
│   │   ├── banrep/               # Respuestas crudas del API SUAMECA (JSON)
│   │   ├── fred/                 # CSV crudo de FRED (Brent)
│   │   └── andi/                 # PDFs descargados de la ANDI (EOIC)
│   └── processed/                # Datasets limpios listos para análisis
├── docs/bib/                     # Bibliografía de referencia
├── logs/                         # Logs del pipeline
├── outputs/                      # Reportes generados (ANDI, etc.)
└── requirements.txt
```

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
`data/processed/`.  No se requiere ningún paso manual adicional.

**Todos los pipelines a la vez:**

```bash
python -m src.main --all
```

**Pipelines individuales (se pueden combinar):**

```bash
python -m src.main --unemployment          # Desempleo (DANE GEIH)
python -m src.main --ipc                   # IPC (DANE)
python -m src.main --banrep                # Inflación (BANREP/SUAMECA)
python -m src.main --tes                   # TES Cero Cupón (BANREP/SUAMECA)
python -m src.main --brent                 # Brent (FRED/EIA)
python -m src.main --andi                  # ANDI EOIC — último mes disponible
python -m src.main --andi-backfill         # ANDI EOIC — todos los PDFs históricos
```

**Combinar varios pipelines en una sola ejecución:**

```bash
python -m src.main --unemployment --ipc --andi
```

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
| `data/processed/unemployment_colombia.csv`     | Tasa de desocupación mensual (desest.)  | `date, year, month, unemployment_rate, source, download_date`            |
| `data/processed/ipc_colombia.csv`              | Índice de precios al consumidor mensual | `date, year, month, ipc_index, source, download_date`                    |
| `data/processed/inflation_banrep_colombia.csv` | Inflación y meta del Banco de la Rep.   | `date, year, month, Inf_Goal, Inf_Rate, Core_Inf, source, download_date` |
| `data/processed/brent_colombia.csv`            | Precio mensual del Brent (USD/barril)    | `date, year, month, brent_usd_per_barrel, source, download_date`         |
| `data/processed/andi_capacidad_instalada.csv`  | Capacidad instalada industrial (EOIC)    | `date, year, month, capacity_utilization, source, download_date`         |
| `data/processed/tes_banrep_colombia.csv`       | Tasas TES Cero Cupón (pesos y UVR, 1Y) | `date, year, month, TES_UVR_1Y, TES_PESOS_1Y, source, download_date`    |

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
- [X] Validaciones de calidad automatizadas (243 tests)
- [X] Arquitectura modular (`sources/dane/` + `sources/banrep/` + `sources/fred/` + `sources/andi/` + `pipelines/`)
- [X] `series_map` configurable para extraer múltiples series (TGP, TO, TES, etc.)
- [ ] Calcular serie de inflación interanual a partir del IPC
- [ ] Construir base mensual unificada (desempleo + IPC + inflación)
- [ ] Análisis exploratorio conjunto (notebook)
- [ ] Implementar estimación de la NAIRU (filtro de Kalman / curva de Phillips)
- [ ] Incorporar estimaciones institucionales (Banco de la República, CARF)
- [ ] Agregar fuentes complementarias (expectativas de inflación, brecha del producto)

---

## Buenas prácticas del proyecto

- **Reproducibilidad:** los datos crudos se descargan desde la fuente original
  en cada ejecución; los archivos procesados son determinísticos dado el mismo
  insumo.
- **Tests offline:** los 243 tests usan fixtures sintéticas que simulan la
  estructura real de los datos del DANE, BANREP, FRED y ANDI, sin requerir conexión a internet.
- **Validaciones de calidad:** cada pipeline verifica columnas, nulos,
  duplicados, rangos y continuidad temporal antes de guardar.
- **Datos ignorados por Git:** los archivos `.xlsx`, `.csv` y `.html`
  descargados se regeneran con el pipeline y no se versionan.
- **Logging estructurado:** cada ejecución genera un log detallado en
  `logs/pipeline.log` con timestamps y trazabilidad completa.

---

## Licencia

[MIT](LICENSE)
