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

| Componente                                                 | Estado              |
| ---------------------------------------------------------- | ------------------- |
| Pipeline de desempleo (DANE – GEIH desestacionalizado)    | Funcional        |
| Pipeline de IPC (DANE – Índice de Precios al Consumidor) | Funcional        |
| Pipeline de inflación (BANREP – SUAMECA)                 | Funcional        |
| Pipeline de Brent (FRED / EIA)                            | Funcional        |
| Validaciones de calidad automatizadas                      | 140 tests pasando |
| Base mensual unificada (desempleo + IPC + inflación)      | Próximo paso    |
| Estimación econométrica de la NAIRU                      | Por implementar  |

**Datos generados actualmente:**

- **Tasa de desocupación (desestacionalizada):** 299 observaciones mensuales (enero 2001 – enero 2026)
- **Índice de Precios al Consumidor:** 278 observaciones mensuales (enero 2003 – febrero 2026)
- **Inflación BANREP:** 858 observaciones mensuales (julio 1955 – diciembre 2026) con meta de inflación, inflación total e inflación sin alimentos ni regulados
- **Precio del Brent:** 302 observaciones mensuales (enero 2001 – febrero 2026) en USD/barril

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

| Campo          | Detalle                                                                                       |
| -------------- | --------------------------------------------------------------------------------------------- |
| Fuente         | Plataforma SUAMECA del Banco de la República                                                  |
| Indicadores    | Meta de inflación (Inf\_Goal), inflación total (Inf\_Rate), inflación sin alimentos ni regulados (Core\_Inf) |
| Periodicidad   | Mensual                                                                                       |
| Cobertura      | Inf\_Rate desde julio 1955, Core\_Inf desde marzo 1983, Inf\_Goal desde enero 1991           |
| Formato origen | API REST JSON (epoch\_ms + valor)                                                            |
| Página        | [suameca.banrep.gov.co](https://suameca.banrep.gov.co/estadisticas-economicas/informacionSerie/100001/inflacion_y_meta) |

### 4. Precio del petróleo Brent — FRED / EIA

| Campo          | Detalle                                                                                       |
| -------------- | --------------------------------------------------------------------------------------------- |
| Fuente         | FRED (Federal Reserve Economic Data) / U.S. Energy Information Administration                 |
| Indicador      | Precio promedio mensual del crudo Brent (POILBREUSDM), USD/barril                            |
| Periodicidad   | Mensual                                                                                       |
| Cobertura      | Enero 2001 – presente                                                                        |
| Formato origen | CSV vía URL directa (fredgraph.csv)                                                          |
| Página        | [fred.stlouisfed.org](https://fred.stlouisfed.org/series/POILBREUSDM)                        |

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
│   │   ├── run_brent.py          # Orquestación: Brent (FRED/EIA)
│   │   └── run_all.py            # Ejecuta todos los pipelines
│   └── sources/
│       ├── dane/
│       │   ├── unemployment.py   # Scraping + parsing GEIH desestacionalizado
│       │   └── ipc.py            # Scraping + parsing IPC
│       ├── banrep/
│       │   └── inflation.py      # API REST SUAMECA: inflación + meta
│       └── fred/
│           └── brent.py          # CSV FRED: precio Brent
├── tests/
│   ├── test_geih.py              # 36 tests — scraping, parsing y calidad GEIH
│   ├── test_ipc.py               # 23 tests — scraping, parsing y calidad IPC
│   ├── test_banrep_inflation.py  # 29 tests — API BANREP, parsing y calidad
│   ├── test_brent.py             # 38 tests — CSV FRED, agregación y calidad
│   └── test_pipeline.py          # 14 tests — estructura, calidad e I/O
├── data/
│   ├── raw/
│   │   ├── dane/                 # Archivos crudos descargados (Excel, HTML)
│   │   ├── banrep/               # Respuestas crudas del API SUAMECA (JSON)
│   │   └── fred/                 # CSV crudo de FRED (Brent)
│   └── processed/                # Datasets limpios listos para análisis
├── docs/bib/                     # Bibliografía de referencia
├── logs/                         # Logs del pipeline
└── requirements.txt
```

**Principios de diseño:**

- **Separación de capas:** scraping → parsing → validación → guardado.
- **Configuración declarativa:** cada fuente se describe con un dataclass
  (`GEIHConfig`, `IPCConfig`, `BanrepInflationConfig`, `BrentConfig`) que
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
- Conexión a internet (para descargar datos del DANE)

### Instalación

```bash
git clone https://github.com/Tupxz/scraping-NAIRU.git
cd scraping-NAIRU
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Ejecución

```bash
# Ejecutar todos los pipelines (desempleo + IPC + inflación BANREP + Brent)
python -m src.main --all

# Solo desempleo
python -m src.main --unemployment

# Solo IPC
python -m src.main --ipc

# Solo inflación BANREP
python -m src.main --banrep

# Solo Brent (FRED/EIA)
python -m src.main --brent
```

### Tests

```bash
python -m pytest tests/ -v
```

---

## Salidas esperadas

Después de ejecutar `python -m src.main --all`, el pipeline genera:

| Archivo                                              | Contenido                                | Columnas                                                                          |
| ---------------------------------------------------- | ---------------------------------------- | --------------------------------------------------------------------------------- |
| `data/processed/unemployment_colombia.csv`         | Tasa de desocupación mensual (desest.) | `date, year, month, unemployment_rate, source, download_date`                   |
| `data/processed/ipc_colombia.csv`                  | Índice de precios al consumidor mensual | `date, year, month, ipc_index, source, download_date`                           |
| `data/processed/inflation_banrep_colombia.csv`     | Inflación y meta del Banco de la Rep.  | `date, year, month, Inf_Goal, Inf_Rate, Core_Inf, source, download_date`       |
| `data/processed/brent_colombia.csv`                | Precio mensual del Brent (USD/barril)  | `date, year, month, brent_usd_per_barrel, source, download_date`               |

Archivos intermedios (en `data/raw/`, ignorados por Git):

- `dane/geih_raw.xlsx` — Anexo GEIH desestacionalizado original
- `dane/geih_page.html` — Snapshot del HTML para auditoría
- `dane/ipc_indices_raw.xlsx` — Anexo IPC original
- `dane/ipc_page.html` — Snapshot del HTML
- `banrep/banrep_inflation_raw.json` — Respuestas crudas del API SUAMECA
- `fred/brent_raw.csv` — CSV crudo descargado de FRED

---

## Dependencias principales

| Paquete            | Uso                                                    |
| ------------------ | ------------------------------------------------------ |
| `pandas`         | Transformación de datos y manejo de DataFrames        |
| `requests`       | Descarga HTTP (DANE), API REST (BANREP), CSV (FRED)   |
| `beautifulsoup4` | Scraping de enlaces desde el HTML del DANE             |
| `openpyxl`       | Lectura de archivos Excel (.xlsx)                      |
| `pytest`         | Suite de tests automatizados                           |

Versiones exactas en [`requirements.txt`](requirements.txt).

---

## Roadmap

- [X] Pipeline de desempleo conectado a fuente real del DANE (GEIH desestacionalizado)
- [X] Pipeline de IPC conectado a fuente real del DANE
- [X] Pipeline de inflación conectado al API SUAMECA de BANREP (Inf_Goal, Inf_Rate, Core_Inf)
- [X] Pipeline de Brent conectado a FRED/EIA (POILBREUSDM)
- [X] Validaciones de calidad automatizadas (140 tests)
- [X] Arquitectura modular (`sources/dane/` + `sources/banrep/` + `sources/fred/` + `pipelines/`)
- [X] `series_map` configurable para extraer múltiples series (TGP, TO, etc.)
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
- **Tests offline:** los 140 tests usan fixtures sintéticas que simulan la
  estructura real de los datos del DANE, BANREP y FRED, sin requerir conexión a internet.
- **Validaciones de calidad:** cada pipeline verifica columnas, nulos,
  duplicados, rangos y continuidad temporal antes de guardar.
- **Datos ignorados por Git:** los archivos `.xlsx`, `.csv` y `.html`
  descargados se regeneran con el pipeline y no se versionan.
- **Logging estructurado:** cada ejecución genera un log detallado en
  `logs/pipeline.log` con timestamps y trazabilidad completa.

---

## Licencia

[MIT](LICENSE)
