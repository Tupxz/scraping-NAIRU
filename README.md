# NAIRU Colombia — Pipeline de Datos

Pipeline de datos reproducible para construir la base empírica necesaria para
estimar la **NAIRU** (*Non-Accelerating Inflation Rate of Unemployment*) en
Colombia.  El proyecto descarga, limpia y valida series macroeconómicas
oficiales del **DANE** mediante scraping automatizado, y las transforma en
datasets analíticos listos para modelamiento econométrico.

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
| Pipeline de desempleo (DANE – GEIH)                       | Funcional        |
| Pipeline de IPC (DANE – Índice de Precios al Consumidor) | Funcional        |
| Validaciones de calidad automatizadas                      | 77 tests pasando |
| Base mensual unificada (desempleo + IPC + inflación)      | Próximo paso    |
| Estimación econométrica de la NAIRU                      | Por implementar  |

**Datos generados actualmente:**

- **Tasa de desocupación:** 296 observaciones mensuales (enero 2001 – enero 2026)
- **Índice de Precios al Consumidor:** 278 observaciones mensuales (enero 2003 – febrero 2026)

---

## Fuentes de datos

### 1. Desempleo — DANE GEIH

| Campo          | Detalle                                                                                                                      |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Fuente         | Gran Encuesta Integrada de Hogares (GEIH)                                                                                    |
| Indicador      | Tasa de Desocupación (TD) — Total Nacional                                                                                 |
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

Ambos pipelines realizan **scraping automatizado** de la página del DANE,
detectan el anexo Excel más reciente, lo descargan, parsean la estructura
pivoteada y producen un CSV en formato largo estandarizado.

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
│   │   └── run_all.py            # Ejecuta ambos pipelines
│   └── sources/
│       └── dane/
│           ├── common.py         # Utilidades compartidas (descarga, detección)
│           ├── unemployment.py   # Scraping + parsing GEIH
│           └── ipc.py            # Scraping + parsing IPC
├── tests/
│   ├── test_geih.py              # 26 tests — scraping, parsing y calidad GEIH
│   ├── test_ipc.py               # 23 tests — scraping, parsing y calidad IPC
│   └── test_pipeline.py          # 28 tests — placeholder, Excel genérico, I/O
├── data/
│   ├── raw/dane/                 # Archivos crudos descargados (Excel, HTML)
│   └── processed/                # Datasets limpios listos para análisis
├── notebooks/
│   └── exploration.ipynb         # Análisis exploratorio
├── docs/bib/                     # Bibliografía de referencia
├── logs/                         # Logs del pipeline
└── requirements.txt
```

**Principios de diseño:**

- **Separación de capas:** scraping → parsing → validación → guardado.
- **Configuración declarativa:** cada fuente se describe con un dataclass
  (`GEIHConfig`, `IPCConfig`) que centraliza URLs, patrones y parámetros
  de parsing.
- **Detección robusta:** el parser no asume posiciones fijas de filas/columnas;
  usa heurísticas (regex, conteo de años, búsqueda de etiquetas) para
  adaptarse a cambios en el formato del DANE.

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
# Ejecutar ambos pipelines (desempleo + IPC)
python -m src.main --all

# Solo desempleo
python -m src.main --unemployment

# Solo IPC
python -m src.main --ipc
```

### Tests

```bash
python -m pytest tests/ -v
```

---

## Salidas esperadas

Después de ejecutar `python -m src.main --all`, el pipeline genera:

| Archivo                                      | Contenido                                | Columnas                                                        |
| -------------------------------------------- | ---------------------------------------- | --------------------------------------------------------------- |
| `data/processed/unemployment_colombia.csv` | Tasa de desocupación mensual            | `date, year, month, unemployment_rate, source, download_date` |
| `data/processed/ipc_colombia.csv`          | Índice de precios al consumidor mensual | `date, year, month, ipc_index, source, download_date`         |

Archivos intermedios (en `data/raw/dane/`, ignorados por Git):

- `geih_raw.xlsx` — Anexo GEIH original
- `geih_page.html` — Snapshot del HTML para auditoría
- `ipc_indices_raw.xlsx` — Anexo IPC original
- `ipc_page.html` — Snapshot del HTML

---

## Dependencias principales

| Paquete            | Uso                                             |
| ------------------ | ----------------------------------------------- |
| `pandas`         | Transformación de datos y manejo de DataFrames |
| `requests`       | Descarga HTTP de páginas y archivos del DANE   |
| `beautifulsoup4` | Scraping de enlaces desde el HTML del DANE      |
| `openpyxl`       | Lectura de archivos Excel (.xlsx)               |
| `pytest`         | Suite de tests automatizados                    |

Versiones exactas en [`requirements.txt`](requirements.txt).

---

## Roadmap

- [X] Pipeline de desempleo conectado a fuente real del DANE (GEIH)
- [X] Pipeline de IPC conectado a fuente real del DANE
- [X] Validaciones de calidad automatizadas (77 tests)
- [X] Arquitectura modular (`sources/dane/` + `pipelines/`)
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
- **Tests offline:** los 77 tests usan fixtures sintéticas que simulan la
  estructura real del Excel del DANE, sin requerir conexión a internet.
- **Validaciones de calidad:** cada pipeline verifica columnas, nulos,
  duplicados, rangos y continuidad temporal antes de guardar.
- **Datos ignorados por Git:** los archivos `.xlsx`, `.csv` y `.html`
  descargados se regeneran con el pipeline y no se versionan.
- **Logging estructurado:** cada ejecución genera un log detallado en
  `logs/pipeline.log` con timestamps y trazabilidad completa.

---

## Licencia

[MIT](LICENSE)
