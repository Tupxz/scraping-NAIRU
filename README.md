# NAIRU Colombia — Pipeline de Datos Macroeconómicos

[![tests](https://img.shields.io/badge/tests-490%20passing-success.svg)](#6-tests)
[![python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](#3-instalación)
[![license](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Pipeline reproducible que descarga, limpia, valida y consolida series macroeconómicas oficiales de Colombia para alimentar dos modelos:

1. **NAIRU/NAICU** — Tasa de desempleo (y de capacidad instalada) consistente con inflación estable. Filtro de Kalman biestado en `src/nairu/`.
2. **PIB potencial vía Cobb-Douglas** — Y\* = A\*·Kᵅ·(H·L\*)^(1−α) con α = 0.4. PTF tendencial y brecha estadística via filtro Boosted Hodrick-Prescott (BHP, Phillips & Shi 2021).

Todas las fuentes se actualizan ejecutando un solo comando; el resultado es un CSV mensual de **21 columnas × ~270 meses (2004-01-01 → presente)**.

---

## 1. Vista general

```
                                            ┌─────────────────────────────┐
   data/raw/   ←── scrapers ── fuentes ──→  │ DANE, BANREP, FRED,         │
   (cacheado)                               │ ANDI, PWT (Groningen)       │
       │                                    └─────────────────────────────┘
       ▼
   src/sources/<fuente>/   ── parsing + validación de calidad
       │
       ▼
   data/processed/<fuente>.csv   ── un CSV por fuente, esquema estándar
       │
       ▼
   src/merge.py   ── outer-merge por `date`, recorte ≥ 2004-01-01
       │
       ▼
   data/final/nairu_dataset.csv   ── dataset consolidado (21 cols)
       │
       ▼
   src/nairu/  ──  estimación NAIRU/NAICU (Kalman)
   src/sources/viog/  ──  brecha del producto (5 filtros + ponderado VIOG)
```

### Patrón de tres capas (uniforme en cada fuente)

| Capa | Responsabilidad | Ejemplos |
|---|---|---|
| 1. **Scraping/Descarga** | Localizar el archivo más reciente y descargarlo a `data/raw/<fuente>/`. Reusa `requests.Session` con retries y keep-alive. | `fetch_geih_page()`, `download_pwt_csv()` |
| 2. **Parsing** | Convertir el formato fuente (Excel pivoteado, JSON SUAMECA, CSV PWT) al esquema largo estándar `date,year,month,<series>,source,download_date`. | `clean_geih_data()`, `parse_pwt_csv()` |
| 3. **Validación + guardado** | `quality_checks.py` aplica chequeos por fuente (rangos, nulos, duplicados); luego `save_csv()` escribe en `data/processed/`. | `run_pwt_checks()`, `run_banrep_checks()` |

---

## 2. Fuentes y columnas del dataset final

`data/final/nairu_dataset.csv` (21 columnas):

| Bloque | Columnas | Fuente | Frecuencia | Cobertura |
|---|---|---|---|---|
| **Identificadores** | `date, year, month` | derivado | mensual | 2004-01-01 → presente |
| **Mercado laboral GEIH** | `unemployment_rate, tgp_rate, pet_thousands` | DANE GEIH desestacionalizado | mensual | 2001-01 → presente |
| **Precios** | `ipc_index` | DANE IPC (índice base 2018) | mensual | 2003-01 → presente |
| **Inflación BANREP** | `Inf_Goal, Inf_Rate, Core_Inf` | SUAMECA (API) | mensual | 1999-12 → presente |
| **Commodities** | `brent_usd_per_barrel` | FRED `POILBREUSDM` | mensual | 1990-01 → presente |
| **Capacidad instalada** | `capacity_utilization` | ANDI EOIC (PDFs) | mensual | 2004-01 → presente |
| **Tasas de interés** | `TES_UVR_1Y, TES_PESOS_1Y` | SUAMECA Cero Cupón | mensual | 2002-12 → presente |
| **Stock de capital y H** | `capital_stock_real, depreciation_rate, human_capital` | PWT 11.0 (Groningen) | anual (NaN en feb-dic) | 1950 → 2023 |
| **Brecha producto USA** | `gap_viog_us, gap_inv_viog_us` | VIOG sobre PIB USA (FRED) | trimestral | 1948-Q1 → presente |
| **Brecha producto CO** | `gap_viog_co, gap_inv_viog_co` | VIOG sobre PIB CO (DANE) | trimestral | 1994-Q1 → presente |

> **Refactor PWT 11.0 (2026-05-11):** se reemplazaron `ck` y `cn` (capital a PPP y a precios nacionales corrientes — no aptos para series de tiempo) por `rnna` (precios nacionales constantes 2017) y `delta` (depreciación). Esto habilita el método del inventario permanente del modelo del profesor Álvaro.

---

## 3. Instalación

Requiere **Python ≥ 3.11**. Recomendado: venv aislado por proyecto.

```bash
# Clonar
git clone https://github.com/Tupxz/scraping-NAIRU.git
cd scraping-NAIRU

# Crear entorno
python3 -m venv .venv
source .venv/bin/activate            # macOS/Linux
# .venv\Scripts\activate.bat         # Windows

# Instalar en modo editable
pip install -e ".[dev]"              # runtime + tests
# o:  pip install -e ".[all]"        # runtime + dev + notebooks
```

El paquete se instala como `scraping-nairu` y queda importable como `src.*` desde cualquier directorio. La fuente de verdad de las dependencias es `pyproject.toml`; `requirements.txt` se mantiene como espejo para usuarios sin `pip install -e`.

### Variables de entorno

- `DANE_VERIFY_TLS=1` activa verificación TLS contra `www.dane.gov.co`
  (default `0` por problemas históricos con la cadena de certificados).

---

## 4. Uso

### 4.1 Pipelines individuales

```bash
python -m src.main --unemployment    # GEIH (TD + TGP + PET)
python -m src.main --ipc             # IPC DANE
python -m src.main --banrep          # Inflación (meta, observada, núcleo)
python -m src.main --tes             # TES Cero Cupón
python -m src.main --brent           # Brent FRED
python -m src.main --andi            # ANDI EOIC (incremental)
python -m src.main --andi-backfill   # ANDI EOIC (reprocesar todos los PDFs)
python -m src.main --pwt             # PWT 11.0 (capital + depreciación + capital humano)
python -m src.main --dane-gdp        # PIB trimestral DANE
python -m src.main --informality     # Informalidad GEIH-EISS
python -m src.main --viog            # VIOG-USA
python -m src.main --viog-co         # VIOG-Colombia (requiere --dane-gdp antes)
```

### 4.2 Pipeline completo y merge

```bash
python -m src.main --all             # Todas las fuentes + merge
python -m src.main --merge           # Solo el merge (si las fuentes ya están en data/processed/)
```

Equivalente programático:

```python
from src.pipelines import run_all
run_all.run()
```

### 4.3 Estimación NAIRU

```bash
python -m src.main --nairu-dataset   # Construye Data_NAIRU.xlsx
python -m src.main --nairu-estim     # Estima NAIRU/NAICU v6 (Kalman biestado)
```

Los outputs se escriben en `outputs/nairu/`.

### 4.4 PIB Potencial Cobb-Douglas

```bash
python -m src.main --pib-potencial   # PIB Potencial + Excel (requiere --nairu-estim previo)
```

Genera `outputs/pib_potencial/PIB_Potencial_Colombia.xlsx` con 4 hojas:
- **Trimestral** — Y\*, brechas CD y BHP, factores L, K, α, PTF
- **Mensual** — TD, UCI, NAIRU\*, NAICU\*, inflación
- **Supuestos** — parámetros del modelo
- **Metadatos** — fechas de descarga por fuente

---

## 5. Estructura del repositorio

```
scraping-NAIRU/
├── src/
│   ├── config.py              # Dataclasses frozen con URLs, paths, rangos de validación
│   ├── main.py                # CLI (argparse) — punto de entrada principal
│   ├── io_utils.py            # save_csv, load_csv (con parse_dates), setup_logging
│   ├── merge.py               # Outer-merge por date → nairu_dataset.csv (21 cols)
│   ├── quality_checks.py      # Validaciones por fuente; QualityCheckError
│   ├── sources/               # Una carpeta por fuente
│   │   ├── andi/eoic.py       # PDF parsing (pdfplumber + fuzzy matching)
│   │   ├── banrep/            # inflation.py, tes.py (API SUAMECA)
│   │   ├── dane/              # unemployment, ipc, informality, gdp, gdp_historical
│   │   │   └── common.py      # MONTH_ABBR_ES, MONTH_FULL_ES, make_dane_session()
│   │   ├── fred/brent.py      # Brent (3 niveles de fallback: curl/requests/urllib)
│   │   ├── pwt/pwt.py         # PWT 11.0 (formato wide/largo auto-detectado)
│   │   └── viog/viog.py       # 5 filtros (BHP, BK, CF, Butterworth, Kalman LLT) + VIOG ponderado
│   ├── nairu/                 # Modelo Kalman biestado para NAIRU/NAICU
│   ├── production/            # Cobb-Douglas: factors.py, tfp.py, pib_potencial.py, excel_writer.py
│   └── pipelines/             # run_*.py — un orquestador por fuente + run_all + run_merge
├── tests/                     # 448 tests offline (CSVs sintéticos, sin red)
├── data/
│   ├── raw/                   # Cacheado de descargas (gitignored)
│   ├── inputs/                # Inputs manuales (PIB_USA.xlsx, PIB_CO.xlsx)
│   ├── processed/             # Un CSV por fuente
│   └── final/                 # nairu_dataset.csv consolidado
├── docs/
│   ├── prompts/               # Prompts para refactors guiados (PWT, code review)
│   ├── bib/                   # Referencias econométricas
│   └── *.pdf                  # NAIRU_estimation_process.pdf, etc.
├── notebooks/                 # Exploración (VIOG.ipynb, validation_v6.ipynb)
├── outputs/                   # Plots, reportes de pipelines, estimaciones NAIRU
├── logs/                      # pipeline.log
├── pyproject.toml             # Fuente de verdad de dependencias
└── requirements.txt           # Espejo compatible con pip install -r
```

---

## 6. Tests

```bash
python -m pytest -q                  # Suite completa
python -m pytest tests/test_merge.py # Solo merge
python -m pytest --cov=src           # Cobertura (requiere pytest-cov)
```

Todos los tests son **offline**: usan CSVs sintéticos en `tmp_path`, sin llamadas de red. Esto permite CI rápido y reproducible.

| Suite | Tests | Cubre |
|---|---:|---|
| `test_merge.py` | 35 | Outer-merge, MERGED_COLUMNS, renombrado VIOG-US/CO |
| `test_pipeline.py` | 14 | Smoke test del CLI |
| `test_geih.py` | 43 | Scraping HTML, parsing Excel pivoteado, series TD+TGP+PET |
| `test_pwt.py` | 37 | Parseo wide/largo, filtro country, validaciones PWT 11.0 |
| `test_ipc.py` | 23 | Selección de archivo, melt, rangos del índice |
| `test_banrep_inflation.py` | 29 | API SUAMECA, inflación meta/observada/núcleo |
| `test_banrep_tes.py` | 40 | SUAMECA TES Cero Cupón, agregación diaria→mensual |
| `test_andi.py` | 63 | Extracción de PDFs con fuzzy matching |
| `test_brent.py` | 38 | 3 niveles de fallback FRED |
| `test_informality.py` | 30 | Reconstrucción de trimestres móviles |
| `test_dane_gdp.py` | 28 | Parseo Cuadro 4, asignación Q→mes |
| `test_merge_derived.py` | 16 | Variables derivadas ipc_yoy, ipc_mom, inflation_gap |
| `test_viog.py` | 33 | 5 filtros (BHP/BK/CF/BW/Kalman LLT), pesos VIOG, empalme 1994/2005/2015 |
| `test_production_factors.py` | 21 | L, K, α dinámico, fallbacks NAIRU/NAICU |
| `test_production_tfp.py` | 27 | `hp_filter`, `boosted_hp_filter` (9 casos), PTF observada y tendencial BHP |
| `test_production_pib_potencial.py` | 13 | PIB*, brechas CD y BHP, pipeline integrado |
| **Total** | **490** | **~ 8 s** |

---

## 7. Convenciones

- **Python 3.11+**, `from __future__ import annotations`, type hints `str | None` (estilo 3.10+).
- **Dataclasses `frozen=True`** para configs (`src/config.py`). Defaults mutables vía `field(default_factory=...)`.
- **Logging jerárquico**: `nairu_pipeline.<submódulo>` configurado en `setup_logging()` (escribe en consola y `logs/pipeline.log`).
- **`requests.Session`** reutilizada por fuente para reusar TLS handshake; helper compartido en `src/sources/dane/common.py:make_dane_session()` (retries + backoff exponencial).
- **Validación obligatoria**: cada pipeline llama a `run_<fuente>_checks(df)` antes de guardar. Si falla → `QualityCheckError` y `sys.exit(1)`.
- **Esquema estándar**: `date, year, month, <series>, source, download_date` en cada CSV de `data/processed/`.

---

## 8. Historial de mejoras y siguientes pasos

### Ciclo 2026-05-26

| # | Mejora | Archivos afectados |
|---|---|---|
| A | **Boosted HP Filter** — reemplaza el HP simple en todo el pipeline. `boosted_hp_filter(series, lamb=1600, iterations=3)` en `src/production/tfp.py`. Columnas renombradas a `PIB_tend_BHP` / `Brecha_BHP` / `brecha_bhp`. Página web y Excel actualizados. | `tfp.py`, `pib_potencial.py`, `export_web_data.py`, `quality_checks.py`, `excel_writer.py`, `viog.py`, `docs/index.html`, `VIOG.ipynb` |
| B | **Kalman UCM → Local Linear Trend** — reemplaza `random walk with drift + damped/stochastic cycle` (en log) por `local linear trend + cycle` (en niveles). Pendiente estocástica, sin `cycle_period_bounds`. | `viog.py`, `VIOG.ipynb` |
| C | **Botones de descarga en la web** — sección dedicada con tarjetas estilizadas, íconos SVG, descripción de cada archivo, atributo `download` y hover effects. | `docs/index.html` |

### Ciclo 2026-05-12

| # | Cuello de botella | Estado | Acción aplicada |
|---|---|---|---|
| 1 | `verify=False` en `informality.py` (riesgo MITM) | ✅ Resuelto | Removido; ahora usa `make_dane_session()` con TLS habilitado |
| 2 | `month_map` duplicado en 6+ archivos | ✅ Resuelto | Constantes canónicas `MONTH_ABBR_ES` y `MONTH_FULL_ES` en `src/sources/dane/common.py` |
| 3 | `requests.get()` por cada call en DANE | ✅ Resuelto | `Session` compartida con keep-alive + retries (~30% menos latencia en pipeline GEIH) |
| 4 | `pd.read_csv` sin `parse_dates` en `load_csv` | ✅ Resuelto | Parseo automático de columna `date` |
| 5 | `run_all.py` omitía PWT + informalidad | ✅ Resuelto | Parity con `--all`, orden documentado |
| 6 | `dane/__init__.py` no re-exportaba informalidad | ✅ Resuelto | Añadido `run_informality_pipeline`, `run_dane_gdp_pipeline` al `__all__` |
| 7 | `src/sources/pwt/__init__.py` vacío | ✅ Resuelto | Re-exporta `run_pwt_pipeline`, `parse_pwt_csv`, `download_pwt_csv` |
| 8 | Referencias `PWT 10.01` desactualizadas | ✅ Resuelto | Reemplazadas por `PWT 11.0` en docs y código |
| 9 | `andi_agent/` legacy (95 MB) | ⚠️ Pendiente | Mover a rama `legacy/andi-agent` o eliminar (la lógica vive en `src/sources/andi/`) |
| 10 | `notebooks/exploration.ipynb` vacío | ⚠️ Pendiente | Borrar manualmente: `rm notebooks/exploration.ipynb` |
| 11 | Pipelines secuenciales en `run_all` | 🔮 Futuro | Paralelizar fuentes independientes con `concurrent.futures` (cuidado con APIs del DANE) |
| 12 | ANDI EOIC procesa PDFs secuencialmente | 🔮 Futuro | `ProcessPoolExecutor` para parseo de N PDFs (CPU-bound) |
| 13 | Falta CI/CD (GitHub Actions) | 🔮 Futuro | Workflow básico `pip install -e ".[dev]" && pytest` |

---

## 9. Troubleshooting

**`No se encontró ningún archivo PWT en data/raw/pwt/`**
El Dataverse de Groningen requiere autenticación. Exporta el CSV desde [pwt-data-tool.streamlit.app](https://pwt-data-tool.streamlit.app/) (Country=Colombia, Variables=`rnna,delta,hc`) y guárdalo en `data/raw/pwt/`.

**`PIB_CO.xlsx no encontrado`**
Ejecuta `python -m src.main --dane-gdp` primero; `--viog-co` lo construye automáticamente empalmando series Base 2015 + 2005 + 1994.

**SSL / certificados expirados del DANE**
El proyecto **no usa `verify=False`** (riesgo MITM). Si la cadena del DANE caduca, exporta `REQUESTS_CA_BUNDLE` apuntando a tu CA interna o actualiza `certifi`. Reactivar `verify=False` solo como último recurso y dejando un *issue* abierto.

**Test fallido `test_pwt.py` con `low_memory not supported with python engine`**
Indica pandas viejo (≤ 2.0) combinado con `engine='python'`. Actualiza pandas a ≥ 2.1: `pip install -U pandas`.

**Los pipelines escriben en `outputs/` pero no veo el dataset final**
El dataset consolidado va a `data/final/nairu_dataset.csv`, no a `outputs/`. `outputs/` está reservado para plots y reportes auxiliares.

---

## 10. Referencias

- Banco de la República — *Reportes de Política Monetaria*. NAIRU oficial.
- Gómez & Julio (2000). *Transmission Mechanisms and Inflation Targeting: the Case of Colombia*.
- *2022-04-28 PIB tendencial.pdf* (ver `docs/bib/`). Método del profesor Álvaro.
- Penn World Tables 11.0 — [www.rug.nl/ggdc/productivity/pwt](https://www.rug.nl/ggdc/productivity/pwt/).
- DANE — Gran Encuesta Integrada de Hogares (GEIH), Cuentas Nacionales, IPC.

---

## 11. Licencia

MIT. Ver `LICENSE`.
