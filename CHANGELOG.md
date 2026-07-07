# Changelog

## [0.4.1] — 2026-06-11

### Corregido
- **`meta.json` del tablero**: `latest_brecha_cd` y `latest_brecha_viog` usan ahora el
  **último valor válido** (`export_web_data._last_valid`) en vez de la última fila — el
  trimestre corriente tiene PIB observado pero aún no potencial CD (FBKF rezagado), lo
  que producía `NaN`. El `meta.json` versionado (generado antes del guard anti-NaN)
  contenía el literal `NaN` (JSON inválido) y un `latest_brecha_viog` del VIOG malo ya
  restaurado; regenerado coherente con los CSV publicados. +10 tests (503 total).

## [0.4.0] — 2026-06-09

### Agregado
- **Vista VIOG en el tablero** — `docs/data/viog_trimestral.csv` (compuesto + 5 filtros) y
  gráfica "Brecha del Producto — VIOG (5 filtros)" en `docs/index.html`, con botón de
  descarga. El tablero muestra ahora las **dos lecturas** del PIB potencial (función de
  producción + VIOG).
- **Capital por inventario permanente con FBKF DANE** (`run_pib_potencial._build_capital_quarterly`):
  el PIB potencial y las brechas llegan al trimestre corriente (antes cortaban en 2023, el
  último año de PWT). De PWT solo se usa δ (promedio) y el capital humano H. K continuo, sin
  empalmes.
- **Capital humano H** en la función de producción: `Y = A·K^α·(H·L)^(1−α)` (confirmado
  contra `FUNCION DE PRODUCCION.xlsx`; 3 tests nuevos).
- **Scraping mensual real** en `.github/workflows/update.yml`: descarga cada fuente
  automatizable de forma resiliente (si una falla → warning + dato cacheado). NO scrapea PWT
  (anual + bloquea bots) ni PIB_USA (muestra).

### Cambiado
- **α fijado en 0.4** (`factors.ALPHA_FIXED`, antes 0.33), alineado con el Boceto y la
  Función de Producción de los profesores. Unifica las dos rutas de producción.
- `run_derived_checks` (`quality_checks.py`): la coherencia del IPC se valida con la suma
  móvil de 12 meses (`Σ ipc_mom ≈ ipc_yoy`) en vez de `ipc_mom × 12` (incorrecto: amplificaba
  la volatilidad estacional). Desbloquea `--merge` y `--all`.
- Badge de tests del README: 490 → 493.

### Removido
- `andi_agent/agent_llm.py` (experimento con Ollama; el ANDI canónico vive en
  `src/sources/andi/eoic.py`).
- `data/inputs/pyproject.toml.xlsx` (duplicado byte-idéntico del Boceto).
- Gráfica "PTF Observada y Potencial · Share del Capital (α)" del tablero.

## [0.3.0] — 2026-05-18

### Agregado
- **Pipeline PIB Potencial Cobb-Douglas** completo y automático:
  - `src/production/factors.py` — factores L, K, α dinámico con fallback
  - `src/production/tfp.py` — PTF observada y tendencial (HP filter, λ=1600)
  - `src/production/pib_potencial.py` — Y\*, brechas CD y HP
  - `src/production/excel_writer.py` — Excel 4 hojas con formato profesional
  - `src/pipelines/run_pib_potencial.py` — orquestador completo
- Flag `--pib-potencial` en `src/main.py`; incluido en `--all`.
- `run_pib_potencial_checks()` en `src/quality_checks.py`.
- 52 tests nuevos: `test_production_factors.py` (21), `test_production_tfp.py` (18),
  `test_production_pib_potencial.py` (13).
- Output: `outputs/pib_potencial/PIB_Potencial_Colombia.xlsx` (hojas Trimestral,
  Mensual, Supuestos, Metadatos).

### Cambiado
- README actualizado: badge 396 → 448, nueva sección §4.4 PIB Potencial, árbol de
  directorios con `src/production/`, tabla de tests con 3 suites nuevas.

## [0.2.0] — 2026-05-17

### Agregado
- Variables derivadas `ipc_yoy`, `ipc_mom`, `inflation_gap` en
  `nairu_dataset.csv`.
- `src/sources/dane/common.py::dane_request_kwargs()` para centralizar
  el manejo de TLS contra DANE (variable de entorno `DANE_VERIFY_TLS`).
- `legacy/README.md` documentando el código histórico.
- 16 tests nuevos en `tests/test_merge_derived.py` (variables derivadas).

### Cambiado
- `verify=False` reemplazado por `dane_request_kwargs(...)` en los 4
  scrapers de PIB DANE (`gdp.py`, `gdp_expenditure.py`, `gdp_income.py`,
  `gdp_historical.py`).
- `andi_agent/` movido a `legacy/andi_agent/` (sustituido por
  `src/sources/andi/eoic.py`).
- Referencias residuales a "PWT 10.01" actualizadas a "PWT 11.0"
  (`src/config.py`, `src/main.py`).
- `pyproject.toml`: `legacy/` excluido de setuptools, pytest y ruff.
- Contador de tests en README actualizado: 340 → 396.

### Removido
- Archivos lock de Excel del index (`~$*` ahora en `.gitignore`).

## [0.1.0] — 2026-04-28
- Versión inicial revisada (`REVIEW.md`).
