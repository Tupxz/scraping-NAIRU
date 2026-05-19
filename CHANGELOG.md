# Changelog

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
