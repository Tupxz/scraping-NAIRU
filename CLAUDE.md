# CLAUDE.md — scraping-NAIRU

Guía para Claude Code (y cualquier agente) que trabaje en este repo.

## Qué es
Pipeline reproducible (Python ≥ 3.11) que descarga series de Colombia (DANE,
Banrep, FRED, ANDI, PWT, SFC), estima NAIRU/NAICU y el PIB potencial por
función de producción estilo CBO, y publica un tablero en GitHub Pages
(`docs/index.html`). Se actualiza solo el día 1 de cada mes
(`.github/workflows/update.yml`).

## Estado (nov-2026)
- `src/` implementa la especificación **v3**. Se está migrando a **v7**
  (`docs/paper/working_paper_v7.pdf`).
- Spec ejecutable v7: `legacy/v7/pib_potencial_integrado_v7.py` — **no se edita**.
- Salidas de referencia v7: `tests/fixtures/baseline_v7/`.
- Plan, inventario de insumos y mapa v7 → `src/`: `docs/integracion_v7.md`.

## Arquitectura
- `src/sources/<fuente>/` — descarga + parsing (una carpeta por fuente).
- `src/pipelines/run_<fuente>.py` — orquestador por fuente; escribe
  `data/processed/<fuente>.csv` con esquema largo
  `date,year,month|quarter,<series...>,source,download_date`.
- `src/quality_checks.py` — validaciones por fuente (`QualityCheckError`).
- `src/config.py` — dataclasses `frozen` con URLs, rutas y rangos.
- `src/main.py` — CLI; cada fuente tiene un flag (`--unemployment`, `--ipc`…).
- `src/nairu/`, `src/production/` — modelos (bloque mensual y trimestral).

## Reglas
- Crudos en `data/raw/<fuente>/` (gitignored); procesados en `data/processed/`.
- Todo scraper nuevo: módulo en `src/sources/`, `run_*.py` en `src/pipelines/`,
  flag en `src/main.py`, checks en `quality_checks.py`, línea en el paso
  "Scrape fuentes" de `update.yml` y **tests offline** (`tests/test_<fuente>.py`,
  con HTML/Excel/JSON de muestra en `tests/fixtures/`, sin red).
- Los scrapers fallan de forma ruidosa (excepción), nunca escriben un CSV vacío
  ni truncado; el workflow ya captura el error y usa el dato cacheado.
- `pct_change(..., fill_method=None)` siempre. pandas `<3.0`, numpy `<2.0`.
- Correr `pytest tests/ -q` antes de comitear. Fallas conocidas aceptadas:
  13 de `TestAgainstV3Baseline` y 2 de `TestComputeVIOGWeights`.
- Registrar cada cambio en `CHANGELOG.md` (español, formato Keep a Changelog).
- Idioma de código y docs: español (nombres de columnas pueden ir en inglés si
  ya existen así).
