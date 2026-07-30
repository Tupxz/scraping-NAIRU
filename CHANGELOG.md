# Changelog

## [0.5.1] — 2026-07-30

### Cambiado
- **`cf_min_obs` default pasa de None (2·cf_high = 64T) a 3** (mínimo
  matemático del filtro asimétrico): la serie C-F de una cola cubre casi
  todo el período (VIOG-CO: 1994Q3–presente en vez de 2009Q4–presente).
  Decisión editorial para el tablero: los primeros años son la mejor
  estimación causal posible en su momento pero con poca historia (el
  filtro no ha visto un período completo de la banda hasta 2001Q4) —
  leerlos con cautela. La causalidad no cambia (la máscara no altera
  ningún valor); beneficio lateral: `rev_cf` acumula error desde el
  inicio y el peso 1/VIOG del CF vuelve a ser comparable al de los
  demás filtros.

## [0.5.0] — 2026-07-30

### Cambiado
- **Filtro Christiano-Fitzgerald del VIOG pasa a UNA COLA (causal) por
  defecto** (`viog.py::apply_filters`, flag `cf_one_sided: bool = True` en
  `VIOGConfig` — aplica a VIOG-USA y VIOG-CO): `trend_cf` en cada t ahora
  depende solo de y₁..y_t (fórmula asimétrica de Christiano & Fitzgerald
  2003 con nf=0 adelantos, implementación analítica en
  `cf_filter_one_sided()`), de modo que la brecha C-F es la de tiempo real
  y no se revisa retroactivamente con cada dato nuevo (crítica de
  Orphanides 2001 AER; 2003 JME). La versión anterior de dos colas
  (statsmodels `cffilter`, drift=False) se conserva con
  `cf_one_sided=False`. Costo: warm-up de `cf_min_obs` obs (default
  2·cf_high = 64T) con `trend_cf = NaN` y peso VIOG 0 (mismo mecanismo que
  los extremos de BK) — en VIOG-CO la serie C-F publicada arranca en
  2009Q4; y mayor persistencia/menor amplitud del ciclo en crisis (en
  2020Q2 la brecha en tiempo real fue −2.0 pp vs −8.3 pp ex-post). En el
  borde derecho ambos filtros coinciden por construcción.
- **`run_viog_pipeline()` acepta `cfg=`** y `run_viog.py` le pasa la config
  del país: antes los filtros usaban siempre `VIOG_CONFIG` (inocuo mientras
  ambas configs compartían parámetros; necesario ahora que flags como
  `cf_one_sided` pueden diferir por país).

### Agregado
- `cf_filter_one_sided()` en `viog.py` (numpy puro; exportada en
  `src.sources.viog`): equivale a precisión de máquina a correr
  `cffilter(y[:t+1], drift=False)` y tomar el último valor para cada t.
- `tests/test_viog.py::TestCFOneSided`: test de la propiedad definitoria de
  causalidad (filtrar y[:T] == filtrar y[:t+1] en el tramo común, varios
  t), equivalencia con el `cffilter` expansivo de statsmodels, cableado en
  `apply_filters`, reproducción del modo dos colas y warm-up configurable.
- `scripts/compare_cf_one_sided.py`: comparación una vs dos colas sobre
  PIB_CO (gráfica + CSV en `outputs/diagnostico_cf/`), con la revisión
  ex-post implícita del filtro de dos colas (media 1.4 pp, máx 6.3 pp en
  2020Q2; sd de la brecha 2.3 pp) y el efecto sobre los compuestos VIOG.

## [0.4.3] — 2026-07-07

### Cambiado
- **Tablero — vista VIOG desde 1994**: `viog_trimestral.csv` exporta la
  serie completa del empalme (antes recortaba a 2005); se ve la crisis
  del 99 igual que en las figuras de `outputs/viog_colombia/`.
- **VIOG-CO se mantiene con 5 filtros sobre el PIB** (BK, CF, BW, BHP,
  Kalman), sin referencia externa — como `notebooks/VIOG.ipynb` aplicado
  al PIB (decisión 2026-07-07; se evaluó y descartó usar el potencial
  C-D del pipeline como sexta variable de referencia).
- **Tablero**: se retira la brecha de inflación de la gráfica de brechas
  mensuales (pendiente decidir núcleo vs total); sigue en el CSV como
  `brecha_inf`. Se restaura el diseño original de la página (revierte el
  rediseño académico de eb8fa7a).

## [0.4.2] — 2026-07-07

### Corregido
- **Filtro Kalman/UCM del VIOG** (`viog.py::apply_filters`): ahora traduce la
  especificación Stata original de `data/inputs/Code1.do` —
  `ucm PIB, model(rwdrift) cycle(1, frequency(.1)) cycle(1, frequency(3))` —
  como `level="random walk with drift"` + ciclo **estocástico amortiguado**
  (Harvey 1989) + `irregular=True` (rol del segundo ciclo de alta frecuencia),
  ajustado sobre 100·ln(Y) en vez de niveles. La especificación anterior
  (`cycle=True` sin `stochastic_cycle` = ciclo determinístico) producía una
  brecha sinusoidal pura de ±13% con transitorio inicial de +170% en VIOG-CO,
  y una brecha degenerada ≈0 en VIOG-USA que además inflaba el ponderador
  1/VIOG. Además: `cycle_period_bounds` de config por fin conectados (nuevo
  default 6–64 trimestres; cubre el ciclo largo ≈63q del .do), chequeo
  explícito de convergencia con reintento Powell→L-BFGS, warnings del fit
  logueados en vez de silenciados, y guarda de cordura si |gap| > 20%
  (auditoría §2.5). +3 tests de regresión. **Regenerar
  `viog_usa.csv`/`viog_colombia.csv` y figuras** con `python -m src.main
  --viog` y `--viog-co`.

### Mantenimiento
- Quick wins de la auditoría: seaborn como dependencia runtime (el panel PNG del
  modelo nunca se generaba en CI), URL correcta del repo en pyproject, código
  muerto eliminado en `export_web_data.py`, rotación del log (5 MB × 3), docstring
  de `main.py` completo y README con 32 columnas + comandos faltantes.

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
