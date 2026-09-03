# Changelog

## [0.5.4] — 2026-09-01

### Corregido
- **`pct_change()` sin `fill_method` explícito (5 sitios)** (hallazgo #2 de
  la auditoría 2026-08-21, Fase 1 ítem 1 del plan de limpieza): heredaban
  el default legacy de pandas 2.1-2.x (`fill_method='pad'`), que rellena
  huecos hacia adelante ANTES de calcular la variación % -- fabricando
  variaciones trimestre/mes a trimestre/mes que nunca ocurrieron en vez de
  propagar NaN. La auditoría había contado 4 sitios; se confirmó un
  quinto al revisar todo el repo de nuevo. Arreglados con
  `fill_method=None` explícito: `src/merge.py` (`ipc_yoy`/`ipc_mom` -- el
  caso con mayor impacto: 6 meses de inflación fabricada en
  `data/final/nairu_dataset.csv`, filas futuras de `Inf_Goal` publicadas
  con anticipación que el outer-merge deja con `ipc_index=NaN`),
  `src/pipelines/build_production_function_dataset.py` (`_pct_change`,
  todas las columnas `Var%*` del dataset trimestral de función de
  producción), `src/quality_checks.py` (`check_ipc_monotonic` -- hallazgo
  nuevo, no estaba en la auditoría original), `src/pipelines/run_pib_potencial.py`
  (`g_q`, sin cambio de comportamiento porque `inv_q` ya llegaba sin
  nulos -- arreglado solo por consistencia y para no emitir el
  `FutureWarning`) y `src/sources/viog/viog.py::plot_filters` (uso
  exclusivamente gráfico, sin consumidor numérico aguas abajo).
- **AIC/BIC calculados sobre el objetivo penalizado del optimizador, no
  la log-verosimilitud real** (`src/nairu/model_core.py`, hallazgo
  confirmado por lectura en la auditoría 2026-08-21, Fase 1 ítem 6 del
  plan de limpieza): `_kalman_pass` acumulaba en una sola variable `nll`
  tanto el término gaussiano genuino de la verosimilitud como las
  penalizaciones cuadráticas que mantienen al optimizador dentro de los
  pisos/techos válidos de NAIRU/NAICU (p.ej. `8·(MIN_NAIRU_LEVEL −
  nairu_pred)²`) -- `compute_mle_inference` calculaba
  `log_likelihood = -fit.nll` directamente, tratando esas penalizaciones
  como si fueran parte del modelo. Arreglo: nuevo campo
  `KalmanHistory.unpenalized_nll` que acumula solo el término gaussiano
  puro; `compute_mle_inference` recalcula una pasada barata (`O(n_obs)`,
  nada que ver con las 97 corridas del multi-start) en el óptimo para
  separar ambos términos, y usa el puro para `log_likelihood`/AIC/BIC.
  Se agregó también `penalty_at_optimum` a `diagnostics_table` como
  diagnóstico de cuánta penalización seguía activa en el punto final
  (0 = ninguna).
- **Sensibilidad de K₀ (capital inicial) no cuantificada**
  (`src/pipelines/run_pib_potencial.py`, hallazgo #5 de la auditoría
  2026-08-21, Fase 1 ítem 3 del plan de limpieza, decisión del usuario
  2026-09-01: "tabla de sensibilidad"): `K_0` (capital de estado
  estacionario, fórmula de Harberger) se ancla en 2005-Q1 y su peso
  decae solo con la depreciación acumulada -- `(1-δ_q)^84 ≈ 0,50` hacia
  2026 -- sin ninguna forma de saber cuánto le importa un error ahí al
  PIB potencial publicado. No existe una serie de capital DANE anterior
  a 2005 con la que anclar mejor `K_0`, así que en vez de "arreglar" el
  ancla se la hizo auditable: `_build_capital_quarterly`/
  `load_and_align_sources` ganan un parámetro `k0_multiplier` (1.0 =
  sin cambios, el comportamiento de siempre) y una función nueva,
  `compute_k0_sensitivity`, corre el pipeline completo con `K_0`
  perturbado ±10 %/±20 % y mide el efecto en `K_pot`, `PIB_pot` y
  `Brecha_CD` a través del tiempo. `run()` la llama después de los
  quality checks y escribe `outputs/pib_potencial/k0_sensitivity.csv`
  -- no cambia la estimación central (multiplicador 1.0), solo la
  documenta.
  Hallazgo adicional al verificar numéricamente: el efecto real
  simulado es mucho menor que la estimación a ojo de la auditoría
  (~2 pp para un error del 10 %). En este pipeline, un error
  multiplicativo en `K` se cancela casi por completo en el residuo de
  TFP (`A_obs = PIB / (K_usado^α · HL_obs^(1-α))`, con PIB observado
  fijo) porque `K_pot/K_usado = NAICU_q/UCI` es exactamente invariante a
  cualquier transformación de `K` -- ambos términos son lineales en la
  misma serie de `K`. El efecto simulado sobre `Brecha_CD` incluso en el
  escenario más extremo (±20 %, sin decaimiento) queda en el orden de
  0,02-0,03 pp, no ~2 pp. `k0_sensitivity.csv` documenta la cifra real en
  vez de dejar la intuición de la auditoría sin contrastar.

### Agregado
- **Tests para los arreglos de `pct_change`**: `tests/test_merge_derived.py`
  gana `TestIpcYoyInternalGap` (3 tests, incluye control negativo con
  `pytest.warns(FutureWarning)` confirmando que el comportamiento viejo
  fabricaba un valor); `tests/test_build_production_function_dataset.py`
  es un archivo nuevo (el módulo no tenía ninguna cobertura) con
  `TestPctChange` (4 tests, mismo patrón de control negativo -- se
  confirmó que el bug reintroducido hace fallar 2 de los 4).
- **Tests para el arreglo de AIC/BIC**: `tests/test_nairu.py` gana
  `TestUnpenalizedNLL` (3 tests) -- confirma que `unpenalized_nll` es
  idéntico a `nll` cuando ninguna penalización está activa, que excluye
  correctamente una penalización real y grande (~226, escenario
  construido con `naicu_initial_level=41.0`), y que
  `compute_mle_inference` de verdad usa el valor recalculado (no
  `fit.nll`) inyectando una penalización artificial de 500 en un
  `FitResult` sintético y confirmando que `log_likelihood`/AIC no la
  heredan.
  Los 17 tests de `tests/test_nairu.py` + los 4 de
  `tests/test_build_production_function_dataset.py`, más los 19 de
  `tests/test_merge_derived.py` y los 35 de `tests/test_merge.py`, se
  corrieron de verdad con pytest usando pandas 2.3.3 (la versión fijada
  en `pyproject.toml`, no la que trae por defecto el contenedor de la
  nube) para reproducir con exactitud el comportamiento legacy que se
  está corrigiendo -- 75 tests, todos pasan.
- **Tests para la sensibilidad de K₀**: `tests/test_run_pib_potencial.py`
  es un archivo nuevo (el módulo no tenía ninguna cobertura) con
  `TestBuildCapitalQuarterly` (4 tests: el multiplicador escala
  linealmente el primer periodo y decae geométricamente con
  `(1-δ_q)` después, exactamente igual que cualquier otro término de la
  recursión del inventario permanente) y `TestComputeK0Sensitivity`
  (4 tests: contrato de la función -- corre sin `statsmodels` -- más
  integración completa con los datos reales del repo -- solo corre si
  `statsmodels` está instalado, p.ej. en el Mac del usuario). 8 tests,
  todos pasan; control negativo confirmado (3/8 fallan si se reintroduce
  un `k0_multiplier` ignorado).

### Nota
- Fase 1 del plan de limpieza 2026-09-01: de los 6 hallazgos, 2 eran
  arreglos mecánicos de bajo riesgo (no cambian ninguna cifra publicada
  salvo `ipc_yoy`/`ipc_mom`, que ya estaba mal) y la sensibilidad de K₀
  resultó ser un tercero de bajo riesgo una vez decidido el enfoque
  (tabla auditable, no cambia la estimación central -- ver arriba). Los
  otros 3 (SE del MLE ≈ identidad, `L_pot` con TGP observada en vez de
  TGP*, filtros VIOG en niveles vs Kalman en logs) son decisiones
  metodológicas que cambiarían cifras ya publicadas o requieren
  construir un sub-modelo nuevo -- se presentaron al usuario para
  decidir antes de tocar código (ver plan de limpieza en la memoria del
  proyecto). De esos 3, la investigación de "SE del MLE ≈ identidad"
  reveló algo más grande de lo esperado: el ajuste MLE publicado
  (`nairu_colombia.csv`) probablemente no está en un óptimo genuino
  (verificado con los datos reales: apretar solo las tolerancias del
  optimizador, cotas sin cambios, mejora la log-verosimilitud de forma
  sustancial) y "reparametrizar quitando las cotas" -- el plan original
  -- no es seguro (sin ellas el optimizador empuja hacia una NAIRU/NAICU
  degenerada que persigue el dato ruidoso en vez de estimar una
  tendencia suave; esas cotas hacen trabajo de regularización real, no
  son solo un límite numérico). Pausado para una segunda decisión del
  usuario sobre el alcance del arreglo -- ver la memoria del proyecto
  para el detalle completo.
- Pendiente en el Mac del usuario (se suma a lo ya pendiente de Fase 0):
  `pytest tests/` completo -- aquí solo se pudieron correr los archivos
  de test relacionados con los módulos tocados (75 tests), no la suite
  entera (varios tests de fuentes/VIOG necesitan `statsmodels`, bloqueado
  por PyPI 403 en este contenedor).

## [0.5.3] — 2026-09-01

### Corregido
- **Off-by-one en el suavizador de Kalman de `src/nairu/model_core.py`**
  (`kalman_filter_and_smoother`, hallazgo #1 de la auditoría de código del
  2026-08-21): armaba `state_transition` con `params[9]`/`params[10]`
  (= `covid_shock_coefficient`/`nairu_adjustment_speed`) en vez de
  `params[10]`/`params[11]` (`nairu_adjustment_speed`/
  `naicu_adjustment_speed`) — se desalineó cuando `covid_shock_coefficient`
  se insertó en la posición 9 de `PARAMETER_NAMES` y nadie actualizó este
  bloque (`_kalman_pass`, que desempaqueta por nombre, no se vio afectado).
  Efecto verificado numéricamente reproduciendo la corrida publicada con
  `nairu_mle_coefficients.csv`: NAIRU 2005-01 publicada 16,67 % vs 13,48 %
  correcta (3,19 pp; decae a <0,1 pp hacia 2007), NAICU máx. 1,43 pp,
  ancho de banda 90 % medio 0,574 pp (publicado) vs 0,723 pp (correcto),
  3/250 obs con varianza pegada al piso `MIN_VARIANCE=1e-10` (de ahí el
  `nairu_se≈1e-05` de las primeras filas). Arreglo: en vez de otro índice
  hardcodeado, `kalman_filter_and_smoother` ahora arma `state_transition`
  con `unpack_params()` — la misma fuente de verdad que ya usa el resto
  del módulo — para que un futuro cambio en `PARAMETER_NAMES` no pueda
  romper esto otra vez en silencio.
- **Disparador de re-estimación ciego al código** (`estimation.py`,
  hallazgo confirmado por lectura en la misma auditoría): la re-estimación
  solo se disparaba si `Data_NAIRU.xlsx` era más nuevo que
  `nairu_colombia.csv` — un cambio en `model_core.py`/`estimation.py`
  nunca la disparaba. Mordía justo al arreglar el bug anterior: sin este
  fix, corregir el suavizador no habría bastado para regenerar el CSV
  publicado. Se extrajo la decisión a una función pura
  `_needs_estimation(data_path, existing_csv, package_dir)` que además
  compara el mtime más reciente de los `.py` de `src/nairu/`.

### Agregado
- **Primeros tests para `src/nairu/`** (antes sin ninguna cobertura,
  confirmado por grep en toda `tests/`): 14 tests nuevos en
  `tests/test_nairu.py` — `TestParameterVectorConsistency` (3),
  `TestUnpackParams` (3), `TestKalmanSmootherRegression` (3, incluye
  regresión directa del off-by-one y un "golden master" contra los
  coeficientes MLE ya publicados), `TestNeedsEstimationTrigger` (5).
  Corridos de verdad con `pytest` (no solo razonados a mano): los 14
  pasan — este módulo solo necesita numpy/pandas/scipy (no statsmodels),
  así que sí corre completo en un contenedor con scipy instalado aunque
  no tenga el `.venv` del Mac.

### Nota
- Fase 0 del plan de limpieza 2026-09-01 completa (los 2 bugs de mayor
  impacto + cobertura de tests). Quedan pendientes en el Mac del usuario:
  `pytest tests/` completo (suite entera, no solo `test_nairu.py`) y
  `python -m src.main --nairu-estim` para regenerar `nairu_colombia.csv`
  con el fix (hoy sigue publicado el valor con el bug hasta que se
  re-corra). `.github/workflows/update.yml` sigue con la reestimación
  automática PAUSADA (ver comentario en el propio archivo) hasta que ese
  `pytest` + esa corrida se validen y se comiteen.

## [0.5.2] — 2026-09-01

### Agregado
- **PIB de Colombia anualizado antes de los 5 filtros del VIOG**: nueva
  `VIOGConfig.annualize_series` (+ `annualize_window`, default 4) en
  `src/config.py`, y `annualize_trailing_sum()` / `_annualize_df()` en
  `src/sources/viog/viog.py`. Suma móvil de 4 trimestres
  (`Y[t]+Y[t-1]+Y[t-2]+Y[t-3]`) sobre `Value(Billions)` — quita la
  estacionalidad por construcción sin pasar por un ajuste estacional
  formal (X-13/TRAMO-SEATS). Solo se anualiza `Y`; `Y_ref` (potencial de
  función de producción) no aplica a VIOG-CO (`ref_col=None`). Activado en
  `VIOG_CO_CONFIG` (`annualize_series=True`); `VIOG_CONFIG` (USA) no
  cambia. Efecto en la muestra: 129 → 126 trimestres (arranca en 1994Q4 en
  vez de 1994Q1 — las primeras 3 obs no tienen historia suficiente y se
  descartan, no se dejan en NaN, porque bkfilter/cffilter/filtfilt no
  toleran NaN intercalado). Verificado numéricamente: el valor anualizado
  en 1994Q4 (393.555,27 mil millones) coincide exactamente con el PIB
  anual DANE de 1994 (suma de sus 4 trimestres).
- 14 tests nuevos en `tests/test_viog.py`: `TestAnnualizeGDP` (9, corren
  sin statsmodels/scipy), `TestRunVIOGPipelineColombia` (4, requieren
  statsmodels/scipy — correr con el `.venv` del Mac),
  `TestRunColombiaUsesOwnConfig` (1, regresión del hallazgo de auditoría
  de abajo).

### Corregido
- **`run_viog.py::_run_for_config` no pasaba `cfg=config` a
  `run_viog_pipeline`** (hallazgo de la auditoría de código del
  2026-08-21): el VIOG-CO corría siempre con `VIOG_CONFIG` (USA) por
  defecto pese a que el docstring de `run_viog_pipeline` ya afirmaba lo
  contrario. Era inocuo mientras `VIOG_CO_CONFIG` no divergiera
  econométricamente de `VIOG_CONFIG`, pero bloqueaba por completo el punto
  anterior — sin este fix, `annualize_series=True` en `VIOG_CO_CONFIG` no
  habría tenido ningún efecto real.

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
