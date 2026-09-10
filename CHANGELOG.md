# Changelog

## [0.5.8] — 2026-09-10

### Cambiado
- Rediseño completo de `docs/index.html`: identidad visual estilo Universidad EAFIT (negro `#000000` + azul `#146AEF`, tipografía Inter, bloques a todo el ancho en vez de tarjetas con sombra) en lugar de la paleta genérica anterior, y reorganización de 3 a 4 pestañas — se separó "Mercado laboral y precios" de "Datos originales" porque ya había quedado muy cargada.

### Agregado
- Pestaña nueva "Mercado laboral y precios": además de NAIRU/NAICU/TGP* (movidas desde "Datos originales"), gráfica de brecha de inflación y la curva de Phillips estimada por el modelo (brecha de inflación vs. brecha laboral, degradado temporal punto a punto, línea de tendencia OLS).
- Pestaña "PIB Potencial": gráfica de descomposición del crecimiento potencial interanual (capital/trabajo/PTF, usando `contrib_capital`/`contrib_trabajo`/`contrib_ptf` de `[0.5.7]`), gráfica de PTF observada vs. tendencial, y KPI de crecimiento del PIB potencial.
- Pestaña "Datos originales": gráfica de los factores de producción (capital y trabajo, observado vs. potencial) y filas nuevas en la tabla de estadísticas descriptivas.

### Nota
- Revisado con el usuario en una vista previa (Artifact) antes de guardarse en el repo.

## [0.5.7] — 2026-09-10

### Corregido
- `src/pipelines/export_web_data.py`: `PIB_EXPORT_COLS` todavía tenía nombres de columna previos a la realineación v3 (`PIB`, `K_usado`, `K_pot`, `L_obs`, `L_pot`, `UCI`, `NAICU_q`, `TD`, `NAIRU_q`, ver `[0.5.6]`) — `_read_and_rename` los omitía en silencio (sin error ni warning) si no existían, así que `docs/data/pib_trimestral.csv` habría quedado sin el PIB observado y sin la mayor parte del contexto de mercado laboral. Se actualizó el mapeo a los nombres reales (`idx_pib`, `idx_K`/`idx_K_star`, `idx_L`/`idx_L_star`, `idx_LH`/`idx_LH_star`, `icu`, `naicu`, `td`, `nairu`, `tgp`/`tgp_star`, `pet`, etc.) y ahora se registra un warning explícito si una columna esperada falta, en vez de omitirla sin dejar rastro.
- Convención de fecha trimestral: el pipeline crudo fecha cada trimestre por su último mes (fin-de-trimestre, p. ej. "2026-03-01" para 2026-T1) mientras el resto de los datos de la página (NAIRU mensual, VIOG) usa inicio-de-trimestre (convención QS de pandas, "2026-01-01" para 2026-T1) — desalineaba el eje temporal del PIB frente a las demás series hasta por 2 meses. Se reconstruye `fecha` desde `year`/`quarter` en vez de confiar en la columna `date` cruda.

### Agregado
- `idx_LH`/`idx_LH_star` (trabajo: horas × capital humano) al export de `pib_trimestral.csv`.
- Descomposición del crecimiento potencial interanual (`contrib_capital`, `contrib_trabajo`, `contrib_ptf`, `crecimiento_potencial`; diferencias logarítmicas a 4 trimestres, en pp) — identidad `contrib_capital + contrib_trabajo + contrib_ptf ≈ crecimiento_potencial` verificada sobre los datos reales (diferencia máxima ≈1e-4, ruido de redondeo).

### Nota
- Motivado por el rediseño de `docs/index.html` (identidad visual EAFIT, más gráficas — descomposición del crecimiento, PTF observada/tendencial, factores de producción, brecha de inflación, curva de Phillips) — el HTML en sí todavía no se comitea, pendiente de aprobación del usuario sobre el diseño.

## [0.5.6] — 2026-09-10

### Cambiado
- **Realineación completa del motor de PIB Potencial (`src/production/factors.py`, `tfp.py`, `pib_potencial.py`, `src/pipelines/run_pib_potencial.py`) con el Módulo 2 de `legacy/pib_potencial_integrado_v3.py`, configuración v2** (a pedido explícito del usuario: "quiero que todo esté como el v3 que te adjunté" — completa la realineación que en `[0.5.5]` solo había cubierto `src/nairu/model_core.py`). Reemplaza, formula por fórmula, cada pieza que antes era una aproximación simplificada del repo por la metodología real de v3:
  - **Factor trabajo**: de nivel observado (`PET×TGP/100×(1−TD/100)`, en miles de personas) a un índice de HORAS trabajadas (`idx_L`/`idx_L_star`, base=100), construido desde una regresión OLS de TGP* (participación potencial, con tendencia por tramos anclada en los picos del ciclo económico) y ajustado por la jornada legal vigente (Ley 2101 de 2021: 48h hasta 2023-Q2, bajando a 42h en 2026-Q3) y por los festivos efectivos de cada trimestre (`data/inputs/festivos_efectivos_colombia_2001_2026.xlsx`).
  - **Factor capital**: de Inventario Permanente (PIM) sobre la FBKF trimestral del DANE (con un ancla K_0 de estado estable cuya fragilidad se había auditado y cuantificado en `[0.5.4]`/`[0.5.5]` vía `compute_k0_sensitivity`) al STOCK DE CAPITAL PRODUCTIVO DANE observado directamente (`data/inputs/alt_capital/dane_stock_capital_productivo.csv`, anual, interpolado a trimestral por PCHIP) — sin ancla asumida. `compute_k0_sensitivity` queda retirada (levanta `NotImplementedError`; ver nota más abajo); `_build_capital_quarterly` (la utilidad PIM en sí) se conserva intacta y sigue pasando sus propios tests, como referencia de la metodología anterior.
  - **Alpha**: de un valor fijo calibrado a mano (0.40) a un valor dinámico estilo CBO, `EBE/(RA+EBE)`, promediado en la ventana 2016-Q1→T (primer trimestre con datos de ingreso DANE).
  - **Tendencia de la PTF que entra al PIB potencial**: de un filtro Boosted-HP puramente estadístico a una regresión OLS ESTRUCTURAL de `ln(PTF)` sobre una tendencia por tramos (rampas-meseta ancladas en los picos del ciclo, detectados con el algoritmo Bry-Boschan trimestral — nuevo módulo `src/production/business_cycle.py`) más brecha de desempleo (contemporánea y rezagada) y dummies de pandemia. El filtro HP se conserva como comparación secundaria (`ptf_hp`/`brecha_pot_hp`/`Brecha_BHP`), pero de una sola pasada (no *boosted*), igual que v3.
  - **Unidades**: el motor completo pasa de NIVELES (miles de personas, millones de pesos) a ÍNDICES base 100 construidos sobre sumas móviles de 4 trimestres ("anualizadas") — la forma en que v3 suaviza el ruido trimestral antes de compararlo en la función Cobb-Douglas. `Brecha_CD`/`Brecha_BHP` (%) y las brechas fracción subyacentes NO cambian de interpretación (son razones, invariantes a la unidad de los índices); `PIB_pot`/`L_obs`/`L_pot`/`K_usado`/`K_pot` sí — estos dos últimos se retiran a favor de `idx_L`/`idx_L_star`/`idx_K`/`idx_K_star` (ver `src/quality_checks.py` y `src/production/excel_writer.py`, actualizados en consecuencia).
  - **Ancla de índices (`BASE_QUARTER`)**: v3 usa 2005-Q1 (el Boceto Excel original del autor tenía historia desde 2004). Los datos reales de este repo no lo permiten: el PIB desestacionalizado del DANE empieza exactamente en 2005-Q1 sin trimestre previo, y la GEIH mensual tiene un hueco real de 2 meses en 2006-07/08 que deja 2006-Q3 completo en NaN y contamina cualquier ventana móvil de 4 trimestres que lo incluya. Se ancla en **2007-Q3** (`factors.BASE_QUARTER`), el primer trimestre cuya ventana `[2006Q4..2007Q3]` ya no toca el hueco — no cambia ninguna brecha ni razón calculada (reescalar el "=100" de un índice multiplicativo es una identidad algebraica), solo qué trimestre vale exactamente 100.
  - El Boceto Excel multi-hoja que el Módulo 2 de v3 leía originalmente (`F1A+FLT+HCI`, con columnas en posiciones fijas) no existe en este repo — solo un `Sheet1` con estructura distinta, y no se encontró ninguna copia en todo el árbol compartido de Coyuntura Económica. Se sustituyó ÚNICAMENTE ese paso de lectura por un adaptador que construye los mismos insumos desde los CSV que el repo ya procesa (`data/processed/*.csv`); toda la metodología (fórmulas, regresiones) es la de v3 sin modificar.

### Agregado
- `src/production/business_cycle.py` (nuevo): algoritmo Bry-Boschan trimestral (BBQ) de fechado del ciclo económico y las variables de tendencia por tramos ("rampas-meseta" estilo CBO/NBER) que usan tanto la regresión de TGP* como la de PTF*.
- `tests/test_pib_potencial_engine.py` (nuevo, 42 tests): unitarios sobre BBQ/rampas-meseta, utilidades de fecha/jornada legal, mercado laboral, interpolación PCHIP del capital DANE, y fórmulas de PIB potencial sobre datos sintéticos; más una clase de regresión (`TestAgainstV3Baseline`, se salta si faltan datos/paquetes) que corre el pipeline real del repo contra los datos reales y lo compara columna por columna contra la línea base congelada de v3.
- `tests/fixtures/baseline_v3/` (nuevo): línea base numérica de v3 congelada (Fase 0) — `v3_CONFIG_V2_baseline_series.csv` (85 trimestres × 60 columnas, 2005-Q1→2026-Q1) y su metadata (`_meta.json`, incluye los parámetros de ambas regresiones OLS), generada corriendo las funciones REALES de `legacy/pib_potencial_integrado_v3.py` (sin editar ese archivo) contra el adaptador de datos reales del repo.

### Verificado
- **Corrida de cierre completo contra el repo real**: `src.pipelines.run_pib_potencial.run()` ejecutado de verdad contra los datos reales del repo (85 trimestres, 2005-Q1→2026-Q1), comparado columna por columna contra la línea base congelada de v3 (`tests/fixtures/baseline_v3/`) — diferencia relativa máxima ≈ 1e-13 en las 22 columnas comparadas (`idx_pib`, `idx_K`, `idx_K_star`, `idx_L`, `idx_L_star`, `idx_LH`, `idx_LH_star`, `alpha`, `alpha_t`, `ptf`, `ptf_star`, `pib_pot`, `brecha_pot`, `tgp`, `tgp_star`, etc.) — ruido de punto flotante, no discrepancia real. Picos del ciclo (BBQ) detectados: 2019-Q4 y 2023-Q1, idénticos en ambas corridas.
- **Excepción documentada, no un error**: las columnas de referencia `ptf_hp`/`pib_pot_hp`/`brecha_pot_hp` salen COMPLETAMENTE vacías en la línea base de v3 (0 de 85 filas), pero pobladas (74/85) en el motor realineado. Causa raíz: v3 llama a `hpfilter` de `statsmodels` DIRECTAMENTE, sin descartar nulos — y el trimestre más reciente (2026-Q1, el propio `T`) tiene `idx_K` en NaN porque el ICU/NAICU mensual aún no llega hasta ese mes (dato de frontera, esperado en series en tiempo real). Ese único NaN hace que la llamada cruda de v3 devuelva la tendencia ENTERA en NaN; el motor realineado usa el wrapper `tfp.hp_filter` (que ya descartaba nulos antes de este cambio, ver `[versión anterior]`) y por lo tanto sí produce las 74 filas válidas. No afecta ninguna columna principal (`Brecha_CD`/`pib_pot`, que usan `ptf_star`, no `ptf_hp`) — se documenta como una mejora de robustez frente a un caso límite que el Boceto original del autor de v3 nunca tuvo que enfrentar (su fuente de datos no tenía este tipo de desfase entre series).
- `pytest tests/` (suite completa, 542 tests) corrida de verdad contra el repo real: **540 passed, 2 failed**. Las 2 fallas son preexistentes y ajenas a este cambio: `tests/test_viog.py::TestComputeVIOGWeights` (`test_rev_positive_for_non_bk`, `test_inv_rev_positive_for_non_bk`) — no se tocó `src/pipelines/run_viog.py` en esta sesión (tarea pendiente: migrar el Kalman del VIOG a niveles). `tests/test_nairu.py` (24 tests) pasa completo contra el repo real.

### Retirado
- `compute_k0_sensitivity()` (`src/pipelines/run_pib_potencial.py`): ya no se llama desde `run()` (no vuelve a escribir `k0_sensitivity.csv`) y levanta `NotImplementedError` si se invoca directamente. Su premisa —cuantificar la sensibilidad del PIB potencial al ancla K_0 del capital PIM— dejó de aplicar al pipeline principal: el capital físico ahora es un dato observado (stock DANE), sin ancla de la que medir sensibilidad. `tests/test_run_pib_potencial.py::TestComputeK0Sensitivity` se actualizó para confirmar el retiro explícitamente en vez de perder cobertura silenciosamente.

### Nota
- Alcance: `src/production/factors.py`, `tfp.py`, `pib_potencial.py`, `business_cycle.py` (nuevo), `src/pipelines/run_pib_potencial.py`, `src/production/excel_writer.py` (columnas/textos de la hoja Trimestral y Supuestos), `src/quality_checks.py` (una columna requerida: `PIB`→`idx_pib`). NO se tocó `src/nairu/` (ver `[0.5.5]`) ni el módulo VIOG.
- Cambios de este segmento aún no comiteados — igual que en `[0.5.5]`, pendiente de que el usuario lo pida explícitamente.

## [0.5.5] — 2026-09-03

### Cambiado
- **Realineación completa de `src/nairu/model_core.py` con la especificación final de `legacy/pib_potencial_integrado_v3.py`** (a pedido explícito del usuario: "v3 es la referencia final" — el pipeline tiene que arrojar los mismos resultados que ese archivo, con la arquitectura modular propia del repo, no una copia del monolito). Esto reemplaza la etapa de 17 parámetros / muestra desde 2005-01 (arreglada parcialmente en `[0.5.3]`) por la spec definitiva de v3, de 18 parámetros / muestra desde 2006-01 (los 12 meses adicionales de arranque los consume el cálculo de `unemployment_ma24`/`icu_ma24`, columnas de medias móviles de 24 meses que no se usan en ningún cálculo pero cuyos NaN iniciales determinan el arranque correcto de la muestra en el loader genérico compartido con v3). Cambios estructurales principales:
  - `covid_shock_coefficient` (parámetro propio de la etapa anterior) desaparece. El manejo de COVID se separa en dos series distintas de `ModelData`: `unemployment_hysteresis_anchor` (interpolada linealmente solo dentro de la ventana 2020-04 a 2021-06, usada únicamente en el término de control de estado) y `unemployment_current`/`lag1`/`lag2` (datos reales sin tocar, usados únicamente en la ecuación de medición). `_apply_covid_anchor_interpolation()` muta solo el ancla.
  - `icu_gap_coefficient` (un solo rezago) se reemplaza por un rezago distribuido de 3 términos, `icu_coefficient_0`/`_1`/`_2`, sumados en `icu_total_coefficient`.
  - Los 4 parámetros débilmente identificados que motivaron el hallazgo "SE del MLE ≈ identidad" (`nairu_adjustment_speed`, `naicu_adjustment_speed`, `log_nairu_transition_std`, `log_naicu_transition_std`) pasan de estimarse a **fijarse** en los valores calibrados de v3, vía cotas de ancho cero en `PARAMETER_BOUNDS` — exactamente el mecanismo que usa v3 para resolver el mismo problema de identificación débil. `FIXED_PARAMETER_INDICES = frozenset({11, 12, 14, 15})` marca estas posiciones; `compute_mle_inference` las excluye del cálculo de errores estándar y las reporta como "Fijo por diseño" en vez de una SE espuria (resuelve así el hallazgo pausado en `[0.5.4]`, por una vía distinta a la reparametrización que se había planteado entonces).
  - `estimate_parameters` se rediseña ("seed-as-answer"): siempre devuelve `BEST_START_PARAMS_V3` (recortado a las cotas) como `fit.params`, de forma determinista. `minimize()` se sigue ejecutando pero solo como diagnóstico (su resultado se reporta en `fit.message`, nunca se adopta). Motivo: en este óptimo casi degenerado, dónde termina L-BFGS-B es sensible tanto a la versión de numpy/BLAS como al orden del vector de parámetros (verificado aislando ambas variables por separado) — no hay un arreglo universal robusto por el lado de "converger mejor", así que se adoptó el mismo criterio que ya usa v3: el punto calibrado manualmente ES la respuesta, no un punto de partida para el optimizador.

### Verificado
- **Fidelidad numérica confirmada de tres formas independientes contra `legacy/pib_potencial_integrado_v3.py`**: (1) función objetivo bit-idéntica entre el módulo realineado y la referencia, para el mismo vector de parámetros; (2) arreglos completos de estados del filtro de Kalman (no solo el resultado final) bit-idénticos, verificado en dos entornos numpy distintos (numpy<2.0 y numpy≥2.0, para descartar que la coincidencia dependiera de una versión específica de BLAS); (3) **corrida de cierre completo contra el repo real**: `src.nairu.estimation.build_outputs()` ejecutado de verdad (no una copia) contra `Data_NAIRU.xlsx` real en un entorno con las versiones exactas de `requirements.txt`, comparado contra la salida de referencia de v3 — diferencia máxima absoluta `nairu_estimate` ≈ 5,3e-15, `naicu_estimate` ≈ 1,4e-14 (ruido de punto flotante).
- `outputs/nairu/nairu_colombia.csv` regenerado con la spec v3-alineada completa. Última fila publicada: NAIRU 2025-12 = 9,45397 %, NAICU = 79,22578 %.
- `pytest tests/` (suite completa) corrida de verdad contra el repo real: **141 passed, 2 failed**. Las 2 fallas (`tests/test_viog.py::TestComputeVIOGWeights::test_rev_positive_for_non_bk` y `test_inv_rev_positive_for_non_bk`) son preexistentes, no relacionadas con este cambio, y no se investigaron a fondo en esta pasada.

### Agregado
- **`tests/test_nairu.py` reescrito completo para la estructura de 18 parámetros** (24 tests, reemplazan los 14 de `[0.5.3]`): `TestParameterVectorConsistency` (incluye `test_fixed_parameter_indices_are_exactly_the_pile_up_params`, que fija `FIXED_PARAMETER_INDICES == frozenset({11,12,14,15})` como contrato explícito), `TestUnpackParams` (incluye `test_icu_total_coefficient_is_sum_of_three`), `TestKalmanSmootherRegression` (golden-master que fija `nairu[0] == 11,972239933106922 ±1e-4` para la muestra 2006-01 con los coeficientes publicados), `TestCovidAnchorInterpolation` (nuevo — verifica que `_apply_covid_anchor_interpolation` toca solo el ancla y no la medición, y que la interpolación es lineal dentro de la ventana), `TestEstimateParametersUsesCalibratedSeedDirectly` (nuevo — confirma que `fit.params` es siempre `BEST_START_PARAMS_V3` recortado a las cotas, que `fit.success` es siempre `True` sin importar la calidad de los datos, y que los 4 parámetros fijos en `fit.params` coinciden con las constantes finales), `TestNeedsEstimationTrigger`, `TestUnpenalizedNLL`. Corridos de verdad con pytest en tres entornos independientes (paquete mínimo de prueba en dos versiones de numpy, y el repo real) antes de portar el archivo.

### Nota
- Alcance de este cambio: solo `src/nairu/model_core.py` y su test suite. El contrato de API de `src/nairu/estimation.py` (`estimate_parameters(model_data)`, `build_outputs(data, model_data, fit, output_dir)`) se conservó sin tocar a propósito, igual que toda la arquitectura Cobb-Douglas del PIB potencial (`src/production/`, `run_pib_potencial.py`) y el módulo VIOG — ninguno de los dos se realineó con v3. El plan original de 2026-07-31 (`docs/prompts/prompt_fable5_pib_potencial_v3.md`) contemplaba un reemplazo total, incluyendo el motor de PIB potencial (PTF estructural CBO en vez de Boosted-HP); no está decidido si ese reemplazo más grande sigue en pie.
- Comitear los archivos tocados en esta realineación y en las sesiones previas (Fase 0/Fase 1 del plan de limpieza, anualización del PIB-CO) sigue pendiente — no solicitado explícitamente por el usuario todavía. `.github/workflows/update.yml` sigue con la reestimación automática pausada hasta que eso ocurra.

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
