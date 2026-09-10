# Integración con v3 (`legacy/pib_potencial_integrado_v3.py`)

**Estado:** completo — motor de PIB potencial realineado con el Módulo 2 de v3.
**Última actualización:** 2026-09-10 (ver `CHANGELOG.md`, entrada `[0.5.6]`).

## Propósito de este documento

`legacy/pib_potencial_integrado_v3.py` es la implementación de referencia del
Módulo 2 (PIB potencial) del curso de Coyuntura Económica. Es un único script
monolítico, sin tests, que lee un Excel hecho a mano ("el boceto") y calcula
PIB potencial, brechas de producto y sus insumos (TGP*, PTF*, alpha, capital).

Este repositorio reimplementa esa misma metodología de forma modular y
automatizada: los datos ya no salen de un Excel armado a mano sino del
scraping (`src/scrapers/`, `src/pipelines/run_*.py`), y el cálculo vive en
`src/production/`. La decisión metodológica que gobierna todo este trabajo
es que **v3 es la referencia final**: dado el mismo periodo y los mismos
datos de entrada, este repo debe producir los mismos números que v3, formula
por formula. Este documento es el mapa entre ambos: qué corresponde a qué,
qué se adaptó y por qué, y cómo se verificó la equivalencia.

No cubre el módulo NAIRU (`src/nairu/`) ni el VIOG (`src/pipelines/run_viog.py`,
Kalman en logs) — ninguno de los dos se tocó en esta realineación.

## Por qué v3 y no el repo es la referencia

Instrucción explícita del usuario, reiterada varias veces a lo largo del
proyecto: *"recuerda que lo hago con base a esto. tiene que arrojar los
mismos resultados. osea el pipeline tiene que ser como eso pero pues como
nosotros lo estamos haciendo"* — y más adelante, al ver que solo NAIRU había
sido realineado: *"quiero que todo esté como el v3 que te adjunté"*. `v3.py`
en sí **no se edita nunca**; sirve solo como oráculo para generar la línea
base numérica congelada que los tests comparan (`tests/fixtures/baseline_v3/`).

## Mapa de metodología

### Factor trabajo: de nivel observado a índice de horas con TGP*

- **v3 / este repo:** `src/production/factors.py::compute_labor_factor` (antes
  `factor_trabajo`).
- **Antes de esta realineación**, el repo usaba la Tasa Global de Participación
  *observada* directamente: `PET × TGP_observada/100 × (1 − TD/100)`, en miles
  de personas.
- **v3 (y ahora el repo)** en cambio construyen un índice de **horas
  trabajadas**, base 100, en tres pasos:
  1. TGP\* (participación *potencial*, no la observada): una regresión OLS de
     la TGP observada sobre una tendencia por tramos ("rampas-meseta" estilo
     CBO/NBER) anclada en los picos del ciclo económico — ver
     `src/production/business_cycle.py`.
  2. La jornada legal vigente en cada trimestre, no las horas efectivamente
     declaradas: Ley 2101 de 2021 (48h hasta 2023-Q2, reducción escalonada
     hasta 42h en 2026-Q3), vía `JORNADA_SCHEDULE` en `factors.py`.
  3. Ajuste por festivos efectivos de cada trimestre
     (`data/inputs/festivos_efectivos_colombia_2001_2026.xlsx`), porque un
     trimestre con más festivos tiene menos horas hábiles aunque la jornada
     legal no cambie.
- Índices resultantes: `idx_L` (observado) e `idx_L_star` (potencial, con
  TGP\* y capital humano PWT). Reemplazan a los antiguos `L_obs`/`L_pot`.

### Factor capital: de PIM (con ancla K₀) a stock DANE observado

- **v3 / este repo:** `src/production/factors.py::compute_capital_factor`.
- **Antes**, el capital físico salía del Inventario Permanente (PIM) aplicado
  a la FBKF trimestral del DANE, con un capital inicial K₀ de estado
  estacionario (fórmula de Harberger) cuya fragilidad se había auditado y
  cuantificado explícitamente (`compute_k0_sensitivity`, tabla de
  sensibilidad ±10%/±20%, hallazgo #16 del plan de limpieza).
- **v3 (y ahora el repo)** usan en cambio el **stock de capital productivo
  del DANE**, un dato observado directamente y publicado anualmente
  (`data/inputs/alt_capital/dane_stock_capital_productivo.csv`), interpolado
  a trimestral con un spline PCHIP monótono (evita oscilaciones espurias
  entre puntos anuales). Al ser un dato observado, **no hay ancla que
  auditar**: `compute_k0_sensitivity` queda retirada (levanta
  `NotImplementedError` si se llama; ver `tests/test_run_pib_potencial.py`).
  `_build_capital_quarterly`, la utilidad PIM en sí, se conserva intacta
  (con sus tests) únicamente como referencia de la metodología anterior —
  ya no la usa el pipeline principal.
- Índices resultantes: `idx_K` (observado) e `idx_K_star` (potencial, vía
  ICU/NAICU). Reemplazan a `K_usado`/`K_pot`.

### Alpha: de valor fijo a participación dinámica estilo CBO

- **v3 / este repo:** `src/production/factors.py::compute_alpha`.
- **Antes:** alpha fijo, calibrado a mano en 0.40 para toda la serie.
- **v3 (y ahora el repo):** alpha dinámico, `EBE / (RA + EBE)` (excedente
  bruto de explotación sobre remuneración a asalariados + EBE, enfoque
  ingreso del PIB), promediado en la ventana 2016-Q1→T (primer trimestre con
  datos de ingreso del DANE hasta el trimestre más reciente disponible). Es
  decir, alpha *se mueve* con la estructura de la economía en vez de asumirse
  constante.

### Tendencia de la PTF: de filtro estadístico a regresión estructural

- **v3 / este repo:** `src/production/tfp.py::compute_tfp_trend`.
- **Antes:** filtro Hodrick-Prescott "boosted" (múltiples pasadas) sobre la
  PTF observada — un filtro puramente estadístico, sin relación económica
  explícita con el ciclo.
- **v3 (y ahora el repo):** regresión OLS **estructural** de `ln(PTF)` sobre:
  - una tendencia por tramos (rampas-meseta ancladas en los picos del ciclo,
    detectados con el algoritmo Bry-Boschan trimestral — `business_cycle.py`,
    el mismo mecanismo que TGP\*);
  - la brecha de desempleo, contemporánea y rezagada un trimestre;
  - dummies de pandemia (2020-Q2/Q3, el choque atípico del COVID).
  - El filtro HP se conserva como comparación secundaria (`ptf_hp`,
    `pib_pot_hp`, `Brecha_BHP`), pero de una sola pasada (no *boosted*),
    igual que v3.

### Unidades: de niveles a índices base 100

Todo el motor pasó de trabajar en **niveles** (miles de personas, millones de
pesos corrientes/constantes) a trabajar en **índices base 100**, construidos
sobre sumas móviles de 4 trimestres ("anualizadas") — así es como v3 suaviza
el ruido trimestral antes de combinar los factores en la función
Cobb-Douglas. Esto no es cosmético: cambia qué representa cada columna.

- `Brecha_CD`, `Brecha_BHP` (%) y las brechas-fracción subyacentes **no
  cambian de interpretación** — son razones, invariantes a la unidad del
  índice.
- `PIB_pot` sí cambia de unidad (índice, no millones de pesos).
- `L_obs`/`L_pot`/`K_usado`/`K_pot` (niveles) se retiran a favor de
  `idx_L`/`idx_L_star`/`idx_K`/`idx_K_star` (índices) — ver
  `src/quality_checks.py` y `src/production/excel_writer.py`, actualizados
  en consecuencia (columna requerida `"PIB"` → `"idx_pib"`).

### Ancla temporal: por qué `BASE_QUARTER` no es 2005-Q1 aquí

v3 usa 2005-Q1 como el trimestre que vale exactamente 100, porque el boceto
original del autor tenía historia desde 2004 (un año de "colchón" antes del
ancla). Los datos reales de este repo no tienen ese colchón:

- el PIB desestacionalizado del DANE empieza exactamente en 2005-Q1, sin
  ningún trimestre previo;
- la GEIH mensual tiene un hueco real de dos meses en 2006-07/2006-08, que
  deja **2006-Q3 completo en NaN** — y contamina cualquier ventana móvil de
  4 trimestres que lo incluya (es decir, cualquier índice "anualizado" cuyo
  trimestre de referencia caiga entre 2006-Q3 y 2007-Q2).

Por eso el repo ancla en **2007-Q3** (`factors.BASE_QUARTER`): el primer
trimestre cuya ventana `[2006-Q4 .. 2007-Q3]` ya no toca el hueco. Esto
**no cambia ninguna brecha ni razón calculada** — reescalar el "=100" de un
índice multiplicativo es una identidad algebraica — solo cambia *qué
trimestre* vale exactamente 100.

## Qué no se portó de v3: la lectura del boceto Excel

v3 lee sus insumos de un único Excel multi-hoja armado a mano (hoja
`F1A+FLT+HCI`, columnas en posiciones fijas: `D`/`G`/`H`/`M`/`Q` mensuales,
`U`/`V`/`X`/`BE`/`BQ`/`BR`/`BS`/`BT` trimestrales). Ese archivo no existe en
este repo — se buscó en todo el árbol compartido de Coyuntura Económica y no
apareció ninguna copia (solo un `Sheet1` con estructura distinta, no
utilizable). Se sustituyó **únicamente ese paso de lectura** por un adaptador
que construye los mismos insumos desde los CSV que el repo ya procesa en
`data/processed/*.csv` (DANE PIB, GEIH, DANE FBKF/ingreso, PWT). Toda la
metodología posterior — fórmulas, regresiones, la propia lectura de
NAIRU/jornada, que v3 tampoco toma del boceto — es la de v3 sin modificar.

## Verificación numérica

`tests/test_pib_potencial_engine.py::TestAgainstV3Baseline` corre el
pipeline real del repo (`src.pipelines.run_pib_potencial.run()`) contra los
datos reales (85 trimestres, 2005-Q1 → 2026-Q1) y compara columna por
columna contra una línea base numérica de v3 congelada en
`tests/fixtures/baseline_v3/` — generada ejecutando las funciones *reales*
de `legacy/pib_potencial_integrado_v3.py` (sin editarlo) contra el mismo
adaptador de datos.

Resultado: diferencia relativa máxima ≈ 1e-13 en las 22 columnas comparadas
(`idx_pib`, `idx_K`, `idx_K_star`, `idx_L`, `idx_L_star`, `idx_LH`,
`idx_LH_star`, `alpha`, `alpha_t`, `ptf`, `ptf_star`, `pib_pot`,
`brecha_pot`, `tgp`, `tgp_star`, entre otras) — ruido de punto flotante, no
discrepancia real. Los picos del ciclo detectados por BBQ (2019-Q4 y
2023-Q1) coinciden exactamente entre ambas corridas.

### Excepción documentada (no es un error)

Las columnas de referencia `ptf_hp`/`pib_pot_hp`/`brecha_pot_hp` salen
completamente vacías en la línea base de v3 (0 de 85 filas) pero pobladas
(74/85) en el motor realineado de este repo. Causa raíz: v3 llama al
`hpfilter` de `statsmodels` directamente, sin descartar nulos primero, y el
trimestre más reciente (2026-Q1) tiene `idx_K` en NaN porque el ICU/NAICU
mensual todavía no llega hasta ese mes (dato de frontera, normal en series
en tiempo real). Un solo NaN hace que la llamada cruda de v3 devuelva la
tendencia *entera* en NaN. El motor de este repo usa el wrapper
`tfp.hp_filter`, que ya descartaba nulos antes de esta realineación, y por
eso sí produce las 74 filas válidas. No afecta ninguna columna principal
(`Brecha_CD`/`pib_pot` usan `ptf_star`, no `ptf_hp`) — es una mejora de
robustez frente a un caso límite que el boceto original de v3 nunca tuvo
que enfrentar (su fuente de datos no tenía este desfase entre series).

## Cobertura de tests

- `tests/test_pib_potencial_engine.py` (nuevo, ~40 tests): BBQ y
  rampas-meseta (`business_cycle.py`), utilidades de fecha/jornada legal,
  mercado laboral, interpolación PCHIP del capital DANE, fórmulas de PIB
  potencial sobre datos sintéticos, y la clase de regresión contra la línea
  base de v3 descrita arriba.
- `tests/test_run_pib_potencial.py`: `_build_capital_quarterly` (la utilidad
  PIM heredada, con `k0_multiplier`) y confirmación de que
  `compute_k0_sensitivity` está retirada.
- `pytest tests/` (suite completa, 542 tests) corrida contra el repo real:
  **540 passed, 2 failed**, al momento de esta realineación. Las 2 fallas
  son preexistentes y ajenas a este cambio — ver "Fuera de alcance" abajo.

## Fuera de alcance de esta realineación

- `src/nairu/` — ya alineado con v3 en una entrega anterior (`[0.5.5]`).
- `src/pipelines/run_viog.py` (Kalman en logs) — pendiente, tarea #17 del
  plan de limpieza: migrar el filtro a niveles en vez de logs.
- Los 2 fallos preexistentes de `tests/test_viog.py::TestComputeVIOGWeights`
  (`test_rev_positive_for_non_bk`, `test_inv_rev_positive_for_non_bk`) — no
  relacionados con este cambio, no investigados a fondo en esta pasada.
  `tests/test_nairu.py` (24 tests) pasa completo contra el repo real.

## Mapa de archivos

| Archivo | Qué cambió |
|---|---|
| `src/production/business_cycle.py` | Nuevo. Algoritmo Bry-Boschan trimestral (BBQ) y variables de tendencia por tramos. |
| `src/production/factors.py` | Reescrito. Factor trabajo (TGP\*, jornada, festivos), factor capital (stock DANE + PCHIP), alpha dinámico CBO, `BASE_QUARTER`. |
| `src/production/tfp.py` | Reescrito. PTF observada + tendencia estructural OLS (más HP de una pasada como comparación secundaria). |
| `src/production/pib_potencial.py` | Reescrito. PIB potencial y las tres brechas sobre los nuevos índices. |
| `src/pipelines/run_pib_potencial.py` | Reescrito. Orquestador y adaptador de datos reales → insumos v3. `compute_k0_sensitivity` retirada. |
| `src/production/excel_writer.py` | Reescrito. Columnas/textos de las hojas Trimestral y Supuestos alineados a los nuevos índices. |
| `src/quality_checks.py` | Una columna requerida: `"PIB"` → `"idx_pib"`. |
| `tests/test_pib_potencial_engine.py` | Nuevo. |
| `tests/test_run_pib_potencial.py` | Reescrito para el nuevo contrato de `_build_capital_quarterly`/`compute_k0_sensitivity`. |
| `tests/fixtures/baseline_v3/` | Nuevo. Línea base numérica de v3 congelada. |
| `tests/test_production_factors.py`, `test_production_tfp.py`, `test_production_pib_potencial.py` | Retirados (importaban nombres de la implementación anterior a esta realineación). |
