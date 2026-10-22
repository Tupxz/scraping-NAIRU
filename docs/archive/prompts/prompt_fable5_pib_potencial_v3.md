# Prompt de integración — `pib_potencial_integrado_v3.py` → pipeline `scraping-NAIRU`

> **Para:** Fable 5, trabajando sobre el repo `scraping-NAIRU` (Coyuntura Económica, EAFIT).
> **Fecha:** 2026-07-31.
> **Naturaleza:** especificación de ingeniería, no sugerencia. Cada sección con ⚠️ describe un
> fallo que ocurre **en silencio** (sin excepción, sin log) si se ignora.

---

## 0. Rol

Actúa simultáneamente como:

- **Economista** con base neoclásica y keynesiana. El modelo que vas a integrar toma decisiones
  teóricas deliberadas (histéresis disciplinada, PTF purgada de ciclo, NAICU como utilización
  estructural del capital). **No las "simplifiques"**: la §9 explica por qué cada una está ahí.
  Si crees que una es incorrecta, dilo en el reporte final — no la cambies.
- **Ingeniero de sistemas y arquitecto**. El repo tiene una arquitectura sana (módulos puros sin
  I/O + orquestadores + tests + config central). El archivo que vas a integrar es un **monolito de
  3.831 líneas con rutas de Windows hardcodeadas**. Tu trabajo es disolverlo dentro de la
  arquitectura existente **sin cambiar un solo número**.

---

## 1. Qué hay hoy y qué lo reemplaza

### 1.1 Estado actual del repo

```
src/
  config.py                 PROJECT_ROOT, DATA_DIR, INPUTS_DIR, PROCESSED_DIR,
                            FINAL_DIR, OUTPUTS_DIR, VIOGConfig/VIOG_CONFIG/VIOG_CO_CONFIG
  main.py                   CLI único (argparse) → src/pipelines/*
  nairu/
    estimation.py           adaptador: carga model_core.py dinámicamente, ejecuta, renombra outputs
    model_core.py           motor NAIRU/NAICU ACTUAL (Kalman biestado "v5")
  production/
    factors.py              L_obs/L_pot, K_usado/K_pot, alpha           ← se reemplaza
    tfp.py                  PTF = residuo de Solow + tendencia Boosted-HP ← se reemplaza
    pib_potencial.py        Y* = A*·K*^α·(H·L*)^(1−α), brechas CD y BHP  ← se reemplaza
    excel_writer.py         Excel multi-hoja                             ← se adapta
  pipelines/
    run_pib_potencial.py    orquestador: carga data/processed/*.csv, alinea a QS, corre production/
    run_nairu_estimation.py llama a nairu.estimation.run()
    export_web_data.py      outputs/ → docs/data/*.csv + meta.json (GitHub Pages)
  quality_checks.py         run_pib_potencial_checks(df) (línea 944)
```

**Un tercer motor que NO es el objetivo de esta integración pero conviene no confundir:**
`src/pipelines/build_production_function_dataset.py` (`--prod-func`) produce
`outputs/production_function_quarterly.csv`, una **réplica del Excel de referencia
`FUNCION DE PRODUCCION.xlsx`** con α = 0.4 fijo y `A = PIB/(K^α·L^β)` **sin capital humano en el
término de trabajo**. Es un dataset de contraste con el material de clase, no un estimador de PIB
potencial. **Déjalo intacto**, pero añade una línea en su docstring y en el README aclarando que
no es la fuente del potencial publicado — hoy nada lo dice y es la confusión más fácil de cometer.

### 1.2 Qué trae el archivo nuevo

`pib_potencial_integrado_v3.py` contiene **tres motores** apilados en un solo archivo:

| Bloque | Líneas | Contenido |
|---|---|---|
| **MÓDULO 0** | 81–255 | Bry-Boschan trimestral (BBQ, Harding & Pagan 2002) + construcción de "variables de ciclo" (rampas-meseta estilo CBO/NBER) |
| **MÓDULO 1** | 219–1.782 | Motor NAIRU/NAICU: Kalman biestado + suavizador RTS, `SpecConfig`/`build_layout`, MLE con multiarranque, errores estándar por Hessiana numérica, y toda la maquinaria de calibración (`phi_sweep`, `sigma_sweep`, `naicu_grid`, `lag_sweep`, `finalize`) |
| **MÓDULO 2** | 1.898–3.538 | Motor PIB potencial "v2": TGP\* por OLS, horas con festivos, capital humano PWT, capital DANE por PCHIP, α por enfoque del ingreso, **PTF estructural estilo CBO por tramos**, PIB potencial y brechas, Excel y figuras |
| **INTEGRACIÓN v3** | 3.541–3.831 | Pega el NAIRU/NAICU estructural al motor PIB v2 + banda 95 % del potencial por delta method |

### 1.3 Decisiones ya tomadas por el usuario (**no re-litigar**)

1. **Reemplazo total.** El motor v3 se vuelve el único motor. `src/production/{factors,tfp,pib_potencial}.py`
   y `src/nairu/model_core.py` se reescriben con la lógica nueva. No se conserva el motor
   Cobb-Douglas/BHP como alternativa en `src/`.
2. **Dos fases con adaptador de datos.**
   - **Fase 1**: el motor PIB lee sus insumos del Boceto (`data/inputs/Boceto Estimación PIB
     Potencial.xlsx`), reproduciendo **exactamente** los números que el usuario ya validó.
   - **Fase 2**: se añade un lector alternativo desde `data/processed/*.csv` detrás de un flag,
     con un test de equivalencia entre ambas fuentes. La Fase 2 **no se empieza hasta que la
     Fase 1 pase su regresión-prueba**.
3. **Los insumos que faltaban ya están colocados** (ver §2).

---

## 2. Insumos (verificados, ya en disco)

| Archivo | Ruta en el repo | Estado |
|---|---|---|
| Boceto del profesor | `data/inputs/Boceto Estimación PIB Potencial.xlsx` | ya estaba |
| Datos NAIRU | `data/inputs/Data_NAIRU.xlsx` | ya estaba |
| Festivos efectivos | `data/inputs/festivos_efectivos_colombia_2001_2026.xlsx` | **colocado 2026-07-31** |
| Capital productivo DANE | `data/inputs/alt_capital/dane_stock_capital_productivo.csv` | **colocado 2026-07-31** |

Esquemas verificados contra los lectores del monolito:

- **Festivos** — hoja `Resumen trimestral`, datos desde la **fila 5**, columnas
  `[0]=Año`, `[1]=Trimestre` (`"T1".."T4"`), `[4]=Festivos efectivos`. Coincide exactamente con
  `leer_festivos_trimestrales()` (línea 2.332). Cobertura 2001T1–2026T4.
- **Capital DANE** — columnas `year`, `K_prod_mmp`; 35 filas, **1990–2024**. Coincide con
  `construir_capital_dane()` (línea 2.580). Ojo: 2025 y 2026 **se extrapolan** con el crecimiento
  log medio de los últimos 4 años observados — eso significa que los dos últimos años del stock de
  capital son proyección, no dato. Debe quedar dicho en el Excel y en `meta.json`.

Guarda el monolito original como **`legacy/pib_potencial_integrado_v3.py`** — congelado, jamás
importado desde `src/`. `legacy/` ya está excluido de `pytest` (`norecursedirs`), de `ruff`
(`extend-exclude`) y del paquete (`[tool.setuptools.packages.find] exclude`). Es tu referencia
para diffs, no código vivo.

---

## 3. ⚠️ FASE 0 — La regresión-prueba numérica (antes de tocar `src/`)

**Esto es lo más importante del encargo.** Sin esta línea base no hay manera de saber si la
modularización cambió los resultados, y el monolito *ya está validado* por el usuario.

1. Copia el monolito a un scratch (`/tmp/v3_baseline/`), parchea **solo** `PROJECT_DIR` y los
   paths derivados para que apunten a `data/inputs/` y a un `outputs/` temporal. **Nada más.**
2. Ejecútalo (`python pib_potencial_integrado_v3.py`).
3. Congela en `tests/fixtures/baseline_v3/` (versionado en git):
   - `nairu_estimates_v3.csv` completo,
   - `pib_potencial_integrado_v3_series.csv` completo,
   - `baseline_scalars.json` con: `alpha`, `T` (ISO), `nairu_last`, `naicu_last`, `nobs_nairu`,
     `nll_final`, `brecha_pot` del último trimestre, `ptf_star` del último trimestre,
     y los coeficientes de la regresión de PTF (`params` completos).

**Criterio de aceptación de TODA la migración:**
el pipeline modularizado, corrido desde `python -m src.main --nairu-estim --pib-potencial`,
reproduce cada serie de la línea base con `numpy.testing.assert_allclose(..., rtol=1e-10, atol=1e-12)`
y cada escalar con `rtol=1e-9`.

Si un número no cuadra, **el bug es tuyo, no del monolito**. No ajustes la tolerancia para que pase.

---

## 4. Arquitectura destino

```
src/
  config.py
    + PotencialConfig            @dataclass(frozen=True)  ← ver §6.6
    + POTENCIAL_CONFIG           instancia por defecto (equivalente a CONFIG_V2)
    + POTENCIAL_CONFIG_LEGACY    instancia equivalente a CONFIG_LEGACY (réplica del boceto)
    + NairuSpec                  @dataclass(frozen=True)  ← phi_n, phi_c, sigma_n, sigma_c,
                                 n_lags, covid_anchor_start/end, best_start_params, expected_nobs
    + BOCETO_XLSX, FESTIVOS_XLSX, DANE_CAPITAL_CSV, DATA_NAIRU_XLSX   (derivados de INPUTS_DIR)

  production/
    dates.py       NUEVO — to_qs(), to_qlabel(), quarter_label(), es la ÚNICA frontera donde
                   se convierte entre convenciones de fecha (§6.1)
    bbq.py         NUEVO — bbq_turning_points(), detectar_picos(), construir_variables_ciclo()
                   (MÓDULO 0 del monolito, funciones puras, sin abrir Excel)
    sources.py     NUEVO — PotencialInputs (dataclass de salida) + BocetoSource (fase 1) +
                   ProcessedSource (fase 2). Ambas devuelven el MISMO PotencialInputs.
    labor.py       REEMPLAZA factors.factor_trabajo — estimar_tgp_star(), calcular_factor_trabajo(),
                   extrapolar_hc_anual(), calcular_capital_humano(), jornada_legal()
    capital.py     REEMPLAZA factors.factor_capital — capital_dane_pchip(), capital_pim(),
                   delta_optima_pim(), calcular_capital_usado()
    alpha.py       REEMPLAZA factors.alpha_dinamico — calcular_alpha(metodo="cbo"|"full")
    tfp.py         REESCRITO — ptf_solow(), estimar_ptf_tendencia_cbo(), tendencia_hp()
    pib_potencial.py REESCRITO — calcular_pib_potencial(), bandas 95 %, QUARTERLY_OUTPUT_COLS
    excel_writer.py  adaptado a las hojas nuevas
    plots.py       NUEVO — graficar_niveles(), graficar_brechas(), graficar_paneles(),
                   graficar_ptf_vs_dane(), graficar_nairu_naicu()

  nairu/
    model_core.py  REESCRITO con el MÓDULO 1 del monolito, **conservando el contrato de 4
                   funciones de estimation.py sin tocar una línea de estimation.py** (§5.1)

  pipelines/
    run_pib_potencial.py  REESCRITO — orquesta sources → labor/capital/alpha → tfp → pib_potencial
    export_web_data.py    mapas de columnas actualizados (§5.3)

  quality_checks.py       run_pib_potencial_checks() reescrito con los invariantes nuevos (§7.5)

scripts/
  nairu_calibration.py  NUEVO — phi_sweep, sigma_sweep, naicu_grid, lag_sweep, main_robust,
                        finalize, SPECS, write_comparison, _lr_tests. CLI propio con argparse.
                        FUERA de src/: es investigación, no producción (§6.5).

legacy/
  pib_potencial_integrado_v3.py   monolito congelado

tests/fixtures/baseline_v3/       línea base numérica (§3)
```

**Regla de oro de la arquitectura del repo:** los módulos de `src/production/` y `src/nairu/` son
**funciones puras sin I/O** (reciben DataFrame, devuelven DataFrame). Todo el I/O vive en
`src/pipelines/` y en `src/production/sources.py`. El monolito viola esto en todas partes
(`leer_festivos_trimestrales` abre Excel dentro del cálculo, `_pib_pot_con_nairu` escribe CSVs
temporales). Al modularizar, **corrige la violación**, no la propagues.

---

## 5. Contratos que NO se pueden romper

### 5.1 Contrato `model_core.py` ↔ `estimation.py`

`src/nairu/estimation.py` (líneas 62–103, 164–193) carga `model_core.py` por `importlib` y llama
**exactamente** esto:

```python
load_and_prepare_data(data_path: Path) -> pd.DataFrame
build_model_data(data: pd.DataFrame) -> ModelData
estimate_parameters(model_data: ModelData) -> FitResult          # ⚠️ UN solo argumento
build_outputs(data, model_data, fit, output_dir: Path) -> tuple[Path, str]
```

**Desajustes del monolito:**

| Función | Monolito | Requerido |
|---|---|---|
| `load_and_prepare_data(path)` | línea 282 ✅ | compatible |
| `build_model_data(df)` | línea 353 ✅ | compatible |
| `estimate_parameters(cfg, data)` | línea 739 ❌ | **dos** argumentos |
| `build_outputs(...)` | **no existe** ❌ | falta por completo |

**Instrucción:** no cambies `estimation.py`. En `model_core.py`:

- Renombra el `estimate_parameters(cfg, data)` interno a `_estimate_with_spec(cfg, data)`.
- Define el `estimate_parameters(model_data)` público, que construye la spec FINAL internamente
  (histéresis + `distributed_lag` con `n_lags=3`, `phi_n=0.08`, `phi_c=0.02`, `σ_nairu=0.05`,
  `σ_naicu=0.20`, dummy COVID en el ancla), arranca de `BEST_START_PARAMS` y hace **una sola**
  optimización local L-BFGS-B (`maxiter=1500`, `ftol=1e-9`, `gtol=1e-6`) — igual que
  `estimar_nairu_naicu_estructural_v3()` (línea 3.597).
- Implementa `build_outputs(data, model_data, fit, output_dir)` que escriba en `output_dir` los
  **siete** archivos que `estimation._V5_TO_FINAL` espera, con **esos nombres exactos**:

  ```
  nairu_estimates_v5.csv        nairu_summary_v5.txt
  nairu_mle_coefficients_v5.csv nairu_mle_covariance_v5.csv
  nairu_mle_diagnostics_v5.txt  nairu_naicu_panel_v5.png
  nairu_naicu_panel_v5.svg
  ```

  y devuelva `(path_del_csv, texto_del_summary)`. Sí, los nombres dicen "v5" — es el contrato
  vigente de `estimation.py` y renombrarlo solo añade riesgo. Documenta en el docstring que
  el sufijo es histórico.

- **Actualiza el docstring de `src/nairu/__init__.py`**, que hoy dice "AGREGAR ESTE ARCHIVO" y
  describe `build_model_data -> dict` (es `ModelData`).

### 5.2 ⚠️ Esquema del CSV de NAIRU — rompe la web en silencio

`outputs/nairu/nairu_colombia.csv` tiene hoy **29 columnas**. Sus consumidores:

- `export_web_data.NAIRU_EXPORT_COLS` usa `nairu_ci_lower_90`, `nairu_ci_upper_90`,
  `naicu_ci_lower_90`, `naicu_ci_upper_90`, `inflation_gap`, `unemployment_gap`, `icu_gap`.
- `run_pib_potencial._build_monthly()` usa `nairu_ci_lower_90`, `nairu_ci_upper_90`.
- `docs/index.html` grafica esas bandas.

`_escribir_nairu_csv_v3()` (línea 3.620) escribe **9 columnas**, solo con bandas del **95 %**.

Y `export_web_data._read_and_rename()` hace:

```python
cols_present = {k: v for k, v in col_map.items() if k in df.columns}
```

es decir, **descarta lo que falta sin avisar**. Resultado: el sitio pierde las bandas de confianza
y `meta.json` pierde campos, sin una sola excepción ni línea de log.

**Instrucción:** el `build_outputs` nuevo debe emitir el **superset** de columnas. Mínimo obligatorio:

```
Date, unemployment_current, icu_current, inflation_gap,
nairu_estimate, nairu_se, nairu_ci_lower_90, nairu_ci_upper_90,
                          nairu_ci_lower_95, nairu_ci_upper_95,
naicu_estimate, naicu_se, naicu_ci_lower_90, naicu_ci_upper_90,
                          naicu_ci_lower_95, naicu_ci_upper_95,
unemployment_gap, icu_gap, covid_dummy
```

Las bandas al 90 % se obtienen del mismo error estándar del suavizador con `Z_90 = 1.6448536269514722`
(ya definido en el monolito junto a `Z_95`). `unemployment_gap = unemployment − nairu_smooth`,
`icu_gap = icu − naicu_smooth`. `covid_dummy` = 1 dentro de la ventana del ancla, 0 fuera.

Añade `REQUIRED_NAIRU_COLS` como constante en `src/config.py` y un test de contrato
(`test_nairu_csv_schema`) que falle si alguna se pierde. Haz que `_read_and_rename` **loguee un
warning** por cada columna esperada que no encuentre.

### 5.3 ⚠️ Vocabulario de columnas del PIB — índices vs. niveles

Los dos motores hablan idiomas distintos, y las unidades **no coinciden**:

| Concepto | Motor viejo (`src/production`) | Motor v3 | Unidad viejo | Unidad v3 |
|---|---|---|---|---|
| PIB observado | `PIB` | `idx_pib` | miles MM COP 2017 | **índice 2005Q1 = 100** |
| PIB potencial | `PIB_pot` | `pib_pot` | miles MM COP 2017 | **índice** |
| Tendencia estadística | `PIB_tend_BHP` | `idx_trend` (HP) | nivel | índice |
| Brecha principal | `Brecha_CD` | `brecha_pot` | **%** | **fracción** |
| Brecha estadística | `Brecha_BHP` | `brecha_hp` | **%** | **fracción** |
| PTF observada | `A_obs` | `ptf` | — | — |
| PTF tendencial | `A_pot` (Boosted-HP) | `ptf_star` (**CBO por tramos**) | — | — |
| Capital usado | `K_usado` | `idx_K` | nivel | índice |
| Capital potencial | `K_pot` | `idx_K_star` | nivel | índice |
| Trabajo observado | `L_obs` (personas) | `idx_LH` (**horas × capital humano**) | miles pers. | índice |
| Trabajo potencial | `L_pot` | `idx_LH_star` | miles pers. | índice |
| α | `alpha` | `alpha` (escalar) + `alpha_t` (serie) | fracción | fracción |

**Dos trampas concretas:**

1. `export_web_data` (línea 144) construye su propio índice 2005 = 100 dividiendo por
   `pib.loc[pib["anio"]==2005, "pib_obs"].mean()`. Si le entregas un índice, lo re-indexa: no
   revienta, pero el resultado ya no es lo que el tablero documenta.
2. `meta.json` publicaría `latest_brecha_cd = 0.026` en vez de `2.6`. Es una diferencia de dos
   órdenes de magnitud que **nadie ve** hasta que alguien lee la web.

**Instrucción:** en `pib_potencial.py`, el DataFrame de salida canónico debe llevar **ambas**
representaciones, y `QUARTERLY_OUTPUT_COLS` debe conservar los nombres del vocabulario público
(los que consumen `excel_writer`, `export_web_data` y `docs/index.html`):

- `PIB`, `PIB_pot`, `PIB_tend_HP` **en nivel** — reescalados multiplicando el índice por
  `output_ann(2005Q1) / 100`, que es el `base_val` que `calcular_producto()` ya devuelve.
  Documenta que el nivel es **PIB anualizado** (suma móvil de 4 trimestres), no trimestral.
- `Brecha_CD`, `Brecha_HP` **en puntos porcentuales** (× 100).
- Las columnas nativas del motor (`idx_*`, `ptf`, `ptf_star`, `brecha_pot`, `alpha_t`, …) se
  conservan también, para el Excel y para auditoría.

Actualiza `PIB_EXPORT_COLS` en `export_web_data`: `A_pot → ptf_pot` sigue sirviendo, pero
**`PIB_tend_BHP` ya no existe**: la tendencia estadística del motor v3 es HP simple (λ = 1600,
`calcular_tendencia_hp`, línea 2.509), **no** Boosted-HP.

⚠️ Ese rename cruza tres archivos y si haces uno solo el tablero queda mudo. Cadena completa:

```
pib_potencial.py:   PIB_tend_BHP        → PIB_tend_HP
                    Brecha_BHP          → Brecha_HP
export_web_data.py: "pib_bhp"           → "pib_tend_hp"      (PIB_EXPORT_COLS)
                    "brecha_bhp"        → "brecha_hp"
                    meta["latest_brecha_bhp"] → "latest_brecha_hp"
docs/index.html:    línea 307  texto "Boosted Hodrick-Prescott (BHP)"  → "Hodrick-Prescott (HP)"
                    línea 330  texto "tendencia BHP"                   → "tendencia HP"
                    línea 631  toNum(rows,'brecha_bhp')                → 'brecha_hp'
                    línea 647  name: 'Brecha BHP'                      → 'Brecha HP'
                    líneas 695/700 toNum(rows,'pib_bhp_idx'|'pib_bhp') → 'pib_tend_hp_idx'|'pib_tend_hp'
                    línea 707  name: 'Tendencia BHP'                   → 'Tendencia HP'
```

**No toques la línea 316 ni la 668** de `index.html`: ese `bhp` pertenece al módulo **VIOG**
(uno de sus 5 filtros) y sí es Boosted-HP legítimo. Dos series distintas con el mismo apodo en el
mismo tablero — por eso el rename hay que hacerlo mirando, no con `sed`.

---

## 6. ⚠️ Las siete trampas silenciosas

### 6.1 Etiquetado de trimestre — el bug #1

- **Repo:** `resample("QS")` → `2005-01-01, 2005-04-01, 2005-07-01, 2005-10-01` (primer día del
  **primer** mes).
- **Monolito:** `quarter_label()` (línea 2.093) → `2005-03-01, 2005-06-01, 2005-09-01, 2005-12-01`
  (primer día del mes de **cierre**).

Un `merge(on="date")` entre las dos convenciones produce **100 % NaN sin lanzar ninguna excepción**.
`run_pib_potencial` haría `join`/`merge` alegremente y escribiría un Excel entero de vacíos.

**Instrucción:** convención canónica **QS** en todo `src/`. La conversión ocurre **solo** en
`production/dates.py` y **solo** en la frontera de `sources.py`. Todas las constantes con etiqueta
de trimestre se trasladan:

| Constante | Monolito | Canónico QS |
|---|---|---|
| `BASE_QUARTER` | `2005-03-01` | `2005-01-01` |
| `KY_ANCHOR_QUARTER` | `2006-06-01` | `2006-04-01` |
| `ALPHA_WINDOW_START` | `2016-03-01` | `2016-01-01` |
| `KNOTS` | `[2008-03-01]` | `[2008-01-01]` |
| `PANDEMIC_DUMMIES` | `2020-06-01, 2020-09-01, 2020-12-01, 2021-03-01` | `2020-04-01, 2020-07-01, 2020-10-01, 2021-01-01` |

**Caso especial `JORNADA_SCHEDULE`** (línea 1.929): esas **no** son etiquetas de trimestre, son
fechas de entrada en vigor de la Ley 2101/2021 (2023-09-01 → 47 h, 2024-09-01 → 46 h, 2025-09-01 →
44 h, 2026-09-01 → 42 h). `jornada_legal()` compara `etiqueta_trimestre >= umbral`. Con etiqueta
de cierre, 2023Q3 = `2023-09-01` ⇒ **ya aplica 47 h**. Con QS, 2023Q3 = `2023-07-01` ⇒ **no
aplicaría** hasta 2023Q4. Eso **cambia las horas trabajadas y por tanto el PIB potencial**.
Para preservar el resultado original, convierte los umbrales a la etiqueta QS del trimestre en que
entran en vigor: `2023-07-01, 2024-07-01, 2025-07-01, 2026-07-01`. Deja un comentario explicando
por qué la fecha del código no es la fecha de la ley, y un test dedicado
(`test_jornada_por_trimestre`) que fije la tabla trimestre→horas esperada.

Test obligatorio: `test_dates_roundtrip` — `to_qs(to_qlabel(d)) == d` para todo trimestre
2000Q1–2030Q4, y un test que verifique que ningún `merge` del orquestador produce columnas
100 % NaN.

### 6.2 `main()` está definido dos veces

Línea 1.775 y línea 3.765. La segunda gana. Todo esto queda **inalcanzable** desde el entry point:
`main_robust`, `lag_sweep`, `phi_sweep`, `sigma_sweep`, `finalize`, `naicu_grid`, `SPECS`,
`write_comparison`, `_lr_tests`, `_write_plots`, `_robust_lag_table`.

**No lo borres** — es el proceso de elección de parámetros y es la evidencia metodológica del
informe. Muévelo íntegro a `scripts/nairu_calibration.py` con un CLI propio:

```
python -m scripts.nairu_calibration phi-sweep   [--smoke]
python -m scripts.nairu_calibration sigma-sweep [--smoke]
python -m scripts.nairu_calibration naicu-grid  [--smoke]
python -m scripts.nairu_calibration finalize --phi 0.08 --sigma-nairu 0.05 --sigma-naicu 0.20
```

Puede importar de `src.nairu.model_core`; **`src/` no puede importar de `scripts/`**. Escribe sus
salidas en `outputs/specs/` (ya cubierto: `.gitignore` línea 24 ignora `outputs/` entero — ojo,
eso significa que los resultados de los barridos **no quedan versionados**; si el informe los cita,
copia las tablas resumen a `docs/`).

### 6.3 Dummy COVID: dos mecanismos con el mismo nombre

- `model_core.py` actual: `COVID_DUMMY_START = "2020-03-01"`, `END = "2021-06-01"` — dummy en la
  ecuación de **medición**.
- v3 (línea 3.565): `2020-04-01` → `2021-06-01` — actúa **solo sobre el ancla de histéresis**
  (`unemployment_lag1`), interpolando linealmente; la ecuación de medición **sigue viendo el
  desempleo observado real**, de modo que la brecha COVID es real y no se absorbe.

Son cosas distintas con el mismo nombre. **Renombra el de v3 a `COVID_ANCHOR_START/END`** y
documenta la diferencia en el docstring.

**Fragilidad estructural que debes blindar:** `build_model_data` (línea 366) hace
`unemployment_current=u_lags[:, 0].copy()` y `unemployment_lag1=u_lags[:, 1].copy()`. Son
**copias**, por eso `_aplicar_dummy_covid()` puede reasignar `data.unemployment_lag1` sin tocar
`data.unemployment_lags`, que es lo que alimenta la holgura de medición. Si alguien quita ese
`.copy()` "optimizando", el dummy pasa a contaminar la medición y **la brecha COVID desaparece
sin ningún error**. Test obligatorio:

```python
def test_dummy_covid_no_toca_la_medicion():
    """El ancla de histéresis se interpola; la holgura de medición NO."""
    data = build_model_data(df)
    antes = data.unemployment_lags[:, 1].copy()
    aplicar_covid_anchor(data)
    assert not np.allclose(data.unemployment_lag1, antes)   # el ancla SÍ cambió
    npt.assert_array_equal(data.unemployment_lags[:, 1], antes)  # la medición NO
```

### 6.4 `BEST_START_PARAMS` es un arranque tibio, no un óptimo eterno

Los 18 parámetros de la línea 3.577 son el óptimo de una búsqueda de 60 arranques **para esa spec
y esa muestra**. Si `Data_NAIRU.xlsx` crece un mes, siguen siendo un buen punto inicial, pero ya
no son el óptimo.

**Instrucción:** en `NairuSpec` guarda junto a los parámetros `expected_nobs` y `expected_nll`
(los de la línea base de §3). En `estimate_parameters`:

1. Loguea `nll` inicial y final y `opt.message`.
2. Si `opt.success is False`, o `data.n_obs != spec.expected_nobs`, o
   `abs(nll_final − spec.expected_nll) > 1e-3` → **loguea un WARNING explícito** y cae a
   `robust_estimate(n_starts=60, seed=20260726)`.
3. Chequeos de sanidad económica antes de devolver: `MIN_NAIRU_LEVEL ≤ nairu ≤ MAX_NAIRU_LEVEL`
   (3–20), `MIN_NAICU_LEVEL ≤ naicu ≤ MAX_NAICU_LEVEL` (55–90); `nairu_last` fuera de [8, 11] →
   warning con el valor.

Nunca dejes que el pipeline publique un CSV de una optimización que no convergió.

### 6.5 Config global mutable

`CONFIG_V2` (línea 2.061) es un `@dataclass` **mutable** a nivel de módulo, y lo mutan desde fuera:
`main()` (líneas 3.788, 3.791) y `_pib_pot_con_nairu()` (líneas 3.674–3.677, patrón
"mutar → correr → restaurar"). Además `ejecutar_pipeline` hace `config = replace(config, picos_bbq=…)`
(línea 2.873), que crea una **copia local**: los picos detectados ahí no se ven fuera, así que se
vuelven a detectar en la siguiente llamada.

**Instrucción:** `PotencialConfig` como `@dataclass(frozen=True)`, igual que `VIOGConfig` en
`src/config.py`. `picos_bbq: tuple[pd.Timestamp, ...]` (tupla, no lista, para que sea hashable) y
la fuente de NAIRU se pasa como **argumento de función** (`nairu: pd.DataFrame | Path`), nunca
como campo mutado del config. Test: `test_potencial_config_is_frozen` que verifique que asignar
un campo lanza `FrozenInstanceError`.

### 6.6 Lecturas repetidas y temporales huérfanos

- `detectar_picos_bbq()` abre el Boceto con `openpyxl` cada vez. Se llama en `main()` (3.784) y de
  nuevo dentro de `ejecutar_pipeline` cuando `picos_bbq is None` (2.873). Con las 4 corridas extra
  de `_ci_pib_potencial` son **hasta 5 aperturas** del mismo workbook.
- `_pib_pot_con_nairu()` (línea 3.668) escribe `outputs/_ci_{tag}.csv` (línea 3.671) y hace `unlink()`
  **después** de `ejecutar_pipeline`. Si el pipeline lanza, los temporales quedan huérfanos en el
  directorio de salidas del proyecto.

**Instrucción:** `functools.lru_cache` sobre los lectores del Boceto (clave: `path` + `st_mtime`);
los picos BBQ se detectan **una vez** en el orquestador y se inyectan en el config. La banda de
confianza pasa el DataFrame de NAIRU **en memoria** — `sources.py` debe aceptar
`nairu: pd.DataFrame | Path`. Cero archivos temporales.

Aprovecha para subir al top del módulo los imports que están dentro de funciones:
`import openpyxl` (2.339, ya está en el top), `from scipy.interpolate import PchipInterpolator`
(2.589), `from scipy.optimize import minimize_scalar` (2.622). `ruff` con las reglas `E`, `I` ya
configuradas los marcaría.

### 6.7 Muestra del NAIRU vs. muestra del PIB

`load_and_prepare_data` hace `dropna()` sobre todas las columnas, incluida `unemployment_ma24`
(rolling de 24 con `min_periods=24` sobre la serie ya rezagada) → **se pierden ~25 meses al
inicio**. El PIB potencial arranca en 2005Q1 y necesita NAIRU/NAICU desde **2004Q2** (las sumas
móviles de 4 trimestres y el índice base 2005Q1).

**Instrucción:** el orquestador debe verificar explícitamente que la primera fecha del CSV de
NAIRU sea `≤ 2004-04-01` y fallar con un mensaje claro si no
(`"NAIRU disponible desde {x}; el PIB potencial requiere desde 2004-04-01"`), en vez de producir
NaN silenciosos en 2005.

---

## 7. Plan por fases y criterios de aceptación

### Fase 0 — Línea base

Ver §3. **Entregable:** `tests/fixtures/baseline_v3/`. **Sin esto no sigas.**

### Fase 1 — Extraer el motor NAIRU/NAICU

- `src/nairu/model_core.py` reescrito (MÓDULO 1 + integración v3), contrato §5.1, CSV §5.2.
- `scripts/nairu_calibration.py` con la maquinaria de calibración.
- `python -m src.main --nairu-estim` produce `outputs/nairu/nairu_colombia.csv`.

**Aceptación:** `nairu_estimate` y `naicu_estimate` idénticos a la línea base
(`rtol=1e-10`); `nairu_last ≈ 9.5`; absorción COVID del NAIRU ≈ 0.22; las 19 columnas de §5.2
presentes.

### Fase 2 — Extraer BBQ y el adaptador de insumos

- `production/dates.py`, `production/bbq.py`, `production/sources.py` con `BocetoSource`.
- `PotencialInputs` congela el contrato de datos: `mensual` (date, pet, fl, ocup, hc_anual, nairu,
  naicu, icu), `trimestral` (date, V_pib, X_vapg, BE_inv, BQ_ra, BR_tms, BS_ebe, BT_im),
  `festivos` (Series), `capital_dane` (DataFrame year/K_prod_mmp), `picos_bbq` (tuple).

**Aceptación:** `bbq_turning_points` reproduce los mismos picos que el monolito sobre la misma
serie; todas las fechas en QS; `test_dates_roundtrip` verde.

### Fase 3 — Extraer el motor PIB potencial

- `labor.py`, `capital.py`, `alpha.py`, `tfp.py`, `pib_potencial.py`, `plots.py`, `excel_writer.py`.
- `run_pib_potencial.py` reescrito.
- `PotencialConfig` frozen en `config.py`.

**Aceptación:** `python -m src.main --pib-potencial` reproduce
`pib_potencial_integrado_v3_series.csv` de la línea base en **todas** sus columnas
(`rtol=1e-10`); `alpha` y `T` idénticos; los coeficientes de la regresión de PTF idénticos a
`rtol=1e-9`.

### Fase 4 — Modo legado (regresión-prueba contra el boceto)

El monolito trae `CONFIG_LEGACY` + `validar_contra_referencia()` (línea 3.320), que reproduce el
boceto del profesor a `1e-12`. Consérvalo como
`POTENCIAL_CONFIG_LEGACY` + `tests/test_legacy_boceto.py`, marcado
`@pytest.mark.skipif(not REFERENCE_CSV.exists())` — `outputs/boceto_reference.csv` no está en el
repo hoy. Si el usuario lo aporta, el test se activa solo. Es la prueba de que la implementación
sigue siendo la del profesor.

### Fase 5 — Salidas, web y limpieza

- `quality_checks.run_pib_potencial_checks` reescrito (§7.5).
- `export_web_data` con los mapas nuevos + warning por columna faltante.
- `docs/index.html` ajustado (etiquetas de brecha, "BHP"→"HP" solo en la sección PIB).
- `README.md` y `CHANGELOG.md` actualizados. `PLAN_PIB_POTENCIAL.md` marcado como histórico.
- El docstring de uso de `src/main.py` refleja la cadena real.

**Cadena de ejecución documentada:**

```
python -m src.main --nairu-dataset     # construye data/inputs/Data_NAIRU.xlsx
python -m src.main --nairu-estim       # NAIRU/NAICU estructural  → outputs/nairu/
python -m src.main --pib-potencial     # PIB potencial + PTF*      → outputs/pib_potencial/
python -m src.main --export-web        # docs/data/*.csv + meta.json
```

### Fase 6 — Adaptador a `data/processed/` (solo si Fase 3 está verde)

`ProcessedSource` en `sources.py`, activado por
`python -m src.main --pib-potencial --fuente-potencial processed` (default: `boceto`).

**Series que hoy NO existen en `data/processed/` y hay que resolver explícitamente** — no las
inventes ni las aproximes en silencio; si falta una, `ProcessedSource` debe **fallar con un
mensaje que nombre la serie y el pipeline que la produciría**:

| Serie del Boceto | Columna | Estado en `data/processed/` |
|---|---|---|
| Valor agregado petróleo-gas | `X_vapg` | **no existe** (solo se usa si `output_measure="ex_oil"` o en el PIM) |
| Impuestos menos subsidios | `BR_tms` | **no existe** (solo se usa si `alpha_method="full"`) |
| Capital productivo DANE | — | `data/inputs/alt_capital/` (input, no procesado) |
| Festivos efectivos | — | `data/inputs/` (input, no procesado) |
| PIB, FBKF, RA, EBE, IM | `V_pib`, `BE_inv`, `BQ_ra`, `BS_ebe`, `BT_im` | ✅ existen |
| PET, FL, ocupados | `pet`, `fl`, `ocup` | ✅ `dane_labor_colombia.csv` |
| Capital humano PWT | `hc_anual` | ✅ `pwt_colombia.csv` (`human_capital`) |

**Aceptación:** `test_sources_equivalence` — para el subconjunto de series presentes en ambas
fuentes, `BocetoSource` y `ProcessedSource` coinciden dentro de `rtol=1e-3` (el DANE revisa sus
series; no exijas igualdad exacta), y las discrepancias mayores se reportan en un log, no se
esconden.

### 7.5 Invariantes nuevos de `quality_checks`

El check actual (línea 944) valida el vocabulario viejo (`PIB_pot`, `Brecha_CD`, `A_obs`, `A_pot`).
Reescríbelo sobre el vocabulario nuevo:

- columnas mínimas presentes;
- `pib_pot > 0` y `ptf_star > 0` en toda la muestra 2005Q1–T;
- `|Brecha_CD| < 50 pp` (COVID 2020Q2 es legítimamente enorme), `|Brecha_HP| < 15 pp`;
- media de `Brecha_CD` sobre la muestra completa dentro de ±1.5 pp de cero — **si la brecha
  estructural no está centrada, el potencial está sesgado** y es el síntoma más útil de todos;
- `alpha ∈ (0.15, 0.85)`;
- `ptf_star` suave: `max |Δ log ptf_star| < 0.02` por trimestre;
- sin NaN internos entre 2005Q1 y T en las columnas publicadas (NaN al inicio por sumas móviles
  sí es esperado — documenta el patrón de burn-in, como ya se hace en el módulo VIOG);
- `T` = último trimestre con **todos** los insumos trimestrales completos.

---

## 8. Suite de tests mínima

Todos con `pytest`, en `tests/`, siguiendo el estilo existente (`test_viog.py` es el mejor modelo
del repo).

| Archivo | Cubre |
|---|---|
| `test_dates.py` | roundtrip QS↔etiqueta de cierre; `jornada_legal` por trimestre; ningún merge produce columnas 100 % NaN |
| `test_bbq.py` | picos/valles alternados, fase mínima 2, ciclo mínimo 5, censura de extremos; rampas-meseta: 0 antes del pico, +0.25/trim, meseta constante tras el pico siguiente, última rampa sin meseta |
| `test_nairu_model_core.py` | contrato de 4 funciones con las firmas exactas; los 7 archivos de salida; esquema de 19 columnas; dummy COVID no toca la medición (§6.3); fallback a multiarranque si no converge |
| `test_production_labor.py` | TGP\* con cíclicos en cero = solo tendencia; horas con y sin festivos; extrapolación de capital humano anclada en el último año PWT |
| `test_production_capital.py` | **el promedio de los 4 trimestres PCHIP reproduce el dato anual del DANE** (propiedad definitoria del interpolador); PIM: recursión hacia adelante y hacia atrás consistentes en el ancla; extrapolación 2025-2026 marcada como proyección |
| `test_production_alpha.py` | `cbo` = EBE/(RA+EBE); `full` = EBE/(RA+TmS+EBE+IM); ventana de promedio respetada |
| `test_production_tfp.py` | PTF = residuo de Solow con α; PTF\* evaluada con cíclicos **y dummies en cero**; base `cumulative` vs `plateau` |
| `test_production_pib_potencial.py` | identidad Y\* = PTF\*·K\*^α·(LH\*)^(1−α); brecha = PIB/PIB\* − 1; conversión índice↔nivel y fracción↔pp |
| `test_potencial_config.py` | `frozen=True`; `picos_bbq` es tupla; defaults equivalentes a `CONFIG_V2` |
| `test_baseline_v3.py` | **la regresión-prueba de §3** — el test más importante del conjunto |
| `test_legacy_boceto.py` | modo legado vs `boceto_reference.csv` (skip si falta) |
| `test_export_web_meta.py` | actualizado: `meta.json` sin `NaN` literal, brechas en pp, columnas presentes |
| `test_sources_equivalence.py` | Fase 6 |

Ejecuta `pytest -q` y `ruff check src/ tests/ scripts/` al cierre de **cada** fase, no solo al final.

---

## 9. Por qué el modelo es así (no lo "simplifiques")

Contexto teórico, para que las decisiones sobrevivan a la refactorización y para que puedas
redactar la sección metodológica del informe:

**Histéresis disciplinada (φ).** La ecuación de estado del NAIRU es
`NAIRU_t = (1−φ)·NAIRU_{t−1} + φ·u_{t−1} + ε_t`. Con `φ = 0` se recupera la **tasa natural
neoclásica** (Friedman-Phelps): el NAIRU es un proceso exógeno y toda desviación del desempleo es
brecha. Con `φ → 1` se obtiene **histéresis completa** (Blanchard-Summers 1986): el NAIRU persigue
al desempleo observado y la brecha se desvanece por construcción — el desempleo de hoy define la
"normalidad" de mañana. `φ_n = 0.08` mensual es un punto intermedio deliberado: media vida
`ln(0.5)/ln(1−0.08) ≈ 8.3 meses`. Es rápido — lo bastante para seguir la caída estructural del
desempleo colombiano post-2022, que un random walk puro no capta.

**Por qué hace falta el dummy COVID.** Ese `φ` alto es justo lo que haría que el NAIRU se tragara
el pico de ~22 % de 2020 y lo declarara "estructural". La solución no es bajar `φ` (se pierde la
tendencia reciente): es neutralizar el pico **solo en el ancla de histéresis**, dejando intacta la
ecuación de medición. La brecha COVID sigue siendo real y observada. Con esto la absorción del
NAIRU cae a ≈ 0.22 (≈ 78 % del choque queda como brecha) y la brecha de producto de fin de muestra
baja de ≈ +5.8 % a ≈ +2.6 %. Es una restricción de identificación con contenido económico, no un
truco de ajuste.

**NAICU y el capital.** El análogo del NAIRU para el capital: la utilización de capacidad
instalada que no acelera la inflación. `K* = K · NAICU/100` en vez de `K · UCI/100`. `φ_c = 0.02`
y `σ_naicu = 0.20` (vs `σ_nairu = 0.05`) porque la UCI de la ANDI es notoriamente más volátil que
el desempleo. Incluir la brecha de utilización del capital `(ICU − NAICU)` en la regresión de
TGP\* es lo que conecta ambos lados de la función de producción.

**PTF estructural vs. filtro estadístico — el corazón del cambio.** El motor viejo obtenía la PTF
tendencial con Boosted-HP: un filtro **puramente estadístico**, que no sabe qué es ciclo y qué es
tendencia; suaviza y ya. El motor nuevo estima

```
ln(PTF_t) = b₀ + b₁·τ_t + Σⱼ γⱼ·Sⱼ(τ_t) + φ₀·gap_t + φ₁·gap_{t−1} + Σₘ δₘ·Dₘ,t + ε_t
```

y evalúa `PTF*_t = exp(b₀ + b₁·τ_t + Σⱼ γⱼ·Sⱼ(τ_t))` — es decir, **con los términos cíclicos y las
dummies de pandemia en cero**. Eso purga explícitamente la prociclicidad de la productividad
medida (atesoramiento de trabajo y capital: en una recesión las empresas retienen factores que no
usan plenamente, y el residuo de Solow cae sin que la tecnología haya empeorado). El resultado es
una PTF **condicionada a la brecha de desempleo**, no simplemente suavizada. Ese es el argumento
para reemplazar el BHP, y va en el informe.

**Los tramos son endógenos.** Los nudos `Sⱼ` no son fechas escogidas a mano: son los **picos del
ciclo colombiano fechados por Bry-Boschan** (Harding & Pagan 2002) sobre el propio PIB. La
tendencia de la PTF puede quebrar su pendiente en cada pico, pero solo ahí. La forma "meseta"
(rampa que crece 0.25/trimestre durante el ciclo y se congela en el pico siguiente) es la
convención CBO/NBER.

**Advertencia sobre la banda de confianza.** `_ci_pib_potencial` propaga **solo** la incertidumbre
del suavizador sobre NAIRU/NAICU, por delta method numérico, sumando en cuadratura e ignorando la
correlación temporal y la incertidumbre de la propia tendencia de PTF. Es **conservadora hacia
abajo**: la banda verdadera es más ancha. Que quede escrito en el docstring, en la figura y en el
informe. Y recuerda la crítica de Orphanides (2001, 2003) y Orphanides & van Norden (2002): estas
brechas se revisan con cada dato nuevo, así que la banda no mide el error de la estimación en
tiempo real.

---

## 10. Entregable y checklist

Al terminar, entrega un reporte con:

1. **Tabla de trazabilidad**: cada función del monolito → su nuevo hogar (`archivo::función`), o
   "eliminada" con la razón.
2. **Resultado de la regresión-prueba** de §3: diff máximo por serie, en notación científica.
3. **Diff de números publicados**: `nairu_last`, `naicu_last`, `alpha`, `T`, brecha del último
   trimestre, PTF\* del último trimestre — antes (motor Cobb-Douglas/BHP) vs. después (v3). El
   usuario necesita saber **cuánto se movió la brecha** al cambiar de metodología, con una frase
   de explicación económica.
4. **`pytest -q` completo** y `ruff check` limpios.
5. Lista de cualquier decisión que hayas tenido que tomar por tu cuenta, marcada como tal.

**Checklist:**

- [ ] Línea base congelada en `tests/fixtures/baseline_v3/` **antes** de tocar `src/`
- [ ] Convención QS única; conversión solo en `production/dates.py`; `JORNADA_SCHEDULE` trasladado
- [ ] Contrato de 4 funciones de `estimation.py` intacto; `estimation.py` sin modificar
- [ ] CSV de NAIRU con las 19 columnas; bandas al 90 % **y** al 95 %
- [ ] Vocabulario público del PIB en niveles y en pp; `docs/index.html` coherente
- [ ] `PotencialConfig` frozen; cero mutación global; cero temporales en `outputs/`
- [ ] Calibración movida a `scripts/`, no importable desde `src/`
- [ ] Test del dummy COVID sobre el ancla vs. la medición
- [ ] Fallback a multiarranque si la optimización de warm start no converge
- [ ] Suite de tests de §8 completa y verde
- [ ] README, CHANGELOG y docstring de `src/main.py` actualizados

---

## 11. Qué NO hacer

- **No** cambies la especificación econométrica: `φ_n = 0.08`, `φ_c = 0.02`, `σ_nairu = 0.05`,
  `σ_naicu = 0.20`, `n_lags = 3`, histéresis + rezagos distribuidos simétricos, suavizador RTS.
  Están elegidos por barridos documentados en `scripts/nairu_calibration.py`.
- **No** relajes las tolerancias de la regresión-prueba para que un test pase.
- **No** borres la maquinaria de calibración ni el modo legado: son la evidencia metodológica.
- **No** dejes que `export_web_data` siga descartando columnas en silencio.
- **No** empieces la Fase 6 (adaptador a `data/processed/`) antes de que la Fase 3 esté verde.
- **No** inventes series faltantes (`X_vapg`, `BR_tms`) con aproximaciones: falla con un mensaje
  que nombre la serie.
- **No** toques el módulo VIOG (`src/sources/viog/`). El filtro Christiano-Fitzgerald de una cola
  quedó cerrado el 2026-07-30 y su serie `bhp` es legítima y distinta de la del PIB potencial.
