# Integración v7 — inventario y lista de verificación

Estado al 2026-10-22. Documento de trabajo para integrar la especificación v7
(working paper `docs/paper/working_paper_v7.pdf`) en el pipeline automático.
Objetivo: que `python -m src.main` (y el workflow mensual) reproduzca **los
mismos números** que el script de referencia v7 cada vez que se actualiza el
scraping.

Piezas disponibles:

| Pieza | Ubicación | Estado |
|---|---|---|
| Working paper v7 | `docs/paper/working_paper_v7.pdf` | ✅ |
| Salidas congeladas v7 | `tests/fixtures/baseline_v7/` | ✅ (NAIRU mensual + 60 columnas trimestrales) |
| Script de referencia v7 | `legacy/v7/pib_potencial_integrado_v7.py` | ⏳ pendiente (lo pasa el compañero) |

---

## 1. Qué cambia de v3 (lo que corre hoy en `src/`) a v7

### Bloque mensual (tasas de pleno uso) — reemplazo completo de `src/nairu/model_core.py`

| | v3 (vigente en `src/nairu/`) | v7 (paper, sección 4.1) |
|---|---|---|
| Implementación | Kalman propio (numpy/scipy) | `statsmodels` (espacio de estados), L-BFGS-B, 6 arranques + refinamiento |
| Estados | NAIRU, NAICU | u*, icu* (constante), ℓ_t, ℓ_{t−1}, tgp*_t, pendiente g_t, error ICU e_t (7) |
| Ecuaciones de medición | 1 (Phillips de precios con rezagos de u e ICU) | 5: desempleo, TGP (con dummy empalme ECH–GEIH jul-2006), ICU, Phillips de salarios, Phillips de precios |
| Parámetros | 18 (4 fijos por calibración) | 24 libres; sin intercepto en precios; homogeneidad γpe = 1 − s |
| NAIRU | histéresis con ancla rezagada | reversión a ū = 10,56 % (promedio del desempleo en episodios de inflación estable: ≥ 6 meses a < 0,5 pp de la meta) |
| NAICU | estado con tendencia | constante, estimada como estado sin innovación (77,53 %) |
| COVID | interpolación del ancla 2020-04 → 2021-06 | desempleo, TGP e ICU como **faltantes** abr-2020 → jun-2021; en precios/salarios la holgura pasa a −κ·ℓ_t |
| Muestra | 2004-03 → 2025-12 (262 filas publicadas) | 2004-01 → 2025-12 (264) |
| Resultado dic-2025 | NAIRU 9,46 %, NAICU 79,23 % | NAIRU 8,63 %, NAICU 77,53 % |

### Bloque trimestral (función de producción) — ajustes sobre `src/production/`

Ya alineado con v3 (TGP*, stock DANE, α CBO, PTF con rampas BBQ). Diferencias v7 a revisar contra el script:

- Base de índices **2005Q1 = 100** (el repo ancla en 2007Q3 por el hueco GEIH 2006Q3; v7 lo resuelve dentro del modelo mensual con la dummy de empalme).
- TGP* por regresión CBO con brechas de desempleo **y de capacidad** + sus MA de 8 trimestres (ec. 18).
- PTF: rampas en picos BBQ (2019Q4, 2023Q1) **+ quiebre exógeno 2014Q3**, brecha de desempleo t y t−1, dummies 2020Q2–2021Q1 (ec. 20).
- Servicios de capital K·ICU/100 y K·icu*/100 (ICU desestacionalizado por STL).
- Capital humano PWT extrapolado con OLS 2014–2023 anclado en 2023; stock de capital extrapolado con crecimiento log medio de 4 años.
- Intervalo de confianza del PIB potencial que propaga la incertidumbre de NAIRU/NAICU.
- Descomposición exacta de la brecha (ec. 22): capital / desempleo / participación / PTF.
- PIB 2004 empalmado con base 2005 (tasas encadenadas).

### Filtros (VIOG) — sin cambios de metodología en el paper; la sección 4.3 se reescribirá aparte.

---

## 2. Inventario de insumos: qué pide v7 y qué produce hoy el scraping

| Serie (paper, cuadros 1 y 3) | Hoy en el repo | Estado | Acción para automatizar |
|---|---|---|---|
| Desempleo desestacionalizado | `dane_labor_colombia.csv` · `unemployment_rate` (2001-01 →) | ✅ | — |
| TGP desestacionalizada | `dane_labor_colombia.csv` · `tgp_rate` | ✅ | verificar hueco jul–ago 2006 = NaN (no interpolado) |
| PET, ocupados, desocupados | `dane_labor_colombia.csv` | ✅ | FL = ocupados + desocupados |
| Inflación básica, total, meta | `inflation_banrep_colombia.csv` · `Core_Inf`, `Inf_Rate`, `Inf_Goal` | ✅ | — |
| IPC total (índice) | `ipc_colombia.csv` (2003-01 →) | ✅ | — |
| Brent USD | `brent_colombia.csv` (2001-01 →) | ✅ | — |
| **TRM mensual** | — | ❌ falta | scraper nuevo (SFC / datos.gov.co, promedio mensual de la diaria) |
| **Expectativa de inflación 12 m (EME)** | — | ❌ falta | scraper nuevo Banrep (Encuesta Mensual de Expectativas, promedio; desde 2003-09) |
| **Salarios EMMET** (masa salarial real / empleo) | — | ❌ falta | scraper nuevo DANE EMMET (base 2018, desde 2014) |
| **ICU** | `andi_capacidad_instalada.csv` — **las 264 filas vienen de `nairu_estimates_v6.csv` (histórico congelado)**, 2004-01 → 2025-12 | ⚠️ no se actualiza | (a) que el parser de PDFs ANDI (`src/sources/andi/eoic.py`) extienda la serie; (b) extender el histórico a **2001** (v7 hace STL desde 2001); (c) STL robusto, periodo 12 |
| PIB trimestral desestacionalizado | `dane_gdp_colombia.csv` (2005Q1 →) | ⚠️ | empalmar 2004 con la base 2005 (`gdp_historical.py` / `dane_gdp_base2005.xls` ya existen) |
| Remuneración de asalariados, EBE | `dane_gdp_income_colombia.csv` (2016Q1 →) | ✅ | — |
| Stock de capital productivo | `data/inputs/alt_capital/dane_stock_capital_productivo.csv` (manual, 1990 → 2024) | ⚠️ manual | anual; aceptable como insumo manual o scraper del anexo DANE |
| Capital humano | `pwt_colombia.csv` · `human_capital` (→ 2023) | ✅ | — |
| Festivos efectivos | `data/inputs/festivos_efectivos_colombia_2001_2026.xlsx` | ⚠️ | extender después de 2026 (o calcularlo con `holidays`) |
| Jornada legal (Ley 2101) | en código (`factors.py`) | ✅ | — |

Además: hoy el bloque mensual lee `data/inputs/Data_NAIRU.xlsx`, que construye
`src/pipelines/build_nairu_dataset.py` desde `data/processed/`. Para v7 ese
paso debe ampliarse con las columnas nuevas (TGP, TRM, EME, EMMET, IPC total,
ICU desde 2001) o, mejor, leer directamente de `data/final/nairu_dataset.csv`.

---

## 3. Lista de verificación cuando llegue el `.py`

1. Copiar a `legacy/v7/pib_potencial_integrado_v7.py`. Correrlo tal cual y
   confirmar que reproduce `tests/fixtures/baseline_v7/` (si no, la línea base
   o el script no corresponden — resolver antes de seguir).
2. Identificar cómo lee sus datos (¿Excel propio? ¿qué hojas/columnas?) y
   comparar **insumo por insumo** contra `data/processed/` (mismo método que con
   el Boceto en v3: alinear por año-mes / año-trimestre, revisar diferencias).
3. Escribir los scrapers que faltan (TRM, EME, EMMET) y arreglar ICU y PIB 2004
   (tabla de la sección 2), cada uno con sus tests offline.
4. Reemplazar `src/nairu/model_core.py` por un port de v7 que conserve el
   contrato de `estimation.py` (`load_and_prepare_data` / `build_model_data` /
   `estimate_parameters` / `build_outputs`); salidas nuevas: tgp*, ℓ_t, IC.
5. Ajustar `src/production/` a las diferencias de la sección 1.
6. Tests: `TestAgainstV7Baseline` (mensual y trimestral, tolerancia ~1e-8)
   reemplazando `TestAgainstV3Baseline`; retirar `tests/fixtures/baseline_v3/`
   y `legacy/pib_potencial_integrado_v3.py` al terminar.
7. `export_web_data.py` / `docs/index.html`: revisar columnas (curva de Phillips
   parcial cambia de ecuación: ahora κ·(u − u*) contemporáneo en precios y salarios).
8. Reactivar el paso "Estimar NAIRU / NAICU" de `.github/workflows/update.yml`
   (pausado desde 2026-09-01). Con statsmodels la estimación tarda más: revisar
   `timeout-minutes`.

---

## 4. Pendientes heredados (no los tocó la limpieza)

- Línea base de tests al 2026-10-22: **553 pasan, 15 fallan**, todos ya
  conocidos:
  - 13 de `TestAgainstV3Baseline` (desde [0.5.11], al mover la muestra NAIRU a
    2004-01; desaparecen al reemplazar la línea base por v7).
  - 2 de `tests/test_viog.py::TestComputeVIOGWeights` (warm-up del C-F).
- Código que v7 vuelve obsoleto y conviene borrar al integrar, no antes:
  `compute_k0_sensitivity` (stub) y `_build_capital_quarterly` en
  `run_pib_potencial.py`; `build_production_function_dataset.py` + `FUNCION DE
  PRODUCCION.xlsx` (flujo α = 0,4 anterior a v3); los TES en `build_nairu_dataset`.
- `README.md` describe todavía la arquitectura de mayo (α = 0,4, BHP); se
  reescribe al cerrar la integración v7.

---

## 5. Registro de la limpieza del 2026-10-22

Solo se movieron archivos (nada se borró). Para registrar los movimientos en
git, desde el Mac:

```bash
cd ~/Documents/EAFIT/Coyuntura/scraping-NAIRU
git rm --cached "data/inputs/Boceto Estimacio$(printf '\xcc\x81')n PIB Potencial.xlsx"   # duplicado NFD del Boceto
git add -A
git status            # revisar: deben verse como "renamed"
git commit -m "chore: limpieza del repo y preparación para v7"
rm -rf _to_delete     # opcional: residuos (ignorados por git)
```

| Antes | Después |
|---|---|
| `CLOSEOUT.md`, `PLAN_PIB_POTENCIAL.md`, `REVIEW.md`, `REVISION_2026-06-08.md` | `docs/archive/` |
| `Claude outputs/` | `docs/archive/claude_outputs/` |
| `working_paper_v7.pdf` | `docs/paper/` |
| `nairu_estimates_v7.csv`, `pib_potencial_integrado_v7_series.csv` | `tests/fixtures/baseline_v7/` |
| `docs/prompt_*.md`, `docs/prompts/*` | `docs/archive/prompts/` |
| `docs/viog_methodology.{tex,pdf,aux,…}` (versión con errores) | `docs/archive/viog_methodology_v1/` |
| `data/inputs/{nairu_estimates_v6.csv, Kalman_filter.xlsx, Code1.do, Empleo/}` | `data/inputs/archive/` |
| `notebooks/{VIOG_original, VIOG_B, validation_v6}.ipynb` | `notebooks/archive/` |
| `scripts/*.py` (diagnósticos de una sola vez del C-F y del Kalman) | `scripts/archive/` |
| `outputs/{validation_kalman, diagnostico_cf, andi_*report.txt, production_function_*.csv}` | `outputs/archive/` (ignorado por git) |
| `~$*.docx/.xlsx` (bloqueos de Office), PNG viejos `*_hp_*` | `_to_delete/limpieza_2026-10-22/` |

`.gitignore`: se añadieron artefactos LaTeX (`*.aux`, `*.fls`, …) y `~$*`.
