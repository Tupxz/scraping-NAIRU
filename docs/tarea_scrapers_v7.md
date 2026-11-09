# Tarea: scrapers para v7 (rama `scrapers-v7`)

Objetivo: que todas las series que lee `legacy/v7/pib_potencial_integrado_v7.py`
se produzcan automáticamente desde `src/sources/` hacia `data/processed/`.
**No tocar** `src/nairu/`, `src/production/`, `legacy/`, `tests/fixtures/baseline_v7/`
(eso lo hace la rama de integración v7). Si hace falta cambiar `src/config.py`,
`src/main.py` o `.github/workflows/update.yml`, hacerlo en commits pequeños y
aislados (se tocan desde las dos ramas).

## Contratos de salida (lo que v7 espera)

Fechas mensuales = primer día del mes (`2025-12-01`). Se pueden añadir `year`,
`month`, `source`, `download_date` (convención del repo), pero las columnas de
la tabla deben existir con ese nombre y unidad.

| # | Archivo en `data/processed/` | Columnas que lee v7 | Fuente | Notas |
|---|---|---|---|---|
| 1 | `trm_cop_usd_promedio_mensual.csv` | `date`, `trm_cop_usd` | SFC vía datos.gov.co (TRM diaria) | promedio simple de los días del mes; desde 2001 |
| 2 | `eme_expectativas_inflacion_total_mensual_promedio.csv` | `date`, `doce_meses` | Banrep, Encuesta Mensual de Expectativas | expectativa de inflación total a 12 meses, **promedio** de analistas; desde 2003-09 |
| 3 | `emmet_salarios_v7.csv` | `date`, `var_anual_salario_nominal` | DANE EMMET (índice real de sueldos y salarios, total nacional) + IPC total | nominal = índice real × IPC total (base 2018); `var_anual = 100·(W_t/W_{t−12} − 1)`; desde 2014. Replicar `construir_salarios_emmet.py` (pedirlo) |
| 4 | `geih_tgp_to_td_desest_mensual.csv` | `date`, `tgp_desest` (y `to_desest`, `td_desest`) | DANE GEIH desestacionalizada | jul–ago 2006 deben quedar **NaN** (empalme ECH→GEIH), no interpolados. Ya existe parte en `dane_labor_colombia.csv` |
| 5 | `geih_subempleo_bruto_mensual.csv` | `date`, `tasa_subocupacion` | DANE GEIH (subempleo subjetivo, serie bruta) | |
| 6 | `banrep_vacantes_prensa_mensual.csv` | `date` + una columna por ciudad (7 ciudades) | Banrep, índice de vacantes en prensa | historia desde 1976; v7 suma las ciudades |
| 7 | `andi_capacidad_instalada.csv` | `date`, `capacity_utilization` | ANDI EOIC (PDF) | **hoy no se actualiza**: las 264 filas vienen del histórico v6. Arreglar el parser para que agregue meses nuevos y extender la historia a **2001-01** (v7 desestacionaliza con STL desde 2001; la serie sin huecos internos) |
| 8 | `dane_gdp_colombia.csv` | `gdp_observed` | DANE CNT | extender a **2004Q1–2004Q4** empalmando con base 2005 por tasas encadenadas (`src/sources/dane/gdp_historical.py`) |
| 9 | `inflation_banrep_colombia.csv` / `tes_banrep_colombia.csv` | sin cambio de columnas | Banrep SUAMECA | **la descarga falla desde 2026-10-01** en GitHub Actions; diagnosticar y arreglar |

Insumos de la función de producción que v7 lee como CSV (`datos/insumos_pib_*.csv`):
se construirán desde `data/processed/` en la rama de integración; aquí solo hace
falta que las fuentes existan (PIB, enfoque del ingreso con `X_vapg` = valor
agregado de petróleo y gas e impuestos menos subvenciones, GEIH PET/FL/ocupados,
PWT `hc`).

## Orden sugerido
**1 (TRM) primero**: es el único insumo que no vino en `legacy/v7/datos/` y sin
él no se puede correr el script v7 para verificar la línea base. Endpoint:
`https://www.datos.gov.co/resource/32sa-8pi3.csv` (columnas `valor`,
`vigenciadesde`, `vigenciahasta`; paginar con `$limit`/`$offset`). Ojo: cada fila
tiene una vigencia que puede cubrir varios días (fines de semana/festivos) —
definir y documentar si el promedio mensual pondera por días calendario o por
registro, y dejarlo explícito. Escribir además una copia en
`legacy/v7/trm_cop_usd_promedio_mensual.csv` (columnas `date`, `trm_cop_usd`,
`date` = primer día del mes) para que el script de referencia corra tal cual.

Luego: 9 (Banrep, rompe el workflow) → 7 (ICU) → 2, 3 (entran a la estimación
final) → 4 → 8 → 5, 6 (los lee el script; verificar si la especificación final
los usa).

## Estado

| # | Estado | Detalle |
|---|---|---|
| 1 | ✅ 2026-11-09 | `--trm` → `data/processed/trm_cop_usd_promedio_mensual.csv` + copia en `legacy/v7/`. Promedio por **días calendario** (`TRMConfig.ponderacion`; alternativa `"registro"`), solo meses completos, 2001-01 → último mes cerrado. Sin CSV del compañero para comparar; ver `CHANGELOG.md` (Sin publicar). |
| 7 | ✅ 2026-11-09 | Histórico ANDI desde 2001-01 + meses nuevos desde los informes EOIC (`src/sources/andi/extraccion_icu.py`), con controles de mes, rango 55–90, huecos y salto ≤ 8 pp. Error del extractor contra el histórico: medio 0,11 pp, máximo 4,81 (may-2021, revisión). Los informes de octubre no traen el dato mensual: ese mes requiere `--andi-ia` o carga manual en `icu_historical.csv`. |
| 9 | ✅ 2026-11-09 | SUAMECA envía su certificado sin el intermedio (*GeoTrust EV RSA CA G2*) desde el 2026-09-11; `src/sources/banrep/tls.py` lo agrega al almacén de confianza de la sesión (verificación completa, sin `verify=False`). |

## Validación
Los CSV originales del compañero están en `legacy/v7/datos/` (y su constructor de
salarios en `legacy/v7/construir_salarios_emmet.py`, que lee el anexo EMMET y
`banrep_ipc_indice_mensual.csv`). Comparar cada serie scrapeada contra la suya alineando por año-mes; diferencias solo en la
frontera (revisiones del DANE/Banrep) son aceptables.
