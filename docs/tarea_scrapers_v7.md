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
| 1 | ✅ 2026-11-09 | `--trm` → `data/processed/trm_cop_usd_promedio_mensual.csv` + copia en `legacy/v7/`. Promedio por **días calendario** (`TRMConfig.ponderacion`; alternativa `"registro"`), solo meses completos, 2001-01 → último mes cerrado. Misma regla que `legacy/v7/download_trm_cop_usd.py` del compañero; 6 decimales. Ver `CHANGELOG.md` [0.6.1]. |
| 2 | ✅ 2026-11-09 | `--eme`: Excel de series históricas de la EME en SUAMECA (URL desde el menú ENCUESTAS); 2003-09 → 2026-10. Igual al CSV del compañero salvo 0,0001 pp por redondeo. |
| 3 | ✅ 2026-11-09 | `--emmet`: anexo EMMET vigente + IPC del repo; idéntico al `emmet_salarios_v7.csv` del compañero (152 meses). |
| 4 | ✅ 2026-11-09 | `--unemployment` escribe `geih_tgp_to_td_desest_mensual.csv` (jul–ago 2006 en NaN). Idéntico al del compañero con el anexo de jul-2026; con el vigente difiere ≤ 0,18 pp (revisión del DANE). |
| 5 | ✅ 2026-11-09 | `--subempleo`: anexo principal GEIH (series brutas); idéntico al del compañero en los 302 meses comunes; mar–jul 2020 sin subocupación (NaN, nota del DANE). |
| 6 | ✅ 2026-11-09 | `--vacantes`: series SUAMECA 16570–16576; idéntico al del compañero (609 meses) + 2026-10. Solo `date` + ciudades (v7 suma el resto de columnas). |
| 7 | ✅ 2026-11-09 | Histórico ANDI desde 2001-01 + meses nuevos desde los informes EOIC (`src/sources/andi/extraccion_icu.py`), con controles de mes, rango 55–90, huecos y salto ≤ 8 pp. Error del extractor contra el histórico: medio 0,11 pp, máximo 4,81 (may-2021, revisión). Los informes de octubre no traen el dato mensual: ese mes requiere `--andi-ia` o carga manual en `icu_historical.csv`. |
| 8 | ✅ 2026-11-09 | `dane_gdp_colombia.csv` desde 2004Q1: tasas de la base 2005 encadenadas sobre la base 2015 (crecimientos 2004 idénticos a la línea base; nivel −/+ 0,236 % por la revisión del DANE). |
| 9 | ✅ 2026-11-09 | SUAMECA envía su certificado sin el intermedio (*GeoTrust EV RSA CA G2*) desde el 2026-09-11; `src/sources/banrep/tls.py` lo agrega al almacén de confianza de la sesión (verificación completa, sin `verify=False`). |

## ICU: meses que hay que cargar a mano (octubre y noviembre)

Regla: el histórico de la ANDI (`data/inputs/icu_historical.csv`) se usa tal
cual y solo se agregan meses nuevos **al final**; nunca se modifica un mes
existente.

**Por qué octubre y noviembre no salen del scraping.** Revisados todos los
informes EOIC locales (2017–2026): el informe de **octubre** no trae el dato
mensual de utilización de capacidad (solo promedios del año o del trimestre),
la ANDI **no publica informe de noviembre**, y el de diciembre solo compara
promedios trimestrales. Ningún informe posterior cita el dato de octubre.
`--andi-ia` tampoco sirve para esos meses: el texto no trae el número y la IA
responde "NA" (probado con octubre 2024).

**Qué pasa mientras faltan.** El pipeline sigue agregando diciembre, enero…
al final de la serie (con un aviso de hueco), pero `build_nairu_dataset.py`
trunca el ICU de `Data_NAIRU.xlsx` en el último mes contiguo (el STL de v7
falla con huecos internos), así que esos meses no entran al modelo hasta que
se complete el hueco.

**Cómo cargarlos.**
1. Pedir el dato del **total de la industria** a la ANDI (Vicepresidencia de
   Desarrollo Económico, `cee@andi.com.co`, el contacto que aparece en los
   informes) o tomarlo de otra publicación oficial de la ANDI.
2. Agregar una fila por mes a `data/inputs/icu_historical.csv`, en orden
   cronológico, sin tocar las demás:
   ```
   date,year,month,capacity_utilization,source,download_date
   2026-10-01,2026,10,78.9,ANDI (dato enviado por la VP Desarrollo Económico 2026-12-05),2026-12-05
   ```
   `date` = primer día del mes; `capacity_utilization` en porcentaje (como lo
   publica la ANDI); `source` dice de dónde salió (sin comas).
3. Correr `python -m src.main --andi` y `python -m src.main --nairu-dataset`
   (o esperar la corrida mensual). Verificar que el aviso de hueco desaparezca.

**Mayo–septiembre de 2026 (verificado el 2026-11-09).** La página de la ANDI
(`/Home/Pagina/3-desarrollo-economico-y-competitividad`, sección "Encuesta
EOIC") solo enlaza los informes de enero a abril de 2026; las URL con los
nombres habituales (`Informe EOIC <Mes> 2026.pdf`, con "- ANDI -", "VF",
"[1]", "de") devuelven 404 y las noticias de la ANDI no anuncian EOIC de esos
meses. No es un problema del scraper: entrarán solos cuando la ANDI los
publique en esa página (el mes sale del enlace o, si no trae año, de la
portada).

## Validación
Los CSV originales del compañero están en `legacy/v7/datos/` (y su constructor de
salarios en `legacy/v7/construir_salarios_emmet.py`, que lee el anexo EMMET y
`banrep_ipc_indice_mensual.csv`). Comparar cada serie scrapeada contra la suya alineando por año-mes; diferencias solo en la
frontera (revisiones del DANE/Banrep) son aceptables.


## Hallazgo 2026-11-09 — insumos exactos de la línea base

`data/inputs/archive/Data_NAIRU_2026-05-11_icu2001.xlsx` (recuperado del commit
`a30fb4b`) es el insumo de la línea base v7:
- `Unemp_Desest` = TD desestacionalizada publicada por el DANE (1 decimal):
  coincide exacto con la línea base en 264/264 meses. La versión actual de
  `build_nairu_dataset.py` la reemplazó por desocupados/FL calculada (hasta 0,5 pp
  de diferencia) → **volver a la TD publicada** (decisión del usuario).
- `ICU` 2001-01 → 2025-12. Los meses 2001–2003 vienen de la ANDI y no están en
  `icu_historical.csv` → **agregarlos al histórico** (son datos de la ANDI, no se
  scrapean). Con eso el STL de v7 reproduce el ICU desestacionalizado de la línea
  base a 8e-5.

→ Ambas decisiones quedaron implementadas en `scrapers-v7` (CHANGELOG [0.6.1]): `unemployment_rate` = TD publicada y `icu_historical.csv` desde 2001-01.
