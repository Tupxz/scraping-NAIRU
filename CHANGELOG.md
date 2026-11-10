# Changelog

## [0.6.2] — 2026-11-09

Scrapers restantes de `docs/tarea_scrapers_v7.md` (uno por commit). `src/v7/insumos.py` toma cada archivo de `data/processed/` en cuanto existe con sus columnas.

### Agregado
- **EME** (punto 2): `src/sources/banrep/eme.py`, `src/pipelines/run_eme.py`, flag `--eme` (también en `--all`), `scrape --eme` en `update.yml`, `run_eme_checks`, `BanrepEMEConfig`. Fuente: Excel de series históricas de la Encuesta Mensual de Expectativas de Analistas Económicos en SUAMECA (`/archivos/encuestas/encuesta_mensual_analistas_economicos/series_historicas.xlsx`), cuya URL se descubre en el menú `ENCUESTAS` de SUAMECA (con la URL fija de respaldo); banrep.gov.co no sirve para un scraper (captcha antibots Radware). Hoja `INFLACION_TOTAL`: promedio de los seis horizontes en % (el Excel trae fracciones), fecha de la encuesta llevada al primer día del mes. Salida `data/processed/eme_expectativas_inflacion_total_mensual_promedio.csv`: 278 meses, 2003-09 → 2026-10, mismas columnas que el CSV de la línea base. Controles: meses consecutivos desde 2003-09, `doce_meses` completa y en [0, 30]; valida antes de escribir.
  - Contra `legacy/v7/datos/` (277 meses comunes): iguales salvo 0,0001 pp en 1–6 meses por columna (mitades exactas del quinto decimal, p. ej. 0,0335375 → 3,3538 vs 3,3537: redondeo binario de cada cálculo; ninguna regla decimal reproduce todos los casos del compañero). `veinticuatro_meses` y `sesenta_meses`: idénticas salvo 1 mes. Suma 2026-10.
  - `tests/test_eme.py` (27, offline): extracto real del Excel y del menú de SUAMECA, URL desde el menú y respaldos, valores en %, fechas, errores de estructura, controles, descarga con sesión simulada, no escribe si falla.
- **EMMET** (punto 3): `src/sources/dane/emmet.py`, `src/pipelines/run_emmet.py`, flag `--emmet` (también en `--all`), `scrape --emmet` en `update.yml` (después de `--ipc`), `run_emmet_checks`, `EMMETConfig`. Replica `legacy/v7/construir_salarios_emmet.py`: anexo `anex-EMMET-TotalNacional-<mes><año>.xlsx` más reciente de la página de la EMMET, hoja "9. Enlace legal hasta 2014", fila `T_IND`; "Total Sueldos" es el índice real y el nominal se reconstruye con el IPC total de `data/processed/ipc_colombia.csv` (idéntico al IPC del Banco de la República que usó el compañero en los 284 meses comunes). Salida `data/processed/emmet_salarios_v7.csv`: 152 meses, 2014-01 → 2026-08 (anexo de ago-2026, el mismo archivo del compañero: md5 igual).
  - Contra `legacy/v7/datos/emmet_salarios_v7.csv`: **idéntico** en las 152 filas y las 8 columnas (diferencia 0).
  - `tests/test_emmet.py` (18, offline): extracto real del anexo y de la página del DANE, selección del anexo más reciente, índices y salarios iguales a la línea base, identidad (1 + w) = (1 + ω)(1 + π), errores de estructura, controles, no escribe si falla.

## [0.6.1] — 2026-11-09

Rama `scrapers-v7` (scrapers para v7), fusionada sobre [0.6.0].

### Agregado
- Scraper de la **TRM promedio mensual** (COP por USD), insumo 1 de `docs/tarea_scrapers_v7.md` y el único que faltaba para correr `legacy/v7/pib_potencial_integrado_v7.py` (choque petrolero en pesos, `OIL_SHOCK_MODE = "cop"`).
  - `src/sources/sfc/trm.py` (nuevo): descarga paginada (`$limit`/`$offset`, orden por `vigenciadesde`) del conjunto `32sa-8pi3` de datos.gov.co (Superintendencia Financiera). El total descargado se compara con `count(*)` del portal; si no coincide, error. Se exige que las vigencias sean continuas (cada una empieza el día siguiente al fin de la anterior: sin huecos ni traslapes), unidad `COP`, valores > 0 y que la última vigencia no tenga más de 15 días de rezago.
  - **Regla del promedio mensual: días calendario.** Cada registro se expande a todos los días de su vigencia y se promedian los días del mes: cada día pesa igual, y una vigencia que cruza de mes (p. ej. sábado 31-oct → martes 3-nov-2026) aporta 1 día a octubre y 3 a noviembre. La alternativa "por registro" (promedio simple de las TRM con `vigenciadesde` en el mes) queda disponible con `TRMConfig.ponderacion = "registro"`. Diferencia entre ambas desde 2001: hasta ±0,64 % en el nivel mensual y hasta 0,72 pp en `fx_shock`; efecto en la NAIRU v7 de dic-2025: 0,0005 pp (ver Nota).
  - Solo se publican **meses completos** (todos sus días calendario con TRM vigente): el mes en curso no entra con un promedio parcial.
  - Salidas: `data/processed/trm_cop_usd_promedio_mensual.csv` (`date, year, month, trm_cop_usd, source, download_date`; 2001-01 → 2026-10, 310 meses) y la copia `legacy/v7/trm_cop_usd_promedio_mensual.csv` (`date, trm_cop_usd`) que el script v7 busca junto a sí. Crudo en `data/raw/sfc/trm_diaria_raw.csv`.
  - `src/pipelines/run_trm.py`: valida **antes** de escribir; si falla la descarga, el parsing o un check, no se escribe ni el procesado ni la copia.
  - `src/quality_checks.py`: `run_trm_checks` (columnas, nulos, rango 1.000–10.000, duplicados, meses consecutivos sin huecos y primer mes = 2001-01). `check_monthly_complete` es genérico (lo pueden usar los demás scrapers v7).
  - `src/config.py`: `TRMConfig`, `TRM_PROCESSED_COLUMNS`, `TRM_MIN/MAX`, `RAW_SFC_DIR`. `src/main.py`: flag `--trm` (también en `--all`). `update.yml`: `scrape --trm` en el paso "Scrape fuentes".
  - `tests/test_trm.py` (42 tests, offline): paginación y descarga truncada con un portal simulado, parsing y errores del CSV, continuidad de vigencias, promedio por días calendario y por registro contra cifras calculadas a mano, meses incompletos, checks de calidad y pipeline completo (incluye leer la copia exactamente como la lee v7). Fixture: extracto real de la API, 81 registros del 2024-11-28 al 2025-04-01 (`tests/fixtures/trm/`).
- **ICU histórico desde 2001-01** (`data/inputs/icu_historical.csv`): se agregaron los 36 meses 2001-01 → 2003-12 tomados tal cual de `data/inputs/Data_NAIRU.xlsx` del commit `a30fb4b` (columna `ICU`), con `source = ANDI_EOIC via Data_NAIRU.xlsx@a30fb4b`. Los valores de 2004 en adelante no se tocaron (el diff solo tiene inserciones); en los 264 meses comunes ese archivo y el histórico difieren ≤ 4,9e-5 (redondeo a 4 decimales). La serie queda continua de 2001-01 a 2025-12 (300 meses, sin huecos ni duplicados). v7 desestacionaliza el ICU con STL desde 2001, así que estos meses entran al ICU desestacionalizado de toda la muestra.
  - `src/sources/andi/eoic.py::_load_icu_historical` descartaba toda fila anterior a 2004-01 ("la EOIC solo existe desde 2004"); ahora el corte es `ICU_HISTORICAL_START = 2001-01-01`. `data/processed/andi_capacidad_instalada.csv` regenerado desde el histórico: 264 → 300 filas, solo inserciones.
  - `tests/test_andi.py::TestLoadIcuHistorical` (2 tests): el cargador conserva 2001 y descarta lo anterior; el archivo del repo no tiene huecos 2001-01 → 2025-12.

- **ICU mensual nuevo desde los informes EOIC de la ANDI** (punto 7 de `docs/tarea_scrapers_v7.md`). La serie final es el histórico de la ANDI (`data/inputs/icu_historical.csv`, nunca se sobrescribe) más los meses publicados que no estén en él. `andi_capacidad_instalada.csv`: 300 → 304 filas, 2026-01 → 2026-04 = 76,6 / 78,3 / 78,2 / 79,5 (diff solo inserciones).
  - `src/sources/andi/extraccion_icu.py` (nuevo, sin red): texto **por columnas** (los informes van a dos columnas y `extract_text` de la página intercalaba las líneas: se busca un canal vertical sin palabras cerca del centro y se extrae cada mitad), frase del dato del total de la industria tras "uso/utilización de la capacidad instalada" (hasta la sección siguiente) con las redacciones de 2017–2026: A "en <mes> de <año> … se situó / se ubicó / alcanzó un nivel de / al ubicarse en X%", C "pasando de A% en <mes> de <año−1> a X% en <mes> de <año> / en el mismo mes de este año", D "fue de X%, dato … registrado en <mes> de <año−1>".
  - Controles (`validar_icu_nuevo`; si uno falla se lanza error y no se escribe nada): mes del dato = mes del informe (el del enlace; la portada solo se usa si el enlace no trae año y, si difiere, se avisa); valor en [55, 90]; mes anterior presente (sin huecos); |salto mensual| ≤ **8 pp**. Calibración con el histórico 2001–2025 (299 saltos): mediana 1,2, p95 3,4; fuera de COVID (mar–jul 2020) y del paro nacional (may–jun 2021) el mayor es 7,40 pp (ene-2015); solo 4 saltos superan 8 pp, todos de esos episodios. Un mes así exige revisión manual.
  - `src/sources/andi/eoic.py` reescrito: se conservan `ANDIScraper` y los helpers de CSV/reporte; se eliminaron `EOICParser` (búsqueda difusa de "capacidad instalada" + el porcentaje más cercano, sin verificar el mes), `process_one_pdf` y el caché por URL. `run_andi_pipeline` ya no escribe: `src/pipelines/run_andi.py` valida (`run_andi_checks`) y luego guarda (`guardar_icu`). `--andi-backfill` vuelve a extraer los meses nuevos ya agregados; `--andi-reprocess` hace lo mismo con los PDF de `data/raw/andi/` (sin red).
  - **Causa de que el ICU no se actualizara**: `ANDIScraper._extract_date` buscaba el año en la URL sin decodificar; en `Enero%202026` el patrón `20[12]\d` tomaba "2020" ("%20" + "20…"), así que los informes de 2026 se fechaban en 2020 y se descartaban como meses ya existentes. Ahora se decodifica la URL y el año debe ser de 4 dígitos exactos.
  - `scripts/validar_icu_eoic.py` (nuevo): corre el extractor sobre los PDF locales y compara con el histórico (detalle en `outputs/andi/validacion_parser_icu.csv`).
  - Tests: `tests/test_andi_icu.py` (nuevo, 55): redacciones reales, 9 páginas reales de informes en `tests/fixtures/andi/` (recortadas con `pdfseparate` y sin imágenes con ghostscript; texto y posiciones intactos, 60–195 KB), controles, `actualizar_icu` con respaldo, pipeline con scraper simulado (no descarga meses del histórico ni infografías, no escribe), validación contra el histórico. En `tests/test_andi.py` se quitaron los tests del parser difuso, de `process_one_pdf` y del caché, y se agregaron 3 casos de fecha con la URL codificada real.
- **Respaldo manual con IA para el ICU: `--andi-ia` (solo local)**. `src/sources/andi/respaldo_ia.py` (nuevo): si el extractor no encuentra la frase del dato de un mes (`ICUExtraccionError`), el texto del informe va por stdin a `claude -p --model haiku --tools "" --strict-mcp-config --no-session-persistence` con el prompt "Del siguiente texto del informe EOIC de la ANDI de <mes año>, devuelve solo el número de la utilización de capacidad instalada del total de la industria, sin texto adicional. Si el texto no trae ese dato para ese mes, responde solo NA." La respuesta debe ser solo un número (si no, o si es NA, sigue el error); el valor pasa por los mismos controles del punto anterior y queda en `source` como `IA(claude -p --model haiku)|<url>`. El respaldo no se usa si el parser encontró el dato ni salta un error de control (p. ej. mes distinto). Nunca corre automáticamente: no está en `update.yml` ni en `--all`, y tanto `consultar_claude` como el flag se niegan a correr con `CI`/`GITHUB_ACTIONS` en el entorno. `src/main.py`: flag `--andi-ia` (implica `--andi`). README: línea del flag.
  - `tests/test_andi_ia.py` (19, sin llamar a claude): comando y stdin, parseo estricto de la respuesta, falla de la llamada, sin ejecutable, bloqueo en CI, integración con `actualizar_icu` (controles y marca en `source`), flag del CLI y que `update.yml` no lo use.
  - Prueba real única (2026-11-09) sobre informes públicos: enero 2026 → 76,6 (igual que el parser, 2 s); octubre 2024, que no trae el dato mensual → "NA" → error, sin inventar el promedio anual.
### Cambiado
- **Desempleo = TD desestacionalizada PUBLICADA por el DANE** (decisión del usuario). En el anexo GEIH desestacionalizado (hoja "Total nacional") la celda de la TD guarda 100·desocupados/(ocupados+desocupados) con precisión completa y el DANE la muestra con formato `0.0`; la cifra publicada es esa celda a 1 decimal.
  - `src/sources/dane/unemployment.py`: `unemployment_rate` pasa a ser la TD publicada (celda redondeada mitad hacia arriba, sobre su representación decimal, como la muestra el anexo; `GEIHConfig.td_decimales_publicados = 1`). La tasa calculada queda en la columna nueva `unemployment_rate_calc` (100·desocupados/FL desde las filas de población del mismo anexo). `data/processed/dane_labor_colombia.csv` regenerado: 2001-01 → 2026-09 (anexo sep-2026), 12 columnas.
  - `src/quality_checks.py::check_td_publicada` (en `run_all_checks`): la TD publicada debe tener 1 decimal y diferir de la calculada en ≤ 0,05; si no, error (el anexo cambió de estructura o el parser tomó otra fila).
  - `src/pipelines/build_nairu_dataset.py`: `Unemp_Desest` ← `unemployment_rate` (publicada) y `Data_NAIRU.xlsx` empieza en **2001-01** (antes 2004-01 + 12 meses "semilla" de TES 2003, que ya no hacen falta). Se quitó el descarte del ICU anterior a 2004. Filas: desde 2001-01 con desempleo, inflación e ICU no nulos; TES vacío hasta 2002-11.
  - Efecto colateral: `unemployment_rate` también la leen `run_pib_potencial.py`, `build_production_function_dataset.py` y `merge.py`; ahora reciben la TD a 1 decimal (diferencia ≤ 0,05 pp frente a la calculada).
  - Tests: `tests/test_geih.py::TestTDPublicada` (14): redondeo como el anexo (8,25 → 8,3; 8,35 → 8,4), publicada vs calculada en un anexo sintético coherente, controles de calidad y, con la hoja real "Total nacional" del anexo de **ene-2026** (`tests/fixtures/geih/`), que la TD publicada reproduce el `Unemp_Desest` de `a30fb4b` en 301/301 meses. `tests/test_build_nairu_dataset.py` (4, nuevo): inicio 2001-01, usa la publicada, conserva ICU 2001–2003, Excel legible por v7. El fixture sintético con poblaciones de `test_geih.py` ahora tiene TD = desocupados/FL (antes eran incoherentes) y el de `test_pipeline.py` incluye la columna nueva.
- `tests/test_nairu.py::test_matches_published_nairu_2004_03_with_published_coefficients` (golden master del modelo v3 sobre el `Data_NAIRU.xlsx` real) marcado con `pytest.mark.skip(reason="v3 se retira; ver docs/integracion_v7.md")`: con `Data_NAIRU.xlsx` desde 2001-01 la muestra v3 arranca en 2004-01, no en 2004-03, y el valor de arranque que fija el test ya no aplica.
- `data/inputs/Data_NAIRU.xlsx` regenerado con el pipeline: 300 filas, 2001-01 → 2025-12, TD publicada del anexo vigente (sep-2026), ICU de la ANDI desde 2001, Brent FRED `POILBREUSDM`. Decisión del usuario: el pipeline automático usa la TD desestacionalizada **vigente** del DANE (no se congela el anexo de ene-2026) y el Brent de FRED; la réplica exacta del paper queda en el modo `--referencia` de `src/pipelines/run_v7.py` (rama main, con `data/inputs/archive/Data_NAIRU_2026-05-11_icu2001.xlsx`), no en el scraping.
- `data/raw/dane/geih_page.html` y `data/raw/dane/geih_raw.xlsx` salen del índice de git (`git rm --cached`): el `.gitignore` ya excluye `data/raw/**/*.html` y `*.xlsx`, pero estaban versionados y cada corrida del scraper los dejaba modificados. Los archivos locales se conservan.
- TRM: el promedio mensual se guarda con 6 decimales (antes 4; cambio máximo 5e-5 COP). El docstring de `src/sources/sfc/trm.py` documenta que la regla por días calendario es la convención del promedio mensual del Banco de la República y la del script del compañero (`legacy/v7/download_trm_cop_usd.py` en main), según confirmó el usuario.

### Corregido
- **`--banrep` y `--tes` (SUAMECA) fallaban desde la corrida del 2026-10-01** (punto 9 de `docs/tarea_scrapers_v7.md`). Causa: el 2026-09-11 el Banco de la República instaló un certificado nuevo en `suameca.banrep.gov.co` (emisor *GeoTrust EV RSA CA G2*, DigiCert) y el servidor envía la hoja y la raíz *DigiCert Global Root G2* **sin el certificado intermedio**. Los navegadores lo completan solos (AIA); `requests`/`certifi` y OpenSSL no: `CERTIFICATE_VERIFY_FAILED: unable to get local issuer certificate` (reproducido localmente; `openssl s_client` → "unable to verify the first certificate"). No se pudo leer el log de GitHub Actions (no hay `gh` en esta máquina), pero el runner usa el mismo `requests`/`certifi` y la fecha del certificado coincide con la primera corrida fallida.
  - `src/sources/banrep/tls.py` (nuevo): el intermedio público (descargado de la URL AIA del certificado, `http://cacerts.digicert.com/GeoTrustEVRSACAG2.crt`; vigente hasta 2030-07-02; SHA-256 `2D:14:…:1A:EF`; verificado con `openssl verify` contra `certifi`) se agrega al almacén de confianza de la sesión **además** de `certifi`, con un `HTTPAdapter` propio. La verificación TLS sigue completa (hoja → intermedio → raíz, nombre y fechas); no se usa `verify=False`. `create_session` de `inflation.py` y `tes.py` lo monta.
  - Corrida real: inflación 858 filas (sin cambios de valores frente al CSV cacheado; SUAMECA llega a sep-2026), TES 287 filas: agrega 2026-09 → 2026-11 y revisa 2026-08.
  - `tests/test_banrep_tls.py` (6, offline): huella del intermedio embebido, que el contexto incluya intermedio + raíces de `certifi` con verificación obligatoria, que el adaptador llegue al pool y que ambos `create_session` lo monten.

- **ICU: solo meses al final y huecos truncados en `Data_NAIRU.xlsx`** (regla del usuario).
  - `extraccion_icu.validar_icu_nuevo`: un mes nuevo debe ser posterior al último de la serie (nunca se modifica un mes existente ni se rellena un hueco desde un informe). Si entre el último mes y el nuevo queda un hueco (octubre y noviembre, que la ANDI no publica), el mes se agrega con un aviso y el salto (≤ 8 pp) se mide contra el último mes disponible; antes un hueco bloqueaba toda actualización posterior. `actualizar_icu` omite con aviso los informes de meses anteriores al último de la serie.
  - `build_nairu_dataset.truncar_en_hueco`: el ICU de `Data_NAIRU.xlsx` se trunca en el último mes contiguo desde su inicio, con un warning que lista los meses faltantes y los descartados (el STL de v7 falla con huecos internos). Tests: 3 en `tests/test_build_nairu_dataset.py`, 5 en `tests/test_andi_icu.py` (mes tras hueco, salto contra el último disponible, solo al final, informe de un hueco omitido).
  - Mayo–septiembre de 2026: la ANDI aún no los publica (la página enlaza enero–abril; los nombres habituales y variantes dan 404; no hay noticias de esos meses). Octubre: el informe no trae el dato mensual y no hay informe de noviembre ni cita posterior → instrucciones de carga manual en `docs/tarea_scrapers_v7.md`.
- Banrep/TLS: confirmado que el intermedio *GeoTrust EV RSA CA G2* va embebido como PEM en `src/sources/banrep/tls.py` (versionado; sin rutas locales ni variables de entorno), así que funciona igual en GitHub Actions. `certifi` (que `tls.py` importa directamente) queda declarado en `pyproject.toml` en vez de depender de que `requests` lo traiga. Test nuevo `test_funciona_en_un_entorno_limpio`: construye el contexto en un proceso `python -I` sin `SSL_CERT_FILE`/`REQUESTS_CA_BUNDLE` y con otro `HOME`.
### Nota
- (Superado: el usuario confirmó que `download_trm_cop_usd.py` del compañero, en main, usa días calendario.) No había CSV de TRM del compañero para comparar, así que la regla de promedio no se pudo contrastar directamente. Prueba indirecta: el paso 1 de v7 (NAIRU/NAICU) corrido sobre una copia con el `data/inputs/Data_NAIRU.xlsx` del repo da NAIRU dic-2025 = 8,5858 % con días calendario y 8,5863 % por registro; la línea base v7 es 8,6256 % (NAICU 77,704 vs 77,530). La regla de la TRM no explica la diferencia; viene de `Data_NAIRU.xlsx` (desempleo e ICU de dic-2025 no coinciden con los de la línea base: 8,38 vs 8,4 y 78,67 vs 78,65). Para verificar la línea base hace falta el `Data_NAIRU.xlsx` con el que se generó.
- TD publicada del anexo vigente (sep-2026) frente a `Unemp_Desest` de `a30fb4b`: iguales 147 de 301 meses; 154 difieren (−0,5 a +0,4 pp; 138 dentro de 2004–2025). **Es una revisión del DANE, no un error del parser**: con el anexo de ene-2026 el mismo parser reproduce 301/301; el de dic-2025 coincide en 296/300 (solo la frontera); desde el anexo de feb-2026 el DANE reestimó la desestacionalización y cambió media historia (anexos feb, mar y sep-2026: 153, 152 y 147 coincidencias). Los anexos históricos se descargan de `https://www.dane.gov.co/files/operaciones/GEIH/anex-GEIH-Desestacionalizado-<mes><año>.xlsx`.
- Paso 1 de v7 (NAIRU/NAICU) sobre una copia, con la TRM nueva, contra `tests/fixtures/baseline_v7/nairu_estimates_v7.csv` (264 meses, 2004-01 → 2025-12):

  | `Data_NAIRU.xlsx` | NAIRU dic-2025 | NAICU dic-2025 | máx. \|dif\| NAIRU | máx. \|dif\| NAICU | máx. \|dif\| ICU desest. |
  |---|---|---|---|---|---|
  | `a30fb4b` tal cual | 8,6256 | 77,5298 | 2,0e-5 | 3,4e-6 | 2,6e-13 |
  | pipeline (este commit) | 8,6092 | 77,4986 | 0,308 (2021-08) | 0,031 (2004-01) | 7,9e-5 |
  | pipeline + desempleo de `a30fb4b` | 8,6256 | 77,5306 | 0,0037 (2004-01) | 0,0009 | 7,9e-5 |
  | pipeline + Brent de `a30fb4b` | 8,6090 | 77,4977 | 0,309 | 0,032 | 7,9e-5 |
  | pipeline + desempleo y Brent de `a30fb4b` | 8,6256 | 77,5298 | 3,5e-5 | 2,0e-6 | 7,9e-5 |

  Línea base: 8,626 / 77,530. Las únicas diferencias son el desempleo (revisión del DANE de feb-2026, domina) y el Brent (el de `a30fb4b` es un promedio por días hábiles de una serie diaria que no es `POILBREUSDM` ni `DCOILBRENTEU`); las TES del repo no cambian nada (solo recortan la muestra). Con ambos insumos de `a30fb4b`, el resto del pipeline (ICU desde 2001, inflación, TES, TRM) reproduce la línea base a 3,5e-5, lo que además valida la TRM por días calendario.
- Validación del extractor del ICU contra el histórico (96 informes únicos de `data/raw/andi/`, 2017-01 → 2026-04; `python -m scripts.validar_icu_eoic`): 85 meses comparables, **error medio |e| = 0,113 pp**, mediano 0,03, **máximo 4,81 pp (may-2021)**; 72 de 85 dentro del redondeo a 1 decimal (≤ 0,05). Las diferencias mayores son revisiones de la ANDI sobre la primera publicación, no errores de lectura: 2020-01 se publicó 80,1 y el informe de un año después cita 79,5 (histórico 79,47); 2021-04: 78,7 → 79,3 (79,29); 2017-02: 73,5 → 72,8 (72,84); may-2021 (paro nacional) se publicó 63,2 y el histórico tiene 68,01 (sin cita posterior en los PDF locales). Fallas: 5 informes de octubre (2017, 2021–2024) no traen el dato mensual (solo promedios), el de "Julio 2020" dice "en junio de 2020 … 77,7%" (el control de mes lo rechaza; el histórico de jul-2020 es 77,70, así que es una errata del informe) y 1 archivo no es PDF (descarga fallida de sep-2018, duplicado de uno que sí lee).
- `pytest tests/`: 588 pasan, 22 fallan; las mismas 22 fallan en `9fa66fb` sin estos cambios (20 de `TestAgainstV3Baseline`, no 13 como dice `CLAUDE.md`, y 2 de `TestComputeVIOGWeights`).

## [0.6.0] — 2026-11-09

### Agregado
- **Especificación v7 integrada al pipeline** (`src/v7/`, `src/pipelines/run_v7.py`).
  - `src/v7/motor.py`: copia literal de `legacy/v7/pib_potencial_integrado_v7.py`; el único cambio es que la raíz de insumos y salidas sale de la variable de entorno `V7_ROOT`. Se regenera con `python scripts/sync_motor_v7.py` si el script de referencia cambia (un test exige que la copia siga siendo literal).
  - `src/v7/insumos.py`: arma la carpeta de insumos del motor. Modo `referencia` = insumos exactos de la línea base; modo `repo` = construidos desde `data/processed/` y `data/inputs/`, usando la referencia solo para lo que el scraping aún no produce (TRM hasta fusionar `scrapers-v7`; EME, EMMET, subempleo, TGP, vacantes; VA petróleo-gas e impuestos menos subvenciones) y registrando la fuente de cada archivo. `Data_NAIRU.xlsx` del repo solo se acepta si su ICU arranca en 2001-01 y su desempleo es la TD publicada por el DANE (decisión del usuario).
  - `python -m src.pipelines.run_v7 [--referencia]`: corre los pasos 1–3 del motor (~40 s) y deja salidas en `outputs/v7/` (o `outputs/v7_referencia/`) con `fuentes_insumos.json` y `resumen_v7.json`.
  - `data/inputs/archive/Data_NAIRU_2026-05-11_icu2001.xlsx`: insumo exacto de la línea base, recuperado del commit `a30fb4b`. `legacy/v7/trm_cop_usd_promedio_mensual.csv`: copia de la TRM generada en `scrapers-v7`.
  - `tests/test_v7.py` (13 tests): la copia del motor es literal; fechas trimestrales; validación de `Data_NAIRU`; estructura de la carpeta de insumos en ambos modos; y **reproducción de la línea base** — NAIRU/NAICU mensual a 1e-4 pp (observado 3e-5, por la TRM) y las 59 columnas trimestrales a rtol 1e-5. NAIRU dic-2025 8,6256 %, NAICU 77,530, brecha 2025T4 −0,10 %, contraste con filtros RMSE 0,71 pp / corr 0,87 / 0,95 / 0,95, idéntico al paper.

### Hallazgo (pendiente de decisión)
- En modo `repo`, con el PIB desestacionalizado del DANE vigente (descarga del 2026-11-01), Bry-Boschan fecha un pico adicional en **2008T3** (además de 2019T4 y 2023T1). Ese nudo extra cambia la tendencia de la PTF: brecha 2025T4 −0,24 % (vs −0,10 %) y el contraste con los filtros empeora (RMSE 1,08 pp, corr niveles 0,74, banda 8–20 años 0,69). Causa: el DANE reestima el ajuste estacional en cada publicación y revisa toda la historia (±0,5 % en niveles; en 2008T2 el crecimiento pasa de +0,93 % a −0,01 %). Verificado aislando insumos: con el PIB de la referencia se vuelve exactamente a la línea base. Hay que decidir si los picos se fijan (como el quiebre de 2014T3) o se re-fechan con cada vintage.

## [0.5.13] — 2026-10-22

### Cambiado
- Limpieza del repo y preparación para integrar la especificación v7 (solo se movieron archivos; ningún cambio de código ni de cifras). Detalle completo de los movimientos en `docs/integracion_v7.md`, sección 5.
  - Raíz: planes/revisiones de mayo–junio → `docs/archive/`; `Claude outputs/` → `docs/archive/claude_outputs/`.
  - Material v7: `working_paper_v7.pdf` → `docs/paper/`; `nairu_estimates_v7.csv` y `pib_potencial_integrado_v7_series.csv` → `tests/fixtures/baseline_v7/`; `legacy/v7/` creado para recibir el script.
  - `docs/`: prompts históricos → `docs/archive/prompts/`; primera nota del VIOG (con errores) y sus artefactos LaTeX → `docs/archive/viog_methodology_v1/`.
  - `data/inputs/`: insumos sin uso en `src/` (`nairu_estimates_v6.csv`, `Kalman_filter.xlsx`, `Code1.do`, `Empleo/`) → `data/inputs/archive/`.
  - `notebooks/` y `scripts/`: obsoletos y diagnósticos de una sola vez → `archive/`.
- `.gitignore`: artefactos de compilación LaTeX y archivos de bloqueo de Office (`~$*`).
- `README.md`: aviso de transición a v7 y estructura del repositorio actualizada.
- `pyproject.toml`: versión sincronizada con el CHANGELOG (0.1.0 → 0.5.13).

### Corregido
- Workflow mensual (`update.yml`): la corrida automática del 2026-10-01 falló en `--pib-potencial` porque `outputs/` estaba ignorado completo y en GitHub no existía `outputs/nairu/nairu_colombia.csv` (la re-estimación automática está pausada). Desde la realineación v3 ese faltante es un error; antes (corrida del 2026-09-01) el código usaba en silencio el desempleo y el ICU observados como NAIRU/NAICU. Ahora `.gitignore` versiona `outputs/nairu/nairu_colombia.csv` y `outputs/nairu/nairu_mle_coefficients.csv` (el resto de `outputs/` sigue ignorado); el paso pausado verifica que existan y falla con un mensaje claro si no; el commit automático incluye `outputs/nairu`. Simulado sobre una copia limpia de los archivos versionados: `--nairu-dataset`, `--merge`, `--pib-potencial` y `--export-web` terminan sin error y regeneran `docs/data/` idéntico al publicado (solo cambia la fecha de `meta.json`).
- Aviso de pausa actualizado: el bug del suavizador ya está corregido; la pausa sigue hasta integrar v7 (el ICU scrapeado no pasa de 2025-12, así que re-estimar no agrega meses).
- `actions/checkout@v5` y `actions/setup-python@v6` en ambos workflows (las versiones anteriores usan Node 20, obsoleto en GitHub).
- Pendiente sin resolver: en la misma corrida fallaron las descargas `--banrep` (inflación) y `--tes` (SUAMECA); el workflow siguió con los datos de septiembre. El log detallado exige iniciar sesión en GitHub.

### Agregado
- `docs/integracion_v7.md`: diferencias v3 → v7, inventario de insumos que pide v7 frente a lo que produce el scraping (faltan TRM, expectativas EME y salarios EMMET; el ICU no se actualiza — las 264 filas vienen del histórico de v6; el PIB arranca en 2005), lista de verificación para cuando llegue el script y registro de la limpieza.

### Nota
- Línea base de tests antes y después de la limpieza: 553 pasan, 15 fallan (13 de `TestAgainstV3Baseline` y 2 de `TestComputeVIOGWeights`, ya conocidos desde [0.5.11]).
- El duplicado NFD de `data/inputs/Boceto Estimación PIB Potencial.xlsx` solo existe en el índice de git; se quita con `git rm --cached` (comando en `docs/integracion_v7.md`).

## [0.5.12] — 2026-10-15

### Cambiado
- `docs/index.html`, pestaña "Mercado laboral y precios", tarjeta "Curva de Phillips estimada": se reemplazó por la curva de Phillips **parcial** ("efecto del desempleo aislado"). La anterior graficaba el NIVEL de la brecha de inflación (básica − meta) contra la brecha laboral, que no es lo que el modelo estima: la dependiente de la ecuación de medición es el CAMBIO mensual de esa brecha, Δg_t = g_t − g_{t-1}. La gráfica nueva muestra, para cada mes, Δg_t menos lo que explican todos los regresores salvo el bloque desempleo/NAIRU (rezagos de Δg, expectativas, choque petrolero y bloque ICU/NAICU), contra la brecha laboral actual (desempleo − NAIRU). Color por año (azul claro = antiguo, azul oscuro = reciente), ajuste OLS de la nube, pendiente agregada que estimó el modelo (Σβ_u, con su significancia) y último dato; el tooltip muestra también el Δg_t observado. Sin datos (CSV viejo) la tarjeta muestra un aviso en vez de fallar; sin la pendiente del modelo en `meta.json` se omite solo esa línea. En pantallas angostas el contenedor es más alto (la leyenda ocupa tres filas).

### Agregado
- `src/nairu/phillips.py` (nuevo): cálculo de la curva de Phillips parcial a partir de `outputs/nairu/nairu_colombia.csv` y los coeficientes de `nairu_mle_coefficients.csv`. Escribe la ecuación fila a fila tal como está en el paper (el MISMO nivel u*_t / c*_t resta a los 3 rezagos de desempleo / ICU) y reproduce, a 1e-15, el gráfico revisado antes en Python.
- `src/pipelines/export_web_data.py`: `docs/data/nairu_monthly.csv` gana las columnas `dg_inf` (Δg_t) y `phillips_parcial`; `docs/data/meta.json` gana `phillips_sum_beta_u` y `phillips_sum_beta_u_p` (pendiente agregada del modelo y su p-valor). Se calculan en el export —no en el navegador— para que la gráfica se actualice sola al reestimar. Si falta `nairu_mle_coefficients.csv` se omiten con un warning. Las 14 columnas que ya consumía el tablero no cambian (verificado exacto).
- `tests/test_phillips_partial.py` (26 tests): identidad con datos sintéticos generados fila a fila (el parcial recupera exactamente bloque de desempleo + error), mismo NAIRU contemporáneo para los 3 rezagos, bloque ICU con NAICU (no NAIRU), propagación de NaN, errores claros por columnas/coeficientes faltantes, lectura de la tabla del MLE, integración del export con CSV sintéticos y consistencia de `docs/data/` publicado contra un recálculo desde `outputs/`. Se verificó con mutaciones (6 errores plausibles introducidos a propósito, todos detectados).

### Nota
- Pedido directo del usuario ("pon esa gráfica de los residuales en la página ... en vez de la actual"), después de revisar la gráfica en Python y de corregir que el eje vertical debía ser Δ(básica − meta), no el nivel.
- Hallazgo a tener presente al leerla: la nube es casi plana (pendiente OLS −0.0058, R² = 0.001; n = 262, 2004-03 a 2025-12). La pendiente agregada del modelo, Σβ_u = −0.0201, tiene el signo teórico pero NO es significativa al 10% (p = 0.118), así que el bloque de desempleo explica muy poco de Δg_t en esta especificación. Se verificó que no es un artefacto del suavizado: con el estado predicho a un paso, el filtrado o el suavizado el R² de la nube es ≈ 0 en los tres casos, y usar la combinación exacta de los 3 rezagos en vez del mes actual tampoco lo cambia. La leyenda lo dice con el p-valor real, sin maquillarlo.
- `tests/fixtures/baseline_v3/` (13 tests de `TestAgainstV3Baseline`) y las 2 fallas de `tests/test_viog.py::TestComputeVIOGWeights` siguen pendientes, igual que en [0.5.11]; este cambio no las toca.

## [0.5.11] — 2026-09-17

### Corregido
- `src/nairu/model_core.py`, `src/pipelines/build_nairu_dataset.py`, `src/sources/dane/unemployment.py`: la ventana muestral de la estimación NAIRU/NAICU (curva de Phillips) estaba recortada a 238 observaciones (2006-01 a 2025-12) en vez de las 264 (2004-01 a 2025-12) que reporta el paper metodológico del proyecto. Tres causas independientes, todas de ventana muestral o calidad de datos, ninguna de metodología del filtro:
  1. `unemployment_ma24`/`icu_ma24` (medias móviles de 24 meses) se calculaban e incluían en la selección de columnas solo para reproducir un recorte de `pib_potencial_integrado_v3.py` que la especificación adoptada ("hysteresis" + "distributed_lag") no usa para nada. Se retiraron.
  2. `Data_NAIRU.xlsx` solo conservaba filas desde 2004-01, aunque la tasa TES a 1 año (Banrep) sí tiene datos válidos desde 2003-01 — necesarios porque `expected_inflation_current_period` se construye desplazando 12 meses esa serie. Se añadieron esos 12 meses previos como filas "semilla" (solo TES; el resto de columnas queda en blanco ahí, y se descartan solas más adelante en el proceso, una vez que ya cumplieron su función).
  3. La serie de desempleo del DANE (GEIH desestacionalizado) tenía un hueco real de 2 meses, julio y agosto de 2006 — la misma fecha que ya se había detectado como hueco de `tgp` en la nota [0.5.9], pero resultó no ser un hueco real de datos: el DANE sí publica un valor para esos 2 meses (12.04 % y 13.05 %), marcados con un asterisco en el Excel oficial ("Jul*", "Ago*") por ser el mes exacto del empalme entre la encuesta anterior (ECH) y la actual (GEIH, que arrancó el 7 de agosto de 2006). El código no reconocía "jul*"/"ago*" como nombres de mes válidos y los descartaba en silencio. Corregido normalizando el texto antes de buscarlo.
- Ventana final resultante: 262 observaciones, 2004-03 a 2025-12 (264 - 2: el ANDI/EOIC, la serie de uso de capacidad instalada, no tiene datos propios anteriores a 2004-01 en las fuentes de este repo, así que los 2 primeros meses de 2004 no alcanzan a tener el rezago de 2 meses que necesita el modelo). Con esta ventana, NAIRU y NAICU de diciembre de 2025 quedan en 9.46 % y 79.23 %, prácticamente idénticos a los 9.46 % y 79.24 % que reporta el paper — la pequeña diferencia de arranque (2004-03 vs. 2004-01) no le importa al resultado final porque el filtro de Kalman "olvida" el estado inicial con el tiempo.

### Nota
- Motivado por pedido directo del usuario ("arreglarlo ya, como dice el paper") tras detectar la discrepancia entre la ventana documentada en el paper y la que usaba el sitio. `BEST_START_PARAMS_V3` (los 14 coeficientes libres del modelo) no cambió: ya coincidía dígito a dígito con el Cuadro 5 del paper desde antes — el problema era solo la ventana de datos que se le pasaba, no los parámetros.
- Como NAIRU/NAICU son insumos estructurales de la función de producción CBO, esta corrección también mueve un poco los indicadores de PIB potencial y sus brechas (que dependen de NAIRU/NAICU/ICU vía `outputs/nairu/nairu_colombia.csv`): brecha del producto (Cobb-Douglas) de 2.51 % a 2.64 %, índice de PIB potencial de 172.57 a 172.34 (dic-2025) — cambios pequeños, en la fila esperada dada la corrección. El peso del capital (α) no cambia (no depende de NAIRU/NAICU).
- Pendiente, no resuelto en esta ronda: `tests/fixtures/baseline_v3/` (la línea base congelada contra la que se compara el motor de PIB potencial en `TestAgainstV3Baseline`) quedó desactualizada por el mismo motivo — se generó ejecutando `legacy/pib_potencial_integrado_v3.py` con la ventana NAIRU vieja. Los 13 tests de esa clase fallan por eso, de forma esperada; regenerar esa línea base correctamente requiere volver a correr v3.py (sin tocarlo) con los datos ya corregidos, tarea que se dejó para una ronda aparte por su tamaño. Las 2 fallas preexistentes de `tests/test_viog.py::TestComputeVIOGWeights` (tarea pendiente de migrar el filtro VIOG de logs a niveles) tampoco están relacionadas con este cambio.

## [0.5.10] — 2026-09-17

### Cambiado
- `docs/index.html`, tarjeta KPI "Participación del capital (α)" → "Peso del capital en la producción": se quitó la jerga técnica ("Estilo CBO: EBE/(remuneración+EBE)") y se reemplazó por una frase en lenguaje llano y dinámica, calculada del propio valor de α: "De cada $100 que produce la economía, ≈$N remuneran al capital... y ≈$(100-N) al trabajo." Se quitó también la mención "(α = N% de participación del capital)" del párrafo diagnóstico superior (lede), que quedaba duplicada y en jerga.
- Gráfica "PIB observado, potencial y tendencial": la leyenda no se veía (confirmado visualmente) porque su posición por defecto (debajo del eje x, `y:-0.16`) coincide con la franja del rangeslider de fechas, que solo tiene esta gráfica. Se movió la leyenda arriba del área de graficación (`y:1.16`) solo para esta gráfica.

### Actualizado
- Pestaña "Comparación de metodologías", tarjetas de referencia institucionales: MFMP actualizado de la edición 2025 a la 2026 (radicada en el Congreso jun-2026: 2.6% en 2026, 2.2% en 2027, 2.9% promedio de mediano plazo) y OCDE actualizado de la Economic Survey 2024 (sin cifra puntual) al Economic Outlook de jun-2026 (2.4% en 2026, 2.1% en 2027). CARF y FMI se revisaron y siguen siendo su publicación más reciente, sin cambios. Banrep se dejó en su edición de enero-2026 tras confirmar (revisando las ediciones de abril y julio) que sigue siendo la última vez que el Emisor publicó esa cifra explícita de crecimiento potencial; se aclaró esto en el texto de la tarjeta.

### Nota
- Motivado por retroalimentación directa del usuario sobre las tres tarjetas/gráficas de esta ronda. Revisado con el usuario en una vista previa (Artifact) antes de guardarse en el repo.

## [0.5.9] — 2026-09-11

### Corregido
- `docs/index.html`: tooltip con fecha equivocada en gráficas cuyas series tienen huecos de datos en fechas distintas (ej. TGP: la GEIH tiene un hueco real en 2006-07, mes en que `tgp` es nulo pero `tgp_star` no) — el modo `hovermode: 'x unified'` de Plotly, al no encontrar un punto válido de una serie en la fecha exacta, mostraba el valor más cercano de esa serie pero bajo el encabezado de fecha de la OTRA serie, aparentando una fecha incorrecta. Se auditaron todos los grupos de gráficas multi-serie de la página (PIB grande, brechas, descomposición, PTF, factores, TGP) y todos tenían al menos algún desfase de fechas nulas entre series. Corregido globalmente cambiando `hovermode` de `'x unified'` a `'closest'` en `baseLayout()`, que siempre muestra el punto (fecha, valor) real de la serie que se está tocando — elimina esta clase de error en todas las gráficas presentes y futuras, no solo en TGP.

### Agregado
- Pestaña "Comparación de metodologías": las tarjetas de referencia CARF, MFMP 2025, Banrep, FMI y OCDE ahora son enlaces (`<a>`) al informe oficial real de cada fuente (URLs verificadas por búsqueda web), y se abren en pestaña nueva. La tarjeta de la estimación propia del modelo ("Este modelo") se mantiene sin enlace, para distinguir visualmente estimación propia de referencias externas.

### Nota
- Motivado por retroalimentación directa del usuario: un error visible en el tooltip de la gráfica de TGP, y su pedido de poder acceder con un clic a los informes de las demás fuentes citadas en la comparación.

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
