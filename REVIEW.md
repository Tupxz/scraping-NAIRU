# Revisión integral del repositorio `scraping-NAIRU`

**Fecha:** 2026-04-28
**Alcance:** todo el repositorio (`src/`, `tests/`, `andi_agent/`, configuración, documentación)
**Perspectivas:** ingeniería de sistemas, desarrollo de proyectos, economía aplicada

---

## 1. Resumen ejecutivo

El repositorio implementa un pipeline modular y bien estructurado para
construir el dataset macroeconómico mensual que alimentará la estimación
de la NAIRU para Colombia. La arquitectura — separación `src/sources/`,
`src/pipelines/`, `config.py` con dataclasses congeladas, `quality_checks.py`
centralizado, `merge.py` por *outer-merge* sobre `date` — es sólida,
testeable y razonablemente *idiomatic*. La suite de pruebas pasa en
limpio (**340 tests OK en 4.3 s**, sin dependencias de red), lo cual es un
indicador muy positivo de la madurez del proyecto.

Los problemas detectados son acotados y **fáciles de corregir**: un par
de bugs concretos (orquestador `run_all` incompleto, `__init__` que no
re-exporta informalidad), tres archivos vacíos que deben eliminarse,
desactivación de TLS en el scraper de informalidad, y referencias
desactualizadas a "PWT 10.01" en código y documentación pese a haber
migrado a PWT 11.0. El subdirectorio `andi_agent/` quedó como código
heredado y duplica funcionalidad ya consolidada en `src/sources/andi/`.

---

## 2. Bugs y defectos concretos (prioridad alta)

### 2.1 `src/pipelines/run_all.py` no ejecuta PWT ni informalidad

`run_all.py` orquesta 7 pipelines (unemployment, ipc, banrep_inflation,
banrep_tes, brent, andi, merge), pero omite `run_pwt` y `run_informality`.
En cambio, `src/main.py --all` (líneas 161–168) sí los incluye porque
arma la llamada manualmente con `run_pwt=True, run_informality=True`.

Resultado: cualquier consumidor que importe `from src.pipelines import
run_all` y llame `run_all.run()` no incluirá esas dos series. Hay que
unificar las dos rutas — preferentemente reescribir `run_all.run()` para
delegar en la misma función `run_pipeline(...)` de `src/main.py` o,
como mínimo, agregar las dos llamadas faltantes:

```python
from src.pipelines import (
    run_andi, run_banrep_inflation, run_banrep_tes, run_brent,
    run_informality, run_ipc, run_merge, run_pwt, run_unemployment,
)

def run() -> None:
    run_unemployment.run()
    run_pwt.run()
    run_informality.run()
    run_ipc.run()
    run_banrep_inflation.run()
    run_banrep_tes.run()
    run_brent.run()
    run_andi.run()
    run_merge.run()
```

### 2.2 `src/sources/dane/__init__.py` no re-exporta informalidad

El `__init__` exporta `unemployment` e `ipc` pero no `informality`,
aunque ese módulo ya está en producción. Provoca que
`from src.sources.dane import run_informality_pipeline` falle. Añadir:

```python
from src.sources.dane.informality import run_informality_pipeline
```

…y agregarlo a `__all__`.

### 2.3 `verify=False` en el scraper de informalidad (riesgo de seguridad)

`src/sources/dane/informality.py` (líneas 57 y 108) desactiva la
verificación TLS al pegar contra `www.dane.gov.co`:

```python
response = requests.get(config.page_url, ..., verify=False)
```

Esto rompe la integridad criptográfica de la conexión y abre la puerta a
*man-in-the-middle*. No es justificable en un pipeline reproducible.
Soluciones por orden de preferencia:
1. Quitar `verify=False`. Si DANE renueva su cadena de certificados, el
   error es informativo y se debe corregir en el sistema, no silenciar.
2. Si en algún entorno corporativo hay un proxy con CA propia, exportar
   `REQUESTS_CA_BUNDLE` o usar `verify=<ruta a la CA>`.
3. Suprimir el warning de `urllib3` solo si es absolutamente necesario,
   y dejar un *issue* abierto para reactivar la verificación.

Adicionalmente, los demás scrapers (`unemployment.py`, `ipc.py`, …)
verifican TLS correctamente — la inconsistencia en informalidad debe
eliminarse.

### 2.4 Archivos vacíos en `src/`

```
src/download_brent_oil_prices.py     0 bytes
src/sources/dane/common.py           0 bytes
src/sources/pwt/__init__.py          0 bytes
```

`download_brent_oil_prices.py` parece un *script* abandonado (la lógica
real vive en `src/sources/fred/brent.py`). `dane/common.py` era un
*placeholder* que nunca llenó. Estos tres archivos deben:
- Eliminarse (`download_brent_oil_prices.py`, `dane/common.py`).
- O completarse: `pwt/__init__.py` debería re-exportar al menos
  `run_pwt_pipeline` para mantener simetría con los otros paquetes.

### 2.5 Referencias desactualizadas a "PWT 10.01"

El proyecto migró a PWT 11.0 (confirmado en `merge.py`, `pwt.py:292`
declarando `pwt_version = "PWT 11.0"`, README), pero quedaron menciones
viejas que confunden al lector y a futuros desarrolladores:

| archivo | línea | contenido |
|---|---|---|
| `src/main.py` | 6 | `# Solo PWT 10.01 (...)` |
| `src/main.py` | 115 | `help="Ejecutar solo el pipeline PWT 10.01..."` |
| `src/config.py` | 640 | `# ── Configuración PWT 10.01 – ...` |
| `src/quality_checks.py` | 700 | `# Validaciones ... para PWT 10.01` |
| `src/quality_checks.py` | 798 | `"""Ejecuta todas las validaciones ... PWT 10.01.` |
| `src/quality_checks.py` | 823 | `logger.info("─── Validaciones ... PWT 10.01 ───")` |
| `src/sources/pwt/pwt.py` | 1 | `"""Extracción ... desde PWT 10.01.` |

`Find & replace` global de `PWT 10.01` → `PWT 11.0` resuelve todo en
≤ 30 segundos.

### 2.6 README con número de tests obsoleto

El README declara "283 tests passing" pero hoy son **340**. Detalle
menor pero da señales sobre frescura de la documentación.

---

## 3. Hallazgos de diseño (prioridad media)

### 3.1 Diccionarios `month_map` duplicados

Existe `month_map` en al menos 6 lugares:
- `config.py:96, 237, 530` (tres dataclasses distintos)
- `sources/dane/ipc.py:111, 133`
- `sources/dane/unemployment.py:114`
- `sources/dane/informality.py:135` (versión local en `_parse_trimestre_date`)

Son todos equivalentes (`{"ene": 1, "feb": 2, …}`). Conviene extraer
**una sola constante canónica** en `src/utils/spanish_dates.py` (o en
`src/sources/dane/common.py`, que ahora está vacío y es el lugar
natural):

```python
MONTH_ABBR_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4,
    "may": 5, "jun": 6, "jul": 7, "ago": 8,
    "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}
MONTH_FULL_ES = {
    "enero": 1, "febrero": 2, ..., "diciembre": 12,
}
```

Y referenciarla desde los dataclasses (`field(default_factory=lambda:
MONTH_ABBR_ES.copy())`) y desde los módulos. Beneficios: reduce DRY, evita
desincronizaciones (p.ej. `andi_agent/scraper.py` define su propio mapa
en español *largo* y ANDI/EOIC usa otro), y centraliza la corrección si
mañana cambia un alias (p.ej. "set" vs "sep").

### 3.2 Subdirectorio `andi_agent/` legacy

`andi_agent/main.py`, `andi_agent/pdf_parser.py`, `andi_agent/scraper.py`
(≈ 1,100 líneas) duplican la funcionalidad de
`src/sources/andi/eoic.py` (1,145 líneas). Convive con un CSV propio en
`andi_agent/outputs/` y caché aparte en `andi_agent/data/`. Es el clásico
caso de "código heredado que sobrevive porque funciona pero contradice
la arquitectura nueva".

Recomendación:
1. Confirmar que `src/sources/andi/eoic.py` cubre todos los casos del
   agente legacy (tests `test_andi.py` ya tienen 720 líneas, pasa).
2. Mover `andi_agent/` a una rama `legacy/andi-agent` o eliminarlo del
   *main*.
3. Si se conserva por motivos históricos, prefijarlo con `_legacy_` y
   añadir un `LEGACY.md` indicando "no usar — reemplazado por
   `src/sources/andi/`."

### 3.3 Falta `pyproject.toml`

El proyecto vive solo con `requirements.txt`. Para un código que ya
usa `from __future__ import annotations`, type hints, dataclasses,
`pytest`, y que se ejecuta con `python -m src.main`, conviene migrar a
`pyproject.toml` con:

```toml
[project]
name = "scraping-nairu"
version = "0.3.0"
requires-python = ">=3.11"
dependencies = ["pandas>=2.1,<3.0", "requests>=2.31,<3.0", ...]

[project.optional-dependencies]
dev = ["pytest>=8.0,<9.0", "ruff", "mypy"]

[tool.pytest.ini_options]
testpaths = ["tests"]

[tool.ruff]
line-length = 100
target-version = "py311"
```

Ventajas: un solo archivo, instalación con `pip install -e .` (resuelve
el `from src.config import …` sin truco de `PYTHONPATH`), permite
publicar como paquete en el futuro.

### 3.4 No hay CI/CD

No se detecta `.github/workflows/`, `.gitlab-ci.yml`, ni tox/nox. Para un
proyecto con 340 tests offline, agregar un workflow mínimo de GitHub
Actions (`pip install -r requirements.txt && pytest`) es trivial y
protegería contra regresiones futuras.

### 3.5 Logging con `logger = logging.getLogger("nairu_pipeline.X")`

Está bien implementado en `setup_logging()`, pero conviene revisar que
todos los módulos sigan la convención jerárquica (`nairu_pipeline.<sub>`)
para que el filtrado por nivel funcione coherentemente. Detectado al
menos un caso fuera del esquema en `andi_agent/main.py` (usa
`logging.basicConfig` global).

### 3.6 Validaciones de calidad — `INFORMALITY_RATE_MIN/MAX`

`run_informality.py` valida con `INFORMALITY_RATE_MIN/MAX`, pero no se
detiene si el valor está fuera de rango — solo emite un `logger.warning`.
Para una variable cuyo rango histórico para Colombia ha sido siempre
40-65 %, vale la pena considerar elevar la severidad a
`QualityCheckError` cuando el valor está manifiestamente fuera de
rango (< 30 % o > 80 %), igual que se hace para desempleo.

### 3.7 Dataclasses con defaults mutables

En `src/config.py`, los `field(default_factory=lambda: {...})` para
`month_map`, `http_headers`, etc. están bien (es el patrón correcto en
dataclasses *frozen*). Solo verificar que no haya un campo usando
`default={}` directamente — `mypy --strict` lo detectaría sin problemas.

---

## 4. Hallazgos de calidad de código (prioridad baja)

- **Type hints**: cobertura amplia, pero algunos módulos mezclan
  `Optional[str]` con `str | None`. Estandarizar a sintaxis 3.10+
  (`str | None`) ya que el proyecto requiere `python 3.11`.
- **Docstrings**: muy buenas en pipelines y `merge.py`; en
  `quality_checks.py` algunas funciones internas (`_check_*`) carecen de
  docstring.
- **`from __future__ import annotations`** se usa consistentemente;
  excelente.
- **Manejo de excepciones**: `run_pipeline` (main.py) captura
  `QualityCheckError` y `Exception` con `sys.exit(1)`. Adecuado para CLI;
  podría enriquecerse para retornar códigos distintos (1 = quality, 2 =
  network, 3 = parsing) si en el futuro se monitorea con cron + alertas.
- **`requests` sin `Session()` reutilizada en informalidad e ipc**: se
  crea un `requests.get` por cada llamada. ANDI sí usa `Session` con
  `Retry` adapter (excelente). Replicar ese patrón en informalidad/ipc
  reduciría latencia y mejoraría resiliencia frente a hipos del DANE.
- **`pd.read_csv(path)` en `load_csv`** carga sin `dtype` ni
  `parse_dates`. Para la columna `date` se convierte después en cada
  consumidor; mover el `parse_dates=["date"]` al `load_csv` simplificaría
  el código aguas abajo (informalidad parsing, merge.py, etc.).

---

## 5. Consideraciones econométricas (rol de economista)

El dataset construido (`nairu_dataset.csv`, 17 columnas × ~860 meses)
está bien dimensionado para los modelos clásicos de NAIRU. Algunos
puntos a tener en cuenta para la siguiente fase:

### 5.1 Variables presentes y su utilidad

| Variable | Frecuencia | Uso típico en NAIRU |
|---|---|---|
| `unemployment_rate` | mensual | TD = base de la curva de Phillips |
| `tgp_rate`, `pet_thousands` | mensual | construir `participation_rate`, `employment_gap` |
| `informality_rate_13c` | trim. móvil → mensual | ajusta TGP "efectiva"; informalidad ≈ holgura oculta |
| `ipc_index` | mensual | derivar **inflación** mensual e interanual (no presente como tal) |
| `Inf_Goal`, `Inf_Rate`, `Core_Inf` | mensual | meta, observada, núcleo — Phillips estándar |
| `brent_usd_per_barrel` | mensual | choque de oferta exógeno |
| `capacity_utilization` | mensual | brecha del producto en alta frecuencia |
| `TES_UVR_1Y`, `TES_PESOS_1Y` | mensual | inflación esperada (1Y break-even) |
| `capital_stock_ck/cn`, `human_capital` | anual | Solow / Cobb-Douglas para producto potencial |

### 5.2 Variable derivada que falta y conviene agregar

**Inflación interanual** del IPC: hoy se tiene `ipc_index` mensual y
`Inf_Rate` (BANREP, ya en variación). Para reconciliar ambas y permitir
el chequeo cruzado, calcular en `merge.py` (o en un módulo
`features.py`):

```python
df["ipc_yoy"] = df["ipc_index"].pct_change(12) * 100
df["ipc_mom"] = df["ipc_index"].pct_change(1) * 100
df["inflation_gap"] = df["Inf_Rate"] - df["Inf_Goal"]
```

Y validar `corr(ipc_yoy, Inf_Rate) > 0.97` como sanity check.

### 5.3 Ruta sugerida para estimar la NAIRU

1. **Modelo backward-looking de Phillips**:
   `π_t - π_t-1 = α + β · (u_t - u*) + γ · (oil_t / oil_t-12 - 1) + ε_t`
   con `u*` constante por tramos (p.ej. pre/post pandemia).
2. **Filtro Kalman / Hodrick-Prescott bivariado** para obtener `u*_t`
   variando suavemente — es lo que usa BANREP en sus *Reportes de
   Política Monetaria*.
3. **Ampliación con informalidad y TGP** (à la Galí–Smets–Wouters):
   reemplazar `u_t` por `slack_t = u_t + λ · informality_t`, justificable
   por la tesis de "subempleo invisible" en Colombia.
4. **Validación**: comparar la NAIRU estimada con la inferida por
   Banco de la República (publica `nairu_estimates_v6.csv` ya está en
   `data/processed/` — útil como benchmark).

### 5.4 Cuidados con la informalidad como serie

La serie EISS arranca en 2021-Q1. Antes de eso, DANE reportaba "ocupados
informales" con una metodología distinta. Para una serie **larga**
(NAIRU pre-2010) hay que empalmar con los anuarios anteriores; el
pipeline actual no lo hace todavía y el README es claro al respecto.
Está bien — pero documentarlo en el modelo final.

### 5.5 Riesgo de *look-ahead* en el merge

El `outer-merge` por `date` no tiene problema mientras se respete la
fecha de publicación. Pero **inflación mensual de mes M se publica en el
mes M+1** (DANE publica el IPC ~5 del mes siguiente). Para *back-testing*
honesto, conviene agregar una columna `publication_lag_days` o
construir versiones *vintage* del dataset. No es un bug — es una
consideración para cuando se pase a estimación.

---

## 6. Fortalezas del proyecto (lo que está bien hecho)

1. **Patrón de 3 capas (scraping → parsing → validación → guardado)** se
   aplica de forma uniforme en cada fuente. Da legibilidad y testeo
   independiente.
2. **Configuración por dataclasses *frozen*** evita mutación accidental
   de URLs, headers, rangos de validación. Excelente práctica.
3. **`quality_checks.py` centralizado** con excepciones tipadas
   (`QualityCheckError`) y validadores por fuente — patrón de "guard
   rails" que rara vez se ve en proyectos de scraping económico.
4. **Tests offline con fixtures sintéticas**: 340 tests pasan en 4.3 s
   sin tocar la red. Permite CI rápido y reproducible.
5. **Manejo robusto de FRED en `brent.py`** (3 niveles de fallback:
   `curl` HTTP/1.1 → `requests` → `urllib`) demuestra cuidado con la
   inestabilidad de la fuente.
6. **Sesión SUAMECA con warm-up** (banrep) muestra entendimiento
   profundo del API real, no solo de la documentación.
7. **Conversión cuidadosa de trimestre móvil → último mes** en
   informalidad — convención macro colombiana correcta.
8. **`merge.py` con `MERGED_COLUMNS` explícita** garantiza orden
   determinístico de columnas en `nairu_dataset.csv`.

---

## 7. Plan de acción recomendado (en orden)

| # | Acción | Esfuerzo | Impacto |
|---|---|---|---|
| 1 | Corregir `run_all.py` para incluir PWT + informalidad | 5 min | Alto |
| 2 | Re-exportar `run_informality_pipeline` en `dane/__init__.py` | 2 min | Alto |
| 3 | Quitar `verify=False` de `informality.py` | 5 min | Alto (seguridad) |
| 4 | Eliminar 3 archivos vacíos / completar `pwt/__init__.py` | 5 min | Medio |
| 5 | Find & replace global `PWT 10.01` → `PWT 11.0` | 5 min | Medio |
| 6 | Actualizar contador de tests en README (283 → 340) | 1 min | Bajo |
| 7 | Consolidar `month_map` en una sola constante | 30 min | Medio |
| 8 | Decidir destino de `andi_agent/` (eliminar o mover a rama legacy) | 15 min | Medio |
| 9 | Migrar a `pyproject.toml` + agregar Ruff/Mypy | 1-2 h | Medio |
| 10 | Agregar GitHub Actions workflow básico | 30 min | Medio |
| 11 | Agregar variables derivadas (`ipc_yoy`, `inflation_gap`) en `merge.py` | 30 min | Alto (econometría) |
| 12 | Considerar elevar warning a error en `INFORMALITY_RATE` fuera de rango | 10 min | Bajo |

Las primeras 6 son arreglos triviales que pueden agruparse en un solo
PR de "limpieza primaveral". Las siguientes 4 son mejoras de plataforma.
La #11 es la única que requiere criterio econométrico — y conviene
hacerla **antes** de empezar a estimar la NAIRU, no después.

---

## 8. Conclusión

El repositorio está en muy buena forma para su objetivo: construir
un dataset macro mensual robusto y reproducible para estimar la NAIRU
colombiana. La base ingenieril es sólida y la cobertura de pruebas es
excelente. Los hallazgos son acotados, casi todos solucionables en una
tarde, y ninguno compromete la validez de los datos ya generados.

El salto siguiente — pasar de pipeline de datos a **modelo
econométrico** — está perfectamente habilitado por la estructura
existente. Recomiendo abordar los puntos 1-6 en un PR de limpieza,
luego 11 (variables derivadas), y a partir de ahí empezar la
implementación del modelo de Phillips/Kalman como un paquete nuevo
`src/models/nairu/`, manteniendo el patrón modular que ya funciona.

---

## 9. Estado al cierre (2026-05-17)

Todos los ítems de la sección 7 cerrados en commits posteriores al
2026-04-28. Resumen:

| # | Acción | Estado | Commit / nota |
|---|---|---|---|
| 1 | `run_all.py` completo | ✅ | incluye PWT, informality, dane_gdp, viog, production_function |
| 2 | `dane/__init__.py` re-exporta informalidad | ✅ | |
| 3 | `verify=False` en informalidad | ✅ corregido + 4 scrapers GDP centralizados vía `dane_request_kwargs` (env override) |
| 4 | Archivos vacíos | ✅ | `dane/common.py` poblado; `pwt/__init__.py` re-exporta; `download_brent_oil_prices.py` eliminado |
| 5 | `PWT 10.01` → `PWT 11.0` | ✅ | bloque B1 |
| 6 | Contador de tests en README | ✅ | bloque D1 |
| 7 | Consolidar `month_map` | ⏸ Diferido a v0.3 | trabajo > 30 min, beneficio bajo |
| 8 | Destino de `andi_agent/` | ✅ | movido a `legacy/andi_agent/` (bloque B3) |
| 9 | `pyproject.toml` | ✅ | ya existía + exclude `legacy/` |
| 10 | GitHub Actions | ✅ | `.github/workflows/tests.yml` |
| 11 | Variables derivadas | ✅ | bloque C1 |
| 12 | `INFORMALITY_RATE` warning → error | ⏸ Diferido a v0.3 | decisión econométrica, no urgente |
