# Plan de cierre del repositorio `scraping-NAIRU`

**Audiencia:** agente Sonnet que ejecutará este plan paso a paso.
**Fecha:** 2026-05-17
**Objetivo:** llevar el repo al estado **v0.2.0 — pipeline completo y publicable**, cerrando los pendientes de `REVIEW.md` que aún viven en el código y limpiando el working tree. Sin trabajo nuevo de modelado: solo lo necesario para cerrar.

---

## 0. Reglas de trabajo (no negociables)

1. **Rama:** trabajar **siempre** en `sandbox`. No crear ramas `Agent/*` ni `feature/*`. Antes de empezar: `git checkout sandbox && git status`.
2. **Idioma:** todos los commits, comentarios y docstrings nuevos en español neutro, manteniendo el estilo existente.
3. **Tests primero:** después de cada bloque, correr `pytest -q` y confirmar verde. Si rompe, **arreglar antes de avanzar** — no acumular deuda.
4. **Commits atómicos:** un commit por bloque (A, B1, B2, …). Mensaje corto en imperativo, ej. `Cleanup working tree and ignore Excel locks`.
5. **Nada de scope creep:** si encuentras algo fuera de este plan, anotarlo al final en la sección "Diferido a v0.3"; no implementarlo.
6. **No tocar `andi_agent/` salvo lo indicado en B3.** Cualquier reescritura "porque vi código duplicado" es trabajo perdido — la decisión ya está tomada (mover a legacy).
7. **No ejecutar pipelines reales** (descarga de DANE/BANREP/PWT) salvo que el bloque lo pida explícitamente. La validación es vía pytest.

---

## 1. Estado actual (resumen verificado el 2026-05-17)

- `sandbox` y `Agent` apuntan al mismo commit `799b833`. Toda la historia útil vive en `sandbox`.
- Working tree no limpio:
  - `deleted: data/inputs/~$Boceto Estimación PIB Potencial.xlsx` (lock de Excel)
  - `untracked: andi_agent/agent_llm.py`
  - `untracked: data/inputs/Boceto Estimación PIB Potencial.xlsx`
- `REVIEW.md` (2026-04-28) lista 12 ítems. Cerrados en commits posteriores: 1, 2, 3 (informality), 4, 9 (pyproject), 10 (workflow `tests.yml`). Pendientes reales abajo.

---

## Bloque A — Limpieza del working tree (≤ 10 min)

**Objetivo:** que `git status` quede limpio sin perder archivos importantes.

### A1. Ignorar archivos lock de Excel y desindexar el residual

1. Añadir al final de `.gitignore`:

   ```gitignore
   # Excel lock files (Office crea estos al abrir un .xlsx)
   ~$*
   *.~lock.*
   ```

2. Desindexar el lock que quedó tracked por accidente:

   ```bash
   git rm --cached "data/inputs/~\$Boceto Estimación PIB Potencial.xlsx"
   ```

   El archivo ya no existe en disco; este `rm --cached` confirma la eliminación en el index.

### A2. Decidir destino de `data/inputs/Boceto Estimación PIB Potencial.xlsx`

Este es un insumo manual del profesor. Política del repo (ver `data/inputs/.gitkeep`): los `.xlsx` de `data/inputs/` que se usan como insumo del pipeline **se versionan** (igual que `Data_NAIRU.xlsx`, `PIB_CO.xlsx`, `FUNCION DE PRODUCCION.xlsx`).

→ Acción: `git add "data/inputs/Boceto Estimación PIB Potencial.xlsx"`.

### A3. Decidir destino de `andi_agent/agent_llm.py`

Este archivo introduce dependencia opcional de `ollama` y solo se usa desde la CLI del agente legacy. **No commitearlo en `andi_agent/` directamente** — el bloque B3 mueve ese directorio entero a legacy. En su lugar:

1. Mover el archivo a un staging dentro de `andi_agent/` y dejarlo ahí; el commit final del bloque B3 lo arrastra con el resto del directorio renombrado.
2. Si `B3.opción = "eliminar"`, este archivo se elimina con el resto y no entra al árbol.

→ Por ahora: **no agregar al index**. Dejarlo untracked hasta B3.

### A4. Verificar

```bash
git status
# Esperado: solo "Untracked: andi_agent/agent_llm.py" y los cambios staged del .gitignore + xlsx.
git diff --staged
pytest -q   # debe pasar
```

**Commit:** `Cleanup working tree: ignore Excel locks, add PIB Potencial xlsx input`

---

## Bloque B — Limpieza de código (≤ 45 min)

### B1. Eliminar referencias residuales a "PWT 10.01"

El proyecto ya usa PWT 11.0 (ver `src/sources/pwt/pwt.py:292` → `pwt_version = "PWT 11.0"`). Quedan 3 menciones obsoletas:

| Archivo | Línea | Cambio |
|---|---|---|
| `src/config.py` | 879 | `# ── Configuración PWT 10.01 …` → `# ── Configuración PWT 11.0 …` |
| `src/main.py` | 6 | `# Solo PWT 10.01 …` → `# Solo PWT 11.0 …` |
| `src/main.py` | 166 | `help="Ejecutar solo el pipeline PWT 10.01 …"` → `help="Ejecutar solo el pipeline PWT 11.0 …"` |

Verificación final:

```bash
grep -rn "PWT 10\.01" src/ tests/ docs/ README.md
# Esperado: sin matches.
pytest -q
```

**Commit:** `Update residual PWT 10.01 references to PWT 11.0`

### B2. Tratar `verify=False` en scrapers DANE de PIB

Aparece en 4 archivos:

- `src/sources/dane/gdp.py:59, 127`
- `src/sources/dane/gdp_expenditure.py:61, 121`
- `src/sources/dane/gdp_income.py:55, 102`
- `src/sources/dane/gdp_historical.py:67`

**Decisión técnica:** el portal `www.dane.gov.co` históricamente ha tenido problemas con la cadena de certificados (mismo motivo por el que el ítem se introdujo). **No quitarlo a ciegas** — rompería la descarga real. La política correcta:

1. **Centralizar** el patrón en `src/sources/dane/common.py` (que ya tiene 3946 bytes y es el lugar natural):

   ```python
   # En src/sources/dane/common.py
   import os
   import warnings
   import urllib3

   def dane_request_kwargs(timeout: float = 30.0) -> dict:
       """Kwargs estándar para `requests.get` contra www.dane.gov.co.

       DANE tiene problemas recurrentes de cadena de certificados. Permitimos
       saltarse la verificación TLS controlado por la variable de entorno
       ``DANE_VERIFY_TLS`` (default: ``0`` → no verifica). Producción puede
       exportarla a ``1`` cuando DANE arregle el certificado.
       """
       verify_tls = os.environ.get("DANE_VERIFY_TLS", "0") == "1"
       if not verify_tls:
           warnings.filterwarnings(
               "ignore", category=urllib3.exceptions.InsecureRequestWarning,
           )
       return {"timeout": timeout, "verify": verify_tls}
   ```

2. **Reemplazar** en cada uno de los 4 archivos:

   ```python
   resp = requests.get(url, timeout=timeout, verify=False)
   ```

   por:

   ```python
   from src.sources.dane.common import dane_request_kwargs
   resp = requests.get(url, **dane_request_kwargs(timeout=timeout))
   ```

3. **Documentar** en `README.md` (sección 3 "Instalación" o nueva subsección "Variables de entorno"):

   ```markdown
   - `DANE_VERIFY_TLS=1` activa verificación TLS contra `www.dane.gov.co`
     (default `0` por problemas históricos con la cadena de certificados).
   ```

Verificación: los tests del bloque GDP (`tests/test_dane_gdp.py`, etc.) usan fixtures locales y no tocan red, así que deben seguir pasando.

```bash
grep -rn "verify=False" src/
# Esperado: 0 matches (solo el docstring informativo en common.py si lo dejas).
pytest -q tests/test_dane_gdp.py tests/test_pipeline.py
```

**Commit:** `Centralize DANE TLS handling via dane_request_kwargs + env override`

### B3. Decisión final sobre `andi_agent/` legacy

`andi_agent/` (1504 líneas + caches + `agent_llm.py` nuevo) duplica `src/sources/andi/eoic.py` (consolidado, con tests en `tests/test_andi.py`). El nuevo `agent_llm.py` agrega LLM (Ollama) como fallback — útil para validación manual, pero no entra al pipeline reproducible.

**Decisión adoptada:** mover `andi_agent/` a `legacy/andi_agent/` dentro del repo y marcarlo como no-productivo. Razones:

- Mantiene el código accesible para referencia (Ollama + cache + scraper PDF tienen valor histórico).
- Saca el directorio del path por defecto de los tools (`pyproject.toml`, IDE, pytest).
- No requiere mantener una rama paralela `legacy/andi-agent`.

Acciones:

1. ```bash
   mv andi_agent legacy/
   mkdir -p legacy && mv andi_agent legacy/andi_agent  # si la línea de arriba falla
   ```

   (En la práctica, hacer en un solo paso con `git mv andi_agent legacy/andi_agent`, creando `legacy/` si hace falta.)

2. Agregar `legacy/agent_llm.py` al árbol cuando se mueva (viene como untracked en working tree).

3. Crear `legacy/README.md` con este contenido literal:

   ```markdown
   # `legacy/` — código no productivo

   Este directorio conserva código histórico que **ya no forma parte del
   pipeline reproducible**. Se mantiene por referencia y para reproducir
   experimentos del periodo de prototipado, no para uso en producción.

   ## `legacy/andi_agent/`

   Agente standalone para procesar PDFs de la encuesta EOIC de ANDI con
   parser regex + fallback a LLM local vía Ollama. **Sustituido por**
   `src/sources/andi/eoic.py`, que está integrado al pipeline, tiene
   tests offline (`tests/test_andi.py`) y respeta el patrón de tres
   capas. No usar `legacy/andi_agent/` para alimentar `nairu_dataset.csv`.
   ```

4. Excluir `legacy/` de:

   - `pyproject.toml` → en `[tool.setuptools.packages.find]`, agregar `exclude = ["legacy*", "tests*"]` (o equivalente según la config actual).
   - `pyproject.toml` → en `[tool.pytest.ini_options]`, asegurar `testpaths = ["tests"]` y agregar `norecursedirs = ["legacy", ".venv", "data"]`.
   - `.gitignore` → agregar `legacy/**/__pycache__/`, `legacy/**/outputs/`, `legacy/**/data/raw/` (caches locales que no deben versionarse).

5. Verificar que **nada en `src/`** importa de `andi_agent`:

   ```bash
   grep -rn "from andi_agent\|import andi_agent" src/ tests/
   # Esperado: sin matches.
   ```

6. Tests verdes:

   ```bash
   pytest -q
   ```

**Commit:** `Move andi_agent to legacy/ (superseded by src/sources/andi/eoic.py)`

---

## Bloque C — Variables derivadas en `merge.py` (≤ 30 min)

### C1. Añadir `ipc_yoy`, `ipc_mom`, `inflation_gap`

Fundamento econométrico: hoy el dataset tiene `ipc_index` (índice base 2018) y `Inf_Rate` (BANREP, ya en variación interanual). Tener `ipc_yoy` calculada del propio `ipc_index` permite (a) chequeo cruzado contra BANREP, (b) modelos que prefieren la serie reconstruida a la publicada (vintage-honest).

Implementación en `src/merge.py`, dentro de `merge_all_sources` justo antes del `return`:

```python
# ── Variables derivadas ─────────────────────────────────────
# IPC: variación interanual y mensual reconstruida del propio índice.
merged["ipc_yoy"] = merged["ipc_index"].pct_change(12) * 100
merged["ipc_mom"] = merged["ipc_index"].pct_change(1) * 100
# Brecha de inflación: desviación de la observada vs la meta BANREP.
merged["inflation_gap"] = merged["Inf_Rate"] - merged["Inf_Goal"]
```

Agregar las tres columnas a `MERGED_COLUMNS` (orden: justo después de `Core_Inf`, antes de `brent_usd_per_barrel`):

```python
"Core_Inf",
"ipc_yoy",            # IPC variación interanual (derivada de ipc_index)
"ipc_mom",            # IPC variación mensual (derivada de ipc_index)
"inflation_gap",      # Inf_Rate - Inf_Goal (BANREP)
"brent_usd_per_barrel",
```

### C2. Sanity check en `quality_checks.py`

Agregar una función `run_derived_checks(df)` que valide:

- `corr(ipc_yoy, Inf_Rate) > 0.97` para fechas con ambas series no nulas (acepta hasta 2 nulos para los primeros 12 meses).
- `(ipc_mom * 12)` y `ipc_yoy` no deben divergir más de 5 pp para el 90 % de las filas (aproximación bruta — la diferencia esperada es por composición geométrica).

Llamarla desde `run_merge.run()` después de `save_merged_dataset`.

### C3. Tests

Agregar `tests/test_merge_derived.py` con casos:

- Dataset sintético con `ipc_index = 100 * (1.05) ** (mes/12)` → `ipc_yoy ≈ 5.0` en t ≥ 12.
- Caso con `Inf_Rate=4.0, Inf_Goal=3.0` → `inflation_gap=1.0`.
- Caso con menos de 12 meses → `ipc_yoy` debe ser NaN sin romper.

```bash
pytest -q tests/test_merge_derived.py
pytest -q   # toda la suite verde
```

**Commit:** `Add derived columns ipc_yoy, ipc_mom, inflation_gap to merged dataset`

---

## Bloque D — Cierre y documentación (≤ 30 min)

### D1. Actualizar contador de tests en README

Después de los bloques A-C, recontar:

```bash
pytest -q --collect-only | tail -3
```

Reemplazar en `README.md`:

- Línea 3: `tests-340%20passing` → `tests-<N>%20passing`
- Línea 158: `340 tests offline` → `<N> tests offline`

(Asumir que el número subió por los tests nuevos de C3.)

### D2. Actualizar `REVIEW.md` con estado final

Al final de `REVIEW.md`, después de la sección 8, agregar:

```markdown
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
```

### D3. CHANGELOG mínimo

Crear `CHANGELOG.md` en la raíz (no existe hoy):

```markdown
# Changelog

## [0.2.0] — 2026-05-17

### Agregado
- Variables derivadas `ipc_yoy`, `ipc_mom`, `inflation_gap` en
  `nairu_dataset.csv`.
- `src/sources/dane/common.py::dane_request_kwargs()` para centralizar
  el manejo de TLS contra DANE (variable de entorno `DANE_VERIFY_TLS`).
- `legacy/README.md` documentando el código histórico.

### Cambiado
- `verify=False` reemplazado por `dane_request_kwargs(...)` en los 4
  scrapers de PIB DANE.
- `andi_agent/` movido a `legacy/andi_agent/` (sustituido por
  `src/sources/andi/eoic.py`).
- Referencias residuales a "PWT 10.01" actualizadas a "PWT 11.0".

### Removido
- Archivos lock de Excel del index (`~$*` ahora en `.gitignore`).

## [0.1.0] — 2026-04-28
- Versión inicial revisada (`REVIEW.md`).
```

### D4. Etiqueta de release

```bash
pytest -q   # última corrida, debe ser verde
git push origin sandbox
# Si el usuario está conforme, merge a main:
#   git checkout main && git merge --ff-only sandbox && git push origin main
#   git tag -a v0.2.0 -m "Pipeline NAIRU Colombia, dataset consolidado v0.2.0"
#   git push origin v0.2.0
```

**No ejecutar el tag ni el merge a main sin confirmación del usuario.** Dejar el repo listo en `sandbox` con todos los commits A1-D3 y notificar.

---

## Criterio de "done"

El bloque está completo cuando:

1. `git status` en `sandbox` reporta `nothing to commit, working tree clean`.
2. `pytest -q` pasa en verde (incluyendo los tests nuevos del bloque C3).
3. `grep -rn "PWT 10\.01" src/ docs/ README.md` no encuentra nada.
4. `grep -rn "verify=False" src/` no encuentra nada (solo el docstring en `common.py` si quedó por explicación).
5. `andi_agent/` ya no existe en raíz; `legacy/andi_agent/` sí.
6. `nairu_dataset.csv` (si se regenera) tiene 24 columnas (21 originales + 3 derivadas).
7. `CHANGELOG.md` y `REVIEW.md §9` reflejan el estado de cierre.

---

## Diferido a v0.3 (no ejecutar en este plan)

- Consolidar `month_map` en una única constante (`REVIEW.md §3.1`).
- Subir severidad de validación de informalidad de warning a error (`REVIEW.md §3.6`).
- Reusar `requests.Session` con retries en informality/ipc (`REVIEW.md §4`).
- `pd.read_csv(..., parse_dates=["date"])` centralizado en `load_csv` (`REVIEW.md §4`).
- Implementación del modelo de Phillips backward-looking en `src/nairu/models/` (queda fuera del cierre de pipeline; el Kalman ya está en `src/nairu/model_core.py`).

---

## Anti-objetivos (lo que **no** debe hacerse en este plan)

- ❌ Refactorizar `src/nairu/model_core.py` (funciona, está testeado, no es parte del cierre).
- ❌ Cambiar el esquema de `MERGED_COLUMNS` más allá de añadir las 3 derivadas.
- ❌ Cambiar formato de columnas existentes (no renombrar, no reordenar).
- ❌ Borrar `andi_agent/` físicamente. La política es moverlo a `legacy/`.
- ❌ Tocar `.venv/`, `logs/`, `outputs/` (caches locales).
- ❌ Crear ramas nuevas. Todo va a `sandbox`.
- ❌ Pedir aprobación intermedia al usuario para cambios mecánicos (PWT 10.01, .gitignore). Sí pedir para D4 (merge/tag).
