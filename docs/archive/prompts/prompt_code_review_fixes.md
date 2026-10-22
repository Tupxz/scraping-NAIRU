# Prompt — Code Review Fixes (NAIRU Colombia Pipeline)

Eres un desarrollador senior de Python y economista cuantitativo. Debes corregir
los bugs, inconsistencias y problemas de organización identificados en el repositorio
`scraping-NAIRU` (branch `sandbox`). A continuación se describen todos los problemas
con su ubicación exacta, causa raíz y corrección esperada.

---

## Contexto del proyecto

Pipeline de datos en Python 3.12 para construir la base empírica de la NAIRU en
Colombia. Estructura principal:

```
src/
  config.py          — configuración central (dataclasses frozen)
  main.py            — CLI con argparse
  merge.py           — merge de todas las bases
  pipelines/
    run_all.py       — ejecuta todos los pipelines
    run_viog.py
    run_unemployment.py
    ...
  sources/
    viog/viog.py     — pipeline VIOG (output gap ponderado)
    ...
data/
  processed/         — CSVs y Excels procesados
  raw/               — datos crudos descargados
tests/
  test_viog.py
  ...
notebooks/
  VIOG .ipynb        — notebook con espacio en el nombre
  VIOG_original.ipynb
  _explore_informality.py  ← está en la RAÍZ, no en notebooks/
```

---

## BUG 1 — `run_all.py` no incluye todos los pipelines

**Archivo:** `src/pipelines/run_all.py`

**Problema:** `run_all.py` omite `run_pwt`, `run_viog` y `run_informality`.
El flag `--all` en `main.py` sí los incluye (llama a `run_pipeline(run_pwt=True,
run_viog=True, run_informality=True, ...)`), pero `run_all.py` no los ejecuta.
Inconsistencia entre los dos puntos de entrada.

**Código actual:**
```python
def run() -> None:
    """Ejecuta todos los pipelines + merge secuencialmente."""
    run_unemployment.run()
    run_ipc.run()
    run_banrep_inflation.run()
    run_banrep_tes.run()
    run_brent.run()
    run_andi.run()
    run_merge.run()
```

**Corrección esperada:** agregar los tres pipelines faltantes en el orden lógico
(primero los de datos de insumo, luego el merge):
```python
def run() -> None:
    """Ejecuta todos los pipelines + merge secuencialmente."""
    run_unemployment.run()
    run_informality.run()
    run_ipc.run()
    run_banrep_inflation.run()
    run_banrep_tes.run()
    run_brent.run()
    run_andi.run()
    run_pwt.run()
    run_viog.run()
    run_merge.run()
```
También añadir los imports faltantes: `run_pwt`, `run_viog`, `run_informality`.

---

## BUG 2 — Variable muerta `_GAP_VARS` en `viog.py`

**Archivo:** `src/sources/viog/viog.py`, línea 34

**Problema:** La variable `_GAP_VARS` se define pero **nunca se usa** en ninguna
función del módulo. El comentario dice "se sobreescribe en runtime" pero es falso —
el código usa directamente `_GAP_VARS_WITH_REF` y `_GAP_VARS_WITHOUT_REF`.
Es código muerto que confunde al lector.

**Código actual:**
```python
_GAP_VARS_WITH_REF    = ["bk", "cf", "bw", "hp", "kalman", "ref"]
_GAP_VARS_WITHOUT_REF = ["bk", "cf", "bw", "hp", "kalman"]
_GAP_VARS = _GAP_VARS_WITH_REF  # default; se sobreescribe en runtime si no hay ref
```

**Corrección esperada:** eliminar la línea `_GAP_VARS = ...` completa.

---

## BUG 3 — Docstring del módulo `viog.py` desactualizado

**Archivo:** `src/sources/viog/viog.py`, línea 17

**Problema:** El docstring dice:
```
- Kalman usa UnobservedComponents con ciclo estocástico amortiguado.
```
Pero el modelo Kalman actual usa `level="random walk with drift"` + `cycle=True`
con `fit(disp=False)` — que sí usa ciclo estocástico amortiguado. Lo que está
desactualizado es la sección de pasos que dice `"Kalman/UCM"` sin mencionar los
parámetros reales. Actualizar el docstring para que refleje el modelo actual:

```
- Kalman usa UnobservedComponents(level="random walk with drift", cycle=True,
  damped_cycle=True, stochastic_cycle=True, cycle_period_bounds=[0.1, 40]).
  El nivel suavizado se usa como tendencia (result.level.smoothed).
```

---

## BUG 4 — Defaults incorrectos en `load_series`

**Archivo:** `src/sources/viog/viog.py`, línea ~47

**Problema:** La función `load_series` tiene como defaults:
```python
def load_series(path, series_col="PIB", ref_col="Potential_PIB"):
```
Pero el Excel real `PIB_USA.xlsx` tiene columnas `Value(Billions)` y
`Potential Value(Billions)`. Los defaults son incorrectos para el archivo real y
solo funcionan para el fixture sintético de los tests (que sí usa `PIB`/`Potential_PIB`).

El pipeline `run_viog_pipeline` ya tiene los defaults correctos:
```python
def run_viog_pipeline(..., series_col="Value(Billions)", ref_col="Potential Value(Billions)"):
```
pero `load_series` queda con defaults engañosos.

**Corrección esperada:** mantener los defaults de `load_series` como `"PIB"` /
`"Potential_PIB"` (para compatibilidad con tests), pero agregar un comentario
claro que documente que el archivo real usa nombres distintos, y que `run_viog_pipeline`
los sobreescribe:

```python
def load_series(
    path: Path,
    series_col: str = "PIB",           # tests usan "PIB"; archivo real usa "Value(Billions)"
    ref_col: Optional[str] = "Potential_PIB",  # tests usan "Potential_PIB"; real usa "Potential Value(Billions)"
) -> pd.DataFrame:
```

---

## BUG 5 — `VIOG_PROCESSED_COLUMNS` en `config.py` tiene columna incorrecta

**Archivo:** `src/config.py`, línea ~743

**Problema:** `VIOG_PROCESSED_COLUMNS` incluye `"gap_potential"`, pero el CSV
real `viog_usa.csv` guarda esa columna como `"gap_ref"`. La columna `gap_potential`
no existe en el output del pipeline.

**Código actual:**
```python
VIOG_PROCESSED_COLUMNS: list[str] = [
    "date", "year", "quarter",
    "gap_viog", "gap_inv_viog",
    "gap_potential",      # ← INCORRECTO
    "gap_hp", "gap_cf", "gap_bk", "gap_bw", "gap_kalman",
    "source",
]
```

**Corrección esperada:**
```python
VIOG_PROCESSED_COLUMNS: list[str] = [
    "date", "year", "quarter",
    "gap_viog", "gap_inv_viog",
    "gap_ref",            # ← nombre real en viog_usa.csv
    "gap_hp", "gap_cf", "gap_bk", "gap_bw", "gap_kalman",
    "source",
]
```

---

## BUG 6 — Comentario desactualizado en `VIOGConfig.kalman_cycle_period_bounds`

**Archivo:** `src/config.py`, línea ~737

**Problema:** El comentario dice "ciclos de 32 a 40 trimestres" pero el valor
es `(0.1, 40.0)`. El comentario quedó de una versión anterior.

**Corrección esperada:** actualizar el comentario para que coincida con el valor:
```python
# Kalman UCM — bounds del período del ciclo en trimestres (notebook original: [0.3, 40])
kalman_cycle_period_bounds: tuple[float, float] = (0.1, 40.0)
```

---

## TAREA DE ORGANIZACIÓN 1 — Mover script de exploración a `notebooks/`

**Archivo:** `_explore_informality.py` (en raíz del proyecto)

**Problema:** Es un script de exploración que está en la raíz del proyecto,
no en `notebooks/`. Los scripts de exploración no deben estar en la raíz.

**Corrección:** mover el archivo a `notebooks/explore_informality.py`
(renombrar quitando el guion bajo inicial). Si el archivo no contiene lógica
útil, eliminarlo.

---

## TAREA DE ORGANIZACIÓN 2 — Eliminar archivo vacío

**Archivo:** `src/download_brent_oil_prices.py`

**Problema:** Archivo Python de 0 bytes. No contiene nada. La lógica de
descarga de Brent está en `src/sources/fred/brent.py`. Este archivo vacío
genera confusión sobre dónde está la implementación real.

**Corrección:** eliminar el archivo.

---

## TAREA DE ORGANIZACIÓN 3 — Renombrar notebook con espacio en el nombre

**Archivo:** `notebooks/VIOG .ipynb` (tiene un espacio antes de `.ipynb`)

**Problema:** El espacio en el nombre del archivo genera problemas en terminales,
scripts de shell y algunos sistemas de archivos.

**Corrección:** renombrar a `notebooks/VIOG.ipynb` usando `git mv`:
```bash
git mv "notebooks/VIOG .ipynb" "notebooks/VIOG.ipynb"
```

---

## TAREA DE ORGANIZACIÓN 4 — Documentar archivos huérfanos en `data/processed/`

Los siguientes archivos están en `data/processed/` pero **ningún módulo del
pipeline los referencia**. Deben ser documentados o eliminados:

| Archivo | Acción recomendada |
|---|---|
| `PIB_USA_tupaz.xlsx` | Documentar origen en README o eliminar |
| `PIB.xlsx` | Documentar origen en README o eliminar |
| `viog_filters.csv` | Eliminar (parece output intermedio de exploración) |
| `nairu_estimates_v6.csv` | Documentar origen en README o eliminar |

---

## BASES DE DATOS QUE NO SE DESCARGAN AUTOMÁTICAMENTE

Los siguientes archivos son **inputs manuales** sin pipeline de descarga:

| Archivo | Fuente real | Estado |
|---|---|---|
| `data/processed/PIB_USA.xlsx` | FRED (GDPC1) + CBO (GDPPOT) | **No tiene scraper.** El pipeline VIOG depende de este archivo pero no lo descarga. Hay archivos `data/raw/fred/GDPC1.xlsx` y `GDPPOT.xlsx` que tampoco tienen pipeline de descarga |
| `data/raw/pwt/2026-04-14T12-33_export.csv` | Penn World Tables 11.0 — pwt.rug.nl | Descarga manual, sin scraper automatizado |

**Para el VIOG (prioridad alta):** crear un pipeline `src/sources/fred/pib_usa.py`
que descargue `GDPC1` y `GDPPOT` desde la API de FRED (`fredapi` o requests directo
a `https://fred.stlouisfed.org/graph/fredgraph.csv?id=GDPC1`) y construya
`PIB_USA.xlsx` con columnas `Year`, `Quarter`, `Value(Billions)`,
`Potential Value(Billions)`.

---

## Restricciones importantes

1. **No romper los tests existentes.** Hay más de 370 tests pasando. Después de
   cada cambio verificar con `python -m pytest tests/ -x -q`.
2. **No cambiar la lógica econométrica** de ningún filtro. Solo corregir bugs
   de código, comentarios y organización.
3. **Los defaults de `load_series`** deben seguir siendo `"PIB"` / `"Potential_PIB"`
   para mantener compatibilidad con los tests sintéticos.
4. El archivo `data/processed/PIB_USA.xlsx` **no debe eliminarse** — es el input
   activo del pipeline VIOG hasta que exista un scraper automatizado.
5. La carpeta `andi_agent/` es código legacy — **no modificarla**, solo
   documentar en README que está deprecada.

---

## Verificación final esperada

Después de aplicar todas las correcciones:

```bash
# Tests deben pasar sin errores
python -m pytest tests/ -x -q

# Pipeline completo debe correr sin errores
python -m src.main --all

# VIOG específicamente
python -m src.main --viog
```
