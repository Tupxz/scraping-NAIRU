# Prompt: Implementar nuevo pipeline en scraping-NAIRU

## Rol
Eres un ingeniero de datos experto en macroeconometría Python. Implementarás un pipeline
nuevo siguiendo **exactamente** las convenciones del proyecto.

---

## Convenciones del proyecto

### Estructura de archivos
```
src/
  config.py                        ← @dataclass(frozen=True) + instancia global + constantes
  sources/<nombre>/__init__.py     ← vacío
  sources/<nombre>/<nombre>.py     ← lógica de scraping/parsing/cálculo
  pipelines/run_<nombre>.py        ← runner (llama run() sin args)
tests/
  test_<nombre>.py                 ← ≥ 25 tests, clases por funcionalidad
data/
  processed/<nombre>.csv           ← output estándar
```

### Patrón config (`src/config.py`)
```python
@dataclass(frozen=True)
class <Nombre>Config:
    """Docstring descriptivo."""
    input_filename: str = "archivo.xlsx"
    processed_filename: str = "<nombre>.csv"
    source_label: str = "FUENTE/ENTIDAD"
    # ... parámetros específicos

<NOMBRE>_CONFIG = <Nombre>Config()

<NOMBRE>_PROCESSED_COLUMNS: list[str] = ["date", "year", ...]
```

### Patrón source (`src/sources/<nombre>/<nombre>.py`)
```python
"""Docstring: qué hace, pipeline de N capas numeradas."""
from __future__ import annotations
import logging
from pathlib import Path
import pandas as pd
from src.config import <NOMBRE>_CONFIG

logger = logging.getLogger("nairu_pipeline.<nombre>")

def load_...(path: Path) -> pd.DataFrame: ...
def compute_...(df: pd.DataFrame) -> pd.DataFrame: ...
def run_<nombre>_pipeline(input_path: Path, output_path: Path) -> pd.DataFrame:
    """Pipeline completo. Guarda CSV y devuelve DataFrame."""
    ...
```

### Patrón runner (`src/pipelines/run_<nombre>.py`)
```python
"""Pipeline runner para <nombre>."""
from __future__ import annotations
from src.config import PROCESSED_DIR, <NOMBRE>_CONFIG
from src.sources.<nombre>.<nombre> import run_<nombre>_pipeline

def run() -> None:
    input_path = PROCESSED_DIR / <NOMBRE>_CONFIG.input_filename
    output_path = PROCESSED_DIR / <NOMBRE>_CONFIG.processed_filename
    df = run_<nombre>_pipeline(input_path, output_path)
    print(f"[<NOMBRE>] {len(df)} observaciones guardadas en {output_path}")
```

### Patrón tests (`tests/test_<nombre>.py`)
```python
import pytest
import numpy as np
import pandas as pd
from src.sources.<nombre>.<nombre> import load_..., compute_...

@pytest.fixture()
def synthetic_df() -> pd.DataFrame:
    """Fixture con datos mínimos para tests unitarios."""
    ...

class TestLoad<Nombre>:        # 5-6 tests: carga, columnas, índice, sin nulls
class TestCompute<Nombre>:     # 5-6 tests: fórmulas, tipos, edge cases
class TestRun<Nombre>Pipeline: # 6-8 tests con archivo REAL (scope="class")
    @pytest.fixture(scope="class")
    def pipeline_output(self, tmp_path_factory):
        ...
```

### Integración en `src/merge.py`
```python
# En _SOURCES:
"<nombre>": ("<nombre>.csv", ["col1", "col2"]),

# En MERGED_COLUMNS (al final de la sección correspondiente):
"col1",   # descripción
"col2",   # descripción
```

### Integración en `src/main.py`
```python
# En run_pipeline():
run_<nombre>: bool = False,
# ...
if run_<nombre>:
    from src.pipelines import run_<nombre> as <nombre>_pipeline
    <nombre>_pipeline.run()

# En argparse:
parser.add_argument("--<nombre>", action="store_true", help="...")

# En --all:
run_<nombre>=True,

# En any_selected:
args.<nombre> or ...
```

---

## Columnas estándar de salida

Todo CSV procesado debe incluir:

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `date`  | datetime | Primer día del período (mensual o trimestral) |
| `year`  | int | Año |
| `month` o `quarter` | int | Mes o trimestre según frecuencia |
| `source` | str | Etiqueta de la fuente (e.g. `"DANE"`, `"FRED/CBO"`) |

---

## Checklist de entrega

- [ ] `src/sources/<nombre>/__init__.py` (vacío)
- [ ] `src/sources/<nombre>/<nombre>.py` con funciones públicas documentadas
- [ ] `src/pipelines/run_<nombre>.py`
- [ ] `src/config.py` — añadir dataclass + instancia + constantes
- [ ] `src/merge.py` — añadir en `_SOURCES` y `MERGED_COLUMNS`
- [ ] `src/main.py` — añadir flag, bloque if, `--all`, `any_selected`
- [ ] `tests/test_<nombre>.py` — ≥ 25 tests, todos pasando
- [ ] `tests/test_merge.py` — actualizar `test_total_column_count` (+N cols)
- [ ] Ejecutar `python -m pytest tests/ -v` → **0 fallos**
- [ ] Ejecutar `python -m src.main --<nombre>` → CSV generado correctamente

---

## Tarea concreta

> **Describe aquí qué pipeline implementar, con:**
> - Nombre del pipeline
> - Fuente de datos (URL o archivo local)
> - Lógica de cálculo (fórmulas, filtros, transformaciones)
> - Columnas de salida específicas
> - Frecuencia (mensual / trimestral / anual)
