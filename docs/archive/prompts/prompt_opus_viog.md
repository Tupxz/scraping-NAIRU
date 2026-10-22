# Implementar pipeline VIOG en scraping-NAIRU

## Contexto del proyecto

Proyecto Python modular para estimación NAIRU Colombia. Estructura:

```
src/
  config.py          ← @dataclass(frozen=True) + instancia global
  sources/<name>/    ← lógica por fuente
  pipelines/run_*.py ← runners
  merge.py           ← une todos los CSV procesados
  main.py            ← CLI argparse
tests/test_*.py
data/processed/      ← CSVs de salida
```

---

## Archivo de entrada

`data/processed/PIB_USA.xlsx` — 296 filas, columnas:
`t` (datetime), `Year` (int), `Quarter` (int), `PIB` (float), `Potential_PIB` (float)

---

## API del módulo — `src/sources/viog/viog.py`

El módulo es **genérico**: opera sobre cualquier serie trimestral renombrando
las columnas de entrada a `Y` (serie observada) y `Y_ref` (tendencia de referencia).

### Funciones públicas

```python
def load_series(path, series_col="PIB", ref_col="Potential_PIB") -> pd.DataFrame:
    """Carga Excel → PeriodIndex trimestral. Renombra series_col→Y, ref_col→Y_ref."""
    # Si hay columnas Year+Quarter → PeriodIndex.from_fields
    # Si hay columna t → DatetimeIndex.to_period("Q")
    df["_series_label"] = series_col  # para labels en gráficas

def apply_filters(df) -> pd.DataFrame:
    """Aplica 5 filtros a df["Y"]. Añade columnas trend_bk, trend_cf, trend_bw, trend_hp, trend_kalman."""
    # BK: bkfilter(y, low=6, high=32, K=12) → trend_bk con NaN en primeros/últimos K trimestres
    # CF: cffilter(y, low=6, high=32, drift=False)[1] → trend_cf
    # BW: butter(N=8, Wn=1/16, btype="low") + filtfilt → trend_bw
    # HP: hpfilter(y, lamb=1600)[1] → trend_hp
    # Kalman: UnobservedComponents(level="random walk with drift", cycle=True,
    #         damped_cycle=True, stochastic_cycle=True, cycle_period_bounds=[0.3,40])
    #         .fit(disp=False).level.smoothed → trend_kalman

def compute_gaps(df) -> pd.DataFrame:
    """ln_Y, ln_Y_ref, ln_trend_{tag}, gap_{tag} para tag in [bk,cf,bw,hp,kalman].
    gap_ref = ln_Y - ln_Y_ref."""

def compute_viog_weights(df) -> pd.DataFrame:
    """_GAP_VARS = ["bk","cf","bw","hp","kalman","ref"]
    N = len(df)
    rev_v = gap_v.abs().cumsum() / N
    inv_rev_v = 1 / rev_v
    weight_rev_v = rev_v / sum(rev_v for all v)
    weight_inv_rev_v = inv_rev_v / sum(inv_rev_v for all v)"""

def compute_weighted_gap(df) -> pd.DataFrame:
    """ln_potential_viog = sum(weight_rev_v * ln_trend_v for all v)
    ln_potential_inv_viog = sum(weight_inv_rev_v * ln_trend_v for all v)
    gap_viog = ln_Y - ln_potential_viog
    gap_inv_viog = ln_Y - ln_potential_inv_viog"""

def plot_filters(df, save_dir=None, show=True) -> None:
    """3 gráficas matplotlib (figsize 11×5, grid=False, legend ncol=2):
    1. Tendencias: Y observada + Y_ref + trend_{bk,cf,bw,hp,kalman}
    2. Brechas: gap_{bk,cf,bw,hp,kalman,ref} con axhline(0)
    3. Brecha final: gap_viog, gap_inv_viog, gap_ref con axhline(0)
    Guarda PNG si save_dir: viog_trends.png, viog_gaps.png, viog_final.png"""

def run_viog_pipeline(input_path, output_path,
                      series_col="PIB", ref_col="Potential_PIB",
                      plot=False, plot_dir=None) -> pd.DataFrame:
    """Pipeline completo. CSV de salida:
    date, year, quarter, gap_viog, gap_inv_viog, gap_ref,
    gap_hp, gap_cf, gap_bk, gap_bw, gap_kalman, source"""
```

---

## Resto de archivos

### `src/sources/viog/__init__.py` — vacío

### `src/pipelines/run_viog.py`
```python
from src.config import PROCESSED_DIR, VIOG_CONFIG
from src.sources.viog.viog import run_viog_pipeline
def run():
    df = run_viog_pipeline(PROCESSED_DIR / VIOG_CONFIG.input_filename,
                           PROCESSED_DIR / VIOG_CONFIG.processed_filename)
    print(f"[VIOG] {len(df)} obs guardadas")
```

### `src/config.py` — añadir al final
```python
@dataclass(frozen=True)
class VIOGConfig:
    input_filename: str = "PIB_USA.xlsx"
    processed_filename: str = "viog_usa.csv"
    source_label: str = "FRED/CBO"
    bk_low: int = 6; bk_high: int = 32; bk_K: int = 12
    cf_low: int = 6; cf_high: int = 32
    hp_lambda: int = 1600
    bw_cutoff: float = 1/16; bw_order: int = 8
    kalman_cycle_period_bounds: tuple[float, float] = (0.3, 40.0)

VIOG_CONFIG = VIOGConfig()
VIOG_PROCESSED_COLUMNS: list[str] = [
    "date","year","quarter","gap_viog","gap_inv_viog","gap_ref",
    "gap_hp","gap_cf","gap_bk","gap_bw","gap_kalman","source",
]
```

### `src/merge.py`
```python
# En _SOURCES:
"viog": ("viog_usa.csv", ["gap_viog", "gap_inv_viog"]),
# En MERGED_COLUMNS (al final): "gap_viog", "gap_inv_viog"
# test_total_column_count: 18 → 20
```

### `src/main.py`
- `run_viog: bool = False` en `run_pipeline()`
- `if run_viog: from src.pipelines import run_viog as p; p.run()`
- `--viog` en argparse, incluir en `--all` y `any_selected`

---

## Tests — `tests/test_viog.py` (≥ 25 tests)

Fixture sintético para tests unitarios:
```python
def synthetic_df():
    n, rng = 120, np.random.default_rng(42)
    idx = pd.period_range("1990Q1", periods=n, freq="Q")
    return pd.DataFrame({
        "Y": 5000 + np.cumsum(rng.normal(50,10,n)),
        "Y_ref": 5000 + np.cumsum(rng.normal(50,8,n)),
        "_series_label": "PIB",
    }, index=idx)
```

Clases:
- `TestLoadSeries` — carga, renombra Y/Y_ref, PeriodIndex, ordenado, sin nulls, custom cols
- `TestApplyFilters` — cada trend_* existe; BK tiene NaN en primeros/últimos K; trends > 0
- `TestComputeGaps` — fórmulas gap_hp y gap_ref; tipos float; gap_bk NaN en extremos; ln_trend_* creados
- `TestComputeVIOGWeights` — pesos suman 1 en filas válidas (excluir extremos BK); rev>0 para no-BK
- `TestRunVIOGPipeline` (scope="class", archivo real) — columnas presentes, tipos, CSV escrito, nrows==input, source label, year int, quarter in 1-4

---

## Notas clave

1. BK recorta K=12 obs de cada extremo → `trend_bk` y `gap_bk` con NaN en extremos
2. Usar `len(df)` como divisor N (no hardcodear 269)
3. Kalman tarda ~1-10 s, `fit(disp=False)`
4. El Excel ya tiene columnas `PIB` y `Potential_PIB` (no renombrar antes de `load_series`)
5. No usar `google.colab`


## Contexto del proyecto

Proyecto Python modular para estimación NAIRU Colombia. Estructura:

```
src/
  config.py          ← @dataclass(frozen=True) + instancia global
  sources/<name>/    ← lógica por fuente
  pipelines/run_*.py ← runners
  merge.py           ← une todos los CSV procesados
  main.py            ← CLI argparse
tests/test_*.py
data/processed/      ← CSVs de salida
```

---

## Archivo de entrada

`data/processed/PIB_USA.xlsx` — 296 filas, columnas:

| t | Year | Quarter | PIB | Potential_PIB |
|---|------|---------|-----|---------------|
| 1949-01-01 | 1949 | 1 | 2182.68 | 2244.70 |

- `t`: datetime (primer mes del trimestre)
- `PIB`: PIB real USA (miles de millones USD, FRED)
- `Potential_PIB`: PIB potencial CBO (función de producción)

---

## Lógica de cálculo (del notebook `VIOG .ipynb`)

### 1. Cargar y construir PeriodIndex trimestral
```python
df["_period"] = pd.PeriodIndex.from_fields(year=df["Year"], quarter=df["Quarter"], freq="Q")
df = df.sort_values("_period").set_index("_period")
```

### 2. Aplicar 5 filtros de tendencia

```python
# Baxter-King (recorta K=12 trimestres de cada extremo → NaN en extremos)
from statsmodels.tsa.filters.bk_filter import bkfilter
bk_cycle = bkfilter(df["PIB"], low=6, high=32, K=12)
# bkt_PIB = tendencia (NaN en primeros/últimos 12 trimestres)

# Christiano-Fitzgerald
from statsmodels.tsa.filters.cf_filter import cffilter
cf_cycle, cf_trend = cffilter(df["PIB"], low=6, high=32, drift=False)
# cft_PIB = cf_trend

# Butterworth
from scipy.signal import butter, filtfilt
b, a = butter(N=8, Wn=1/16, btype="low")
# bwt_PIB = filtfilt(b, a, df["PIB"])

# Hodrick-Prescott (lambda=1600 para datos trimestrales)
from statsmodels.tsa.filters.hp_filter import hpfilter
hp_cycle, hp_trend = hpfilter(df["PIB"], lamb=1600)
# hpt_PIB = hp_trend

# Kalman / UCM
from statsmodels.tsa.statespace.structural import UnobservedComponents
ucm = UnobservedComponents(
    endog=df["PIB"], level="random walk with drift",
    cycle=True, damped_cycle=True, stochastic_cycle=True,
    cycle_period_bounds=[0.3, 40]
)
result = ucm.fit(disp=False)
# kalmant_PIB = result.level.smoothed
```

### 3. Logaritmos y brechas
```python
# gap_X = ln(PIB) - ln(tendencia_X)
# para X in: bk, cf, bw, hp, kalman, potential
```

### 4. Ponderadores VIOG
```python
N = len(df)  # usar len(df), no hardcodear
for v in ["bk", "cf", "bw", "hp", "kalman", "potential"]:
    rev_v     = gap_v.abs().cumsum() / N       # VIOG acumulado normalizado
    inv_rev_v = 1 / rev_v

weight_rev_v     = rev_v / sum(rev_v for all v)
weight_inv_rev_v = inv_rev_v / sum(inv_rev_v for all v)
```

### 5. PIB potencial y brecha ponderados
```python
weighted_rev_potential     = sum(weight_rev_v     * ln_trend_v for all v)
weighted_inv_rev_potential = sum(weight_inv_rev_v * ln_trend_v for all v)

gap_viog     = ln(PIB) - weighted_rev_potential
gap_inv_viog = ln(PIB) - weighted_inv_rev_potential
```

---

## Archivos a crear

### `src/sources/viog/__init__.py` — vacío

### `src/sources/viog/viog.py`
Funciones públicas:
- `load_pib_usa(path) → pd.DataFrame`
- `apply_filters(df) → pd.DataFrame`
- `compute_gaps(df) → pd.DataFrame`
- `compute_viog_weights(df) → pd.DataFrame`
- `compute_weighted_gap(df) → pd.DataFrame`
- `run_viog_pipeline(input_path, output_path) → pd.DataFrame`

CSV de salida con columnas:
`date, year, quarter, gap_viog, gap_inv_viog, gap_potential, gap_hp, gap_cf, gap_bk, gap_bw, gap_kalman, source`

### `src/pipelines/run_viog.py` — runner estándar

### Añadir a `src/config.py`
```python
@dataclass(frozen=True)
class VIOGConfig:
    input_filename: str = "PIB_USA.xlsx"
    processed_filename: str = "viog_usa.csv"
    source_label: str = "FRED/CBO"
    bk_low: int = 6; bk_high: int = 32; bk_K: int = 12
    cf_low: int = 6; cf_high: int = 32
    hp_lambda: int = 1600
    bw_cutoff: float = 1/16; bw_order: int = 8
    kalman_cycle_period_bounds: tuple[float, float] = (0.3, 40.0)

VIOG_CONFIG = VIOGConfig()
VIOG_PROCESSED_COLUMNS: list[str] = [...]
```

### `tests/test_viog.py` — ≥ 25 tests en 5 clases:
- `TestLoadPibUsa` — carga, columnas, PeriodIndex, orden, sin nulls
- `TestApplyFilters` — cada filtro genera tendencia; BK tiene NaN en extremos
- `TestComputeGaps` — fórmulas, tipos, NaN de BK se propaga
- `TestComputeVIOGWeights` — pesos suman 1 en filas válidas (excluir extremos BK)
- `TestRunVIOGPipeline` — pipeline completo con archivo real (`scope="class"`)

Fixture sintético para tests unitarios:
```python
def make_synthetic(n=120):
    periods = pd.period_range("1990Q1", periods=n, freq="Q")
    rng = np.random.default_rng(42)
    pib = 5000 + np.cumsum(rng.normal(50, 10, n))
    pot = 5000 + np.cumsum(rng.normal(50, 8, n))
    return pd.DataFrame({"PIB": pib, "Potential_PIB": pot}, index=periods)
```

### Actualizar `src/merge.py`
```python
# En _SOURCES:
"viog": ("viog_usa.csv", ["gap_viog", "gap_inv_viog"]),

# En MERGED_COLUMNS (al final):
"gap_viog",      # VIOG - Brecha del producto ponderada (VIOG)
"gap_inv_viog",  # VIOG - Brecha del producto ponderada (1/VIOG)
```
Actualizar `test_total_column_count`: 18 → 20.

### Actualizar `src/main.py`
- `run_viog: bool = False` en `run_pipeline()`
- Bloque `if run_viog: ...`
- Flag `--viog` en argparse
- Incluir en `--all` y `any_selected`

---

## Notas importantes

1. **No usar `google.colab`** — leer el archivo directamente con `Path`
2. **BK recorta extremos**: las primeras/últimas `K=12` filas serán `NaN` en `bkt_PIB` y `gap_bk`
3. **Divisor N**: usar `len(df)`, no el valor hardcodeado `269` del notebook
4. **Kalman tarda ~1-10 s** — es normal, usar `fit(disp=False)`
5. **El Excel ya tiene columnas `PIB` y `Potential_PIB`** — no renombrar

## Verificación

```bash
python -m pytest tests/test_viog.py -v   # todos pasando
python -m src.main --viog                # genera data/processed/viog_usa.csv
```
