# Prompt — Corrección PWT 11.0 y dataset para PIB potencial automático

> **Modelo destino:** Claude Opus  
> **Fecha:** 2026-05-11  
> **Proyecto:** `Tupxz/scraping-NAIRU`, rama `sandbox`

---

## Objetivo

Este pipeline Python construye un **dataset macroeconómico mensual/anual de Colombia** que alimenta dos modelos:

1. **NAIRU/NAICU** — Modelo de Kalman biestado (ya implementado en `src/nairu/`).
2. **PIB potencial vía función de producción Cobb-Douglas** — Método del profesor Álvaro, actualmente en `data/inputs/FUNCION DE PRODUCCION.xlsx` (Excel manual). La tarea es **automatizar** ese cálculo cada vez que lleguen datos nuevos.

La función de producción es:

```
Y* = A* · K^α · (H · L*)^(1−α)       α = 0.4,  1−α = 0.6
```

| Símbolo | Descripción | Fuente | Frecuencia |
|---|---|---|---|
| `Y` | PIB a precios constantes (desest.) | DANE Cuentas Nacionales | Trimestral |
| `K` | Stock de capital real | PWT 11.0 → `rnna` | Anual |
| `δ` | Tasa de depreciación del capital | PWT 11.0 → `delta` | Anual |
| `H` | Índice de capital humano | PWT 11.0 → `hc` | Anual |
| `L` | Trabajo efectivo = PET × TGP × (1 − TD) | DANE GEIH | Mensual |
| `UCI` | Utilización de capacidad instalada | ANDI EOIC | Mensual |
| `A` | Productividad total de factores (residuo de Solow) | Calculada | — |

> Las variables con `*` son las filtradas (Hodrick-Prescott) para obtener niveles potenciales.  
> Referencia estructural: `data/inputs/FUNCION DE PRODUCCION.xlsx`  
> — Hoja **Niveles**: `Y, UCI, K, PET, TGP, TD, H, A` (anual 1950–2020)  
> — Hoja **Potencial**: mismas series filtradas + `Y*` (anual 1988–2031)

---

## Problema — Variables PWT incorrectas en el pipeline

El pipeline extrae actualmente `ck` y `cn`, que son **incorrectas** para series de tiempo:

| Variable actual | Nombre en PWT 11.0 | Problema |
|---|---|---|
| `ck` → `capital_stock_ck` | Capital stock at current PPP | Varía con el tipo de cambio PPP, que se revisa cada publicación → no apto para series de tiempo |
| `cn` → `capital_stock_cn` | Capital stock at current national prices | En USD corrientes → depende del tipo de cambio |

**Además, falta `delta` (tasa de depreciación)**, imprescindible para el método de inventario permanente que usa el modelo del profesor Álvaro.

### Variables correctas a extraer de PWT 11.0

| Variable PWT | Columna destino | Descripción | Unidad |
|---|---|---|---|
| `rnna` | `capital_stock_real` | Stock de capital a precios nacionales constantes 2017 | Millones COP (2017) — Colombia ≈ 900 000–2 700 000 |
| `delta` | `depreciation_rate` | Tasa de depreciación promedio del stock de capital | Fracción 0–1 — Colombia ≈ 0.047–0.048 |
| `hc` | `human_capital` | Índice de capital humano (escolaridad + retornos) | Índice — Colombia ≈ 1.9–2.6 |

---

## Dataset final requerido

`data/final/nairu_dataset.csv` debe tener **exactamente** estas columnas, en este orden:

```
# ── Identificadores ───────────────────────────────────────────
date                    # YYYY-MM-01 (mensual)
year
month

# ── DANE GEIH — Mensual, desestacionalizado ───────────────────
unemployment_rate       # Tasa de desempleo TD (%) nacional
tgp_rate                # Tasa Global de Participación (%)
pet_thousands           # Población en Edad de Trabajar (miles)

# ── IPC DANE — Mensual ────────────────────────────────────────
ipc_index               # Índice de Precios al Consumidor

# ── Inflación BANREP — Mensual ────────────────────────────────
Inf_Goal                # Meta de inflación (%)
Inf_Rate                # Inflación anual observada (%)
Core_Inf                # Inflación sin alimentos ni regulados (%)

# ── Brent FRED — Mensual ──────────────────────────────────────
brent_usd_per_barrel    # Precio del Brent (USD/barril)

# ── ANDI EOIC — Mensual (desde 2004-01-01) ───────────────────
capacity_utilization    # Utilización de capacidad instalada UCI (%)

# ── TES BANREP — Mensual ──────────────────────────────────────
TES_UVR_1Y              # Rendimiento TES UVR 1 año (%)
TES_PESOS_1Y            # Rendimiento TES Pesos 1 año (%)

# ── DANE PIB — Trimestral (NaN en meses no-fin-de-trimestre) ──
gdp_constant_prices     # PIB precios constantes desest. (miles mill. COP 2015)
gdp_investment          # Inversión FBCF precios constantes desest.
gdp_labor_income        # Remuneración de asalariados
gdp_operating_surplus   # Excedente bruto de explotación + ingreso mixto

# ── PWT 11.0 — Anual (NaN en meses que no son enero) ─────────
capital_stock_real      # Stock de capital real rnna (millones COP 2017)
depreciation_rate       # Tasa de depreciación delta (fracción)
human_capital           # Índice de capital humano hc

# ── VIOG-USA — Trimestral ─────────────────────────────────────
gap_viog_us
gap_inv_viog_us

# ── VIOG-Colombia — Trimestral ────────────────────────────────
gap_viog_co
gap_inv_viog_co
```

> **Total esperado: 25 columnas** (actualmente son 21; se añaden las 4 del PIB por enfoque de ingreso/gasto).

---

## Archivos a modificar

### `src/config.py`

```python
# ANTES
PWT_PROCESSED_COLUMNS = [
    "date", "year", "month",
    "capital_stock_ck",   # ← ck (PPP corrientes)  INCORRECTO
    "capital_stock_cn",   # ← cn (nac. corrientes) INCORRECTO
    "human_capital",
    "source", "download_date",
]
CAPITAL_STOCK_MIN: float = 0.0
CAPITAL_STOCK_MAX: float = 5000.0

# DESPUÉS
PWT_PROCESSED_COLUMNS = [
    "date", "year", "month",
    "capital_stock_real",   # ← rnna (precios nac. const. 2017)
    "depreciation_rate",    # ← delta (fracción)
    "human_capital",        # ← hc (índice)
    "source", "download_date",
]
CAPITAL_STOCK_MIN: float = 0.0
CAPITAL_STOCK_MAX: float = 5_000_000.0   # millones COP 2017
DEPRECIATION_RATE_MIN: float = 0.01
DEPRECIATION_RATE_MAX: float = 0.15
```

### `src/sources/pwt/pwt.py`

```python
# ANTES — en _parse_wide_format y parse_pwt_csv
vars_needed = {"ck", "cn", "hc"}
df_col = df_col.rename(columns={
    "ck": "capital_stock_ck",
    "cn": "capital_stock_cn",
    "hc": "human_capital",
})
df_col = df_col.dropna(subset=["ck"])

# DESPUÉS
vars_needed = {"rnna", "delta", "hc"}
df_col = df_col.rename(columns={
    "rnna":  "capital_stock_real",
    "delta": "depreciation_rate",
    "hc":    "human_capital",
})
df_col = df_col.dropna(subset=["capital_stock_real"])
```

### `src/merge.py`

```python
# ANTES
"pwt": (
    "pwt_colombia.csv",
    ["capital_stock_ck", "capital_stock_cn", "human_capital"],
),

# DESPUÉS
"pwt": (
    "pwt_colombia.csv",
    ["capital_stock_real", "depreciation_rate", "human_capital"],
),
```

Actualizar también:
- `MERGED_COLUMNS`: reemplazar los dos nombres de columna
- Docstring del módulo (cabecera): reflejar nuevas columnas
- `test_total_column_count`: el conteo **no** cambia (sigue 21; solo cambian nombres)

### `tests/test_merge.py`

```python
# ANTES — _make_pwt_csv()
"capital_stock_ck": [100.0, 105.0, 110.0],
"capital_stock_cn": [500_000.0, 510_000.0, 520_000.0],

# DESPUÉS — valores realistas para Colombia (PWT 11.0)
"capital_stock_real": [900_000.0, 950_000.0, 1_000_000.0],  # millones COP 2017
"depreciation_rate":  [0.0478, 0.0479, 0.0480],              # ≈ 4.8% Colombia

# Actualizar también test_pwt_source_columns():
assert "capital_stock_real" in cols
assert "depreciation_rate" in cols
assert "human_capital" in cols
```

---

## Convenciones del proyecto

- Python 3.12, `pathlib`, `pandas`, `dataclasses(frozen=True)`
- Logging: `logger.info(...)` / `logger.warning(...)` en cada módulo
- Guardado: `save_csv(df, path)` de `src.io_utils`
- Tests: offline, CSV sintéticos en `tmp_path` (pytest), sin mocks de red
- Ediciones: `replace_string_in_file` con ≥ 3 líneas de contexto antes/después

---

## Verificación final

```bash
python -m pytest tests/test_merge.py tests/test_pipeline.py -q
# Esperado: todos los tests pasan (actualmente 49 passing)

python -m src.main --pwt
# Esperado: data/processed/pwt_colombia.csv con columnas:
#   capital_stock_real, depreciation_rate, human_capital
```
