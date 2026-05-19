# Plan de implementación: Pipeline PIB Potencial Colombia (Cobb-Douglas)

**Fecha:** 2026-05-18  
**Objetivo:** añadir al pipeline existente un nuevo output automático —
`outputs/pib_potencial/PIB_Potencial_Colombia.xlsx` — que replique y extienda
el Boceto manual, y que se regenere solo cada vez que llegan datos nuevos.

**Principio de diseño:** ningún número se hardcodea dos veces. Todo fluye desde
las fuentes que ya se descargan (`nairu_dataset.csv`, `production_function_quarterly.csv`,
`outputs/nairu/nairu_colombia.csv`). El pipeline se activa con un solo flag:
`python -m src.main --pib-potencial`.

---

## 0. Restricciones (no negociables)

1. **Rama:** trabajar en `sandbox`. Commits atómicos por bloque.
2. **Idioma:** español neutro en commits, comentarios y docstrings.
3. **Tests primero:** `pytest -q` verde después de cada bloque.
4. **No tocar `src/nairu/model_core.py`** — funciona, está testeado.
5. **No tocar `build_production_function_dataset.py`** — se reutiliza su output.
6. **No ejecutar el pipeline real** durante desarrollo; validar con fixtures.
7. **`--all` incluirá `--pib-potencial`** al final de la cadena.

---

## 1. Cadena de cálculo completa

```
Fuentes existentes (procesadas)
│
├── dane_gdp_colombia.csv          → PIB trimestral (miles MM COP 2017)
├── andi_capacidad_instalada.csv   → UCI mensual → trimestral
├── dane_labor_colombia.csv        → TD, TGP, PET mensuales → trimestrales
├── pwt_colombia.csv               → K, delta, hc anuales → trimestrales
├── dane_gdp_income_colombia.csv   → RA, EBE, Ingreso Mixto trimestrales
├── outputs/nairu/nairu_colombia.csv → NAIRU*, NAICU* mensuales → trimestrales
│
▼
[E1] Módulo src/production/factors.py
│   ├── factor_trabajo(TD, TGP, PET, NAIRU)
│   │   ├── L_obs  = PET × (TGP/100) × (1 − TD/100)           [miles personas]
│   │   └── L_pot  = PET × (TGP/100) × (1 − NAIRU*/100)       [miles personas]
│   │
│   ├── factor_capital(K, UCI, NAICU)
│   │   ├── K_usado = K × (UCI/100)
│   │   └── K_pot   = K × (NAICU*/100)                         [millones COP 2017]
│   │
│   └── alpha_dinamico(RA, EBE, IM)
│       └── α = RA / (RA + EBE + IM)                            [fracción 0–1]
│
[E2] Módulo src/production/tfp.py
│   ├── A_obs = PIB / (K_usado^α × L_obs^(1−α))               [PTF observada]
│   └── A_pot = HP(A_obs, lambda=1600)                          [PTF tendencial]
│
[E3] Módulo src/production/pib_potencial.py
│   ├── PIB_pot = A_pot × K_pot^α × L_pot^(1−α)
│   ├── Brecha_CD   = (PIB − PIB_pot) / PIB_pot × 100          [% del potencial]
│   └── PIB_tend_HP = HP(PIB, lambda=1600)                      [tendencia HP pura]
│       Brecha_HP   = (PIB − PIB_tend_HP) / PIB_tend_HP × 100
│
[E4] Pipeline src/pipelines/run_pib_potencial.py
│   ├── Lee fuentes → llama E1, E2, E3
│   ├── Valida con quality_checks (E5)
│   └── Escribe Excel (E6)
│
[E5] Validaciones src/quality_checks.py (ampliar)
│
[E6] Excel output: outputs/pib_potencial/PIB_Potencial_Colombia.xlsx
    ├── Hoja "Trimestral"   — serie desde 2004-Q1 (cuando hay todas las fuentes)
    ├── Hoja "Mensual"      — NAIRU*, NAICU*, TD, TGP, UCI mensual
    ├── Hoja "Supuestos"    — parámetros del modelo (α promedio, lambda HP, …)
    └── Hoja "Metadatos"    — fechas de descarga por fuente, versión del pipeline
```

### Frecuencias y alineación

| Variable | Frecuencia fuente | Frecuencia modelo | Regla |
|---|---|---|---|
| PIB | Trimestral | Trimestral | directo |
| K, delta, hc | Anual | Trimestral | ley de acumulación (ya en `build_production_function_dataset.py`) |
| UCI | Mensual | Trimestral | promedio del trimestre |
| TD, TGP, PET | Mensual | Trimestral | promedio del trimestre |
| NAIRU*, NAICU* | Mensual | Trimestral | promedio del trimestre |
| RA, EBE, IM | Trimestral | Trimestral | directo |

---

## 2. Estructura de archivos nuevos

```
src/
└── production/               ← módulo nuevo
    ├── __init__.py
    ├── factors.py            ← L_obs, L_pot, K_usado, K_pot, alpha_dinamico
    ├── tfp.py                ← A_obs, A_pot (HP filter)
    ├── pib_potencial.py      ← PIB_pot, brechas, ensamble trimestral
    └── excel_writer.py       ← escribe el xlsx multi-hoja

src/pipelines/
└── run_pib_potencial.py      ← orquestador (sigue el patrón run_*.py)

tests/
├── test_production_factors.py
├── test_production_tfp.py
└── test_production_pib_potencial.py

outputs/
└── pib_potencial/            ← creado en runtime, no versionado
    └── PIB_Potencial_Colombia.xlsx
```

---

## 3. Bloques de implementación

### Bloque E1 — `src/production/factors.py` (≤ 3h)

**`factor_trabajo(df) → DataFrame`**

```python
def factor_trabajo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula Factor Trabajo observado y potencial (miles de personas).

    L_obs = PET × (TGP/100) × (1 − TD/100)
    L_pot = PET × (TGP/100) × (1 − NAIRU_q/100)

    df requiere columnas: PET, TGP, TD, NAIRU_q (promedio trimestral de NAIRU*).
    """
```

**`factor_capital(df) → DataFrame`**

```python
def factor_capital(df: pd.DataFrame) -> pd.DataFrame:
    """
    K_usado = K × (UCI/100)     ← capital efectivamente utilizado
    K_pot   = K × (NAICU_q/100) ← capital al nivel potencial (NAICU*)
    """
```

**`alpha_dinamico(df) → pd.Series`**

```python
def alpha_dinamico(df: pd.DataFrame) -> pd.Series:
    """
    α_t = RA_t / (RA_t + EBE_t + IM_t)

    Requiere: compensation_employees, gross_operating_surplus, mixed_income.
    Retorna α entre 0 y 1. Si alguna fuente es NaN (antes 2016-Q1),
    usa α de respaldo = 0.40 (calibrado en Boceto).
    """
```

**Tests (`tests/test_production_factors.py`):**
- Dataset sintético 12 trimestres: `L_pot < L_obs` cuando `NAIRU* < TD` ✓
- `K_pot > K_usado` cuando `NAICU* > UCI` ✓
- `0 < alpha < 1` para todos los periodos ✓
- Fallback α = 0.40 cuando RA/EBE/IM son NaN ✓

**Commit:** `Add src/production/factors.py: L, K, alpha Cobb-Douglas`

---

### Bloque E2 — `src/production/tfp.py` (≤ 2h)

**`hp_filter(series, lamb=1600) → tuple[Series, Series]`**

```python
def hp_filter(series: pd.Series, lamb: float = 1600.0) -> tuple[pd.Series, pd.Series]:
    """
    Wrapper de statsmodels.tsa.filters.hp_filter.hpfilter.
    Retorna (cycle, trend). Lambda 1600 = estándar trimestral (Hodrick-Prescott 1997).
    Maneja NaN: excluye extremos con NaN y re-indexa.
    """
```

**`compute_tfp(df) → DataFrame`**

```python
def compute_tfp(df: pd.DataFrame) -> pd.DataFrame:
    """
    A_obs = PIB / (K_usado ** alpha × L_obs ** (1 - alpha))
    A_pot = hp_filter(A_obs, lamb=1600)[1]   ← tendencia

    Requiere: PIB, K_usado, L_obs, alpha (serie).
    """
```

**Tests (`tests/test_production_tfp.py`):**
- HP filter sobre serie sintética AR(1): ciclo tiene media ≈ 0 ✓
- HP filter con NaN al inicio: no rompe ✓
- `A_obs` positiva para inputs positivos ✓
- `A_pot` más suave que `A_obs` (var(A_pot) < var(A_obs)) ✓

**Commit:** `Add src/production/tfp.py: PTF observada y tendencial (HP filter)`

---

### Bloque E3 — `src/production/pib_potencial.py` (≤ 3h)

**`compute_pib_potencial(df) → DataFrame`**

```python
def compute_pib_potencial(df: pd.DataFrame) -> pd.DataFrame:
    """
    PIB_pot   = A_pot × K_pot ** alpha × L_pot ** (1 - alpha)
    Brecha_CD = (PIB - PIB_pot) / PIB_pot * 100

    PIB_tend_HP = hp_filter(PIB, lamb=1600)[1]
    Brecha_HP   = (PIB - PIB_tend_HP) / PIB_tend_HP * 100

    Requiere: PIB, A_pot, K_pot, L_pot, alpha, todas las columnas de E1 y E2.
    Retorna DataFrame con todas las columnas intermedias y finales.
    """
```

**Columnas de salida del DataFrame trimestral:**

```python
QUARTERLY_COLS = [
    "date", "year", "quarter",
    # Insumos
    "PIB", "K", "UCI", "NAICU_q", "TD", "TGP", "PET", "NAIRU_q",
    "RA", "EBE", "IM",
    # Factores
    "alpha", "L_obs", "L_pot", "K_usado", "K_pot",
    # PTF
    "A_obs", "A_pot",
    # PIB Potencial
    "PIB_pot",
    "Brecha_CD",     # % del potencial (Cobb-Douglas)
    "PIB_tend_HP",
    "Brecha_HP",     # % del potencial (HP puro)
]
```

**Tests (`tests/test_production_pib_potencial.py`):**
- `PIB_pot > 0` para todos los periodos ✓
- `|Brecha_CD|` promedio < 10 pp (no diverge) ✓
- Con factores al 100 % de utilización y α constante: `PIB_pot ≈ A_obs × K^α × L^(1-α)` ✓
- Dataset de 1 trimestre → no rompe (manejo de borde del HP filter) ✓

**Commit:** `Add src/production/pib_potencial.py: Cobb-Douglas + HP gap`

---

### Bloque E4 — `src/production/excel_writer.py` (≤ 3h)

Escribe `PIB_Potencial_Colombia.xlsx` con 4 hojas:

**Hoja "Trimestral"** — columnas del Boceto en el mismo orden:

| Columna | Fuente |
|---|---|
| Fecha, Año, Trimestre | `date` |
| PIB observado | `PIB` |
| PIB tendencial HP | `PIB_tend_HP` |
| Brecha HP (%) | `Brecha_HP` |
| Factor Capital K | `K` |
| UCI observada | `UCI` |
| UCI potencial (NAICU) | `NAICU_q` |
| K usado | `K_usado` |
| K potencial | `K_pot` |
| Factor Trabajo L obs | `L_obs` |
| Tasa desempleo obs | `TD` |
| NAIRU* | `NAIRU_q` |
| L potencial | `L_pot` |
| Alpha (participación laboral) | `alpha` |
| PTF observada | `A_obs` |
| PTF tendencial | `A_pot` |
| PIB Potencial (CD) | `PIB_pot` |
| Brecha CD (%) | `Brecha_CD` |

**Hoja "Mensual"** — desde `nairu_colombia.csv` y `nairu_dataset.csv`:

| Columna | Fuente |
|---|---|
| Fecha | date |
| NAIRU* | nairu_smooth |
| NAICU* | naicu_smooth |
| TD observada | unemployment_rate |
| TGP observada | tgp_rate |
| UCI observada | capacity_utilization |
| IPC interanual | ipc_yoy |
| Brecha inflación | inflation_gap |

**Hoja "Supuestos"** — tabla de parámetros:

```
Lambda HP    : 1600 (Hodrick-Prescott, estándar trimestral)
Alpha respaldo: 0.40 (para periodos sin datos de ingreso DANE)
K depreciación: ley de acumulación PWT (delta trimestralizada = 1-(1-delta)^0.25)
NAIRU fuente : Kalman biestado (src/nairu/model_core.py)
NAICU fuente : ANDI EOIC (capacity_utilization)
Inicio serie : 2005-Q1 (primer trimestre con todas las fuentes)
```

**Hoja "Metadatos"** — generada automáticamente:

```python
{
    "Generado":       datetime.now().isoformat(),
    "Pipeline":       "src/pipelines/run_pib_potencial.py",
    "Versión":        "0.3.0",
    "DANE PIB":       fecha_descarga de processed/dane_gdp_colombia.csv,
    "PWT":            fecha_descarga de processed/pwt_colombia.csv,
    "ANDI EOIC":      fecha_descarga de processed/andi_capacidad_instalada.csv,
    "NAIRU estimado": última fecha en outputs/nairu/nairu_colombia.csv,
}
```

**Commit:** `Add src/production/excel_writer.py: output multi-hoja PIB Potencial`

---

### Bloque E5 — `src/pipelines/run_pib_potencial.py` (≤ 2h)

Orquestador que sigue exactamente el patrón `run_*.py` existente:

```python
def run(
    processed_dir: Path = PROCESSED_DIR,
    nairu_dir: Path = OUTPUTS_DIR / "nairu",
    output_dir: Path = OUTPUTS_DIR / "pib_potencial",
) -> pd.DataFrame:
    """Pipeline PIB Potencial: carga fuentes → calcula factores → escribe Excel."""
    setup_logging()
    logger.info("── Pipeline PIB POTENCIAL (Cobb-Douglas) ──")

    # 1. Cargar fuentes ya procesadas
    df_quarterly = _load_and_align_sources(processed_dir, nairu_dir)

    # 2. Calcular factores
    df = compute_factors(df_quarterly)          # E1
    df = compute_tfp(df)                        # E2
    df = compute_pib_potencial(df)              # E3

    # 3. Validar
    run_pib_potencial_checks(df)                # E6

    # 4. Escribir Excel
    output_dir.mkdir(parents=True, exist_ok=True)
    path = write_pib_potencial_excel(df, output_dir)
    logger.info("Excel escrito: %s", path)
    return df
```

**Función `_load_and_align_sources`** — manejo robusto de fuentes faltantes:

```python
def _load_and_align_sources(processed_dir, nairu_dir) -> pd.DataFrame:
    """
    Lee y alinea todas las fuentes a frecuencia trimestral.
    Si una fuente falta, lanza FileNotFoundError con mensaje claro.
    Si NAIRU falta, usa TD como proxy (degradación graceful con warning).
    """
```

**Integración en `src/main.py`:**
```python
# Nuevo flag
parser.add_argument(
    "--pib-potencial", action="store_true",
    help="Calcular PIB Potencial Cobb-Douglas y exportar Excel",
)
# En --all: incluir después de --nairu-estim
```

**Commit:** `Add run_pib_potencial.py: orchestrator + CLI flag --pib-potencial`

---

### Bloque E6 — Validaciones `quality_checks.py` (≤ 1h)

Nueva función `run_pib_potencial_checks(df)`:

```python
def run_pib_potencial_checks(df: pd.DataFrame) -> bool:
    """
    Valida el dataset de PIB Potencial antes de escribir el Excel.

    Checks:
    - PIB_pot > 0 para todos los periodos
    - |Brecha_CD| < 15 pp (Colombia históricamente ±10 pp)
    - |Brecha_HP| < 15 pp
    - alpha en (0.2, 0.7) para todos los periodos
    - A_pot > 0
    - Al menos 60 trimestres (≥ 2005-Q1)
    """
```

**Commit:** incluido en el commit de E5.

---

### Bloque E7 — `.gitignore` y auto-update (≤ 30 min)

1. Añadir al `.gitignore`:
   ```gitignore
   # Output PIB Potencial (generado en runtime)
   outputs/pib_potencial/
   ```

2. Actualizar `README.md` sección "4. Uso":
   ```markdown
   python -m src.main --pib-potencial   # PIB Potencial Cobb-Douglas + Excel
   ```

3. **Auto-update flow** — cuando llega dato nuevo:
   ```bash
   python -m src.main --dane-gdp --investment --income  # nuevos datos DANE
   python -m src.main --andi                            # nueva UCI
   python -m src.main --nairu-dataset --nairu-estim     # re-estimar NAIRU/NAICU
   python -m src.main --pib-potencial                   # regenera Excel
   ```
   O todo de una vez:
   ```bash
   python -m src.main --all    # incluirá --pib-potencial
   ```

**Commit:** `Wire --pib-potencial into --all, update README and .gitignore`

---

## 4. Dependencias nuevas

Solo `statsmodels` (para `hpfilter`) y `openpyxl` (para Excel). Ambas ya están
en el `.venv` del proyecto (verificar en `requirements.txt`):

```toml
# pyproject.toml — ya deberían estar; si no, añadir:
"statsmodels>=0.14",
"openpyxl>=3.1",
```

---

## 5. Orden de ejecución del pipeline completo (para referencia)

```
1. --unemployment    → dane_labor_colombia.csv
2. --dane-gdp        → dane_gdp_colombia.csv
3. --investment      → dane_gdp_expenditure_colombia.csv
4. --income          → dane_gdp_income_colombia.csv
5. --pwt             → pwt_colombia.csv
6. --andi            → andi_capacidad_instalada.csv
7. --banrep          → inflation_banrep_colombia.csv
8. --tes             → tes_banrep_colombia.csv
9. --brent           → brent_oil_price.csv
10. --nairu-dataset  → inputs/Data_NAIRU.xlsx
11. --nairu-estim    → outputs/nairu/nairu_colombia.csv ← NAIRU*, NAICU*
12. --prod-func      → outputs/production_function_quarterly.csv
13. --pib-potencial  → outputs/pib_potencial/PIB_Potencial_Colombia.xlsx ← NUEVO
14. --merge          → data/final/nairu_dataset.csv
```

---

## 6. Criterio de "done" (v0.3.0)

1. `pytest -q` pasa en verde (incluyendo nuevos tests de `test_production_*.py`).
2. `python -m src.main --pib-potencial` produce el Excel sin errores.
3. El Excel tiene las 4 hojas con datos desde 2005-Q1.
4. `|Brecha_CD|` promedio histórica < 10 pp (coherencia económica).
5. La Hoja "Metadatos" muestra fechas reales de descarga.
6. `python -m src.main --all` incluye el pipeline y termina sin errores.

---

## 7. Diferido a v0.4

- Gráficos incrustados en el Excel (PIB observado vs potencial, brecha histórica).
- Proyección a 4 trimestres adelante con supuestos de NAIRU/UCI constante.
- Exportación en PDF del informe de coyuntura.
- Integración con el modelo de Phillips backward-looking.
