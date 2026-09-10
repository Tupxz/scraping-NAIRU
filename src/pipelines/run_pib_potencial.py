"""Pipeline PIB Potencial Colombia (Cobb-Douglas), metodología alineada con
``legacy/pib_potencial_integrado_v3.py`` (Módulo 2, configuración v2).

Ver ``docs/integracion_v3.md`` para el mapeo completo de la metodología y las
decisiones de adaptación (ancla de índices, fuentes de datos, etc.).

Orquesta la cadena completa:

    1. Cargar insumos mensuales y trimestrales                → _load_*
    2. Calcular factores (trabajo, capital, alpha, producto)  → src.production.factors
    3. Calcular PTF observada y tendencial (HP + CBO)         → src.production.tfp
    4. Calcular PIB Potencial y brechas                        → src.production.pib_potencial
    5. Validar con quality checks                              → src.quality_checks
    6. Escribir Excel multi-hoja                                → src.production.excel_writer

Fuentes requeridas:
    dane_gdp_colombia.csv               → PIB trimestral (V_pib)
    dane_gdp_expenditure_colombia.csv   → inversión trimestral (BE_inv; solo
                                           para el chequeo de completitud de T)
    dane_gdp_income_colombia.csv        → RA, EBE trimestrales (alpha CBO)
    dane_labor_colombia.csv             → PET, FL, Ocupados mensuales (GEIH)
    pwt_colombia.csv                    → capital humano anual (PWT)
    outputs/nairu/nairu_colombia.csv    → NAIRU*, NAICU*, ICU mensuales
    data/inputs/festivos_efectivos_colombia_2001_2026.xlsx → festivos efectivos/trimestre
    data/inputs/alt_capital/dane_stock_capital_productivo.csv → stock de capital DANE (anual)

Uso
---
    python -m src.main --pib-potencial
    # o directamente:
    from src.pipelines import run_pib_potencial; run_pib_potencial.run()
"""

from __future__ import annotations

import logging
import warnings
from pathlib import Path

import pandas as pd

from src.config import FINAL_DIR, INPUTS_DIR, OUTPUTS_DIR, PROCESSED_DIR
from src.io_utils import setup_logging
from src.production import factors
from src.production.business_cycle import detect_bbq_peaks
from src.production.excel_writer import write_pib_potencial_excel
from src.production.factors import BASE_QUARTER, compute_all_factors, detect_last_complete_quarter
from src.production.pib_potencial import QUARTERLY_OUTPUT_COLS, compute_pib_potencial
from src.production.tfp import compute_tfp
from src.quality_checks import run_pib_potencial_checks

logger = logging.getLogger("nairu_pipeline.pib_potencial")

# Directorios de salida
PIB_POT_OUTPUT_DIR = OUTPUTS_DIR / "pib_potencial"
NAIRU_OUTPUT_DIR   = OUTPUTS_DIR / "nairu"

# Insumos que no vienen de data/processed/ (archivos fuente colocados a mano)
FESTIVOS_XLSX    = INPUTS_DIR / "festivos_efectivos_colombia_2001_2026.xlsx"
DANE_CAPITAL_CSV = INPUTS_DIR / "alt_capital" / "dane_stock_capital_productivo.csv"


# ═══════════════════════════════════════════════════════════════════════
# Carga de fuentes
# ═══════════════════════════════════════════════════════════════════════

def _require(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(
            f"Fuente no encontrada: {path}\n"
            f"Ejecute el pipeline correspondiente antes de '--pib-potencial'."
        )
    return path


def _load_monthly_labor(processed_dir: Path) -> pd.DataFrame:
    """PET, FL (=ocupados+desocupados), Ocupados mensuales (GEIH DANE)."""
    labor = pd.read_csv(_require(processed_dir / "dane_labor_colombia.csv"), parse_dates=["date"])
    m = pd.DataFrame({
        "date": labor["date"],
        "pet":  labor["pet_thousands"],
        "fl":   labor["occupied_thousands"] + labor["unemployed_thousands"],
        "ocup": labor["occupied_thousands"],
    })
    return m.sort_values("date").reset_index(drop=True)


def _load_hc_anual(processed_dir: Path) -> pd.DataFrame:
    """Capital humano PWT (hc), con dato solo en enero de cada año — se
    mergea sobre el mismo DataFrame mensual de labor (left join por fecha)."""
    pwt = pd.read_csv(_require(processed_dir / "pwt_colombia.csv"), parse_dates=["date"])
    return pwt[["date", "human_capital"]].rename(columns={"human_capital": "hc_anual"})


def _load_quarterly_national_accounts(processed_dir: Path) -> pd.DataFrame:
    """V_pib, BE_inv, BQ_ra, BS_ebe trimestrales (DANE Cuentas Nacionales),
    con fechas re-etiquetadas a la convención de v3 (``quarter_label``:
    primer día del mes FINAL del trimestre, no del inicial)."""
    gdp = pd.read_csv(_require(processed_dir / "dane_gdp_colombia.csv"), parse_dates=["date"])
    exp = pd.read_csv(_require(processed_dir / "dane_gdp_expenditure_colombia.csv"), parse_dates=["date"])
    inc = pd.read_csv(_require(processed_dir / "dane_gdp_income_colombia.csv"), parse_dates=["date"])

    q = gdp[["date", "gdp_observed"]].rename(columns={"gdp_observed": "V_pib"})
    q = q.merge(
        exp[["date", "investment"]].rename(columns={"investment": "BE_inv"}),
        on="date", how="left",
    )
    q = q.merge(
        inc[["date", "compensation_employees", "gross_operating_surplus"]]
        .rename(columns={"compensation_employees": "BQ_ra", "gross_operating_surplus": "BS_ebe"}),
        on="date", how="left",
    )
    q["date"] = q["date"].apply(factors.quarter_label)
    q["year"] = q["date"].dt.year
    q["quarter"] = q["date"].dt.quarter
    return q.sort_values("date").reset_index(drop=True)


def _load_nairu_monthly(nairu_dir: Path) -> pd.DataFrame | None:
    """NAIRU*, NAICU*, ICU mensuales (salida del modelo Kalman biestado).

    ``icu`` se toma de la MISMA fuente que ``naicu`` (la corrida del modelo
    de NAIRU/NAICU guarda su propio insumo ICU junto a su salida) para que
    ``brecha_icu = icu - naicu`` compare series construidas de forma
    consistente, tal como hace v3 (``leer_nairu_csv``).
    """
    path = nairu_dir / "nairu_colombia.csv"
    if not path.exists():
        warnings.warn(
            f"Estimaciones NAIRU no encontradas en {nairu_dir}. "
            "Ejecute '--nairu-estim' antes de '--pib-potencial'.",
            stacklevel=3,
        )
        return None
    df = pd.read_csv(path, usecols=["Date", "nairu_estimate", "naicu_estimate", "icu_current"])
    df["date"] = pd.to_datetime(df["Date"])
    df = df.rename(columns={
        "nairu_estimate": "nairu", "naicu_estimate": "naicu", "icu_current": "icu",
    })
    return df[["date", "nairu", "naicu", "icu"]].sort_values("date").reset_index(drop=True)


def _load_dane_capital(path: Path) -> pd.DataFrame:
    """Stock de capital productivo DANE, anual (``year``, ``K_prod_mmp``)."""
    return pd.read_csv(_require(path))


def _load_festivos_trimestrales(path: Path) -> pd.Series | None:
    """Festivos efectivos por trimestre (hoja 'Resumen trimestral' del Excel
    ``festivos_efectivos_colombia_*.xlsx``), indexado por fecha-etiqueta de
    trimestre. ``None`` si el archivo no existe (las horas quedan sin
    ajustar por festivos, con un aviso)."""
    if not path.exists():
        warnings.warn(
            f"Festivos efectivos no encontrados en {path} — horas trabajadas sin ajuste.",
            stacklevel=3,
        )
        return None
    import openpyxl
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    ws = wb["Resumen trimestral"]
    q_month = {"T1": 3, "T2": 6, "T3": 9, "T4": 12}
    registros: dict[pd.Timestamp, float] = {}
    for row in ws.iter_rows(min_row=5, values_only=True):
        anio, trim, festivos_n = row[0], row[1], row[4]
        if anio is None or trim not in q_month or festivos_n is None:
            continue
        fecha = pd.Timestamp(year=int(anio), month=q_month[trim], day=1)
        registros[fecha] = float(festivos_n)
    return pd.Series(registros).sort_index()


def _build_monthly(
    labor: pd.DataFrame,
    andi: pd.DataFrame,
    nairu_df: pd.DataFrame | None,
    final_dir: Path = FINAL_DIR,
) -> pd.DataFrame:
    """Construye el dataset mensual para la hoja Mensual del Excel (solo
    presentación — no alimenta el cálculo del PIB potencial)."""
    m = labor[["date", "tgp_rate", "unemployment_rate"]].copy() if "date" in labor.columns else labor.reset_index()
    m = m.set_index("date")

    if "capacity_utilization" in andi.columns:
        andi_idx = andi.set_index("date") if "date" in andi.columns else andi
        m = m.join(andi_idx[["capacity_utilization"]], how="outer")

    if nairu_df is not None:
        nairu_idx = nairu_df.set_index("date")
        cols_nairu = [c for c in ["nairu", "naicu"] if c in nairu_idx.columns]
        m = m.join(nairu_idx[cols_nairu].rename(
            columns={"nairu": "nairu_estimate", "naicu": "naicu_estimate"}
        ), how="outer")

    nairu_ds_path = final_dir / "nairu_dataset.csv"
    if nairu_ds_path.exists():
        ds = pd.read_csv(nairu_ds_path, parse_dates=["date"]).set_index("date")
        cols_ds = [c for c in ["ipc_yoy", "inflation_gap"] if c in ds.columns]
        if cols_ds:
            m = m.join(ds[cols_ds], how="outer")

    m = m.reset_index().rename(columns={"index": "date"})
    m["date"] = pd.to_datetime(m["date"])
    m = m.sort_values("date").reset_index(drop=True)
    m = m[m["date"] >= SERIE_INICIO].reset_index(drop=True)
    return m


SERIE_INICIO = "2005-01-01"


# ═══════════════════════════════════════════════════════════════════════
# Entry point principal
# ═══════════════════════════════════════════════════════════════════════

def run(
    processed_dir: Path = PROCESSED_DIR,
    nairu_dir: Path = NAIRU_OUTPUT_DIR,
    output_dir: Path = PIB_POT_OUTPUT_DIR,
) -> pd.DataFrame:
    """Ejecuta el pipeline completo de PIB Potencial (metodología v3/CONFIG_V2).

    Parameters
    ----------
    processed_dir : Path
        Directorio con los CSV procesados por fuente.
    nairu_dir : Path
        Directorio con las estimaciones NAIRU (outputs/nairu/).
    output_dir : Path
        Directorio de salida para el Excel/CSV.

    Returns
    -------
    pd.DataFrame
        Dataset trimestral completo con PIB Potencial y brechas (índices
        base 100 en ``factors.BASE_QUARTER`` — ver docstring de ese módulo).
    """
    setup_logging()
    logger.info("══ Pipeline PIB POTENCIAL Colombia (Cobb-Douglas, metodología v3) ══")

    # ── 1. Cargar insumos ────────────────────────────────────────────────
    monthly = _load_monthly_labor(processed_dir)
    hc_anual = _load_hc_anual(processed_dir)
    monthly = monthly.merge(hc_anual, on="date", how="left")

    quarterly = _load_quarterly_national_accounts(processed_dir)
    nairu_monthly = _load_nairu_monthly(nairu_dir)
    if nairu_monthly is None:
        raise RuntimeError(
            "No hay estimaciones NAIRU/NAICU/ICU — requeridas por la metodología v3 "
            "(TGP*, factor trabajo potencial, brecha de capital). "
            "Ejecute '--nairu-estim' primero."
        )
    dane_capital = _load_dane_capital(DANE_CAPITAL_CSV)
    festivos = _load_festivos_trimestrales(FESTIVOS_XLSX)

    T = detect_last_complete_quarter(quarterly)
    picos_bbq = detect_bbq_peaks(quarterly)
    logger.info("Último trimestre completo T = %s | Picos BBQ: %s",
                T.date(), [str(p.date()) for p in picos_bbq])

    # ── 2. Factores de producción: trabajo, capital, alpha, producto ────
    logger.info("Calculando factores de producción (TGP*, horas, capital DANE, alpha CBO) …")
    df = compute_all_factors(
        monthly, quarterly, nairu_monthly, dane_capital,
        festivos=festivos, picos_bbq=picos_bbq, T=T, base_quarter=BASE_QUARTER,
    )

    # ── 3. PTF observada y tendencial (HP + CBO estructural) ────────────
    logger.info("Calculando PTF (observada, HP y tendencia estructural CBO) …")
    df = compute_tfp(df, T=T, base_quarter=BASE_QUARTER, picos_bbq=picos_bbq)

    # ── 4. PIB Potencial y brechas ───────────────────────────────────────
    logger.info("Calculando PIB Potencial y brechas …")
    df = compute_pib_potencial(df, T=T, base_quarter=BASE_QUARTER)

    # ── 5. Validar ────────────────────────────────────────────────────────
    logger.info("Ejecutando quality checks …")
    run_pib_potencial_checks(df)

    # ── 6. Ordenar columnas (solo las que existen en el df) ─────────────
    cols_salida = [c for c in QUARTERLY_OUTPUT_COLS if c in df.columns]
    df_out = df[cols_salida].copy()

    # ── 7. Dataset mensual (hoja Mensual del Excel) ──────────────────────
    labor_raw = pd.read_csv(processed_dir / "dane_labor_colombia.csv", parse_dates=["date"])
    andi_raw = pd.read_csv(processed_dir / "andi_capacidad_instalada.csv", parse_dates=["date"]) \
        if (processed_dir / "andi_capacidad_instalada.csv").exists() else pd.DataFrame(columns=["date"])
    df_monthly = _build_monthly(labor_raw, andi_raw, nairu_monthly, final_dir=FINAL_DIR)

    # ── 8. Construir metadatos de descarga ───────────────────────────────
    metadatos = _build_metadatos(processed_dir, nairu_dir)
    metadatos["Ancla de índices (BASE_QUARTER)"] = str(BASE_QUARTER.date())
    metadatos["Último trimestre completo (T)"] = str(T.date())

    # ── 9. Escribir Excel + CSV (el CSV alimenta la página web) ─────────
    logger.info("Escribiendo Excel y CSV …")
    path = write_pib_potencial_excel(df_out, df_monthly, output_dir, metadatos)
    csv_path = output_dir / "pib_potencial_colombia.csv"
    df_out.to_csv(csv_path, index=False)
    logger.info("CSV: %s", csv_path)

    logger.info(
        "══ PIB Potencial listo: %d trimestres | %s → %s ══",
        len(df_out),
        str(df_out["date"].iloc[0])[:10],
        str(df_out["date"].iloc[-1])[:10],
    )
    logger.info("Excel: %s", path)
    return df_out


def _build_metadatos(processed_dir: Path, nairu_dir: Path) -> dict[str, str]:
    """Recopila fechas de descarga desde los CSV procesados."""
    meta = {}

    def _ultima_fecha(filename: str, date_col: str = "download_date") -> str:
        path = processed_dir / filename
        if not path.exists():
            return "—"
        try:
            df = pd.read_csv(path)
            if date_col in df.columns:
                return str(df[date_col].dropna().iloc[-1])[:10]
        except Exception:
            pass
        return "—"

    meta["DANE PIB — última descarga"]       = _ultima_fecha("dane_gdp_colombia.csv")
    meta["DANE Inversión — última descarga"] = _ultima_fecha("dane_gdp_expenditure_colombia.csv")
    meta["DANE Ingreso — última descarga"]   = _ultima_fecha("dane_gdp_income_colombia.csv")
    meta["PWT 11.0 — última descarga"]       = _ultima_fecha("pwt_colombia.csv")
    meta["ANDI EOIC — última descarga"]      = _ultima_fecha("andi_capacidad_instalada.csv")
    meta["DANE Labor — última descarga"]     = _ultima_fecha("dane_labor_colombia.csv")

    nairu_path = nairu_dir / "nairu_colombia.csv"
    if nairu_path.exists():
        try:
            df_n = pd.read_csv(nairu_path)
            fecha_col = "Date" if "Date" in df_n.columns else "date"
            meta["NAIRU/NAICU — última estimación"] = str(df_n[fecha_col].dropna().iloc[-1])[:10]
        except Exception:
            meta["NAIRU/NAICU — última estimación"] = "—"
    else:
        meta["NAIRU/NAICU — última estimación"] = "no disponible"

    return meta


# ═══════════════════════════════════════════════════════════════════════
# Metodología legada (PIM/FBKF): NO forma parte del pipeline por defecto
# ═══════════════════════════════════════════════════════════════════════
#
# Antes de la alineación con v3 (CHANGELOG, alineación metodológica Módulo 2,
# 2026-09-10) el capital físico se construía por Inventario Permanente (PIM)
# desde la FBKF trimestral del DANE, con un ancla de estado estable K_0
# (Harberger) cuya fragilidad se auditó y cuantificó con
# ``compute_k0_sensitivity``/``_build_capital_quarterly`` (hallazgo #5 de la
# auditoría 2026-08-21). v3 (Cambio 4 de SPEC_V2.md) reemplaza el PIM por el
# STOCK DE CAPITAL PRODUCTIVO DANE observado directamente (sin ancla asumida,
# sin K_0) — ver ``src.production.factors.calcular_capital_dane``, que es la
# fuente de capital que usa ``run()`` desde ahora.
#
# ``_build_capital_quarterly`` se conserva TAL CUAL (sigue siendo una utilidad
# PIM válida y auto-contenida, con sus propios tests en
# ``tests/test_run_pib_potencial.py::TestBuildCapitalQuarterly``, que no
# tocan el resto del pipeline). ``compute_k0_sensitivity`` en cambio SÍ
# dependía de las funciones de ``src.production.factors``/``tfp``/
# ``pib_potencial`` que se reescribieron con la metodología v3 — su premisa
# (cuantificar la sensibilidad al ancla K_0 del PIM) ya no aplica al
# pipeline principal (no hay más K_0: el capital es un dato observado). Se
# retira de ``run()`` y queda como no-implementada (ver más abajo) en vez de
# reescribirse contra un pipeline paralelo solo para esta comparación legada.

from pathlib import Path as _Path  # noqa: E402  (import tardío, solo para el bloque legado)


def _build_capital_quarterly(processed_dir: _Path, k0_multiplier: float = 1.0) -> pd.DataFrame:
    """Construye el stock de capital trimestral por Inventario Permanente (PIM).

    **Metodología legada** (pre-alineación v3): NO es la fuente de capital
    del pipeline principal desde la alineación con v3 (ver nota de módulo
    más arriba) — ``run()`` usa ``factors.calcular_capital_dane`` (stock de
    capital productivo DANE observado). Se conserva tal cual, auto-contenida
    y con sus propios tests, como utilidad PIM independiente.

        K_t = K_{t-1} · (1 − δ_q) + I_t          (inventario permanente)

    - I_t : FBKF real DANE (``dane_gdp_expenditure_colombia.csv``, col. ``investment``)
    - δ   : promedio de la depreciación PWT  →  δ_q = 1 − (1 − δ)^(1/4)
    - K_0 : estado estacionario  K_0 = I_0 / (g_q + δ_q)  (Harberger 1978),
            con I_0 = FBKF media del primer año y g_q = crecimiento trim. medio.
    - H   : capital humano PWT (hc), trimestralizado por arrastre y constante desde
            el último año PWT hasta el presente.

    Parameters
    ----------
    k0_multiplier : float
        Multiplica el K_0 de estado estacionario (Harberger) por este factor
        antes de la recursión del inventario permanente. 1.0 = sin cambios.

    Devuelve un DataFrame indexado por fecha con columnas ``K``, ``delta``, ``H``,
    cubriendo todo el rango de la FBKF DANE (2005-Q1 → presente).
    """
    inv = pd.read_csv(_require(processed_dir / "dane_gdp_expenditure_colombia.csv"), parse_dates=["date"])
    inv = inv.sort_values("date").set_index("date")
    inv_q = inv["investment"].resample("QS").last().dropna()

    pwt = pd.read_csv(_require(processed_dir / "pwt_colombia.csv"), parse_dates=["date"])
    pwt = pwt.sort_values("date").set_index("date")
    delta_annual = float(pwt["depreciation_rate"].mean())
    delta_q = 1.0 - (1.0 - delta_annual) ** 0.25

    g_q = max(float(inv_q.pct_change(fill_method=None).mean()), 0.0)
    i0 = float(inv_q.iloc[:4].mean())
    k0 = (i0 / (g_q + delta_q)) * k0_multiplier

    k_vals: list[float] = []
    k = k0
    for i_t in inv_q.to_numpy():
        k = k * (1.0 - delta_q) + float(i_t)
        k_vals.append(k)
    k_series = pd.Series(k_vals, index=inv_q.index, name="K")

    h_q = (
        pwt["human_capital"]
        .resample("QS").ffill()
        .reindex(inv_q.index, method="ffill")
    )

    out = pd.DataFrame({"K": k_series, "delta": delta_annual, "H": h_q})
    out.index.name = "date"
    return out


K0_SENSITIVITY_MULTIPLIERS: tuple[float, ...] = (0.8, 0.9, 1.0, 1.1, 1.2)


def compute_k0_sensitivity(
    processed_dir: _Path = PROCESSED_DIR,
    nairu_dir: _Path = NAIRU_OUTPUT_DIR,
    multipliers: tuple[float, ...] = K0_SENSITIVITY_MULTIPLIERS,
) -> pd.DataFrame:
    """**Retirada** (metodología legada, PIM): no forma parte del pipeline principal.

    Antes de la alineación con v3 esta función cuantificaba la sensibilidad
    del PIB potencial al ancla K_0 del capital PIM (hallazgo #5 de la
    auditoría 2026-08-21). Desde que el capital físico se construye a partir
    del stock de capital productivo DANE (dato observado, sin ancla asumida
    — ver nota de módulo más arriba), no existe un K_0 del que medir
    sensibilidad en el pipeline principal, y esta función ya no puede
    llamarse contra ``src.production.factors``/``tfp``/``pib_potencial``
    (sus firmas cambiaron con la metodología v3).

    ``multipliers`` sigue validándose (debe incluir 1.0) para no romper ese
    contrato, pero la función no ejecuta ningún pipeline.

    Raises
    ------
    ValueError
        Si ``1.0`` no está en ``multipliers``.
    NotImplementedError
        Siempre (tras la validación) — ver docstring.
    """
    if 1.0 not in multipliers:
        raise ValueError("multipliers debe incluir 1.0 (la corrida base de referencia).")
    raise NotImplementedError(
        "compute_k0_sensitivity quedó retirada al alinear el capital físico con la "
        "metodología v3 (stock de capital productivo DANE, sin ancla K_0/PIM). "
        "Ver CHANGELOG.md y docs/integracion_v3.md."
    )
