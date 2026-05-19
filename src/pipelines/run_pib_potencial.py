"""Pipeline PIB Potencial Colombia (Cobb-Douglas).

Orquesta la cadena completa:

    1. Cargar y alinear fuentes procesadas a frecuencia trimestral
    2. Calcular factores (L, K, alpha)           → src.production.factors
    3. Calcular PTF observada y tendencial (HP)  → src.production.tfp
    4. Calcular PIB Potencial y brechas          → src.production.pib_potencial
    5. Validar con quality checks                → src.quality_checks
    6. Escribir Excel multi-hoja                 → src.production.excel_writer

Fuentes requeridas (todas en PROCESSED_DIR salvo NAIRU):
    dane_gdp_colombia.csv              → PIB trimestral
    andi_capacidad_instalada.csv       → UCI mensual
    dane_labor_colombia.csv            → TD, TGP, PET mensuales
    pwt_colombia.csv                   → K, delta, hc anuales
    dane_gdp_income_colombia.csv       → RA, EBE, IM trimestrales
    outputs/nairu/nairu_colombia.csv   → NAIRU*, NAICU* mensuales

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

import numpy as np
import pandas as pd

from src.config import FINAL_DIR, OUTPUTS_DIR, PROCESSED_DIR
from src.io_utils import setup_logging
from src.production.excel_writer import write_pib_potencial_excel
from src.production.factors import compute_all_factors
from src.production.pib_potencial import QUARTERLY_OUTPUT_COLS, compute_pib_potencial
from src.production.tfp import compute_tfp
from src.quality_checks import run_pib_potencial_checks

logger = logging.getLogger("nairu_pipeline.pib_potencial")

# Directorio de salida
PIB_POT_OUTPUT_DIR = OUTPUTS_DIR / "pib_potencial"
NAIRU_OUTPUT_DIR   = OUTPUTS_DIR / "nairu"

# Recorte inicial de la serie (primer trimestre con todas las fuentes)
SERIE_INICIO = "2005-01-01"


# ═══════════════════════════════════════════════════════════════════════
# Carga y alineación de fuentes
# ═══════════════════════════════════════════════════════════════════════

def _load_csv(filename: str, processed_dir: Path) -> pd.DataFrame:
    """Carga un CSV procesado y devuelve DataFrame indexado por fecha."""
    path = processed_dir / filename
    if not path.exists():
        raise FileNotFoundError(
            f"Fuente no encontrada: {path}\n"
            f"Ejecute el pipeline correspondiente antes de '--pib-potencial'."
        )
    df = pd.read_csv(path, parse_dates=["date"])
    return df.sort_values("date").set_index("date")


def _resample_mean(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Agrega a frecuencia trimestral tomando el PROMEDIO del trimestre."""
    return df[cols].resample("QS").mean()


def _resample_last(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Agrega a frecuencia trimestral tomando el ÚLTIMO valor del trimestre."""
    return df[cols].resample("QS").last()


def _load_pwt_quarterly(processed_dir: Path) -> pd.DataFrame:
    """Reutiliza la lógica de ley de acumulación de capital de build_production_function_dataset."""
    pwt = _load_csv("pwt_colombia.csv", processed_dir)
    ann = pwt[["capital_stock_real", "depreciation_rate", "human_capital"]].copy()

    K = ann["capital_stock_real"].values
    d = ann["depreciation_rate"].values
    I_ann = np.empty(len(K))
    I_ann[:-1] = K[1:] - K[:-1] * (1 - d[:-1])
    I_ann[-1]  = I_ann[-2]

    rows = []
    for i, (date, row) in enumerate(ann.iterrows()):
        delta_q = 1 - (1 - row["depreciation_rate"]) ** 0.25
        I_q = I_ann[i] / 4
        K_q = row["capital_stock_real"]
        for q in range(4):
            rows.append({
                "date":  date + pd.DateOffset(months=3 * q),
                "K":     K_q,
                "delta": row["depreciation_rate"],
                "H":     row["human_capital"],
            })
            K_q = K_q * (1 - delta_q) + I_q

    return pd.DataFrame(rows).set_index("date").sort_index()


def _load_nairu(nairu_dir: Path) -> pd.DataFrame | None:
    """Carga el output del modelo NAIRU/NAICU. Retorna None si no existe."""
    path = nairu_dir / "nairu_colombia.csv"
    if not path.exists():
        warnings.warn(
            f"Estimaciones NAIRU no encontradas en {nairu_dir}. "
            "Se usarán proxies (TD para NAIRU*, UCI para NAICU*). "
            "Ejecute '--nairu-estim' para resultados correctos.",
            stacklevel=3,
        )
        return None
    df = pd.read_csv(path, parse_dates=["Date"])
    df = df.rename(columns={"Date": "date"})
    return df.sort_values("date").set_index("date")


def load_and_align_sources(
    processed_dir: Path = PROCESSED_DIR,
    nairu_dir: Path = NAIRU_OUTPUT_DIR,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Carga y alinea todas las fuentes a frecuencia trimestral y mensual.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        ``(df_quarterly, df_monthly)``
        - ``df_quarterly`` : todas las columnas alineadas a QS
        - ``df_monthly``   : columnas mensuales para la hoja Mensual del Excel
    """
    logger.info("Cargando fuentes procesadas …")

    # ── 1. PIB (ya es trimestral) ──────────────────────────────────────
    gdp = _load_csv("dane_gdp_colombia.csv", processed_dir)
    pib_q = gdp[["gdp_observed"]].resample("QS").last().rename(
        columns={"gdp_observed": "PIB"}
    )

    # ── 2. UCI (mensual → promedio trimestral) ─────────────────────────
    andi = _load_csv("andi_capacidad_instalada.csv", processed_dir)
    uci_q = _resample_mean(andi, ["capacity_utilization"]).rename(
        columns={"capacity_utilization": "UCI"}
    )

    # ── 3. Labor (mensual → promedio trimestral) ───────────────────────
    labor = _load_csv("dane_labor_colombia.csv", processed_dir)
    labor_q = _resample_mean(
        labor, ["tgp_rate", "unemployment_rate", "pet_thousands"]
    ).rename(columns={
        "tgp_rate":          "TGP",
        "unemployment_rate": "TD",
        "pet_thousands":     "PET",
    })

    # ── 4. PWT (anual → trimestral vía acumulación) ────────────────────
    pwt_q = _load_pwt_quarterly(processed_dir)

    # ── 5. Ingreso DANE (trimestral, desde 2016-Q1) ────────────────────
    income_path = processed_dir / "dane_gdp_income_colombia.csv"
    if income_path.exists():
        income = pd.read_csv(income_path, parse_dates=["date"])
        income = income.sort_values("date").set_index("date")
        income_q = income[
            ["compensation_employees", "gross_operating_surplus", "mixed_income"]
        ].resample("QS").last()
    else:
        logger.warning(
            "dane_gdp_income_colombia.csv no encontrado — alpha usará valor de respaldo."
        )
        income_q = pd.DataFrame(
            columns=["compensation_employees", "gross_operating_surplus", "mixed_income"]
        )

    # ── 6. NAIRU/NAICU (mensual → promedio trimestral) ─────────────────
    nairu_df = _load_nairu(nairu_dir)
    if nairu_df is not None:
        nairu_q = nairu_df[["nairu_estimate", "naicu_estimate"]].resample("QS").mean().rename(
            columns={
                "nairu_estimate": "NAIRU_q",
                "naicu_estimate": "NAICU_q",
            }
        )
    else:
        nairu_q = None

    # ── 7. Unir todo en frecuencia trimestral ──────────────────────────
    df = (
        pib_q
        .join(uci_q,    how="left")
        .join(labor_q,  how="left")
        .join(pwt_q,    how="left")
        .join(income_q, how="left")
    )
    if nairu_q is not None:
        df = df.join(nairu_q, how="left")

    df = df.reset_index().rename(columns={"date": "date"})
    df["date"]    = pd.to_datetime(df["date"])
    df["year"]    = df["date"].dt.year
    df["quarter"] = df["date"].dt.quarter

    # Recortar al inicio de la serie (2005-Q1)
    df = df[df["date"] >= SERIE_INICIO].reset_index(drop=True)

    # Eliminar trimestres sin PIB ni K (sin ellos el cálculo no tiene sentido)
    df = df.dropna(subset=["PIB", "K"]).reset_index(drop=True)

    logger.info(
        "Dataset trimestral: %d trimestres, %s → %s",
        len(df),
        str(df["date"].iloc[0])[:10],
        str(df["date"].iloc[-1])[:10],
    )

    # ── 8. Dataset mensual para la hoja Mensual del Excel ─────────────
    df_monthly = _build_monthly(
        labor, andi, nairu_df,
        final_dir=FINAL_DIR,
    )

    return df, df_monthly


def _build_monthly(
    labor: pd.DataFrame,
    andi: pd.DataFrame,
    nairu_df: pd.DataFrame | None,
    final_dir: Path = FINAL_DIR,
) -> pd.DataFrame:
    """Construye el dataset mensual para la hoja Mensual del Excel."""
    # Base: labor mensual
    cols_labor = ["tgp_rate", "unemployment_rate"]
    m = labor[[c for c in cols_labor if c in labor.columns]].copy()
    m = m.rename(columns={
        "tgp_rate":          "tgp_rate",
        "unemployment_rate": "unemployment_rate",
    })

    # UCI
    if "capacity_utilization" in andi.columns:
        m = m.join(andi[["capacity_utilization"]], how="outer")

    # NAIRU/NAICU
    if nairu_df is not None:
        cols_nairu = [c for c in ["nairu_estimate", "naicu_estimate",
                                   "nairu_ci_lower_90", "nairu_ci_upper_90"]
                      if c in nairu_df.columns]
        m = m.join(nairu_df[cols_nairu], how="outer")

    # ipc_yoy e inflation_gap desde nairu_dataset.csv (si existe)
    nairu_ds_path = final_dir / "nairu_dataset.csv"
    if nairu_ds_path.exists():
        ds = pd.read_csv(nairu_ds_path, parse_dates=["date"]).set_index("date")
        cols_ds = [c for c in ["ipc_yoy", "inflation_gap"] if c in ds.columns]
        if cols_ds:
            m = m.join(ds[cols_ds], how="outer")

    m = m.reset_index().rename(columns={"date": "date"})
    m["date"] = pd.to_datetime(m["date"])
    m = m.sort_values("date").reset_index(drop=True)

    # Recortar al mismo inicio
    m = m[m["date"] >= SERIE_INICIO].reset_index(drop=True)
    return m


# ═══════════════════════════════════════════════════════════════════════
# Entry point principal
# ═══════════════════════════════════════════════════════════════════════

def run(
    processed_dir: Path = PROCESSED_DIR,
    nairu_dir: Path = NAIRU_OUTPUT_DIR,
    output_dir: Path = PIB_POT_OUTPUT_DIR,
) -> pd.DataFrame:
    """Ejecuta el pipeline completo de PIB Potencial.

    Parameters
    ----------
    processed_dir : Path
        Directorio con los CSV procesados por fuente.
    nairu_dir : Path
        Directorio con las estimaciones NAIRU (outputs/nairu/).
    output_dir : Path
        Directorio de salida para el Excel.

    Returns
    -------
    pd.DataFrame
        Dataset trimestral completo con PIB Potencial y brechas.
    """
    setup_logging()
    logger.info("══ Pipeline PIB POTENCIAL Colombia (Cobb-Douglas) ══")

    # 1. Cargar y alinear fuentes
    df, df_monthly = load_and_align_sources(processed_dir, nairu_dir)

    # 2. Factores de producción: L, K, alpha
    logger.info("Calculando factores de producción …")
    df = compute_all_factors(df)

    # 3. PTF observada y tendencial
    logger.info("Calculando PTF (HP filter, λ=1600) …")
    df = compute_tfp(df)

    # 4. PIB Potencial y brechas
    logger.info("Calculando PIB Potencial y brechas …")
    df = compute_pib_potencial(df)

    # 5. Validar
    logger.info("Ejecutando quality checks …")
    run_pib_potencial_checks(df)

    # 6. Ordenar columnas (solo las que existen en el df)
    cols_salida = [c for c in QUARTERLY_OUTPUT_COLS if c in df.columns]
    df_out = df[cols_salida].copy()

    # 7. Construir metadatos de descarga
    metadatos = _build_metadatos(processed_dir, nairu_dir)

    # 8. Escribir Excel
    logger.info("Escribiendo Excel …")
    path = write_pib_potencial_excel(df_out, df_monthly, output_dir, metadatos)

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

    # NAIRU: última fecha estimada
    nairu_path = nairu_dir / "nairu_colombia.csv"
    if nairu_path.exists():
        try:
            df_n = pd.read_csv(nairu_path)
            fecha_col = "Date" if "Date" in df_n.columns else "date"
            meta["NAIRU/NAICU — última estimación"] = str(
                df_n[fecha_col].dropna().iloc[-1]
            )[:10]
        except Exception:
            meta["NAIRU/NAICU — última estimación"] = "—"
    else:
        meta["NAIRU/NAICU — última estimación"] = "no disponible"

    return meta
