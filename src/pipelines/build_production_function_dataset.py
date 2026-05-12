"""Pipeline: Dataset anual para la función de producción Cobb-Douglas.

Construye un CSV anual que replica la estructura de la hoja ``Niveles``
del Excel de referencia (``FUNCION DE PRODUCCION.xlsx``):

    Periodo, PIB, Var%PIB, UCI, Var%UCI, K, Var%K, PET, Var%PET,
    TGP, Var%TGP, TD, Var%TD, H, Var%H, delta, Var%delta,
    L, Var%L, A, Var%A

Fórmulas clave (con alpha = 0.4, beta = 0.6)
--------------------------------------------
    L = (TGP/100) × PET × (1 − TD/100)      [miles de personas]
    A = PIB / (K^alpha × L^beta)              [PTF — Productividad Total de Factores]

Variables fuente
----------------
    PIB      ← dane_gdp_colombia.csv      (suma anual de 4 trimestres)
    UCI      ← andi_capacidad_instalada.csv (media anual)
    K        ← pwt_colombia.csv           (capital_stock_real, anual — PWT 11.0 rnna)
    delta    ← pwt_colombia.csv           (depreciation_rate, anual)
    H        ← pwt_colombia.csv           (human_capital, anual)
    PET      ← dane_labor_colombia.csv    (media anual, miles)
    TGP      ← dane_labor_colombia.csv    (media anual, %)
    TD       ← dane_labor_colombia.csv    (media anual, %)

Salida
------
    data/outputs/production_function_annual.csv
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.config import OUTPUTS_DIR, PROCESSED_DIR

logger = logging.getLogger("nairu_pipeline.prod_func")

ALPHA: float = 0.4      # elasticidad del capital (Cobb-Douglas)
BETA: float  = 0.6      # elasticidad del trabajo
OUTPUT_FILENAME = "production_function_annual.csv"


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

def _load(filename: str) -> pd.DataFrame:
    path = PROCESSED_DIR / filename
    df = pd.read_csv(path, parse_dates=["date"])
    return df.sort_values("date")


def _pct_change(s: pd.Series) -> pd.Series:
    """Variación porcentual año a año (fracción, no %)."""
    return s.pct_change()


# ═══════════════════════════════════════════════════════════════════════
# Pipeline principal
# ═══════════════════════════════════════════════════════════════════════

def build_production_function_dataset(
    output_dir: Path | None = None,
) -> pd.DataFrame:
    """Construye el dataset anual de función de producción y lo guarda.

    Returns
    -------
    pd.DataFrame
        Columnas: Periodo, PIB, Var%PIB, UCI, Var%UCI, K, Var%K,
        PET, Var%PET, TGP, Var%TGP, TD, Var%TD, H, Var%H,
        delta, Var%delta, L, Var%L, A, Var%A
    """
    out_dir = output_dir or OUTPUTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── 1. Cargar fuentes ──────────────────────────────────────────
    logger.info("[ProdFunc] Cargando fuentes …")

    # PIB trimestral → suma anual
    gdp = _load("dane_gdp_colombia.csv")
    gdp["year"] = gdp["date"].dt.year
    pib_annual = gdp.groupby("year")["gdp_observed"].sum().rename("PIB")

    # UCI mensual → media anual
    andi = _load("andi_capacidad_instalada.csv")
    andi["year"] = andi["date"].dt.year
    uci_annual = andi.groupby("year")["capacity_utilization"].mean().rename("UCI")

    # PWT: anual (solo enero de cada año en el CSV)
    pwt = _load("pwt_colombia.csv")
    pwt["year"] = pwt["date"].dt.year
    pwt_annual = pwt.groupby("year").agg(
        K    =("capital_stock_real", "first"),
        delta=("depreciation_rate",  "first"),
        H    =("human_capital",       "first"),
    )

    # Labor: media anual
    labor = _load("dane_labor_colombia.csv")
    labor["year"] = labor["date"].dt.year
    labor_annual = labor.groupby("year").agg(
        PET=("pet_thousands",      "mean"),
        TGP=("tgp_rate",           "mean"),
        TD =("unemployment_rate",  "mean"),
    )

    # ── 2. Unir todo por año ───────────────────────────────────────
    df = (
        pib_annual
        .to_frame()
        .join(uci_annual, how="outer")
        .join(pwt_annual,   how="left")
        .join(labor_annual, how="left")
    )
    df.index.name = "Periodo"
    df = df.reset_index()
    df = df.dropna(subset=["PIB", "K", "TGP", "TD", "PET"]).copy()
    df = df.sort_values("Periodo").reset_index(drop=True)

    # ── 3. Derivar L y A ──────────────────────────────────────────
    # L en miles de personas ocupadas
    df["L"] = (df["TGP"] / 100.0) * df["PET"] * (1.0 - df["TD"] / 100.0)

    # PTF: A = PIB / (K^alpha × L^beta)
    df["A"] = df["PIB"] / (df["K"] ** ALPHA * df["L"] ** BETA)

    # ── 4. Variaciones porcentuales ──────────────────────────────
    var_cols = ["PIB", "UCI", "K", "PET", "TGP", "TD", "H", "delta", "L", "A"]
    for col in var_cols:
        if col in df.columns:
            df[f"Var%{col}"] = _pct_change(df[col]).round(6)

    # ── 5. Ordenar columnas (igual que el Excel de referencia) ────
    ordered = [
        "Periodo",
        "PIB",    "Var%PIB",
        "UCI",    "Var%UCI",
        "K",      "Var%K",
        "PET",    "Var%PET",
        "TGP",    "Var%TGP",
        "TD",     "Var%TD",
        "H",      "Var%H",
        "delta",  "Var%delta",
        "L",      "Var%L",
        "A",      "Var%A",
    ]
    df = df[[c for c in ordered if c in df.columns]]

    # ── 6. Guardar ────────────────────────────────────────────────
    out_path = out_dir / OUTPUT_FILENAME
    df.to_csv(out_path, index=False)
    logger.info(
        "[ProdFunc] Guardado: %s  (%d años, %d–%d)",
        out_path.name, len(df),
        int(df["Periodo"].min()), int(df["Periodo"].max()),
    )
    return df


def run() -> pd.DataFrame:
    """Entry-point para el pipeline principal."""
    return build_production_function_dataset()
