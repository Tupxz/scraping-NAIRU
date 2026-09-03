"""Pipeline: Dataset trimestral para la función de producción Cobb-Douglas.

Construye un CSV **trimestral** que replica la estructura de la hoja
``Niveles`` del Excel de referencia (``FUNCION DE PRODUCCION.xlsx``):

    Periodo, PIB, Var%PIB, UCI, Var%UCI, K, Var%K, PET, Var%PET,
    TGP, Var%TGP, TD, Var%TD, H, Var%H, delta, Var%delta,
    L, Var%L, A, Var%A

Reglas de agregación trimestral
---------------------------------
    PIB      → valor directo del trimestre (dane_gdp_colombia.csv)
    UCI      → último mes del trimestre   (andi_capacidad_instalada.csv)
    TGP, TD  → último mes del trimestre   (dane_labor_colombia.csv, tasas)
    PET      → último mes del trimestre   (dane_labor_colombia.csv, stock)
    K, H, δ  → PWT es anual; se forward-fill al trimestre (valor de enero
               se propaga a Q2–Q4 del mismo año)

Fórmulas clave (alpha = 0.4, beta = 0.6)
-----------------------------------------
    L = (TGP/100) × PET × (1 − TD/100)   [miles de personas ocupadas]
    A = PIB / (K^alpha × L^beta)           [PTF — Productividad Total de Factores]

Salida
------
    outputs/production_function_quarterly.csv
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import OUTPUTS_DIR, PROCESSED_DIR

logger = logging.getLogger("nairu_pipeline.prod_func")

ALPHA: float = 0.4
BETA: float  = 0.6
OUTPUT_FILENAME = "production_function_quarterly.csv"


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

def _load(filename: str) -> pd.DataFrame:
    path = PROCESSED_DIR / filename
    df = pd.read_csv(path, parse_dates=["date"])
    return df.sort_values("date").set_index("date")


def _resample_last(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Resamplea a frecuencia trimestral tomando el ÚLTIMO dato del trimestre."""
    return df[cols].resample("QS").last()


def _pct_change(s: pd.Series) -> pd.Series:
    """Variación porcentual trimestre a trimestre (fracción).

    Fix 2026-09-01: fill_method=None explícito (ver src/merge.py para el
    detalle del bug) -- si alguna columna tiene un hueco interior, ahora
    se propaga NaN en vez de fabricar una variación contra un valor
    rellenado hacia adelante.
    """
    return s.pct_change(fill_method=None)


# ═══════════════════════════════════════════════════════════════════════
# Pipeline principal
# ═══════════════════════════════════════════════════════════════════════

def build_production_function_dataset(
    output_dir: Path | None = None,
) -> pd.DataFrame:
    """Construye el dataset trimestral de función de producción y lo guarda.

    Returns
    -------
    pd.DataFrame
        Columnas: Periodo, PIB, Var%PIB, UCI, Var%UCI, K, Var%K,
        PET, Var%PET, TGP, Var%TGP, TD, Var%TD, H, Var%H,
        delta, Var%delta, L, Var%L, A, Var%A
    """
    out_dir = output_dir or OUTPUTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("[ProdFunc] Cargando fuentes …")

    # ── 1. PIB — ya es trimestral (inicio de cada trimestre) ──────
    gdp = _load("dane_gdp_colombia.csv")
    pib_q = gdp[["gdp_observed"]].resample("QS").last().rename(
        columns={"gdp_observed": "PIB"}
    )

    # ── 2. UCI — mensual → último del trimestre ───────────────────
    andi = _load("andi_capacidad_instalada.csv")
    uci_q = _resample_last(andi, ["capacity_utilization"]).rename(
        columns={"capacity_utilization": "UCI"}
    )

    # ── 3. Labor (tasas y PET) — mensual → último del trimestre ───
    labor = _load("dane_labor_colombia.csv")
    labor_q = _resample_last(
        labor, ["tgp_rate", "unemployment_rate", "pet_thousands"]
    ).rename(columns={
        "tgp_rate":           "TGP",
        "unemployment_rate":  "TD",
        "pet_thousands":      "PET",
    })

    # ── 4. PWT — anual → trimestral vía ley de acumulación de capital ────
    # K(t+1) = K(t)·(1−δ) + I(t)
    # Pasos:
    #   a) Derivar inversión anual implícita: I_t = K_{t+1} − K_t·(1−δ_t)
    #   b) Convertir δ anual a trimestral:    δ_q = 1 − (1−δ)^(1/4)
    #   c) Distribuir inversión uniformemente: I_q = I_t / 4
    #   d) Propagar Q2, Q3, Q4 dentro de cada año desde el ancla Q1 = K_t anual
    #   e) Capital humano H (sin ley de acumulación propia): ffill intra-año
    pwt = _load("pwt_colombia.csv")
    ann = pwt[["capital_stock_real", "depreciation_rate", "human_capital"]].copy()
    ann = ann.sort_index()

    # Inversión anual implícita (último año: extrapolar con I del año anterior)
    K  = ann["capital_stock_real"].values
    d  = ann["depreciation_rate"].values
    I_ann = np.empty(len(K))
    I_ann[:-1] = K[1:] - K[:-1] * (1 - d[:-1])
    I_ann[-1]  = I_ann[-2]                       # extrapolar último año

    # Construir trimestres vía ley de acumulación
    rows = []
    for i, (date, row) in enumerate(ann.iterrows()):
        delta_q = 1 - (1 - row["depreciation_rate"]) ** 0.25
        I_q     = I_ann[i] / 4
        K_q     = row["capital_stock_real"]        # ancla Q1 = dato anual PWT
        for q in range(4):
            rows.append({
                "date":  date + pd.DateOffset(months=3 * q),
                "K":     K_q,
                "delta": row["depreciation_rate"],
                "H":     row["human_capital"],
            })
            K_q = K_q * (1 - delta_q) + I_q      # siguiente trimestre

    pwt_q = (
        pd.DataFrame(rows)
        .set_index("date")
        .sort_index()
    )
    pwt_q.index.name = "date"

    # ── 5. Unir todo por trimestre ────────────────────────────────
    df = (
        pib_q
        .join(uci_q,   how="left")
        .join(labor_q, how="left")
        .join(pwt_q,   how="left")
    )
    df.index.name = "Periodo"
    df = df.reset_index()
    df = df.dropna(subset=["PIB", "K", "TGP", "TD", "PET"]).copy()
    df = df.sort_values("Periodo").reset_index(drop=True)

    # ── 6. Derivar L y A ─────────────────────────────────────────
    df["L"] = (df["TGP"] / 100.0) * df["PET"] * (1.0 - df["TD"] / 100.0)
    # Trabajo efectivo = H · L (capital humano dentro del término de trabajo,
    # como en la Función de Producción de los profesores: Y = A·K^α·(H·L)^(1−α)).
    df["A"] = df["PIB"] / (df["K"] ** ALPHA * (df["H"] * df["L"]) ** BETA)

    # ── 7. Variaciones porcentuales (trim-a-trim) ─────────────────
    var_cols = ["PIB", "UCI", "K", "PET", "TGP", "TD", "H", "delta", "L", "A"]
    for col in var_cols:
        if col in df.columns:
            df[f"Var%{col}"] = _pct_change(df[col]).round(6)

    # ── 8. Ordenar columnas ───────────────────────────────────────
    ordered = [
        "Periodo",
        "PIB",   "Var%PIB",
        "UCI",   "Var%UCI",
        "K",     "Var%K",
        "PET",   "Var%PET",
        "TGP",   "Var%TGP",
        "TD",    "Var%TD",
        "H",     "Var%H",
        "delta", "Var%delta",
        "L",     "Var%L",
        "A",     "Var%A",
    ]
    df = df[[c for c in ordered if c in df.columns]]

    # ── 9. Guardar ────────────────────────────────────────────────
    out_path = out_dir / OUTPUT_FILENAME
    df.to_csv(out_path, index=False)
    logger.info(
        "[ProdFunc] Guardado: %s  (%d trimestres, %s → %s)",
        out_path.name, len(df),
        str(df["Periodo"].iloc[0])[:10],
        str(df["Periodo"].iloc[-1])[:10],
    )
    return df


def run() -> pd.DataFrame:
    """Entry-point para el pipeline principal."""
    return build_production_function_dataset()
