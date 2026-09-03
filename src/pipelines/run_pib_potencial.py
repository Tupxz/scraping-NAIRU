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


def _build_capital_quarterly(processed_dir: Path, k0_multiplier: float = 1.0) -> pd.DataFrame:
    """Construye el stock de capital trimestral por inventario permanente (PIM).

    Parsimonia (decisión 2026-06-08): el capital se arma SOLO con datos del DANE
    —la FBKF real trimestral, disponible hasta el presente— en vez de empalmar la
    serie anual de PWT (que corta en 2023). De PWT se toma un único número: la
    depreciación promedio.

        K_t = K_{t-1} · (1 − δ_q) + I_t          (inventario permanente)

    - I_t : FBKF real DANE (``dane_gdp_expenditure_colombia.csv``, col. ``investment``)
    - δ   : promedio de la depreciación PWT  →  δ_q = 1 − (1 − δ)^(1/4)
    - K_0 : estado estacionario  K_0 = I_0 / (g_q + δ_q)  (Harberger 1978),
            con I_0 = FBKF media del primer año y g_q = crecimiento trim. medio.
            Se estima con el propio DANE; su influencia decae con la depreciación.
    - H   : capital humano PWT (hc), trimestralizado por arrastre y constante desde
            el último año PWT hasta el presente (índice muy suave; sin análogo DANE).

    Fase 1 del plan de limpieza (hallazgo #5 de la auditoría 2026-08-21,
    decisión del usuario 2026-09-01): K_0 se ancla en el primer trimestre de
    la serie FBKF DANE (2005-Q1) y su peso decae solo con la depreciación
    -- (1−δ_q)^84 ≈ 0,50 hacia 2026, es decir que un error en K_0 SIGUE
    pesando la mitad de su magnitud original dos décadas después. No hay
    forma de "arreglar" eso sin otro ancla (no existe una serie de capital
    DANE anterior a 2005 con la que contrastar); ``k0_multiplier`` existe
    para poder CUANTIFICAR esa sensibilidad en vez de dejarla implícita
    -- ver ``compute_k0_sensitivity`` más abajo.

    Parameters
    ----------
    k0_multiplier : float
        Multiplica el K_0 de estado estacionario (Harberger) por este factor
        antes de la recursión del inventario permanente. 1.0 = sin cambios
        (comportamiento por defecto). Úsalo solo para análisis de
        sensibilidad -- el pipeline principal siempre corre con 1.0.

    Devuelve un DataFrame indexado por fecha con columnas ``K``, ``delta``, ``H``,
    cubriendo todo el rango de la FBKF DANE (2005-Q1 → presente).
    """
    # ── Flujo de inversión: FBKF real DANE (único insumo del nivel de capital) ──
    inv = _load_csv("dane_gdp_expenditure_colombia.csv", processed_dir)
    inv_q = inv["investment"].resample("QS").last().dropna()

    # ── Depreciación: un solo número desde PWT (promedio de su serie) ──
    pwt = _load_csv("pwt_colombia.csv", processed_dir)
    delta_annual = float(pwt["depreciation_rate"].mean())
    delta_q = 1.0 - (1.0 - delta_annual) ** 0.25

    # ── Capital inicial en estado estacionario (estimado con DANE) ──
    # Fix 2026-09-01: fill_method=None explícito (ver src/merge.py). Sin
    # efecto numérico aquí (inv_q ya viene sin NaN por el .dropna() de
    # arriba) pero mantiene el mismo patrón en todo el repo y evita el
    # FutureWarning de pandas.
    g_q = max(float(inv_q.pct_change(fill_method=None).mean()), 0.0)
    i0 = float(inv_q.iloc[:4].mean())
    k0 = (i0 / (g_q + delta_q)) * k0_multiplier
    if k0_multiplier != 1.0:
        logger.info("Capital PIM: K_0 perturbado x%.2f (análisis de sensibilidad)", k0_multiplier)

    # ── Recursión del inventario permanente ──
    k_vals: list[float] = []
    k = k0
    for i_t in inv_q.to_numpy():
        k = k * (1.0 - delta_q) + float(i_t)
        k_vals.append(k)
    k_series = pd.Series(k_vals, index=inv_q.index, name="K")

    # ── Capital humano PWT → trimestral por arrastre, constante tras el último año ──
    h_q = (
        pwt["human_capital"]
        .resample("QS").ffill()
        .reindex(inv_q.index, method="ffill")
    )

    out = pd.DataFrame({"K": k_series, "delta": delta_annual, "H": h_q})
    out.index.name = "date"
    logger.info(
        "Capital PIM (FBKF DANE): K_0=%.0f, delta=%.4f, %d trimestres %s -> %s",
        k0, delta_annual, len(out),
        str(out.index[0])[:10], str(out.index[-1])[:10],
    )
    return out


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
    k0_multiplier: float = 1.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Carga y alinea todas las fuentes a frecuencia trimestral y mensual.

    Parameters
    ----------
    k0_multiplier : float
        Ver ``_build_capital_quarterly``. 1.0 = comportamiento normal; solo
        se usa distinto de 1.0 desde ``compute_k0_sensitivity`` (Fase 1,
        análisis de sensibilidad de K_0).

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

    # ── 4. Capital (inventario permanente con FBKF DANE; δ y H desde PWT) ──
    pwt_q = _build_capital_quarterly(processed_dir, k0_multiplier=k0_multiplier)

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
# Análisis de sensibilidad de K₀ (Fase 1 del plan de limpieza, 2026-09-01)
# ═══════════════════════════════════════════════════════════════════════

K0_SENSITIVITY_MULTIPLIERS: tuple[float, ...] = (0.8, 0.9, 1.0, 1.1, 1.2)


def compute_k0_sensitivity(
    processed_dir: Path = PROCESSED_DIR,
    nairu_dir: Path = NAIRU_OUTPUT_DIR,
    multipliers: tuple[float, ...] = K0_SENSITIVITY_MULTIPLIERS,
) -> pd.DataFrame:
    """Cuantifica cuánto le importa al PIB potencial un error en K_0.

    Hallazgo #5 de la auditoría 2026-08-21 (Fase 1 del plan de limpieza,
    decisión del usuario 2026-09-01): K_0 (capital inicial de estado
    estacionario, ver ``_build_capital_quarterly``) se ancla en 2005-Q1 y
    su peso decae solo con la depreciación acumulada -- (1−δ_q)^84 ≈ 0,50
    hacia 2026, así que un error de 10 % en K_0 todavía se nota dos
    décadas después. No existe un ancla alternativa mejor (no hay serie de
    capital DANE anterior a 2005), así que en vez de "arreglar" K_0 este
    análisis lo hace auditable: corre el pipeline completo con K_0
    perturbado ±10 %/±20 % y mide el efecto en PIB_pot y Brecha_CD a
    través del tiempo -- la estimación central (multiplicador 1.0) NO
    cambia.

    Parameters
    ----------
    multipliers : tuple[float, ...]
        Factores por los que se multiplica K_0. Debe incluir 1.0 (la
        corrida base, usada como referencia para las columnas ``delta_*``).

    Returns
    -------
    pd.DataFrame
        Formato largo, una fila por (fecha, multiplicador): ``date``,
        ``k0_multiplier``, ``K_pot``, ``PIB_pot``, ``Brecha_CD`` y, contra
        la corrida base (multiplicador 1.0), ``delta_K_pot_pct``,
        ``delta_PIB_pot_pct`` (ambos en %) y ``delta_Brecha_CD_pp`` (en
        puntos porcentuales).
    """
    if 1.0 not in multipliers:
        raise ValueError("multipliers debe incluir 1.0 (la corrida base de referencia).")

    escenarios: list[pd.DataFrame] = []
    for m in multipliers:
        df, _ = load_and_align_sources(processed_dir, nairu_dir, k0_multiplier=m)
        df = compute_all_factors(df)
        df = compute_tfp(df)
        df = compute_pib_potencial(df)
        sub = df[["date", "K_pot", "PIB_pot", "Brecha_CD"]].copy()
        sub["k0_multiplier"] = m
        escenarios.append(sub)

    largo = pd.concat(escenarios, ignore_index=True)

    base = largo[largo["k0_multiplier"] == 1.0][["date", "K_pot", "PIB_pot", "Brecha_CD"]]
    base = base.rename(columns={
        "K_pot": "K_pot_base", "PIB_pot": "PIB_pot_base", "Brecha_CD": "Brecha_CD_base",
    })

    largo = largo.merge(base, on="date", how="left")
    largo["delta_K_pot_pct"] = (largo["K_pot"] / largo["K_pot_base"] - 1.0) * 100.0
    largo["delta_PIB_pot_pct"] = (largo["PIB_pot"] / largo["PIB_pot_base"] - 1.0) * 100.0
    largo["delta_Brecha_CD_pp"] = largo["Brecha_CD"] - largo["Brecha_CD_base"]
    largo = largo.drop(columns=["K_pot_base", "PIB_pot_base", "Brecha_CD_base"])

    largo = largo.sort_values(["date", "k0_multiplier"]).reset_index(drop=True)

    ultima_fecha = largo["date"].max()
    resumen_final = largo[largo["date"] == ultima_fecha]
    logger.info(
        "Sensibilidad K_0 en %s (más reciente): %s",
        str(ultima_fecha)[:10],
        ", ".join(
            f"x{row.k0_multiplier:.1f}->Δbrecha={row.delta_Brecha_CD_pp:+.3f}pp"
            for row in resumen_final.itertuples()
        ),
    )
    return largo


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

    # 5b. Sensibilidad de K_0 (Fase 1 del plan de limpieza, 2026-09-01):
    # no cambia la estimación central (df de arriba), solo la documenta.
    logger.info("Calculando sensibilidad de K_0 (±10%%/±20%%) …")
    sens_df = compute_k0_sensitivity(processed_dir, nairu_dir)
    sens_path = output_dir / "k0_sensitivity.csv"
    sens_df.to_csv(sens_path, index=False)
    logger.info("Sensibilidad de K_0: %s", sens_path)

    # 6. Ordenar columnas (solo las que existen en el df)
    cols_salida = [c for c in QUARTERLY_OUTPUT_COLS if c in df.columns]
    df_out = df[cols_salida].copy()

    # 7. Construir metadatos de descarga
    metadatos = _build_metadatos(processed_dir, nairu_dir)

    # 8. Escribir Excel + CSV (el CSV alimenta la página web)
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
