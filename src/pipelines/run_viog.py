"""Pipeline runner para el VIOG (output gap ponderado por filtros).

Soporta dos países:
  - **USA** (default histórico): VIOG_CONFIG, lee PIB_USA.xlsx.
  - **Colombia**: VIOG_CO_CONFIG, construye PIB_CO.xlsx automáticamente
    desde dane_gdp_colombia.csv (scraper DANE) empalmado con la serie
    histórica Base 2005 del DANE (2000Q1–2011Q2) para extender hacia atrás.
    Si existe outputs/pib_potencial/pib_potencial_colombia.csv, agrega el
    PIB potencial Cobb-Douglas como columna de referencia → el compuesto
    VIOG pondera 6 variables (5 filtros + referencia), igual que el
    cuaderno notebooks/VIOG.ipynb. Sin ese archivo, degrada a 5 filtros.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.config import (
    INPUTS_DIR,
    OUTPUTS_DIR,
    PROCESSED_DIR,
    RAW_DANE_DIR,
    VIOG_CO_CONFIG,
    VIOG_CONFIG,
    VIOGConfig,
)
from src.sources.dane.gdp_historical import (
    download_gdp_base1994,
    download_gdp_historical,
    parse_gdp_base1994,
    parse_gdp_historical,
    splice_series,
)
from src.sources.viog.viog import run_viog_pipeline

logger = logging.getLogger("nairu_pipeline.pipelines.viog")


def _build_pib_co_xlsx(output_path: Path) -> None:
    """Construye PIB_CO.xlsx con doble empalme DANE: Base 2015 <- Base 2005 <- Base 1994.

    Pasos:
        1. Lee dane_gdp_colombia.csv (Base 2015, 2005Q1-presente).
        2. Base 2005 (2000Q1-2011Q2): empalme -> extiende a 2000Q1.
        3. Base 1994 (1994Q1-2007Q4): empalme -> extiende a 1994Q1.
        Resultado: ~120 obs (1994Q1-presente).

    Columnas de salida: Year, Quarter, Value(Billions), Variation
    """
    csv_path = PROCESSED_DIR / "dane_gdp_colombia.csv"
    if not csv_path.exists():
        raise FileNotFoundError(
            f"dane_gdp_colombia.csv no encontrado en {PROCESSED_DIR}. "
            "Ejecuta primero: python -m src.main --dane-gdp"
        )

    # ── 1. Serie actual (Base 2015) ───────────────────────────────────
    df_new = pd.read_csv(csv_path).sort_values(["year", "quarter"]).reset_index(drop=True)
    spliced = pd.Series(
        df_new["gdp_observed"].values,
        index=pd.PeriodIndex.from_fields(
            year=df_new["year"].values,
            quarter=df_new["quarter"].values,
            freq="Q",
        ),
        name="gdp_base2015",
    )

    # ── 2. Empalme Base 2005 (2000Q1-2011Q2) ─────────────────────────
    xls_b2005 = RAW_DANE_DIR / "dane_gdp_base2005.xls"
    try:
        download_gdp_historical(xls_b2005)
        s2005  = parse_gdp_historical(xls_b2005)
        spliced = splice_series(new=spliced, old=s2005)
    except Exception as exc:
        logger.warning("[VIOG-CO] Empalme Base 2005 fallido (%s); omitiendo.", exc)

    # ── 3. Empalme Base 1994 (1994Q1-2007Q4) ─────────────────────────
    xls_b1994 = RAW_DANE_DIR / "dane_gdp_base1994.xls"
    try:
        download_gdp_base1994(xls_b1994)
        s1994   = parse_gdp_base1994(xls_b1994)
        spliced = splice_series(new=spliced, old=s1994)
    except Exception as exc:
        logger.warning("[VIOG-CO] Empalme Base 1994 fallido (%s); omitiendo.", exc)

    # ── 4. Construir DataFrame de salida ──────────────────────────────
    values    = spliced.values
    variation = pd.Series(values) / pd.Series(values).shift(1) - 1

    out = pd.DataFrame({
        "Year":             spliced.index.year,
        "Quarter":          spliced.index.quarter,
        "Value(Billions)":  values,
        "Variation":        variation.values,
    })

    # ── 5. Referencia: PIB potencial Cobb-Douglas del propio pipeline ─
    # Igual que el cuaderno (gap_vars incluye "potential"): si existe la
    # estimación C-D, se agrega como columna de referencia. Cobertura
    # 2005Q1→presente; antes queda NaN y los ponderadores renormalizan
    # (mismo mecanismo que los extremos NaN del Baxter-King).
    pot_csv = OUTPUTS_DIR / "pib_potencial" / "pib_potencial_colombia.csv"
    ref_col = "Potential Value(Billions)"
    try:
        if pot_csv.exists():
            pot = pd.read_csv(pot_csv)[["year", "quarter", "PIB_pot"]].rename(
                columns={"year": "Year", "quarter": "Quarter", "PIB_pot": ref_col}
            )
            out = out.merge(pot, on=["Year", "Quarter"], how="left")
            n_ref = int(out[ref_col].notna().sum())
            logger.info(
                "[VIOG-CO] Referencia C-D agregada: %d/%d trimestres con potencial.",
                n_ref, len(out),
            )
        else:
            logger.warning(
                "[VIOG-CO] %s no existe — VIOG-CO sin referencia (solo 5 filtros). "
                "Ejecuta --pib-potencial primero para el compuesto de 6 variables.",
                pot_csv,
            )
    except Exception as exc:  # nunca romper la construcción del insumo
        logger.warning("[VIOG-CO] No se pudo agregar la referencia C-D (%s).", exc)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_excel(output_path, index=False)
    logger.info(
        "[VIOG-CO] PIB_CO.xlsx construido: %d trimestres (%dQ%d - %dQ%d)",
        len(out),
        int(out["Year"].iloc[0]), int(out["Quarter"].iloc[0]),
        int(out["Year"].iloc[-1]), int(out["Quarter"].iloc[-1]),
    )


def _run_for_config(
    config: VIOGConfig,
    *,
    plot_subdir: str,
    skip_if_missing: bool = False,
) -> None:
    """Corre el pipeline VIOG con una configuración específica."""
    input_path = INPUTS_DIR / config.input_filename
    output_path = PROCESSED_DIR / config.processed_filename
    plot_dir = OUTPUTS_DIR / plot_subdir

    if not input_path.exists():
        msg = f"[VIOG] Input no encontrado: {input_path}"
        if skip_if_missing:
            logger.warning(msg + " — pipeline omitido.")
            print(msg + " — pipeline omitido.")
            return
        raise FileNotFoundError(msg)

    df = run_viog_pipeline(
        input_path, output_path,
        series_col=config.series_col,
        ref_col=config.ref_col,
        source_label=config.source_label,
        plot=True, plot_dir=plot_dir,
    )
    print(f"[VIOG] {len(df)} observaciones guardadas en {output_path}")
    print(f"[VIOG] Gráficas guardadas en {plot_dir}")


def run() -> None:
    """Ejecuta VIOG para USA (comportamiento histórico/default)."""
    _run_for_config(VIOG_CONFIG, plot_subdir="viog")


def run_colombia() -> None:
    """Ejecuta VIOG para Colombia.

    Construye PIB_CO.xlsx automáticamente desde dane_gdp_colombia.csv,
    luego aplica los 5 filtros estadísticos y genera las gráficas.
    Si dane_gdp_colombia.csv no existe, omite el pipeline con warning.
    """
    input_path = INPUTS_DIR / VIOG_CO_CONFIG.input_filename
    try:
        _build_pib_co_xlsx(input_path)
    except FileNotFoundError as e:
        logger.warning("[VIOG-CO] %s — pipeline omitido.", e)
        return

    _run_for_config(VIOG_CO_CONFIG, plot_subdir="viog_colombia")
