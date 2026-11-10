"""Insumos trimestrales del PIB que v7 lee y el repo no tenía: X_vapg y BR_tms.

- ``X_vapg`` = valor agregado de **extracción de petróleo crudo y gas
  natural** (y actividades de apoyo): anexo de producción a precios
  constantes (el mismo de ``--dane-gdp``), hoja "Cuadro 6" (series
  encadenadas de volumen, ajustadas por estacionalidad y calendario), primer
  bloque.  Miles de millones de pesos.
- ``BR_tms`` = **impuestos menos subvenciones** sobre la producción e
  importaciones: anexo del enfoque del ingreso a precios corrientes (el mismo
  de ``--income``), hoja "PIB_Ingreso".  Miles de millones de pesos.

v7 no los usa en el cálculo vigente (producto total, alpha CBO), pero los
exige para fechar T, el último trimestre con insumos completos (columnas de
``legacy/v7/datos/insumos_pib_trimestral.csv``).  Se reutilizan los parsers
de ``gdp.py`` y ``gdp_income.py`` (misma estructura de años × trimestres).

Salida: ``data/processed/dane_pib_insumos_v7_trimestral.csv``
(``date`` = inicio del trimestre, ``year``, ``quarter``, ``X_vapg``,
``BR_tms``, ``source``, ``download_date``); ``src/v7/insumos.py`` la usa en
vez de la referencia cuando existe.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import date
from pathlib import Path

import pandas as pd

from src.config import (
    DANE_GDP_CONFIG,
    DANE_GDP_INCOME_CONFIG,
    LABEL_IMPUESTOS_NETOS,
    LABEL_VA_PETROLEO_GAS,
    PIB_INSUMOS_V7_COLUMNS,
    PIB_INSUMOS_V7_FILENAME,
    PROCESSED_DIR,
    RAW_DANE_DIR,
)
from src.io_utils import save_csv

logger = logging.getLogger("nairu_pipeline.dane.pib_insumos_v7")

CONFIG_CUADRO6 = replace(
    DANE_GDP_CONFIG, sheet_name="Cuadro 6", year_row=11, quarter_row=12,
    concept_col=3, data_start_col=4, concept_label=LABEL_VA_PETROLEO_GAS,
)
# parse_income_excel extrae tres filas; se le pasa la de impuestos en el lugar
# de la remuneración y se toma esa columna.
CONFIG_IMPUESTOS = replace(DANE_GDP_INCOME_CONFIG, label_compensation=LABEL_IMPUESTOS_NETOS)


def parse_va_petroleo_gas(xlsx_path: Path) -> pd.DataFrame:
    """``date, X_vapg`` del Cuadro 6 (primer bloque: volumen encadenado desestacionalizado)."""
    from src.sources.dane.gdp import parse_gdp_excel

    df = parse_gdp_excel(xlsx_path, CONFIG_CUADRO6)
    return df[["date", "gdp_observed"]].rename(columns={"gdp_observed": "X_vapg"})


def parse_impuestos_netos(xlsx_path: Path) -> pd.DataFrame:
    """``date, BR_tms`` de la hoja PIB_Ingreso (precios corrientes)."""
    from src.sources.dane.gdp_income import parse_income_excel

    df = parse_income_excel(xlsx_path, CONFIG_IMPUESTOS)
    return df[["date", "compensation_employees"]].rename(columns={"compensation_employees": "BR_tms"})


def combinar(va: pd.DataFrame, imp: pd.DataFrame, hoy: date | None = None) -> pd.DataFrame:
    out = va.merge(imp, on="date", how="outer").sort_values("date").reset_index(drop=True)
    out["date"] = pd.to_datetime(out["date"])
    out["year"] = out["date"].dt.year
    out["quarter"] = out["date"].dt.quarter
    out["source"] = "DANE CNT: producción Cuadro 6 (X_vapg) + enfoque del ingreso (BR_tms)"
    out["download_date"] = (hoy or date.today()).isoformat()
    return out[PIB_INSUMOS_V7_COLUMNS]


def construir_pib_insumos_v7(raw_dir: Path = RAW_DANE_DIR, hoy: date | None = None) -> pd.DataFrame:
    """Descarga los dos anexos vigentes del DANE y arma las dos series (sin escribir)."""
    from src.sources.dane import gdp, gdp_income

    url_p = gdp.extract_gdp_xlsx_link(gdp.fetch_gdp_page(DANE_GDP_CONFIG), DANE_GDP_CONFIG)
    xlsx_p = gdp.download_gdp_excel(url_p, raw_dir, DANE_GDP_CONFIG)
    url_i = gdp_income.extract_income_xlsx_link(gdp_income.fetch_income_page(DANE_GDP_INCOME_CONFIG),
                                                DANE_GDP_INCOME_CONFIG)
    xlsx_i = gdp_income.download_income_excel(url_i, raw_dir, DANE_GDP_INCOME_CONFIG)
    df = combinar(parse_va_petroleo_gas(xlsx_p), parse_impuestos_netos(xlsx_i), hoy)
    logger.info("Insumos PIB v7: X_vapg %s → %s, BR_tms %s → %s",
                *[df.dropna(subset=[c])["date"].agg(f).date() for c in ("X_vapg",) for f in ("min", "max")],
                *[df.dropna(subset=[c])["date"].agg(f).date() for c in ("BR_tms",) for f in ("min", "max")])
    return df


def guardar_pib_insumos_v7(df: pd.DataFrame, output_dir: Path = PROCESSED_DIR) -> Path:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    path = output_dir / PIB_INSUMOS_V7_FILENAME
    save_csv(out, path)
    logger.info("Insumos PIB v7 guardados: %s (%d filas)", path, len(out))
    return path
