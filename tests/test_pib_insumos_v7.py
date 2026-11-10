"""Tests de los insumos trimestrales del PIB para v7 (X_vapg, BR_tms), offline.

Fixtures reales (``tests/fixtures/pib_v7/``): primer bloque del Cuadro 6 del
anexo de producción a precios constantes y del enfoque del ingreso a precios
corrientes (II trimestre de 2026).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.config import PIB_INSUMOS_V7_COLUMNS
from src.quality_checks import QualityCheckError, run_pib_insumos_v7_checks
from src.sources.dane.gdp_insumos_v7 import (
    combinar,
    guardar_pib_insumos_v7,
    parse_impuestos_netos,
    parse_va_petroleo_gas,
)

FIX = Path(__file__).parent / "fixtures" / "pib_v7"
CUADRO6 = FIX / "anex-ProduccionConstantes-IItrim2026_cuadro6_extracto.xlsx"
INGRESO = FIX / "anex-PIB-EnfoqueCorriente-IItrim2026_extracto.xlsx"
REF = Path(__file__).parent.parent / "legacy" / "v7" / "datos" / "insumos_pib_trimestral.csv"


@pytest.fixture(scope="module")
def df():
    return combinar(parse_va_petroleo_gas(CUADRO6), parse_impuestos_netos(INGRESO), hoy=date(2026, 11, 9))


def test_columnas_y_cobertura(df):
    assert list(df.columns) == PIB_INSUMOS_V7_COLUMNS
    x = df.dropna(subset=["X_vapg"])
    assert x["date"].min() == pd.Timestamp("2005-01-01") and x["date"].max() == pd.Timestamp("2026-04-01")
    assert df.dropna(subset=["BR_tms"])["date"].min() == pd.Timestamp("2014-01-01")


def test_valores(df):
    d = df.set_index("date")
    assert d.loc["2005-01-01", "X_vapg"] == pytest.approx(4089.6764, abs=1e-4)
    assert d.loc["2025-01-01", "BR_tms"] == 76254.0
    assert d.loc["2026-04-01", "BR_tms"] == 52653.0


def test_cerca_de_la_linea_base(df):
    """X_vapg: ≤ 0,25 % (revisiones estacionales); BR_tms: ±1 (redondeo entre
    versiones del anexo) hasta 2025T4; 2026T1 lo revisó el DANE (+2,2 %)."""
    ref = pd.read_csv(REF, parse_dates=["date"])
    ref.index = ref["date"].dt.to_period("Q")
    d = df.set_index(pd.to_datetime(df["date"]).dt.to_period("Q"))
    com = d.index.intersection(ref.dropna(subset=["X_vapg"]).index)
    assert (100 * (d.loc[com, "X_vapg"] / ref.loc[com, "X_vapg"] - 1)).abs().max() < 0.25
    com_b = d.index.intersection(ref.dropna(subset=["BR_tms"]).index)
    dif = (d.loc[com_b, "BR_tms"] - ref.loc[com_b, "BR_tms"]).abs()
    assert (dif.loc[:"2025Q4"] <= 1.0).all()
    assert (dif.loc[:"2025Q4"] == 0).sum() >= 35


def test_calidad(df):
    assert run_pib_insumos_v7_checks(df)


@pytest.mark.parametrize("caso", ["x_tarde", "hueco_br", "rango", "trimestre_faltante"])
def test_calidad_falla(df, caso):
    malo = df.copy()
    if caso == "x_tarde":
        malo.loc[0, "X_vapg"] = float("nan")
    elif caso == "hueco_br":
        malo.loc[malo["date"] == "2020-01-01", "BR_tms"] = float("nan")
    elif caso == "rango":
        malo.loc[10, "X_vapg"] = 50.0
    else:
        malo = malo.drop(index=20)
    with pytest.raises(QualityCheckError):
        run_pib_insumos_v7_checks(malo)


def test_guardar(df, tmp_path):
    leido = pd.read_csv(guardar_pib_insumos_v7(df, tmp_path))
    assert leido["date"].iloc[0] == "2005-01-01" and list(leido.columns) == PIB_INSUMOS_V7_COLUMNS


def test_v7_insumos_usa_el_archivo_del_repo(tmp_path):
    from src.config import PROCESSED_DIR
    from src.v7 import insumos

    if not (PROCESSED_DIR / "dane_pib_insumos_v7_trimestral.csv").exists():
        pytest.skip("sin dane_pib_insumos_v7_trimestral.csv en data/processed")
    f = insumos.preparar_raiz(tmp_path / "raiz", "repo")
    assert f["insumos_pib_trimestral.X_vapg"] == "data/processed/dane_pib_insumos_v7_trimestral.csv"
    assert f["insumos_pib_trimestral.BR_tms"] == "data/processed/dane_pib_insumos_v7_trimestral.csv"
    trim = pd.read_csv(tmp_path / "raiz" / "datos" / "insumos_pib_trimestral.csv", parse_dates=["date"])
    assert trim.loc[trim["date"] == "2005-03-01", "X_vapg"].notna().all()     # fechas de cierre
