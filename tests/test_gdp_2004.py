"""Tests del empalme del PIB 2004 (tasas de la base 2005 encadenadas), offline.

Usa la base 2005 versionada en ``data/raw/dane/dane_gdp_base2005.xls``
(serie histórica que el DANE ya no revisa).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.sources.dane.gdp import FUENTE_EMPALME, empalmar_2004
from src.sources.dane.gdp_historical import encadenar_hacia_atras, parse_gdp_historical

RAIZ = Path(__file__).parent.parent
BASE2005 = RAIZ / "data" / "raw" / "dane" / "dane_gdp_base2005.xls"
REF = RAIZ / "legacy" / "v7" / "datos" / "insumos_pib_trimestral.csv"
Q = lambda s: pd.Period(s, freq="Q")  # noqa: E731


def test_encadenar_hacia_atras_sintetico():
    base = pd.Series([100.0, 110.0, 121.0], index=pd.period_range("2004Q3", "2005Q1", freq="Q"))
    nuevo = pd.Series([242.0, 250.0], index=pd.period_range("2005Q1", "2005Q2", freq="Q"))
    out = encadenar_hacia_atras(nuevo, base, Q("2004Q3"))
    assert out.loc[Q("2004Q4")] == pytest.approx(220.0)      # 242 · 110/121
    assert out.loc[Q("2004Q3")] == pytest.approx(200.0)
    assert out.loc[Q("2005Q2")] == 250.0


def test_base_incompleta_es_error():
    base = pd.Series([110.0, 121.0], index=pd.period_range("2004Q4", "2005Q1", freq="Q"))
    nuevo = pd.Series([242.0], index=[Q("2005Q1")])
    with pytest.raises(ValueError, match="2004Q3"):
        encadenar_hacia_atras(nuevo, base, Q("2004Q3"))


def _df_base2015(v_2005q1: float) -> pd.DataFrame:
    fechas = pd.date_range("2005-01-01", periods=4, freq="QS")
    return pd.DataFrame({
        "date": fechas, "year": fechas.year, "quarter": fechas.quarter,
        "gdp_observed": [v_2005q1, 129000.0, 130000.0, 131000.0],
        "source": "DANE - Cuentas Nacionales Trimestrales", "download_date": "2026-11-09",
    })


@pytest.mark.skipif(not BASE2005.exists(), reason="falta la base 2005 versionada")
def test_reproduce_el_2004_de_la_linea_base():
    ref = pd.read_csv(REF, parse_dates=["date"])
    ref.index = ref["date"].dt.to_period("Q")
    df = empalmar_2004(_df_base2015(ref.loc[Q("2005Q1"), "V_pib"]),
                       base2005=parse_gdp_historical(BASE2005))
    out = df.set_index(pd.to_datetime(df["date"]).dt.to_period("Q"))
    for q in pd.period_range("2004Q1", "2004Q4", freq="Q"):
        assert out.loc[q, "gdp_observed"] == pytest.approx(ref.loc[q, "V_pib"], abs=1e-3)
        assert out.loc[q, "source"] == FUENTE_EMPALME
    assert out.index.min() == Q("2004Q1") and len(out) == 8


@pytest.mark.skipif(not BASE2005.exists(), reason="falta la base 2005 versionada")
def test_idempotente_y_columnas():
    base = parse_gdp_historical(BASE2005)
    una = empalmar_2004(_df_base2015(126000.0), base2005=base)
    dos = empalmar_2004(una, base2005=base)
    assert len(dos) == len(una) == 8
    assert list(una.columns) == ["date", "year", "quarter", "gdp_observed", "source", "download_date"]
    assert una["date"].dt.month.isin([1, 4, 7, 10]).all()
