"""Tests de src/pipelines/build_nairu_dataset.py (offline, CSV sintéticos).

Verifica que Data_NAIRU.xlsx:
- empiece en 2001-01 (como el de a30fb4b que usó la línea base v7);
- tome ``Unemp_Desest`` de la TD PUBLICADA (``unemployment_rate``), no de
  la calculada (``unemployment_rate_calc``);
- conserve el ICU anterior a 2004;
- descarte las filas sin desempleo, inflación o ICU.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.pipelines import build_nairu_dataset as mod


def _mensual(inicio: str, fin: str, **cols) -> pd.DataFrame:
    fechas = pd.date_range(inicio, fin, freq="MS")
    df = pd.DataFrame({"date": fechas})
    for nombre, valor in cols.items():
        df[nombre] = valor(fechas) if callable(valor) else valor
    return df


@pytest.fixture
def processed(tmp_path, monkeypatch):
    d = tmp_path / "processed"
    d.mkdir()
    _mensual(
        "2001-01-01", "2026-01-01",
        unemployment_rate=lambda f: [10.0 + (i % 10) / 10 for i in range(len(f))],
        unemployment_rate_calc=lambda f: [10.0 + (i % 10) / 10 + 0.0123 for i in range(len(f))],
    ).to_csv(d / "dane_labor_colombia.csv", index=False)
    _mensual("1999-12-01", "2026-02-01", Inf_Goal=3.0, Inf_Rate=5.0, Core_Inf=4.5).to_csv(
        d / "inflation_banrep_colombia.csv", index=False)
    _mensual("2001-01-01", "2026-02-01", brent_usd_per_barrel=60.0).to_csv(
        d / "brent_colombia.csv", index=False)
    _mensual("2002-12-01", "2026-02-01", TES_PESOS_1Y=9.0, TES_UVR_1Y=3.0).to_csv(
        d / "tes_banrep_colombia.csv", index=False)
    _mensual("2001-01-01", "2025-12-01", capacity_utilization=75.0).to_csv(
        d / "andi_capacidad_instalada.csv", index=False)
    monkeypatch.setattr(mod, "PROCESSED_DIR", d)
    return d


def test_empieza_en_2001_y_termina_con_el_icu(processed, tmp_path):
    df = mod.build_nairu_dataset(tmp_path / "Data_NAIRU.xlsx")
    assert df["Date"].iloc[0] == pd.Timestamp("2001-01-01")
    assert df["Date"].iloc[-1] == pd.Timestamp("2025-12-01")   # el ICU llega a 2025-12
    assert len(df) == 300
    assert df["Date"].diff().dropna().dt.days.between(28, 31).all()


def test_usa_td_publicada(processed, tmp_path):
    df = mod.build_nairu_dataset(tmp_path / "Data_NAIRU.xlsx")
    labor = pd.read_csv(processed / "dane_labor_colombia.csv", parse_dates=["date"]).set_index("date")
    u = df.set_index("Date")["Unemp_Desest"]
    assert (u - labor.loc[u.index, "unemployment_rate"]).abs().max() == 0
    assert ((u * 10) - (u * 10).round()).abs().max() < 1e-9      # 1 decimal


def test_icu_2001_2003_se_conserva_y_tes_vacio_al_inicio(processed, tmp_path):
    df = mod.build_nairu_dataset(tmp_path / "Data_NAIRU.xlsx").set_index("Date")
    assert df.loc["2001-01-01", "ICU"] == 75.0
    assert pd.isna(df.loc["2001-01-01", "TES_Rate_1yr_COP"])
    assert df.loc["2003-01-01", "TES_Rate_1yr_COP"] == 9.0


def test_excel_legible_como_v7(processed, tmp_path):
    out = tmp_path / "Data_NAIRU.xlsx"
    mod.build_nairu_dataset(out)
    leido = pd.read_excel(out)
    assert list(leido.columns[:3]) == ["Year", "Month", "Date"]
    for c in ("Unemp_Desest", "Core_Inf", "Inf_Goal", "Inf_Rate", "Brent_Oil_Price",
              "TES_Rate_1yr_COP", "TES_Rate_1yr_UVR", "ICU"):
        assert c in leido.columns
