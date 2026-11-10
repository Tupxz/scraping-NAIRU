"""Tests de subocupación y tasas brutas GEIH (anexo principal), offline.

Fixture real: bloque Total Nacional de la hoja "Total nacional" del anexo
principal de jul-2026 (``anex-GEIH-jul2026.xlsx``, el que usó el compañero),
con sus notas (incluida la de mar–jul 2020).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.config import GEIH_BRUTA_CONFIG, SUBEMPLEO_COLUMNS
from src.quality_checks import QualityCheckError, run_subempleo_checks
from src.sources.dane.geih_subempleo import guardar_subempleo, parsear_subempleo
from src.sources.dane.unemployment import extract_geih_xlsx_links, select_geih_link

FIX = Path(__file__).parent / "fixtures" / "geih" / "anex-GEIH-jul2026_total_nacional_bloque_tn.xlsx"
REF = Path(__file__).parent.parent / "legacy" / "v7" / "datos" / "geih_subempleo_bruto_mensual.csv"


@pytest.fixture(scope="module")
def df():
    return parsear_subempleo(FIX, hoy=date(2026, 11, 9))


def test_columnas_y_cobertura(df):
    assert list(df.columns) == SUBEMPLEO_COLUMNS
    assert df["date"].min() == pd.Timestamp("2001-01-01")
    assert df["date"].max() == pd.Timestamp("2026-07-01")
    assert len(df) == 307


def test_igual_a_la_linea_base(df):
    ref = pd.read_csv(REF, parse_dates=["date"]).set_index("date")
    d = df.set_index("date").loc[ref.index]
    for c in SUBEMPLEO_COLUMNS[1:7]:
        assert (d[c] - ref[c]).abs().max() < 1e-9, c


def test_pandemia_sin_subocupacion(df):
    """Mar–jul 2020: el DANE no midió subocupación (publica 0) → NaN; TD/TGP/TO se conservan."""
    d = df.set_index("date")
    meses = pd.date_range("2020-03-01", "2020-07-01", freq="MS")
    assert d.loc[meses, ["tasa_subocupacion", "tcsd_subempleo_subjetivo",
                         "tcdftp_subempleo_objetivo"]].isna().all().all()
    assert d.loc[meses, ["td_bruta", "tgp_bruta", "to_bruta"]].notna().all().all()
    assert d.loc["2020-04-01", "td_bruta"] == pytest.approx(20.4853, abs=1e-4)
    assert d["tasa_subocupacion"].isna().sum() == 5


def test_calidad(df):
    assert run_subempleo_checks(df)


@pytest.mark.parametrize("caso", ["hueco", "td_nula", "muchos_nan", "fuera_rango"])
def test_calidad_falla(df, caso):
    malo = df.copy()
    if caso == "hueco":
        malo = malo.drop(index=100)
    elif caso == "td_nula":
        malo.loc[50, "td_bruta"] = float("nan")
    elif caso == "muchos_nan":
        malo.loc[10:20, "tasa_subocupacion"] = float("nan")
    else:
        malo.loc[30, "tasa_subocupacion"] = 55.0
    with pytest.raises(QualityCheckError):
        run_subempleo_checks(malo)


def test_selecciona_el_anexo_principal_no_el_desestacionalizado():
    html = """
    <a href="/files/operaciones/GEIH/anex-GEIH-Desestacionalizado-sep2026.xlsx">d</a>
    <a href="/files/operaciones/GEIH/anex-GEIH-ago2026.xlsx">a</a>
    <a href="/files/operaciones/GEIH/anex-GEIH-sep2026.xlsx">b</a>
    <a href="/files/operaciones/GEIH/anex-GEIHEISS-sep2026.xlsx">e</a>
    """
    sel = select_geih_link(extract_geih_xlsx_links(html, GEIH_BRUTA_CONFIG))
    assert sel["url"].endswith("/anex-GEIH-sep2026.xlsx")


def test_guardar(df, tmp_path):
    leido = pd.read_csv(guardar_subempleo(df, tmp_path))
    assert leido["date"].iloc[0] == "2001-01-01" and len(leido) == 307


def test_run_no_escribe_si_falla(df, monkeypatch):
    from src.pipelines import run_subempleo

    escritos = []
    monkeypatch.setattr(run_subempleo, "construir_subempleo", lambda: df.drop(index=7))
    monkeypatch.setattr(run_subempleo, "guardar_subempleo", lambda d: escritos.append(d))
    with pytest.raises(QualityCheckError):
        run_subempleo.run()
    assert escritos == []
