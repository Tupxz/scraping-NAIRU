"""Tests de vacantes en prensa (Banrep / SUAMECA), offline.

Fixture real: respuestas de SUAMECA para las 7 series (ids 16570–16576),
primeros 36 y últimos 3 datos de cada una.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.quality_checks import QualityCheckError, run_vacantes_checks
from src.sources.banrep.vacantes import armar_vacantes, descargar_vacantes, guardar_vacantes

FIX = Path(__file__).parent / "fixtures" / "vacantes" / "suameca_vacantes_extracto.json"
REF = Path(__file__).parent.parent / "legacy" / "v7" / "datos" / "banrep_vacantes_prensa_mensual.csv"
SERIES = json.loads(FIX.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def df():
    return armar_vacantes(SERIES)


def test_solo_date_y_ciudades(df):
    """v7 suma todas las columnas que no son date: no puede haber metadatos."""
    ref_cols = pd.read_csv(REF, nrows=0).columns.tolist()
    assert df.columns.tolist() == ref_cols
    assert df["date"].dt.day.eq(1).all()


def test_igual_a_la_linea_base(df):
    ref = pd.read_csv(REF, parse_dates=["date"])
    ref["date"] = ref["date"].dt.to_period("M").dt.to_timestamp()
    ref = ref.set_index("date")
    d = df.set_index("date")
    comunes = d.index.intersection(ref.index)
    assert len(comunes) >= 60
    for c in ref.columns:
        a, b = d.loc[comunes, c], ref.loc[comunes, c]
        presentes = a.notna()                     # el extracto no trae todos los meses de cada ciudad
        assert presentes.sum() >= 30, c
        assert ((a[presentes] - b[presentes]).abs() < 1e-9).all(), c


def test_inicios_por_ciudad(df):
    d = df.set_index("date")
    assert d["Vacantes, Barranquilla, Diario El Heraldo"].first_valid_index() == pd.Timestamp("1978-01-01")
    assert d["Vacantes, Pasto, Diario Del Sur"].first_valid_index() == pd.Timestamp("2000-01-01")


def test_serie_ajena_es_error():
    otra = [dict(SERIES[0], nombre="Tasa de cambio")]
    with pytest.raises(ValueError, match="no es de vacantes"):
        armar_vacantes(otra)


def _sintetica(meses: int = 30) -> pd.DataFrame:
    fechas = pd.date_range("1976-01-01", periods=meses, freq="MS")
    out = pd.DataFrame({"date": fechas})
    for s in SERIES:
        out[s["nombre"]] = 5.0
    out.loc[:23, "Vacantes, Barranquilla, Diario El Heraldo"] = float("nan")   # empieza después
    return out


def test_calidad():
    assert run_vacantes_checks(_sintetica())


@pytest.mark.parametrize("caso", ["hueco_interno", "negativo", "columna_extra", "mes_faltante"])
def test_calidad_falla(caso):
    df = _sintetica()
    if caso == "hueco_interno":
        df.loc[10, "Vacantes, Cali, Diario El País"] = float("nan")
    elif caso == "negativo":
        df.loc[5, "Vacantes, Cali, Diario El País"] = -1.0
    elif caso == "columna_extra":
        df["source"] = "BANREP"
    else:
        df = df.drop(index=3)
    with pytest.raises(QualityCheckError):
        run_vacantes_checks(df)


class SesionFalsa:
    def __init__(self, vacia: int | None = None):
        self.vacia = vacia

    def get(self, url, params=None, headers=None, timeout=None):
        import requests

        sid = params["idSerie"]
        s = next(x for x in SERIES if x["id"] == sid)
        r = requests.Response()
        r.status_code = 200
        cuerpo = [] if sid == self.vacia else [{"nombre": s["nombre"], "data": s["data"]}]
        r._content = json.dumps(cuerpo).encode()
        return r


def test_descarga_las_siete_series():
    series = descargar_vacantes(session=SesionFalsa())
    assert [s["id"] for s in series] == list(range(16570, 16577))


def test_serie_vacia_es_error():
    with pytest.raises(ValueError, match="16575"):
        descargar_vacantes(session=SesionFalsa(vacia=16575))


def test_guardar(df, tmp_path):
    leido = pd.read_csv(guardar_vacantes(df, tmp_path))
    assert leido.columns[0] == "date" and leido["date"].iloc[0] == "1976-01-01"


def test_run_no_escribe_si_falla(monkeypatch):
    from src.pipelines import run_vacantes

    escritos = []
    malo = _sintetica().drop(index=4)
    monkeypatch.setattr(run_vacantes, "construir_vacantes", lambda: malo)
    monkeypatch.setattr(run_vacantes, "guardar_vacantes", lambda d: escritos.append(d))
    with pytest.raises(QualityCheckError):
        run_vacantes.run()
    assert escritos == []
