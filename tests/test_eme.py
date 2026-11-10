"""Tests de la EME (Banrep, expectativas de inflación), offline.

Fixtures reales (``tests/fixtures/eme/``): extracto del Excel de series
históricas de SUAMECA (hoja ``INFLACION_TOTAL`` con sus encabezados,
2003-09 → 2004-02 y 2026-07 → 2026-10; más la hoja ``TRM``) y el nodo de la
EME del menú ``ENCUESTAS`` de SUAMECA.
"""

from __future__ import annotations

import io
from datetime import date
from pathlib import Path

import openpyxl
import pandas as pd
import pytest

from src.config import EME_HORIZONTES, EME_PROCESSED_COLUMNS, BanrepEMEConfig
from src.quality_checks import QualityCheckError, run_eme_checks
from src.sources.banrep.eme import (
    construir_eme,
    descargar_eme,
    guardar_eme,
    limpiar_eme,
    parsear_eme,
    url_series_historicas,
)

FIX = Path(__file__).parent / "fixtures" / "eme"
XLSX = FIX / "eme_series_historicas_extracto.xlsx"
MENU = (FIX / "suameca_menu_encuestas_eme.json").read_text(encoding="utf-8")
URL_SH = ("https://suameca.banrep.gov.co/archivos/encuestas/"
          "encuesta_mensual_analistas_economicos/series_historicas.xlsx")


class TestURL:

    def test_desde_el_menu_real(self):
        assert url_series_historicas(MENU, "respaldo") == URL_SH

    @pytest.mark.parametrize("texto", ["no es json", "[]", '[{"nombreAdjunto": "x.pdf"}]'])
    def test_respaldo(self, texto):
        assert url_series_historicas(texto, "respaldo") == "respaldo"


class TestParseo:

    @pytest.fixture(scope="class")
    def df(self):
        return parsear_eme(XLSX)

    def test_columnas_y_fechas(self, df):
        assert list(df.columns) == ["date"] + EME_HORIZONTES
        assert df["date"].dt.day.eq(1).all()
        assert df["date"].min() == pd.Timestamp("2003-09-01")
        assert df["date"].max() == pd.Timestamp("2026-10-01")    # encuesta del 2026-10-15

    @pytest.mark.parametrize("mes, col, valor", [
        ("2003-09-01", "mes_actual", 0.3001),
        ("2003-09-01", "doce_meses", 6.34),
        ("2003-10-01", "fin_ano_presente", 6.575),
        ("2026-09-01", "doce_meses", 5.4293),
        ("2026-09-01", "sesenta_meses", 3.439),
        ("2026-10-01", "doce_meses", 5.3982),
    ])
    def test_valores_en_porcentaje(self, df, mes, col, valor):
        assert df.set_index("date").loc[mes, col] == pytest.approx(valor, abs=1e-9)

    def test_nulos_donde_la_encuesta_no_preguntaba(self, df):
        assert pd.isna(df.set_index("date").loc["2003-09-01", "fin_ano_presente"])

    def test_bytes_igual_que_ruta(self, df):
        assert parsear_eme(XLSX.read_bytes()).equals(df)

    def _con_cambio(self, tmp_path, fila, col, valor, hoja="INFLACION_TOTAL"):
        wb = openpyxl.load_workbook(XLSX)
        wb[hoja].cell(row=fila, column=col, value=valor)
        p = tmp_path / "x.xlsx"
        wb.save(p)
        return p

    def test_falta_horizonte(self, tmp_path):
        p = self._con_cambio(tmp_path, 3, 8, "OTRA COSA")    # encabezado "A DOCE MESES"
        with pytest.raises(ValueError, match="doce_meses"):
            parsear_eme(p)

    def test_sin_fila_de_encabezados(self, tmp_path):
        p = self._con_cambio(tmp_path, 4, 1, "Fecha")
        with pytest.raises(ValueError, match="FECHA ENCUESTA"):
            parsear_eme(p)

    def test_mes_duplicado(self, tmp_path):
        p = self._con_cambio(tmp_path, 6, 1, pd.Timestamp("2003-09-20").to_pydatetime())
        with pytest.raises(ValueError, match="duplicados"):
            parsear_eme(p)

    def test_sin_hoja(self, tmp_path):
        wb = openpyxl.load_workbook(XLSX)
        del wb["INFLACION_TOTAL"]
        p = tmp_path / "y.xlsx"
        wb.save(p)
        with pytest.raises(ValueError, match="INFLACION_TOTAL"):
            parsear_eme(p)


def _df_eme(meses: int = 30, inicio: str = "2003-09-01") -> pd.DataFrame:
    fechas = pd.date_range(inicio, periods=meses, freq="MS")
    base = pd.DataFrame({"date": fechas, **{h: 5.0 for h in EME_HORIZONTES}})
    return limpiar_eme(base, hoy=date(2026, 11, 9))


class TestCalidad:

    def test_pasa(self):
        df = _df_eme()
        assert list(df.columns) == EME_PROCESSED_COLUMNS
        assert run_eme_checks(df)

    def test_hueco(self):
        with pytest.raises(QualityCheckError, match="Faltan"):
            run_eme_checks(_df_eme().drop(index=5))

    def test_inicio_tardio(self):
        with pytest.raises(QualityCheckError, match="empieza"):
            run_eme_checks(_df_eme(inicio="2004-01-01"))

    def test_fuera_de_rango(self):
        df = _df_eme()
        df.loc[3, "doce_meses"] = 45.0
        with pytest.raises(QualityCheckError, match="doce_meses"):
            run_eme_checks(df)


class SesionFalsa:
    def __init__(self, menu=MENU, archivo=None, menu_falla=False):
        self.menu, self.archivo, self.menu_falla = menu, archivo or XLSX.read_bytes(), menu_falla
        self.urls: list[str] = []

    def get(self, url, timeout=None):
        import requests

        self.urls.append(url)
        r = requests.Response()
        r.status_code = 200
        if "consultaMenuXopcion" in url:
            if self.menu_falla:
                raise requests.ConnectionError("caído")
            r._content = self.menu.encode()
        else:
            r._content = self.archivo
        return r


class TestDescargaYPipeline:

    def test_usa_la_url_del_menu(self):
        s = SesionFalsa(menu=MENU.replace("series_historicas.xlsx", "series_historicas_v2.xlsx"))
        descargar_eme(session=s)
        assert s.urls[-1].endswith("series_historicas_v2.xlsx")

    def test_respaldo_si_el_menu_falla(self):
        s = SesionFalsa(menu_falla=True)
        descargar_eme(session=s)
        assert s.urls[-1] == BanrepEMEConfig().archivo_url

    def test_no_xlsx_es_error(self):
        with pytest.raises(RuntimeError, match="no es un .xlsx"):
            descargar_eme(session=SesionFalsa(archivo=b"<html>captcha</html>"))

    def test_construir_guardar(self, tmp_path):
        df = construir_eme(session=SesionFalsa(), raw_dir=tmp_path / "raw", hoy=date(2026, 11, 9))
        assert (tmp_path / "raw" / "eme_series_historicas.xlsx").exists()
        p = guardar_eme(df, tmp_path)
        leido = pd.read_csv(p)
        assert leido["date"].iloc[0] == "2003-09-01"
        assert list(leido.columns) == EME_PROCESSED_COLUMNS
        # el extracto tiene un hueco 2004-03 → 2026-06: el control lo detecta
        with pytest.raises(QualityCheckError, match="Faltan"):
            run_eme_checks(df)

    def test_run_no_escribe_si_falla(self, monkeypatch):
        from src.pipelines import run_eme

        escritos = []
        monkeypatch.setattr(run_eme, "construir_eme", lambda: _df_eme().drop(index=4))
        monkeypatch.setattr(run_eme, "guardar_eme", lambda df: escritos.append(df))
        with pytest.raises(QualityCheckError):
            run_eme.run()
        assert escritos == []

    def test_bytes_io(self):
        assert parsear_eme(io.BytesIO(XLSX.read_bytes()).getvalue())["doce_meses"].notna().all()
