"""Tests de la EMMET (salarios de la industria, DANE + IPC), offline.

Fixtures reales (``tests/fixtures/emmet/``): extracto del anexo Total
Nacional de ago-2026 (hoja "9. Enlace legal hasta 2014": encabezados, 26
meses de ``T_IND`` y 3 filas de otro dominio), las filas de la página del
DANE con los enlaces, y el IPC 2014-01 → 2016-02 del repo.  Los valores
esperados son los del CSV de la línea base (``legacy/v7/datos/emmet_salarios_v7.csv``).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import openpyxl
import pandas as pd
import pytest

from src.config import EMMET_PROCESSED_COLUMNS
from src.quality_checks import QualityCheckError, run_emmet_checks
from src.sources.dane.emmet import (
    construir_emmet,
    construir_salarios,
    descargar_emmet,
    guardar_emmet,
    leer_anexo,
    seleccionar_anexo,
)

FIX = Path(__file__).parent / "fixtures" / "emmet"
ANEXO = FIX / "anex-EMMET-TotalNacional-extracto.xlsx"
PAGINA = (FIX / "pagina_emmet_extracto.html").read_text(encoding="utf-8")
IPC = FIX / "ipc_2014_2016.csv"
REF = Path(__file__).parent.parent / "legacy" / "v7" / "datos" / "emmet_salarios_v7.csv"


class TestSeleccion:

    def test_pagina_real(self):
        url = seleccionar_anexo(PAGINA)
        assert url == "https://www.dane.gov.co/files/operaciones/EMMET/anex-EMMET-TotalNacional-ago2026.xlsx"

    def test_elige_el_mas_reciente(self):
        html = PAGINA + ('<a href="/files/operaciones/EMMET/anex-EMMET-TotalNacional-dic2025.xlsx">x</a>'
                         '<a href="/files/operaciones/EMMET/anex-EMMET-TotalNacional-sep2026.xlsx">x</a>')
        assert seleccionar_anexo(html).endswith("TotalNacional-sep2026.xlsx")

    def test_sin_anexo(self):
        with pytest.raises(ValueError, match="No se encontró"):
            seleccionar_anexo('<a href="/files/operaciones/EMMET/bol-EMMET-ago2026.pdf">x</a>')


class TestAnexo:

    @pytest.fixture(scope="class")
    def indices(self):
        return leer_anexo(ANEXO)

    def test_solo_total_industria(self, indices):
        assert len(indices) == 26
        assert indices["date"].min() == pd.Timestamp("2014-01-01")
        assert indices["date"].max() == pd.Timestamp("2016-02-01")

    def test_igual_a_la_linea_base(self, indices):
        ref = pd.read_csv(REF, parse_dates=["date"]).set_index("date")
        for c in ("idx_sueldos_real", "idx_empleo", "idx_horas"):
            assert (indices.set_index("date")[c] - ref.loc[indices["date"], c]).abs().max() < 1e-12

    def _modificado(self, tmp_path, fila, col, valor):
        wb = openpyxl.load_workbook(ANEXO)
        wb["9. Enlace legal hasta 2014"].cell(row=fila, column=col, value=valor)
        p = tmp_path / "a.xlsx"
        wb.save(p)
        return p

    def test_falta_columna(self, tmp_path):
        wb = openpyxl.load_workbook(ANEXO)
        ws = wb["9. Enlace legal hasta 2014"]
        for c in range(1, ws.max_column + 1):
            if "Sueldos" in str(ws.cell(row=10, column=c).value):
                ws.cell(row=10, column=c, value="Otra")
        p = tmp_path / "b.xlsx"
        wb.save(p)
        with pytest.raises(ValueError, match="Total Sueldos"):
            leer_anexo(p)

    def test_sin_fila_dominios(self, tmp_path):
        with pytest.raises(ValueError, match="Dominios"):
            leer_anexo(self._modificado(tmp_path, 10, 1, "Otra"))

    def test_sin_hoja(self, tmp_path):
        wb = openpyxl.load_workbook(ANEXO)
        del wb["9. Enlace legal hasta 2014"]
        p = tmp_path / "c.xlsx"
        wb.save(p)
        with pytest.raises(ValueError, match="no tiene la hoja"):
            leer_anexo(p)


class TestSalarios:

    @pytest.fixture(scope="class")
    def df(self):
        return construir_salarios(leer_anexo(ANEXO), pd.read_csv(IPC), hoy=date(2026, 11, 9))

    def test_columnas(self, df):
        assert list(df.columns) == EMMET_PROCESSED_COLUMNS

    def test_igual_a_la_linea_base(self, df):
        ref = pd.read_csv(REF, parse_dates=["date"]).set_index("date").loc[df["date"]]
        d = df.set_index("date")
        for c in ("ipc", "salario_real_por_ocupado", "salario_nominal_por_ocupado",
                  "var_anual_salario_real", "var_anual_salario_nominal"):
            a, b = d[c], ref[c]
            assert (a.isna() == b.isna()).all(), c
            assert (a - b).abs().max() < 1e-9, c

    def test_identidad_nominal(self, df):
        """(1 + w) = (1 + ω)(1 + π): la variación nominal combina real e IPC."""
        d = df.set_index("date")
        pi = d["ipc"] / d["ipc"].shift(12) - 1
        w = (1 + d["var_anual_salario_real"] / 100) * (1 + pi) - 1
        assert (100 * w - d["var_anual_salario_nominal"]).dropna().abs().max() < 1e-9
        assert d["var_anual_salario_nominal"].iloc[:12].isna().all()

    def test_calidad(self, df):
        assert run_emmet_checks(df)

    def test_calidad_hueco(self, df):
        with pytest.raises(QualityCheckError, match="Faltan"):
            run_emmet_checks(df.drop(index=3))

    def test_calidad_nominal_nulo_con_ipc(self, df):
        malo = df.copy()
        malo.loc[15, "var_anual_salario_nominal"] = float("nan")
        with pytest.raises(QualityCheckError, match="nulos donde hay IPC"):
            run_emmet_checks(malo)


class SesionFalsa:
    def __init__(self, archivo: bytes | None = None):
        self.archivo = archivo if archivo is not None else ANEXO.read_bytes()
        self.urls: list[str] = []

    def get(self, url, timeout=None):
        import requests

        self.urls.append(url)
        r = requests.Response()
        r.status_code = 200
        r._content = self.archivo if url.endswith(".xlsx") else PAGINA.encode()
        return r


class TestPipeline:

    def test_descarga(self):
        s = SesionFalsa()
        contenido, url = descargar_emmet(session=s)
        assert url.endswith("TotalNacional-ago2026.xlsx") and contenido[:2] == b"PK"

    def test_no_xlsx(self):
        with pytest.raises(RuntimeError, match="no es un .xlsx"):
            descargar_emmet(session=SesionFalsa(archivo=b"<html>"))

    def test_construir_y_guardar(self, tmp_path):
        df = construir_emmet(session=SesionFalsa(), raw_dir=tmp_path / "raw", ipc_path=IPC,
                             hoy=date(2026, 11, 9))
        assert (tmp_path / "raw" / "emmet_total_nacional_raw.xlsx").exists()
        leido = pd.read_csv(guardar_emmet(df, tmp_path))
        assert leido["date"].iloc[0] == "2014-01-01" and len(leido) == 26

    def test_run_no_escribe_si_falla(self, monkeypatch):
        from src.pipelines import run_emmet

        df = construir_salarios(leer_anexo(ANEXO), pd.read_csv(IPC)).drop(index=5)
        escritos = []
        monkeypatch.setattr(run_emmet, "construir_emmet", lambda: df)
        monkeypatch.setattr(run_emmet, "guardar_emmet", lambda d: escritos.append(d))
        with pytest.raises(QualityCheckError):
            run_emmet.run()
        assert escritos == []
