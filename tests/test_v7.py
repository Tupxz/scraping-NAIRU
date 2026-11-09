"""Tests de la integración v7 (src/v7/ + src/pipelines/run_v7.py).

`TestReproduceLineaBaseV7` corre el motor completo (pasos 1 y 2, ~40 s) con los
insumos de referencia y lo compara con tests/fixtures/baseline_v7/, que son las
salidas del script del compañero. Es el criterio de aceptación de la integración.
"""
from __future__ import annotations

import contextlib
import io
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.config import PROJECT_ROOT
from src.v7 import insumos

BASE = PROJECT_ROOT / "tests" / "fixtures" / "baseline_v7"
MOTOR = PROJECT_ROOT / "src" / "v7" / "motor.py"
LEGACY = PROJECT_ROOT / "legacy" / "v7" / "pib_potencial_integrado_v7.py"


# --------------------------------------------------------------------------- #
class TestMotorEsCopiaLiteral:
    def test_unica_diferencia_es_la_raiz(self):
        motor = MOTOR.read_text(encoding="utf-8").splitlines()
        ref = LEGACY.read_text(encoding="utf-8").splitlines()
        # la cabecera agregada son solo comentarios; el resto debe ser idéntico salvo _HERE
        cabecera, cuerpo = motor[:len(motor) - len(ref)], motor[len(motor) - len(ref):]
        assert all(l.startswith("#") for l in cabecera)
        difs = [(a, b) for a, b in zip(cuerpo, ref) if a != b]
        assert len(cuerpo) == len(ref)
        assert len(difs) == 1 and difs[0][1].startswith("_HERE = ")
        assert "V7_ROOT" in difs[0][0]


class TestFechasTrimestrales:
    def test_inicio_a_cierre(self):
        s = pd.Series(pd.to_datetime(["2005-01-01", "2005-04-01", "2025-10-01"]))
        r = insumos._inicio_trimestre_a_cierre(s)
        assert list(r.dt.strftime("%Y-%m-%d")) == ["2005-03-01", "2005-06-01", "2025-12-01"]


class TestDataNairuCumpleV7:
    def _xlsx(self, tmp_path, desde, decimales):
        f = pd.date_range(desde, "2025-12-01", freq="MS")
        u = np.round(np.linspace(14, 8, len(f)), decimales)
        df = pd.DataFrame({"Year": f.year, "Month": f.month, "Unemp_Desest": u, "ICU": 75.0})
        p = tmp_path / "Data_NAIRU.xlsx"
        df.to_excel(p, index=False)
        return p

    def test_acepta_icu_2001_y_td_publicada(self, tmp_path):
        assert insumos.data_nairu_cumple_v7(self._xlsx(tmp_path, "2001-01-01", 1))[0]

    def test_rechaza_icu_desde_2004(self, tmp_path):
        ok, motivo = insumos.data_nairu_cumple_v7(self._xlsx(tmp_path, "2004-01-01", 1))
        assert not ok and "2001" in motivo

    def test_rechaza_td_calculada(self, tmp_path):
        ok, motivo = insumos.data_nairu_cumple_v7(self._xlsx(tmp_path, "2001-01-01", 6))
        assert not ok and "TD publicada" in motivo

    def test_insumo_de_referencia_cumple(self):
        assert insumos.data_nairu_cumple_v7(insumos.REF_DATA_NAIRU)[0]


class TestPreparaRaiz:
    @pytest.mark.parametrize("modo", ["referencia", "repo"])
    def test_estructura_completa(self, tmp_path, modo):
        raiz = tmp_path / "v7"
        fuentes = insumos.preparar_raiz(raiz, modo=modo)
        esperados = ["Data_NAIRU.xlsx", "trm_cop_usd_promedio_mensual.csv",
                     "festivos_efectivos_colombia_2001_2026.xlsx"] + [
            f"datos/{n}" for n in ("insumos_pib_trimestral.csv", "insumos_pib_mensual.csv",
                                   "insumos_pib_hc_anual.csv", "dane_stock_capital_productivo.csv",
                                   *insumos.MENSUALES_LAB)]
        for e in esperados:
            assert (raiz / e).exists(), e
        assert fuentes

    def test_repo_trimestral_tiene_columnas_y_2004(self, tmp_path):
        insumos.preparar_raiz(tmp_path / "v7", modo="repo")
        d = pd.read_csv(tmp_path / "v7" / "datos" / "insumos_pib_trimestral.csv", parse_dates=["date"])
        assert {"date", "V_pib", "X_vapg", "BQ_ra", "BR_tms", "BS_ebe", "BT_im"} <= set(d.columns)
        assert d["date"].min() == pd.Timestamp("2004-03-01")
        assert (d["date"].dt.month % 3 == 0).all()          # mes de cierre del trimestre
        assert d["V_pib"].notna().all()

    def test_modo_invalido(self, tmp_path):
        with pytest.raises(ValueError):
            insumos.preparar_raiz(tmp_path / "x", modo="otro")


# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def corrida_referencia(tmp_path_factory):
    from src.pipelines.run_v7 import cargar_motor
    raiz = tmp_path_factory.mktemp("v7ref")
    insumos.preparar_raiz(raiz, modo="referencia")
    with contextlib.redirect_stdout(io.StringIO()):
        m = cargar_motor(raiz)
        m.OUT_DIR.mkdir(parents=True, exist_ok=True)
        _, _, _, _, _, out_nairu, _ = m.paso1_nairu_naicu()
        m.paso2_pib_potencial(out_nairu)
    return raiz / "outputs"


class TestReproduceLineaBaseV7:
    def test_nairu_naicu_mensual(self, corrida_referencia):
        a = pd.read_csv(corrida_referencia / "nairu_estimates_v7.csv")
        b = pd.read_csv(BASE / "nairu_estimates_v7.csv")
        assert list(a["Date"]) == list(b["Date"])
        for c in b.columns[1:]:
            # 1e-4 pp: la TRM del repo (promedio por días calendario) difiere
            # levemente de la del compañero; efecto observado ~3e-5.
            np.testing.assert_allclose(a[c], b[c], atol=1e-4, rtol=0, err_msg=c)
        assert a["nairu_estimate"].iloc[-1] == pytest.approx(8.6256, abs=1e-3)
        assert a["naicu_estimate"].iloc[-1] == pytest.approx(77.530, abs=1e-3)

    def test_series_trimestrales(self, corrida_referencia):
        a = pd.read_csv(corrida_referencia / "pib_potencial_integrado_v7_series.csv")
        b = pd.read_csv(BASE / "pib_potencial_integrado_v7_series.csv")
        assert a.shape == b.shape and list(a.columns) == list(b.columns)
        for c in b.columns[1:]:
            x, y = a[c].astype(float), b[c].astype(float)
            assert (x.isna() == y.isna()).all(), c
            np.testing.assert_allclose(x, y, rtol=1e-5, atol=1e-4, err_msg=c)

    def test_brecha_2025q4(self, corrida_referencia):
        a = pd.read_csv(corrida_referencia / "pib_potencial_integrado_v7_series.csv", parse_dates=["date"])
        g = a.loc[a["date"] == "2025-12-01", "brecha_pot"].iloc[0] * 100
        assert g == pytest.approx(-0.10, abs=0.01)
