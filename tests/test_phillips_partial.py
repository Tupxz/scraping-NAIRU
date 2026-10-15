"""Tests de la curva de Phillips parcial (src/nairu/phillips.py) y de su export web.

La gráfica "Curva de Phillips" del tablero muestra Δg_t menos todo lo que el
modelo explica con regresores distintos del bloque desempleo/NAIRU, contra la
brecha de desempleo actual (ver el docstring de ``src/nairu/phillips.py``).

Los tests de cálculo generan la dependiente con la ecuación de medición escrita
FILA POR FILA (bucles explícitos, independiente de la implementación
vectorizada) y un error ε conocido, de modo que ``phillips_parcial`` debe
recuperar exactamente ``bloque_desempleo + ε``. Los coeficientes sintéticos son
todos distintos entre sí, para que un cruce de índices (β_u,1 con u_{t-2}, o el
bloque ICU con el NAIRU en vez del NAICU) se note.

Clases:
  TestPartialPhillipsCalculation  — identidad con datos sintéticos
  TestCoefficientHelpers          — lectura de la tabla del MLE y Σβ_u
  TestExportWebIntegration        — nairu_monthly.csv / meta.json con CSV sintéticos
  TestPublishedOutputs            — consistencia del docs/data/ versionado
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.nairu.phillips import (
    REQUIRED_COEFFICIENTS,
    REQUIRED_COLUMNS,
    coefficients_from_table,
    load_coefficient_table,
    partial_phillips,
    unemployment_slope_summary,
)

# Todos distintos a propósito (un cruce de índices cambiaría el resultado).
COEF = {
    "intercept": 0.11,
    "inflation_lag1_coefficient": -0.23,
    "inflation_lag2_coefficient": 0.07,
    "unemployment_coefficient_0": -0.031,
    "unemployment_coefficient_1": -0.017,
    "unemployment_coefficient_2": -0.009,
    "icu_coefficient_0": 0.043,
    "icu_coefficient_1": 0.021,
    "icu_coefficient_2": 0.005,
    "expectations_coefficient": -0.095,
    "oil_shock_coefficient": -0.002,
}


def _synthetic_nairu(n: int = 48, seed: int = 7):
    """Panel sintético + ε conocido; la dependiente sigue la ecuación fila a fila."""
    rng = np.random.default_rng(seed)
    df = pd.DataFrame(
        {
            "Date": pd.date_range("2010-01-01", periods=n, freq="MS"),
            "inflation_gap_change_lag1": rng.normal(0, 0.3, n),
            "inflation_gap_change_lag2": rng.normal(0, 0.3, n),
            "unemployment_current": rng.normal(10, 1.5, n),
            "unemployment_lag1": rng.normal(10, 1.5, n),
            "unemployment_lag2": rng.normal(10, 1.5, n),
            "icu_current": rng.normal(75, 3, n),
            "icu_lag1": rng.normal(75, 3, n),
            "icu_lag2": rng.normal(75, 3, n),
            "expected_inflation_term": rng.normal(0, 1, n),
            "oil_shock": rng.normal(0, 6, n),
            # estados que se MUEVEN mes a mes, para distinguir u*_t de u*_{t-j}
            "nairu_estimate": 9 + np.cumsum(rng.normal(0, 0.15, n)),
            "naicu_estimate": 78 + np.cumsum(rng.normal(0, 0.3, n)),
        }
    )
    eps = rng.normal(0, 0.05, n)
    c = COEF
    dg = np.empty(n)
    for t in range(n):
        row = df.iloc[t]
        u_star, c_star = row["nairu_estimate"], row["naicu_estimate"]
        dg[t] = (
            c["intercept"]
            + c["inflation_lag1_coefficient"] * row["inflation_gap_change_lag1"]
            + c["inflation_lag2_coefficient"] * row["inflation_gap_change_lag2"]
            + c["expectations_coefficient"] * row["expected_inflation_term"]
            + c["oil_shock_coefficient"] * row["oil_shock"]
            + c["unemployment_coefficient_0"] * (row["unemployment_current"] - u_star)
            + c["unemployment_coefficient_1"] * (row["unemployment_lag1"] - u_star)
            + c["unemployment_coefficient_2"] * (row["unemployment_lag2"] - u_star)
            + c["icu_coefficient_0"] * (row["icu_current"] - c_star)
            + c["icu_coefficient_1"] * (row["icu_lag1"] - c_star)
            + c["icu_coefficient_2"] * (row["icu_lag2"] - c_star)
            + eps[t]
        )
    df["inflation_gap_change"] = dg
    return df, eps


class TestPartialPhillipsCalculation:
    def test_parcial_recupera_bloque_desempleo_mas_error(self):
        df, eps = _synthetic_nairu()
        res = partial_phillips(df, COEF)
        bloque = np.array(
            [
                sum(
                    COEF[f"unemployment_coefficient_{j}"]
                    * (df.iloc[t][col] - df.iloc[t]["nairu_estimate"])
                    for j, col in enumerate(
                        ["unemployment_current", "unemployment_lag1", "unemployment_lag2"]
                    )
                )
                for t in range(len(df))
            ]
        )
        np.testing.assert_allclose(res["bloque_desempleo"], bloque, atol=1e-12)
        np.testing.assert_allclose(res["phillips_parcial"], bloque + eps, atol=1e-12)

    def test_mismo_nairu_contemporaneo_resta_a_los_tres_rezagos(self):
        """u*_t (no u*_{t-j}) resta a u_t, u_{t-1} y u_{t-2}: así es la ecuación."""
        df, eps = _synthetic_nairu()
        res = partial_phillips(df, COEF)
        # Variante equivocada: NAIRU rezagado en cada término. Debe dar OTRA cosa,
        # si no este test no distinguiría nada.
        wrong = sum(
            COEF[f"unemployment_coefficient_{j}"]
            * (df[col] - df["nairu_estimate"].shift(j))
            for j, col in enumerate(["unemployment_current", "unemployment_lag1", "unemployment_lag2"])
        )
        assert not np.allclose(res["bloque_desempleo"].iloc[2:], wrong.iloc[2:], atol=1e-6)

    def test_sin_coeficientes_de_desempleo_el_parcial_es_solo_el_error(self):
        """Si el desempleo no entra al modelo, todo lo demás ya se restó: queda ε."""
        df, eps = _synthetic_nairu()
        coef_sin_u = {k: v for k, v in COEF.items()}
        for j in range(3):
            coef_sin_u[f"unemployment_coefficient_{j}"] = 0.0
        # la dependiente se regenera con esos coeficientes
        df2, eps2 = df.copy(), eps
        dg = df["inflation_gap_change"] - partial_phillips(df, COEF)["bloque_desempleo"]
        df2["inflation_gap_change"] = dg
        res = partial_phillips(df2, coef_sin_u)
        np.testing.assert_allclose(res["phillips_parcial"], eps2, atol=1e-12)
        np.testing.assert_allclose(res["bloque_desempleo"], 0.0, atol=1e-15)

    def test_bloque_icu_usa_naicu_no_nairu(self):
        """Mover el NAICU (solo) debe mover el ajustado y NO el bloque de desempleo."""
        df, _ = _synthetic_nairu()
        base = partial_phillips(df, COEF)
        shifted = df.copy()
        shifted["naicu_estimate"] = shifted["naicu_estimate"] + 5.0
        moved = partial_phillips(shifted, COEF)
        np.testing.assert_allclose(base["bloque_desempleo"], moved["bloque_desempleo"], atol=1e-15)
        suma_icu = COEF["icu_coefficient_0"] + COEF["icu_coefficient_1"] + COEF["icu_coefficient_2"]
        np.testing.assert_allclose(
            moved["ajustado_sin_desempleo"] - base["ajustado_sin_desempleo"],
            -suma_icu * 5.0,
            atol=1e-12,
        )

    def test_brecha_laboral_es_desempleo_menos_nairu(self):
        df, _ = _synthetic_nairu()
        res = partial_phillips(df, COEF)
        np.testing.assert_allclose(
            res["brecha_laboral"], df["unemployment_current"] - df["nairu_estimate"], atol=1e-15
        )

    def test_dg_inf_es_la_dependiente_sin_transformar(self):
        df, _ = _synthetic_nairu()
        res = partial_phillips(df, COEF)
        pd.testing.assert_series_equal(res["dg_inf"], df["inflation_gap_change"], check_names=False)

    def test_regresor_faltante_propaga_nan_no_cero(self):
        df, _ = _synthetic_nairu()
        df.loc[3, "oil_shock"] = np.nan
        res = partial_phillips(df, COEF)
        assert math.isnan(res.loc[3, "phillips_parcial"])
        assert math.isnan(res.loc[3, "ajustado_sin_desempleo"])
        # y NO contamina las demás filas ni el bloque de desempleo de esa fila
        assert res["phillips_parcial"].drop(index=3).notna().all()
        assert not math.isnan(res.loc[3, "bloque_desempleo"])

    def test_conserva_el_indice_de_entrada(self):
        df, _ = _synthetic_nairu()
        df.index = df.index + 100
        res = partial_phillips(df, COEF)
        assert list(res.index) == list(df.index)

    def test_columna_ausente_da_error_claro(self):
        df, _ = _synthetic_nairu()
        with pytest.raises(ValueError, match="oil_shock"):
            partial_phillips(df.drop(columns=["oil_shock"]), COEF)

    def test_coeficiente_ausente_da_error_claro(self):
        df, _ = _synthetic_nairu()
        coef = {k: v for k, v in COEF.items() if k != "icu_coefficient_2"}
        with pytest.raises(ValueError, match="icu_coefficient_2"):
            partial_phillips(df, coef)

    def test_las_constantes_requeridas_cubren_la_ecuacion(self):
        assert set(REQUIRED_COEFFICIENTS) == set(COEF)
        assert "nairu_estimate" in REQUIRED_COLUMNS and "naicu_estimate" in REQUIRED_COLUMNS


def _coef_table(rows: dict[str, float], *, with_derived: bool = True, p_value: float = 0.118):
    """Tabla con el formato de nairu_mle_coefficients.csv (índice = parameter)."""
    data = [{"parameter": k, "estimate": v, "p_value": np.nan} for k, v in rows.items()]
    if with_derived:
        total = sum(rows[f"unemployment_coefficient_{j}"] for j in range(3))
        data.append(
            {"parameter": "unemployment_gap_coefficient", "estimate": total, "p_value": p_value}
        )
    return pd.DataFrame(data).set_index("parameter")


class TestCoefficientHelpers:
    def test_coefficients_from_table_devuelve_floats(self):
        coef = coefficients_from_table(_coef_table(COEF))
        assert coef == pytest.approx(COEF)
        assert all(isinstance(v, float) for v in coef.values())

    def test_coefficients_from_table_falta_uno(self):
        rows = {k: v for k, v in COEF.items() if k != "oil_shock_coefficient"}
        with pytest.raises(ValueError, match="oil_shock_coefficient"):
            coefficients_from_table(_coef_table(rows))

    def test_resumen_usa_la_fila_derivada_y_su_pvalor(self):
        s = unemployment_slope_summary(_coef_table(COEF, p_value=0.0421))
        assert s["sum_beta_u"] == pytest.approx(-0.031 - 0.017 - 0.009)
        assert s["p_value"] == pytest.approx(0.0421)

    def test_resumen_sin_fila_derivada_suma_los_tres_y_pvalor_none(self):
        s = unemployment_slope_summary(_coef_table(COEF, with_derived=False))
        assert s["sum_beta_u"] == pytest.approx(-0.057)
        assert s["p_value"] is None

    def test_resumen_pvalor_nan_se_vuelve_none(self):
        s = unemployment_slope_summary(_coef_table(COEF, p_value=float("nan")))
        assert s["p_value"] is None

    def test_load_coefficient_table_lee_el_csv_del_mle(self, tmp_path):
        path = tmp_path / "coef.csv"
        _coef_table(COEF).reset_index().to_csv(path, index=False)
        table = load_coefficient_table(path)
        assert coefficients_from_table(table) == pytest.approx(COEF)


# ── Export web con CSV sintéticos ────────────────────────────────────────────

def _write_nairu_csv(path: Path, df: pd.DataFrame) -> pd.DataFrame:
    """CSV con todas las columnas que lee el export (NAIRU_EXPORT_COLS + regresores)."""
    out = df.copy()
    out["unemployment_gap"] = out["unemployment_current"] - out["nairu_estimate"]
    out["icu_gap"] = out["icu_current"] - out["naicu_estimate"]
    out["inflation_gap"] = np.linspace(-0.5, 2.0, len(out))
    for prefix in ("nairu", "naicu"):
        base = out[f"{prefix}_estimate"]
        for lvl, width in (("90", 1.0), ("95", 1.3)):
            out[f"{prefix}_ci_lower_{lvl}"] = base - width
            out[f"{prefix}_ci_upper_{lvl}"] = base + width
    out.to_csv(path, index=False)
    return out


@pytest.fixture()
def export_env(tmp_path, monkeypatch):
    """Apunta el módulo de export a CSV sintéticos dentro de tmp_path."""
    from src.pipelines import export_web_data as ew

    df, _ = _synthetic_nairu()
    nairu_csv = tmp_path / "nairu_colombia.csv"
    coef_csv = tmp_path / "nairu_mle_coefficients.csv"
    _write_nairu_csv(nairu_csv, df)
    _coef_table(COEF).reset_index().to_csv(coef_csv, index=False)
    monkeypatch.setattr(ew, "NAIRU_CSV", nairu_csv)
    monkeypatch.setattr(ew, "NAIRU_COEF_CSV", coef_csv)
    monkeypatch.setattr(ew, "PIB_CSV", tmp_path / "no_existe_pib.csv")
    monkeypatch.setattr(ew, "VIOG_CSV", tmp_path / "no_existe_viog.csv")
    return ew, tmp_path, df


@pytest.mark.filterwarnings("ignore")
class TestExportWebIntegration:
    def test_nairu_monthly_trae_las_columnas_nuevas_con_los_valores_del_modulo(self, export_env):
        ew, tmp, df = export_env
        out_dir = tmp / "docs_data"
        ew.export_web_data(out_dir)
        web = pd.read_csv(out_dir / "nairu_monthly.csv")
        assert {"dg_inf", "phillips_parcial", "brecha_laboral"}.issubset(web.columns)
        esperado = partial_phillips(pd.read_csv(ew.NAIRU_CSV), COEF)
        np.testing.assert_allclose(web["dg_inf"], esperado["dg_inf"], atol=1e-4)
        np.testing.assert_allclose(web["phillips_parcial"], esperado["phillips_parcial"], atol=1e-4)
        np.testing.assert_allclose(web["brecha_laboral"], esperado["brecha_laboral"], atol=1e-4)

    def test_meta_trae_pendiente_agregada_y_pvalor(self, export_env):
        ew, tmp, _ = export_env
        out_dir = tmp / "docs_data"
        ew.export_web_data(out_dir)
        meta = json.loads((out_dir / "meta.json").read_text(encoding="utf-8"))
        assert meta["phillips_sum_beta_u"] == pytest.approx(-0.057, abs=1e-4)
        assert meta["phillips_sum_beta_u_p"] == pytest.approx(0.118, abs=1e-3)

    def test_sin_csv_de_coeficientes_se_omite_con_warning_y_no_falla(self, export_env):
        ew, tmp, _ = export_env
        ew.NAIRU_COEF_CSV.unlink()
        out_dir = tmp / "docs_data"
        with pytest.warns(UserWarning, match="curva de Phillips parcial"):
            ew.export_web_data(out_dir)
        web = pd.read_csv(out_dir / "nairu_monthly.csv")
        assert "phillips_parcial" not in web.columns and "dg_inf" not in web.columns
        assert "brecha_laboral" in web.columns  # lo demás sigue exportándose
        meta = json.loads((out_dir / "meta.json").read_text(encoding="utf-8"))
        assert "phillips_sum_beta_u" not in meta

    def test_pvalor_ausente_sale_null_y_el_json_es_valido(self, export_env):
        ew, tmp, _ = export_env
        _coef_table(COEF, p_value=float("nan")).reset_index().to_csv(ew.NAIRU_COEF_CSV, index=False)
        out_dir = tmp / "docs_data"
        ew.export_web_data(out_dir)
        texto = (out_dir / "meta.json").read_text(encoding="utf-8")
        assert "NaN" not in texto
        assert json.loads(texto)["phillips_sum_beta_u_p"] is None

    def test_las_demas_columnas_de_nairu_monthly_no_cambian(self, export_env):
        """Agregar columnas no debe tocar las que ya consumía el tablero."""
        ew, tmp, _ = export_env
        out_dir = tmp / "docs_data"
        ew.export_web_data(out_dir)
        web = pd.read_csv(out_dir / "nairu_monthly.csv")
        for col in ew.NAIRU_EXPORT_COLS.values():
            assert col in web.columns, col


# ── Salidas publicadas (docs/data versionado) ────────────────────────────────

REPO = Path(__file__).resolve().parents[1]


class TestPublishedOutputs:
    @pytest.fixture()
    def web(self):
        path = REPO / "docs" / "data" / "nairu_monthly.csv"
        if not path.exists():
            pytest.skip("docs/data/nairu_monthly.csv no existe en este checkout")
        return pd.read_csv(path)

    def test_columnas_de_phillips_parcial_publicadas(self, web):
        assert {"dg_inf", "phillips_parcial", "brecha_laboral"}.issubset(web.columns)
        assert web["phillips_parcial"].notna().all()

    def test_dg_inf_es_la_primera_diferencia_de_brecha_inf(self, web):
        """Δg_t = g_t − g_{t-1}: mismo dato que la gráfica 'Brecha de inflación'."""
        diff = web["brecha_inf"].diff()
        ok = diff.notna()
        # (a 4 decimales: ambas columnas salen redondeadas)
        np.testing.assert_allclose(web.loc[ok, "dg_inf"], diff[ok], atol=2e-4)

    def test_meta_publicada_tiene_pendiente_agregada(self):
        path = REPO / "docs" / "data" / "meta.json"
        if not path.exists():
            pytest.skip("docs/data/meta.json no existe en este checkout")
        meta = json.loads(path.read_text(encoding="utf-8"))
        assert "phillips_sum_beta_u" in meta and meta["phillips_sum_beta_u"] < 0

    def test_publicado_coincide_con_recalculo_desde_outputs(self, web):
        nairu_csv = REPO / "outputs" / "nairu" / "nairu_colombia.csv"
        coef_csv = REPO / "outputs" / "nairu" / "nairu_mle_coefficients.csv"
        if not (nairu_csv.exists() and coef_csv.exists()):
            pytest.skip("outputs/nairu/ no existe (se genera con --nairu-estim)")
        raw = pd.read_csv(nairu_csv)
        esperado = partial_phillips(raw, coefficients_from_table(load_coefficient_table(coef_csv)))
        assert len(web) == len(esperado)
        np.testing.assert_allclose(web["phillips_parcial"], esperado["phillips_parcial"], atol=2e-4)
