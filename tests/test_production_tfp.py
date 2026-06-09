"""Tests para src/production/tfp.py."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.production.tfp import (
    BHP_ITERATIONS,
    HP_LAMBDA_QUARTERLY,
    boosted_hp_filter,
    compute_tfp,
    compute_tfp_observed,
    compute_tfp_trend,
    hp_filter,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _serie_sintetica(n: int = 60, seed: int = 42) -> pd.Series:
    """Serie AR(1) estacionaria con tendencia, índice trimestral."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2005-01-01", periods=n, freq="QS")
    trend = np.linspace(100, 150, n)
    cycle = np.zeros(n)
    for i in range(1, n):
        cycle[i] = 0.7 * cycle[i - 1] + rng.normal(0, 1)
    return pd.Series(trend + cycle, index=dates, name="serie")


def _base_df(n: int = 60) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    dates = pd.date_range("2005-01-01", periods=n, freq="QS")
    k = np.linspace(1_500_000, 2_000_000, n)
    l = np.linspace(15_000, 18_000, n)
    alpha = np.full(n, 0.40)
    a = np.full(n, 1.25) + rng.normal(0, 0.02, n)
    pib = a * (k ** alpha) * (l ** (1 - alpha))
    return pd.DataFrame({
        "date": dates,
        "PIB": pib,
        "K_usado": k,
        "L_obs": l,
        "alpha": alpha,
    })


# ── hp_filter ─────────────────────────────────────────────────────────────────

class TestHpFilter:
    def test_retorna_cycle_y_trend(self):
        s = _serie_sintetica()
        cycle, trend = hp_filter(s)
        assert len(cycle) == len(s)
        assert len(trend) == len(s)

    def test_ciclo_media_cercana_a_cero(self):
        """El ciclo HP tiene media aproximadamente cero."""
        s = _serie_sintetica(80)
        cycle, _ = hp_filter(s)
        assert abs(cycle.mean()) < 1.0

    def test_trend_mas_suave_que_serie(self):
        """La tendencia varía menos que la serie original."""
        s = _serie_sintetica(60)
        _, trend = hp_filter(s)
        assert trend.std() <= s.std()

    def test_maneja_nan_al_inicio(self):
        s = _serie_sintetica(60)
        s.iloc[:12] = np.nan
        cycle, trend = hp_filter(s)
        assert cycle.iloc[:12].isna().all()
        assert trend.iloc[:12].isna().all()
        assert trend.iloc[12:].notna().all()

    def test_error_con_pocos_datos(self):
        s = pd.Series([1.0, 2.0, 3.0])
        with pytest.raises(ValueError, match="8 observaciones"):
            hp_filter(s)

    def test_indice_preservado(self):
        s = _serie_sintetica(40)
        cycle, trend = hp_filter(s)
        assert list(cycle.index) == list(s.index)
        assert list(trend.index) == list(s.index)

    def test_cycle_mas_trend_igual_serie(self):
        """cycle + trend debe reconstruir la serie original."""
        s = _serie_sintetica(50)
        cycle, trend = hp_filter(s)
        reconstruido = cycle + trend
        pd.testing.assert_series_equal(
            reconstruido, s, check_names=False, rtol=1e-6,
        )


# ── boosted_hp_filter ────────────────────────────────────────────────────────

class TestBoostedHpFilter:
    def test_retorna_cycle_y_trend(self):
        s = _serie_sintetica()
        cycle, trend = boosted_hp_filter(s)
        assert len(cycle) == len(s)
        assert len(trend) == len(s)

    def test_cycle_mas_trend_igual_serie(self):
        """cycle + trend debe reconstruir la serie original."""
        s = _serie_sintetica(50)
        cycle, trend = boosted_hp_filter(s)
        reconstruido = cycle + trend
        pd.testing.assert_series_equal(
            reconstruido, s, check_names=False, rtol=1e-6,
        )

    def test_ciclo_bhp_menor_que_hp(self):
        """Con iter>1, el ciclo BHP es más pequeño que el ciclo HP simple.

        Cada iteración aplica HP sobre el ciclo residual, extrayendo
        componentes de alta frecuencia sucesivos; el ciclo final es menor.
        """
        s = _serie_sintetica(60)
        cycle_hp,  _ = hp_filter(s)
        cycle_bhp, _ = boosted_hp_filter(s, iterations=3)
        assert cycle_bhp.std() < cycle_hp.std()

    def test_trend_bhp_mas_cercana_a_serie(self):
        """La tendencia BHP (iter>1) tiene mayor varianza que la tendencia HP.

        Como el ciclo BHP es más pequeño, la tendencia = serie − ciclo
        retiene más varianza de la serie original.
        """
        s = _serie_sintetica(60)
        _, trend_hp  = hp_filter(s)
        _, trend_bhp = boosted_hp_filter(s, iterations=3)
        assert trend_bhp.std() >= trend_hp.std()

    def test_maneja_nan_al_inicio(self):
        s = _serie_sintetica(60)
        s.iloc[:12] = np.nan
        cycle, trend = boosted_hp_filter(s)
        assert cycle.iloc[:12].isna().all()
        assert trend.iloc[:12].isna().all()
        assert trend.iloc[12:].notna().all()

    def test_error_con_pocos_datos(self):
        s = pd.Series([1.0, 2.0, 3.0])
        with pytest.raises(ValueError, match="8 observaciones"):
            boosted_hp_filter(s)

    def test_indice_preservado(self):
        s = _serie_sintetica(40)
        cycle, trend = boosted_hp_filter(s)
        assert list(cycle.index) == list(s.index)
        assert list(trend.index) == list(s.index)

    def test_iteraciones_1_equivale_hp(self):
        """Con iterations=1 el resultado debe ser igual al HP estándar."""
        s = _serie_sintetica(50)
        cycle_hp,  trend_hp  = hp_filter(s)
        cycle_bhp, trend_bhp = boosted_hp_filter(s, iterations=1)
        pd.testing.assert_series_equal(trend_bhp, trend_hp, check_names=False, rtol=1e-6)
        pd.testing.assert_series_equal(cycle_bhp, cycle_hp, check_names=False, rtol=1e-6)

    def test_constante_bhp_iterations(self):
        assert BHP_ITERATIONS == 3


# ── compute_tfp_observed ──────────────────────────────────────────────────────

class TestComputeTfpObserved:
    def test_a_obs_positivo(self):
        df = compute_tfp_observed(_base_df())
        assert (df["A_obs"] > 0).all()

    def test_formula_correcta(self):
        df = _base_df(1)
        df["PIB"] = 130_000.0
        df["K_usado"] = 1_600_000.0
        df["L_obs"] = 16_000.0
        df["alpha"] = 0.40
        df = compute_tfp_observed(df)
        esperado = 130_000 / (1_600_000 ** 0.4 * 16_000 ** 0.6)
        assert abs(df["A_obs"].iloc[0] - esperado) < 1e-6

    def test_nan_cuando_k_cero(self):
        df = _base_df(1)
        df["K_usado"] = 0.0
        df = compute_tfp_observed(df)
        assert df["A_obs"].isna().all()

    def test_no_modifica_original(self):
        df = _base_df()
        cols_antes = set(df.columns)
        compute_tfp_observed(df)
        assert set(df.columns) == cols_antes


# ── compute_tfp_trend ─────────────────────────────────────────────────────────

class TestComputeTfpTrend:
    def test_produce_a_pot_y_a_cycle(self):
        df = compute_tfp_observed(_base_df())
        df = compute_tfp_trend(df)
        assert "A_pot" in df.columns
        assert "A_cycle" in df.columns

    def test_a_pot_mas_suave_que_a_obs(self):
        df = compute_tfp(_base_df())
        assert df["A_pot"].std() <= df["A_obs"].std()

    def test_error_sin_a_obs(self):
        df = _base_df()
        with pytest.raises(KeyError, match="A_obs"):
            compute_tfp_trend(df)

    def test_a_pot_positivo(self):
        df = compute_tfp(_base_df())
        assert (df["A_pot"] > 0).all()


# ── compute_tfp ───────────────────────────────────────────────────────────────

class TestComputeTfp:
    def test_produce_tres_columnas(self):
        df = compute_tfp(_base_df())
        for col in ("A_obs", "A_pot", "A_cycle"):
            assert col in df.columns

    def test_a_cycle_es_diferencia(self):
        """A_cycle = A_obs - A_pot (hasta precisión numérica)."""
        df = compute_tfp(_base_df())
        diff = (df["A_obs"] - df["A_pot"] - df["A_cycle"]).abs()
        assert diff.max() < 1e-10

    def test_no_modifica_original(self):
        df = _base_df()
        cols_antes = set(df.columns)
        compute_tfp(df)
        assert set(df.columns) == cols_antes


# ── Capital humano H en el término de trabajo ─────────────────────────────────

class TestCapitalHumano:
    """A_obs = PIB / (K^alpha · (H · L_obs)^(1−alpha)) — H = 1 si la columna falta."""

    def test_h_ausente_equivale_a_h_uno(self):
        """Sin columna H, A_obs es idéntico a pasar H = 1 (compatibilidad hacia atrás)."""
        df = _base_df()
        a_sin = compute_tfp_observed(df)["A_obs"]
        a_h1 = compute_tfp_observed(df.assign(H=1.0))["A_obs"]
        pd.testing.assert_series_equal(a_sin, a_h1, check_names=False)

    def test_h_mayor_reduce_la_ptf(self):
        """Con H > 1 el trabajo efectivo sube → A_obs baja para el mismo PIB."""
        df = _base_df()
        a_base = compute_tfp_observed(df.assign(H=1.0))["A_obs"]
        a_h = compute_tfp_observed(df.assign(H=3.0))["A_obs"]
        assert (a_h < a_base).all()

    def test_formula_h_explicita(self):
        """Verifica la fórmula exacta con H presente."""
        df = _base_df().assign(H=2.5)
        out = compute_tfp_observed(df)
        esperado = df["PIB"] / (
            df["K_usado"] ** df["alpha"]
            * (df["H"] * df["L_obs"]) ** (1.0 - df["alpha"])
        )
        pd.testing.assert_series_equal(out["A_obs"], esperado, check_names=False)
