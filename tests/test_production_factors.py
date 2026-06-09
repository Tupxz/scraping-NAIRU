"""Tests para src/production/factors.py."""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

import src.production.factors as factors_module
from src.production.factors import (
    ALPHA_FALLBACK,
    alpha_dinamico,
    compute_all_factors,
    factor_capital,
    factor_trabajo,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _base_df(n: int = 12) -> pd.DataFrame:
    """DataFrame trimestral sintético con todas las columnas fuente."""
    dates = pd.date_range("2016-01-01", periods=n, freq="QS")
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "date":    dates,
        "PET":     rng.uniform(28_000, 32_000, n),   # miles de personas
        "TGP":     rng.uniform(62, 68, n),            # %
        "TD":      rng.uniform(9, 14, n),             # %
        "NAIRU_q": rng.uniform(9, 12, n),             # % (< TD en promedio)
        "K":       rng.uniform(1_500_000, 2_000_000, n),  # MM COP 2017
        "UCI":     rng.uniform(72, 80, n),            # %
        "NAICU_q": rng.uniform(76, 84, n),            # % (> UCI en promedio)
        "compensation_employees":   rng.uniform(60_000, 80_000, n),
        "gross_operating_surplus":  rng.uniform(55_000, 75_000, n),
        "mixed_income":             rng.uniform(35_000, 45_000, n),
    })


# ── factor_trabajo ────────────────────────────────────────────────────────────

class TestFactorTrabajo:
    def test_l_obs_positivo(self):
        df = factor_trabajo(_base_df())
        assert (df["L_obs"] > 0).all()

    def test_l_pot_positivo(self):
        df = factor_trabajo(_base_df())
        assert (df["L_pot"] > 0).all()

    def test_l_pot_mayor_que_l_obs_cuando_nairu_menor_que_td(self):
        """Si NAIRU* < TD, hay más desempleo del estructural → L_pot > L_obs."""
        df = _base_df()
        # Forzar NAIRU* < TD
        df["TD"] = 13.0
        df["NAIRU_q"] = 10.0
        df = factor_trabajo(df)
        assert (df["L_pot"] > df["L_obs"]).all()

    def test_l_pot_igual_l_obs_cuando_nairu_igual_td(self):
        df = _base_df()
        df["TD"] = 11.0
        df["NAIRU_q"] = 11.0
        df = factor_trabajo(df)
        pd.testing.assert_series_equal(
            df["L_pot"], df["L_obs"], check_names=False, rtol=1e-6,
        )

    def test_proxy_td_cuando_no_hay_nairu(self):
        df = _base_df().drop(columns=["NAIRU_q"])
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            df_out = factor_trabajo(df)
            assert any("NAIRU_q" in str(warning.message) for warning in w)
        pd.testing.assert_series_equal(
            df_out["L_pot"], df_out["L_obs"], check_names=False,
        )

    def test_formula_correcta(self):
        df = _base_df(1)
        df["PET"] = 30_000.0
        df["TGP"] = 65.0
        df["TD"] = 12.0
        df["NAIRU_q"] = 10.0
        df = factor_trabajo(df)
        esperado_l_obs = 30_000 * 0.65 * (1 - 0.12)
        esperado_l_pot = 30_000 * 0.65 * (1 - 0.10)
        assert abs(df["L_obs"].iloc[0] - esperado_l_obs) < 0.01
        assert abs(df["L_pot"].iloc[0] - esperado_l_pot) < 0.01

    def test_no_modifica_df_original(self):
        df = _base_df()
        cols_antes = set(df.columns)
        factor_trabajo(df)
        assert set(df.columns) == cols_antes


# ── factor_capital ────────────────────────────────────────────────────────────

class TestFactorCapital:
    def test_k_usado_positivo(self):
        df = factor_capital(_base_df())
        assert (df["K_usado"] > 0).all()

    def test_k_pot_positivo(self):
        df = factor_capital(_base_df())
        assert (df["K_pot"] > 0).all()

    def test_k_pot_mayor_que_k_usado_cuando_naicu_mayor_que_uci(self):
        """Si NAICU > UCI, hay capacidad sin usar → K_pot > K_usado."""
        df = _base_df()
        df["UCI"] = 74.0
        df["NAICU_q"] = 80.0
        df = factor_capital(df)
        assert (df["K_pot"] > df["K_usado"]).all()

    def test_formula_correcta(self):
        df = _base_df(1)
        df["K"] = 1_600_000.0
        df["UCI"] = 75.0
        df["NAICU_q"] = 80.0
        df = factor_capital(df)
        assert abs(df["K_usado"].iloc[0] - 1_200_000.0) < 1.0
        assert abs(df["K_pot"].iloc[0] - 1_280_000.0) < 1.0

    def test_proxy_uci_cuando_no_hay_naicu(self):
        df = _base_df().drop(columns=["NAICU_q"])
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            df_out = factor_capital(df)
            assert any("NAICU_q" in str(warning.message) for warning in w)
        pd.testing.assert_series_equal(
            df_out["K_pot"], df_out["K_usado"], check_names=False,
        )

    def test_no_modifica_df_original(self):
        df = _base_df()
        cols_antes = set(df.columns)
        factor_capital(df)
        assert set(df.columns) == cols_antes


# ── alpha_dinamico ────────────────────────────────────────────────────────────

class TestAlphaDinamico:
    def test_alpha_entre_cero_y_uno(self):
        df = alpha_dinamico(_base_df())
        assert (df["alpha"] > 0).all()
        assert (df["alpha"] < 1).all()

    def test_fallback_sin_columnas_ingreso(self, monkeypatch):
        # Con ALPHA_FIXED=None se activa la lógica de respaldo
        monkeypatch.setattr(factors_module, "ALPHA_FIXED", None)
        df = _base_df().drop(
            columns=["compensation_employees", "gross_operating_surplus", "mixed_income"]
        )
        df = alpha_dinamico(df)
        assert (df["alpha"] == ALPHA_FALLBACK).all()

    def test_fallback_para_nan(self, monkeypatch):
        monkeypatch.setattr(factors_module, "ALPHA_FIXED", None)
        df = _base_df()
        df.loc[:, "compensation_employees"] = np.nan
        df.loc[:, "gross_operating_surplus"] = np.nan
        df.loc[:, "mixed_income"] = np.nan
        df = alpha_dinamico(df)
        assert (df["alpha"] == ALPHA_FALLBACK).all()

    def test_formula_correcta(self, monkeypatch):
        """alpha = (EBE + IM) / (RA + EBE + IM)."""
        monkeypatch.setattr(factors_module, "ALPHA_FIXED", None)
        df = _base_df(1)
        df["compensation_employees"] = 60_000.0   # RA
        df["gross_operating_surplus"] = 70_000.0  # EBE
        df["mixed_income"] = 30_000.0             # IM
        df = alpha_dinamico(df)
        # alpha = (70_000 + 30_000) / (60_000 + 70_000 + 30_000) = 100_000/160_000 ≈ 0.625
        assert abs(df["alpha"].iloc[0] - 100_000 / 160_000) < 1e-6

    def test_clamping(self, monkeypatch):
        """alpha se clipa a [0.20, 0.80]."""
        monkeypatch.setattr(factors_module, "ALPHA_FIXED", None)
        df = _base_df(1)
        df["compensation_employees"] = 100.0
        df["gross_operating_surplus"] = 1.0
        df["mixed_income"] = 1.0    # alpha muy bajo → se clipa a 0.20
        df = alpha_dinamico(df)
        assert df["alpha"].iloc[0] >= 0.20

    def test_no_modifica_df_original(self):
        df = _base_df()
        cols_antes = set(df.columns)
        alpha_dinamico(df)
        assert set(df.columns) == cols_antes


# ── compute_all_factors ───────────────────────────────────────────────────────

class TestComputeAllFactors:
    def test_produce_todas_las_columnas(self):
        df = compute_all_factors(_base_df())
        for col in ("L_obs", "L_pot", "K_usado", "K_pot", "alpha"):
            assert col in df.columns, f"Falta columna: {col}"

    def test_no_nan_inesperados(self):
        df = compute_all_factors(_base_df())
        for col in ("L_obs", "L_pot", "K_usado", "K_pot", "alpha"):
            assert df[col].notna().all(), f"NaN inesperado en {col}"
