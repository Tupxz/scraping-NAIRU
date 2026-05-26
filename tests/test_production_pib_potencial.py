"""Tests para src/production/pib_potencial.py."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.production.factors import compute_all_factors
from src.production.pib_potencial import QUARTERLY_OUTPUT_COLS, compute_pib_potencial
from src.production.tfp import compute_tfp


# ── Helper ────────────────────────────────────────────────────────────────────

def _df_completo(n: int = 60) -> pd.DataFrame:
    """DataFrame con todas las columnas necesarias tras factors + tfp."""
    rng = np.random.default_rng(42)
    dates = pd.date_range("2005-01-01", periods=n, freq="QS")
    alpha = 0.40

    K = np.linspace(1_500_000, 2_000_000, n)
    L = np.linspace(15_000, 18_000, n)
    A = 1.25 + rng.normal(0, 0.01, n)
    PIB = A * (K ** alpha) * (L ** (1 - alpha))

    return pd.DataFrame({
        "date":    dates,
        "year":    [d.year for d in dates],
        "quarter": [d.quarter for d in dates],
        "PIB":     PIB,
        "A_pot":   A * 1.00,   # simulamos que la tendencia ≈ observada
        "K_pot":   K * 1.02,   # K potencial ligeramente mayor
        "L_pot":   L * 1.03,   # L potencial ligeramente mayor
        "alpha":   np.full(n, alpha),
        # columnas adicionales para QUARTERLY_OUTPUT_COLS
        "K":       K,
        "UCI":     rng.uniform(72, 80, n),
        "NAICU_q": rng.uniform(76, 84, n),
        "TD":      rng.uniform(9, 14, n),
        "TGP":     rng.uniform(62, 68, n),
        "PET":     rng.uniform(28_000, 32_000, n),
        "NAIRU_q": rng.uniform(9, 12, n),
        "compensation_employees":  rng.uniform(60_000, 80_000, n),
        "gross_operating_surplus": rng.uniform(55_000, 75_000, n),
        "mixed_income":            rng.uniform(35_000, 45_000, n),
        "L_obs":   L,
        "K_usado": K * 0.76,
        "A_obs":   A,
        "A_cycle": rng.normal(0, 0.01, n),
    })


# ── compute_pib_potencial ─────────────────────────────────────────────────────

class TestComputePibPotencial:
    def test_produce_cuatro_columnas(self):
        df = compute_pib_potencial(_df_completo())
        for col in ("PIB_pot", "Brecha_CD", "PIB_tend_BHP", "Brecha_BHP"):
            assert col in df.columns, f"Falta columna: {col}"

    def test_pib_pot_positivo(self):
        df = compute_pib_potencial(_df_completo())
        assert (df["PIB_pot"] > 0).all()

    def test_brecha_cd_razonable(self):
        """Brecha CD dentro de ±20 pp para datos sintéticos coherentes."""
        df = compute_pib_potencial(_df_completo())
        assert df["Brecha_CD"].abs().max() < 20.0

    def test_brecha_hp_razonable(self):
        df = compute_pib_potencial(_df_completo())
        assert df["Brecha_BHP"].abs().max() < 20.0

    def test_brecha_cero_cuando_pib_igual_potencial(self):
        """Si PIB == PIB_pot, la brecha CD debe ser 0."""
        df = _df_completo(60)
        alpha = df["alpha"].iloc[0]
        # Construir PIB tal que == A_pot * K_pot^alpha * L_pot^(1-alpha)
        df["PIB"] = (
            df["A_pot"]
            * df["K_pot"] ** alpha
            * df["L_pot"] ** (1 - alpha)
        )
        df = compute_pib_potencial(df)
        assert df["Brecha_CD"].abs().max() < 1e-6

    def test_error_sin_columna_requerida(self):
        df = _df_completo().drop(columns=["A_pot"])
        with pytest.raises(KeyError, match="A_pot"):
            compute_pib_potencial(df)

    def test_no_modifica_original(self):
        df = _df_completo()
        cols_antes = set(df.columns)
        compute_pib_potencial(df)
        assert set(df.columns) == cols_antes

    def test_pib_tend_bhp_mas_suave_que_pib(self):
        """La tendencia BHP del PIB debe ser más suave que el PIB observado."""
        df = compute_pib_potencial(_df_completo())
        assert df["PIB_tend_BHP"].std() <= df["PIB"].std()


# ── Pipeline integrado (factors → tfp → pib_potencial) ───────────────────────

class TestPipelineIntegrado:
    def _base_pipeline(self, n: int = 60) -> pd.DataFrame:
        rng = np.random.default_rng(0)
        dates = pd.date_range("2005-01-01", periods=n, freq="QS")
        K = np.linspace(1_500_000, 2_000_000, n)
        L_obs = np.linspace(15_000, 18_000, n)
        alpha = 0.40
        A = 1.25 + rng.normal(0, 0.01, n)
        PIB = A * (K * 0.76 ** alpha) * (L_obs ** (1 - alpha))

        return pd.DataFrame({
            "date":    dates,
            "year":    [d.year for d in dates],
            "quarter": [d.quarter for d in dates],
            "PIB":     PIB,
            "K":       K,
            "UCI":     np.full(n, 76.0),
            "NAICU_q": np.full(n, 80.0),
            "TD":      np.full(n, 12.0),
            "TGP":     np.full(n, 65.0),
            "PET":     np.full(n, 30_000.0),
            "NAIRU_q": np.full(n, 10.0),
            "compensation_employees":  np.full(n, 70_000.0),
            "gross_operating_surplus": np.full(n, 65_000.0),
            "mixed_income":            np.full(n, 40_000.0),
        })

    def test_pipeline_completo_sin_errores(self):
        df = self._base_pipeline()
        df = compute_all_factors(df)
        df = compute_tfp(df)
        df = compute_pib_potencial(df)
        for col in ("PIB_pot", "Brecha_CD", "PIB_tend_BHP", "Brecha_BHP"):
            assert col in df.columns
            assert df[col].notna().any(), f"Columna {col} todo NaN"

    def test_brecha_promedio_cercana_a_cero(self):
        """En datos de largo plazo, la brecha media debe ser cercana a 0."""
        df = self._base_pipeline(80)
        df = compute_all_factors(df)
        df = compute_tfp(df)
        df = compute_pib_potencial(df)
        # La brecha BHP tiene media estructuralmente cercana a 0 por construcción del BHP
        assert abs(df["Brecha_BHP"].mean()) < 3.0


# ── QUARTERLY_OUTPUT_COLS ─────────────────────────────────────────────────────

class TestQuarterlyOutputCols:
    def test_sin_duplicados(self):
        assert len(QUARTERLY_OUTPUT_COLS) == len(set(QUARTERLY_OUTPUT_COLS))

    def test_date_es_primero(self):
        assert QUARTERLY_OUTPUT_COLS[0] == "date"

    def test_columnas_resultado_presentes(self):
        for col in ("PIB_pot", "Brecha_CD", "PIB_tend_BHP", "Brecha_BHP"):
            assert col in QUARTERLY_OUTPUT_COLS
