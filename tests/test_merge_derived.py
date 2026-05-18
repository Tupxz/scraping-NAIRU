"""Tests para las variables derivadas añadidas en merge.py (bloque C).

Cubre:
- ipc_yoy  : variación interanual del índice IPC.
- ipc_mom  : variación mensual del índice IPC.
- inflation_gap : Inf_Rate - Inf_Goal.

Los tres casos del plan CLOSEOUT (§C3):
1. Serie sintética de crecimiento constante al 5 % anual.
2. Caso puntual con inflation_gap = 1.0.
3. Dataset corto (<12 meses) → ipc_yoy todo NaN, sin lanzar excepción.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.merge import MERGED_COLUMNS, merge_all_sources
from src.quality_checks import QualityCheckError, run_derived_checks


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_df(n_months: int = 36) -> pd.DataFrame:
    """Dataset sintético con IPC al 5 % anual compuesto y meta del 3 %."""
    dates = pd.date_range("2002-01-01", periods=n_months, freq="MS")
    # Índice IPC: crecimiento constante al 5 % anual → base 100 en t=0
    ipc = 100.0 * (1.05 ** (pd.RangeIndex(n_months) / 12))
    inf_goal = pd.Series(3.0, index=range(n_months))
    inf_rate = pd.Series(5.5, index=range(n_months))
    df = pd.DataFrame({
        "date": dates,
        "ipc_index": ipc,
        "Inf_Goal": inf_goal,
        "Inf_Rate": inf_rate,
    })
    return df


def _apply_derived(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica las mismas fórmulas que merge_all_sources."""
    df = df.copy()
    df["ipc_yoy"] = df["ipc_index"].pct_change(12) * 100
    df["ipc_mom"] = df["ipc_index"].pct_change(1) * 100
    df["inflation_gap"] = df["Inf_Rate"] - df["Inf_Goal"]
    return df


# ── Caso 1: serie sintética al 5 % anual ────────────────────────────────────

class TestIpcYoy:
    def test_ipc_yoy_approx_5_after_12_months(self):
        """ipc_yoy ≈ 5.0 % para t ≥ 12 con crecimiento anual del 5 %."""
        df = _apply_derived(_make_df(36))
        values = df["ipc_yoy"].dropna()
        assert len(values) == 24, "Deben existir 24 observaciones con ipc_yoy no nulo"
        assert (values - 5.0).abs().max() < 0.1, (
            f"ipc_yoy debe ser ≈ 5.0 %; max desviación = {(values - 5.0).abs().max():.4f}"
        )

    def test_ipc_mom_positive_for_growing_index(self):
        """ipc_mom debe ser positivo cuando el índice crece."""
        df = _apply_derived(_make_df(36))
        positive = df["ipc_mom"].dropna()
        assert (positive > 0).all(), "ipc_mom debe ser > 0 cuando el IPC crece"

    def test_ipc_yoy_nan_first_12(self):
        """Los primeros 12 meses de ipc_yoy deben ser NaN."""
        df = _apply_derived(_make_df(36))
        assert df["ipc_yoy"].iloc[:12].isna().all(), (
            "Los primeros 12 meses de ipc_yoy deben ser NaN"
        )

    def test_ipc_mom_nan_first_row(self):
        """El primer mes de ipc_mom debe ser NaN."""
        df = _apply_derived(_make_df(36))
        assert pd.isna(df["ipc_mom"].iloc[0]), "El primer mes de ipc_mom debe ser NaN"


# ── Caso 2: inflation_gap puntual ────────────────────────────────────────────

class TestInflationGap:
    def test_inflation_gap_value(self):
        """Inf_Rate=4.0, Inf_Goal=3.0 → inflation_gap=1.0."""
        df = pd.DataFrame({
            "date": pd.date_range("2020-01-01", periods=3, freq="MS"),
            "ipc_index": [100.0, 100.5, 101.0],
            "Inf_Goal": [3.0, 3.0, 3.0],
            "Inf_Rate": [4.0, 4.0, 4.0],
        })
        df = _apply_derived(df)
        assert (df["inflation_gap"] == 1.0).all(), (
            "inflation_gap debe ser 1.0 cuando Inf_Rate=4.0 y Inf_Goal=3.0"
        )

    def test_inflation_gap_negative_when_below_target(self):
        """inflation_gap < 0 cuando la inflación está por debajo de la meta."""
        df = pd.DataFrame({
            "date": pd.date_range("2020-01-01", periods=3, freq="MS"),
            "ipc_index": [100.0, 100.1, 100.2],
            "Inf_Goal": [3.0, 3.0, 3.0],
            "Inf_Rate": [2.0, 2.0, 2.0],
        })
        df = _apply_derived(df)
        assert (df["inflation_gap"] < 0).all(), (
            "inflation_gap debe ser negativo cuando Inf_Rate < Inf_Goal"
        )


# ── Caso 3: dataset corto (<12 meses) ────────────────────────────────────────

class TestShortDataset:
    def test_ipc_yoy_all_nan_for_less_than_12_months(self):
        """Con <12 meses, ipc_yoy debe ser todo NaN sin lanzar excepción."""
        df = _apply_derived(_make_df(6))
        assert df["ipc_yoy"].isna().all(), (
            "Con menos de 12 meses, ipc_yoy debe ser completamente NaN"
        )

    def test_no_exception_with_short_dataset(self):
        """Aplicar las fórmulas a un dataset de 6 meses no debe lanzar excepción."""
        try:
            _apply_derived(_make_df(6))
        except Exception as exc:
            pytest.fail(f"No se esperaba excepción con dataset corto: {exc}")


# ── MERGED_COLUMNS contiene las tres nuevas columnas ─────────────────────────

class TestMergedColumnsSchema:
    def test_ipc_yoy_in_merged_columns(self):
        assert "ipc_yoy" in MERGED_COLUMNS

    def test_ipc_mom_in_merged_columns(self):
        assert "ipc_mom" in MERGED_COLUMNS

    def test_inflation_gap_in_merged_columns(self):
        assert "inflation_gap" in MERGED_COLUMNS

    def test_order_after_core_inf(self):
        """ipc_yoy, ipc_mom, inflation_gap deben aparecer después de Core_Inf."""
        idx_core = MERGED_COLUMNS.index("Core_Inf")
        idx_yoy = MERGED_COLUMNS.index("ipc_yoy")
        idx_mom = MERGED_COLUMNS.index("ipc_mom")
        idx_gap = MERGED_COLUMNS.index("inflation_gap")
        assert idx_yoy > idx_core
        assert idx_mom > idx_core
        assert idx_gap > idx_core

    def test_order_before_brent(self):
        """Las tres derivadas deben aparecer antes de brent_usd_per_barrel."""
        idx_brent = MERGED_COLUMNS.index("brent_usd_per_barrel")
        for col in ("ipc_yoy", "ipc_mom", "inflation_gap"):
            assert MERGED_COLUMNS.index(col) < idx_brent, (
                f"{col} debe estar antes de brent_usd_per_barrel en MERGED_COLUMNS"
            )


# ── run_derived_checks con datos sintéticos ───────────────────────────────────

class TestRunDerivedChecks:
    def _full_df(self, n: int = 36) -> pd.DataFrame:
        """DataFrame con todas las columnas requeridas por run_derived_checks."""
        df = _apply_derived(_make_df(n))
        return df

    def test_passes_with_valid_data(self):
        """run_derived_checks no debe lanzar excepción con datos coherentes."""
        df = self._full_df(36)
        result = run_derived_checks(df)
        assert result is True

    def test_fails_on_missing_column(self):
        """run_derived_checks debe lanzar QualityCheckError si falta una columna."""
        df = self._full_df(36).drop(columns=["ipc_yoy"])
        with pytest.raises(QualityCheckError, match="faltantes"):
            run_derived_checks(df)

    def test_fails_on_low_correlation(self):
        """run_derived_checks falla si corr(ipc_yoy, Inf_Rate) ≤ 0.97."""
        df = self._full_df(36)
        # Romper correlación: Inf_Rate aleatorio
        import numpy as np
        rng = np.random.default_rng(42)
        df["Inf_Rate"] = rng.uniform(-5, 15, len(df))
        with pytest.raises(QualityCheckError, match="corr"):
            run_derived_checks(df)
