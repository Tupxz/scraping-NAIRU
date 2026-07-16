"""Tests para el pipeline VIOG (output gap ponderado por filtros).

Clases:
  TestLoadSeries         — carga del Excel y construcción del índice
  TestApplyFilters       — cada filtro genera su columna de tendencia
  TestComputeGaps        — logaritmos y brechas
  TestComputeVIOGWeights — ponderadores VIOG y 1/VIOG
  TestRunVIOGPipeline    — pipeline completo con archivo real
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.config import INPUTS_DIR, VIOG_CONFIG
from src.sources.viog.viog import (
    apply_filters,
    compute_gaps,
    compute_viog_weights,
    compute_weighted_gap,
    load_series,
    run_viog_pipeline,
)


# ── Fixture sintético ─────────────────────────────────────────────────

def _make_excel(tmp_path: Path) -> Path:
    rng = np.random.default_rng(0)
    n = 40
    years   = [1990 + i // 4 for i in range(n)]
    quarters = [(i % 4) + 1 for i in range(n)]
    pib = 5_000.0 + np.cumsum(rng.normal(50, 10, n))
    pot = 5_000.0 + np.cumsum(rng.normal(50, 8, n))
    df = pd.DataFrame({"Year": years, "Quarter": quarters, "PIB": pib, "Potential_PIB": pot})
    path = tmp_path / "PIB_USA.xlsx"
    df.to_excel(path, index=False)
    return path


@pytest.fixture()
def synthetic_df() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    n = 120
    periods = pd.period_range("1990Q1", periods=n, freq="Q")
    pib = 5_000.0 + np.cumsum(rng.normal(50, 10, n))
    pot = 5_000.0 + np.cumsum(rng.normal(50, 8, n))
    df = pd.DataFrame({"Y": pib, "Y_ref": pot, "_series_label": "PIB"}, index=periods)
    df.index.name = "t"
    return df


@pytest.fixture()
def filtered_df(synthetic_df):
    return apply_filters(synthetic_df.copy())


@pytest.fixture()
def gaps_df(filtered_df):
    return compute_gaps(filtered_df.copy())


@pytest.fixture()
def weights_df(gaps_df):
    return compute_viog_weights(gaps_df.copy())


# ── TestLoadSeries ────────────────────────────────────────────────────

class TestLoadSeries:
    def test_loads_without_error(self, tmp_path):
        df = load_series(_make_excel(tmp_path))
        assert df is not None

    def test_renames_to_Y_and_Y_ref(self, tmp_path):
        df = load_series(_make_excel(tmp_path))
        assert "Y" in df.columns
        assert "Y_ref" in df.columns

    def test_custom_col_names(self, tmp_path):
        path = _make_excel(tmp_path)
        df = load_series(path, series_col="PIB", ref_col="Potential_PIB")
        assert "Y" in df.columns

    def test_index_is_period(self, tmp_path):
        df = load_series(_make_excel(tmp_path))
        assert isinstance(df.index, pd.PeriodIndex)
        assert df.index.freqstr == "Q-DEC"

    def test_sorted_by_date(self, tmp_path):
        df = load_series(_make_excel(tmp_path))
        assert df.index.is_monotonic_increasing

    def test_no_nulls_in_Y(self, tmp_path):
        df = load_series(_make_excel(tmp_path))
        assert df["Y"].notna().all()


# ── TestApplyFilters ──────────────────────────────────────────────────

class TestApplyFilters:
    def test_bhp_trend_no_nan(self, filtered_df):
        assert filtered_df["trend_bhp"].notna().all()

    def test_cf_trend_no_nan(self, filtered_df):
        assert filtered_df["trend_cf"].notna().all()

    def test_bw_trend_no_nan(self, filtered_df):
        assert filtered_df["trend_bw"].notna().all()

    def test_kalman_trend_no_nan(self, filtered_df):
        burnin = VIOG_CONFIG.kalman_burnin_periods
        # Las primeras `burnin` obs son NaN por diseño (condiciones iniciales del filtro)
        assert filtered_df["trend_kalman"].iloc[:burnin].isna().all()
        assert filtered_df["trend_kalman"].iloc[burnin:].notna().all()

    def test_kalman_gap_bounded(self, filtered_df):
        """Regresión: el UCM con ciclo determinístico divergía (VIOG-CO
        llegó a +170% en el transitorio inicial). Con ciclo estocástico
        amortiguado la brecha debe quedar en rango de ciclo de negocio."""
        burnin = VIOG_CONFIG.kalman_burnin_periods
        gap = np.log(filtered_df["Y"]) - np.log(filtered_df["trend_kalman"])
        assert gap.iloc[burnin:].abs().max() < 0.20, \
            f"gap_kalman máx {gap.iloc[burnin:].abs().max():.3f} — divergencia"

    def test_kalman_gap_not_degenerate(self, filtered_df):
        """Regresión: el UCM en niveles colapsaba la brecha a ~0 (VIOG-USA),
        distorsionando el ponderador 1/VIOG. La brecha debe tener variación."""
        burnin = VIOG_CONFIG.kalman_burnin_periods
        gap = np.log(filtered_df["Y"]) - np.log(filtered_df["trend_kalman"])
        assert gap.iloc[burnin:].abs().max() > 1e-5, \
            "gap_kalman degenerado (≈0 en toda la muestra)"

    def test_kalman_trend_tracks_series(self, filtered_df):
        """El potencial no puede alejarse arbitrariamente de la serie."""
        burnin = VIOG_CONFIG.kalman_burnin_periods
        ratio = filtered_df["trend_kalman"].iloc[burnin:] / filtered_df["Y"].iloc[burnin:]
        assert ratio.between(0.8, 1.25).all()

    def test_bk_has_nan_at_extremes(self, filtered_df):
        K = VIOG_CONFIG.bk_K
        assert filtered_df["trend_bk"].iloc[:K].isna().all()
        assert filtered_df["trend_bk"].iloc[-K:].isna().all()

    def test_bk_valid_in_middle(self, filtered_df):
        K = VIOG_CONFIG.bk_K
        assert filtered_df["trend_bk"].iloc[K:-K].notna().all()

    def test_trends_are_positive(self, filtered_df):
        burnin = VIOG_CONFIG.kalman_burnin_periods
        for col in ["trend_bhp", "trend_cf", "trend_bw"]:
            assert (filtered_df[col] > 0).all(), f"{col} tiene valores ≤ 0"
        # Kalman: positivo después del burn-in
        assert (filtered_df["trend_kalman"].iloc[burnin:] > 0).all(), \
            "trend_kalman tiene valores ≤ 0 fuera del burn-in"


# ── TestComputeGaps ───────────────────────────────────────────────────

class TestComputeGaps:
    def test_gap_bhp_formula(self, gaps_df):
        expected = np.log(gaps_df["Y"]) - np.log(gaps_df["trend_bhp"])
        pd.testing.assert_series_equal(gaps_df["gap_bhp"], expected, check_names=False)

    def test_gap_ref_formula(self, gaps_df):
        expected = np.log(gaps_df["Y"]) - np.log(gaps_df["Y_ref"])
        pd.testing.assert_series_equal(gaps_df["gap_ref"], expected, check_names=False)

    def test_gaps_are_numeric(self, gaps_df):
        for col in ["gap_bhp", "gap_cf", "gap_bw", "gap_kalman", "gap_ref"]:
            assert pd.api.types.is_float_dtype(gaps_df[col])

    def test_gap_bk_nan_at_extremes(self, gaps_df):
        K = VIOG_CONFIG.bk_K
        assert gaps_df["gap_bk"].iloc[:K].isna().all()

    def test_log_columns_created(self, gaps_df):
        for tag in ["bk", "cf", "bw", "bhp", "kalman"]:
            assert f"ln_trend_{tag}" in gaps_df.columns


# ── TestComputeVIOGWeights ────────────────────────────────────────────

class TestComputeVIOGWeights:
    def test_rev_weights_sum_to_one(self, weights_df):
        gap_vars = ["bk", "cf", "bw", "bhp", "kalman", "ref"]
        valid = weights_df["weight_rev_bk"].notna()
        total = sum(weights_df.loc[valid, f"weight_rev_{v}"] for v in gap_vars)
        np.testing.assert_allclose(total, 1.0, atol=1e-10)

    def test_inv_rev_weights_sum_to_one(self, weights_df):
        gap_vars = ["bk", "cf", "bw", "bhp", "kalman", "ref"]
        valid = weights_df["weight_inv_rev_bk"].notna()
        total = sum(weights_df.loc[valid, f"weight_inv_rev_{v}"] for v in gap_vars)
        np.testing.assert_allclose(total, 1.0, atol=1e-10)

    def test_rev_positive_for_non_bk(self, weights_df):
        burnin = VIOG_CONFIG.kalman_burnin_periods
        for col in ["rev_cf", "rev_bw", "rev_bhp", "rev_ref"]:
            assert (weights_df[col] > 0).all(), f"{col} tiene valores ≤ 0"
        # rev_kalman: NaN en primeras `burnin` filas, positivo después
        assert weights_df["rev_kalman"].iloc[:burnin].isna().all()
        assert (weights_df["rev_kalman"].iloc[burnin:] > 0).all(), \
            "rev_kalman tiene valores ≤ 0 fuera del burn-in"

    def test_inv_rev_positive_for_non_bk(self, weights_df):
        burnin = VIOG_CONFIG.kalman_burnin_periods
        for col in ["inv_rev_cf", "inv_rev_bw", "inv_rev_bhp", "inv_rev_ref"]:
            assert (weights_df[col] > 0).all()
        # inv_rev_kalman: NaN en primeras `burnin` filas, positivo después
        assert weights_df["inv_rev_kalman"].iloc[:burnin].isna().all()
        assert (weights_df["inv_rev_kalman"].iloc[burnin:] > 0).all()


# ── TestRunVIOGPipeline ───────────────────────────────────────────────

class TestRunVIOGPipeline:
    """Pipeline completo con archivo real PIB_USA.xlsx."""

    @pytest.fixture(scope="class")
    def pipeline_output(self, tmp_path_factory):
        tmp = tmp_path_factory.mktemp("viog_out")
        input_path = INPUTS_DIR / VIOG_CONFIG.input_filename
        output_path = tmp / VIOG_CONFIG.processed_filename
        df = run_viog_pipeline(input_path, output_path)
        return df, output_path

    def test_returns_dataframe(self, pipeline_output):
        df, _ = pipeline_output
        assert isinstance(df, pd.DataFrame)

    def test_output_columns_present(self, pipeline_output):
        df, _ = pipeline_output
        expected = ["date", "year", "quarter", "gap_viog", "gap_inv_viog",
                    "gap_ref", "gap_bhp", "gap_cf", "gap_bk", "gap_bw", "gap_kalman", "source"]
        for col in expected:
            assert col in df.columns, f"Falta columna: {col}"

    def test_gap_viog_numeric(self, pipeline_output):
        df, _ = pipeline_output
        assert pd.api.types.is_float_dtype(df["gap_viog"])

    def test_gap_inv_viog_numeric(self, pipeline_output):
        df, _ = pipeline_output
        assert pd.api.types.is_float_dtype(df["gap_inv_viog"])

    def test_csv_written(self, pipeline_output):
        _, output_path = pipeline_output
        assert output_path.exists()

    def test_row_count_matches_input(self, pipeline_output):
        df, _ = pipeline_output
        input_df = pd.read_excel(INPUTS_DIR / VIOG_CONFIG.input_filename)
        assert len(df) == len(input_df)

    def test_source_label(self, pipeline_output):
        df, _ = pipeline_output
        assert (df["source"] == VIOG_CONFIG.source_label).all()

    def test_year_column_integer(self, pipeline_output):
        df, _ = pipeline_output
        assert pd.api.types.is_integer_dtype(df["year"])

    def test_quarter_values_valid(self, pipeline_output):
        df, _ = pipeline_output
        assert df["quarter"].isin([1, 2, 3, 4]).all()


# ── TestVIOGColombiaRunner ────────────────────────────────────────────

class TestVIOGColombiaRunner:
    """Tests del orquestador VIOG-Colombia (skip elegante si falta input)."""

    def test_run_colombia_skips_when_input_missing(
        self, tmp_path, monkeypatch, caplog
    ):
        """Si data/inputs/PIB_CO.xlsx no existe, run_colombia() omite el pipeline sin fallar."""
        import logging
        from src.pipelines import run_viog as viog_pipeline

        # Apuntar INPUTS_DIR a un tmp_path vacío para simular ausencia del archivo
        monkeypatch.setattr(viog_pipeline, "INPUTS_DIR", tmp_path)
        monkeypatch.setattr(viog_pipeline, "PROCESSED_DIR", tmp_path)
        monkeypatch.setattr(viog_pipeline, "OUTPUTS_DIR", tmp_path)

        # No debe lanzar excepción
        with caplog.at_level(logging.WARNING):
            viog_pipeline.run_colombia()

        # Debe haber emitido un mensaje de log informativo
        assert any("omitido" in r.message.lower() for r in caplog.records)

    def test_viog_co_config_distinct_from_us(self):
        """VIOG_CO_CONFIG y VIOG_CONFIG difieren en filename y source label."""
        from src.config import VIOG_CO_CONFIG, VIOG_CONFIG
        assert VIOG_CO_CONFIG.input_filename != VIOG_CONFIG.input_filename
        assert VIOG_CO_CONFIG.processed_filename != VIOG_CONFIG.processed_filename
        assert VIOG_CO_CONFIG.source_label != VIOG_CONFIG.source_label
        # Pero comparten parámetros econométricos
        assert VIOG_CO_CONFIG.bk_low == VIOG_CONFIG.bk_low
        assert VIOG_CO_CONFIG.hp_lambda == VIOG_CONFIG.hp_lambda
        assert VIOG_CO_CONFIG.kalman_cycle_period_bounds == VIOG_CONFIG.kalman_cycle_period_bounds
