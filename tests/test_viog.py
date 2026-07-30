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

from src.config import INPUTS_DIR, VIOG_CONFIG, VIOGConfig
from src.sources.viog.viog import (
    apply_filters,
    cf_filter_one_sided,
    compute_gaps,
    compute_viog_weights,
    compute_weighted_gap,
    load_series,
    run_viog_pipeline,
)

# Warm-up del CF de una cola: primeras (CF_WARMUP−1) obs de trend_cf son NaN.
CF_WARMUP = VIOG_CONFIG.cf_min_obs or 2 * VIOG_CONFIG.cf_high


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

    def test_cf_trend_warmup_then_valid(self, filtered_df):
        """CF de una cola: NaN durante el warm-up, válido después.

        Mismo patrón que los extremos de BK y el burn-in del Kalman: durante
        el warm-up el peso VIOG del CF es 0 y los demás filtros se
        renormalizan. (Antes trend_cf era de dos colas y no tenía NaN.)"""
        assert filtered_df["trend_cf"].iloc[: CF_WARMUP - 1].isna().all()
        assert filtered_df["trend_cf"].iloc[CF_WARMUP - 1:].notna().all()

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
        for col in ["trend_bhp", "trend_bw"]:
            assert (filtered_df[col] > 0).all(), f"{col} tiene valores ≤ 0"
        # CF: positivo después del warm-up (una cola)
        assert (filtered_df["trend_cf"].iloc[CF_WARMUP - 1:] > 0).all(), \
            "trend_cf tiene valores ≤ 0 fuera del warm-up"
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
        for col in ["rev_bw", "rev_bhp", "rev_ref"]:
            assert (weights_df[col] > 0).all(), f"{col} tiene valores ≤ 0"
        # rev_cf: NaN durante el warm-up del CF de una cola, positivo después
        assert weights_df["rev_cf"].iloc[: CF_WARMUP - 1].isna().all()
        assert (weights_df["rev_cf"].iloc[CF_WARMUP - 1:] > 0).all(), \
            "rev_cf tiene valores ≤ 0 fuera del warm-up"
        # rev_kalman: NaN en primeras `burnin` filas, positivo después
        assert weights_df["rev_kalman"].iloc[:burnin].isna().all()
        assert (weights_df["rev_kalman"].iloc[burnin:] > 0).all(), \
            "rev_kalman tiene valores ≤ 0 fuera del burn-in"

    def test_inv_rev_positive_for_non_bk(self, weights_df):
        burnin = VIOG_CONFIG.kalman_burnin_periods
        for col in ["inv_rev_bw", "inv_rev_bhp", "inv_rev_ref"]:
            assert (weights_df[col] > 0).all()
        # inv_rev_cf: NaN durante el warm-up, positivo después
        assert weights_df["inv_rev_cf"].iloc[: CF_WARMUP - 1].isna().all()
        assert (weights_df["inv_rev_cf"].iloc[CF_WARMUP - 1:] > 0).all()
        # inv_rev_kalman: NaN en primeras `burnin` filas, positivo después
        assert weights_df["inv_rev_kalman"].iloc[:burnin].isna().all()
        assert (weights_df["inv_rev_kalman"].iloc[burnin:] > 0).all()


# ── TestCFOneSided ────────────────────────────────────────────────────

class TestCFOneSided:
    """Propiedades del filtro Christiano-Fitzgerald de una cola (causal).

    La propiedad definitoria (test_causalidad_muestra_expansiva): el valor
    de trend_cf en t calculado con la muestra completa y[:T] debe COINCIDIR
    con el calculado usando solo y[:t+1]. Si agregar datos futuros cambia un
    valor pasado, el filtro no es de una cola.
    """

    @staticmethod
    def _serie(n: int = 160, seed: int = 7) -> np.ndarray:
        """Serie sintética tipo log-PIB en niveles: RW con drift + ciclo AR(2)."""
        rng = np.random.default_rng(seed)
        cyc = np.zeros(n)
        for t in range(2, n):
            cyc[t] = 1.5 * cyc[t - 1] - 0.6 * cyc[t - 2] + rng.normal(0, 40)
        return 5_000.0 + np.cumsum(rng.normal(50, 10, n)) + cyc

    def test_causalidad_muestra_expansiva(self):
        """trend_cf sobre y[:T] == trend_cf sobre y[:t+1] para varios t < T."""
        y = self._serie()
        n = len(y)
        _, trend_full = cf_filter_one_sided(y, low=VIOG_CONFIG.cf_low,
                                            high=VIOG_CONFIG.cf_high)
        for t in [CF_WARMUP + 2, 90, 110, 130, n - 2]:
            _, trend_sub = cf_filter_one_sided(y[: t + 1], low=VIOG_CONFIG.cf_low,
                                               high=VIOG_CONFIG.cf_high)
            np.testing.assert_allclose(
                trend_sub, trend_full[: t + 1], rtol=1e-12, atol=1e-8,
                err_msg=f"Agregar datos después de t={t} revisó valores pasados "
                        f"→ el filtro NO es de una cola",
            )

    def test_equivalencia_cffilter_expansivo(self):
        """Ruta analítica (fórmula de borde CF 2003) == ruta pragmática
        (statsmodels cffilter sobre y[:t+1], último valor)."""
        from statsmodels.tsa.filters.cf_filter import cffilter

        y = self._serie()
        cycle_1s, trend_1s = cf_filter_one_sided(
            y, low=VIOG_CONFIG.cf_low, high=VIOG_CONFIG.cf_high
        )
        for t in [CF_WARMUP - 1, CF_WARMUP + 10, 100, 125, len(y) - 1]:
            cyc_sub, tr_sub = cffilter(
                y[: t + 1], low=VIOG_CONFIG.cf_low, high=VIOG_CONFIG.cf_high,
                drift=False,
            )
            assert np.isclose(cycle_1s[t], np.asarray(cyc_sub)[-1],
                              rtol=1e-10, atol=1e-8)
            assert np.isclose(trend_1s[t], np.asarray(tr_sub)[-1],
                              rtol=1e-10, atol=1e-8)

    def test_apply_filters_usa_una_cola(self, filtered_df, synthetic_df):
        """apply_filters (cf_one_sided=True default) cablea cf_filter_one_sided."""
        _, trend = cf_filter_one_sided(
            synthetic_df["Y"].to_numpy(dtype=float),
            low=VIOG_CONFIG.cf_low, high=VIOG_CONFIG.cf_high,
            min_obs=VIOG_CONFIG.cf_min_obs,
        )
        np.testing.assert_allclose(
            filtered_df["trend_cf"].to_numpy(), trend, rtol=1e-12, atol=1e-8
        )

    def test_flag_false_reproduce_dos_colas(self, synthetic_df):
        """cf_one_sided=False reproduce la versión anterior (cffilter, dos
        colas, sin NaN) — para comparación/depuración."""
        from statsmodels.tsa.filters.cf_filter import cffilter

        cfg_legacy = VIOGConfig(cf_one_sided=False)
        df = apply_filters(synthetic_df.copy(), cfg=cfg_legacy)
        assert df["trend_cf"].notna().all()
        _, tr_2s = cffilter(
            synthetic_df["Y"].astype(float),
            low=cfg_legacy.cf_low, high=cfg_legacy.cf_high, drift=False,
        )
        np.testing.assert_allclose(
            df["trend_cf"].to_numpy(), np.asarray(tr_2s), rtol=1e-12, atol=1e-8
        )

    def test_warmup_configurable(self):
        """cf_min_obs se respeta: NaN antes, válido después."""
        y = self._serie(n=80)
        _, trend = cf_filter_one_sided(y, low=6, high=32, min_obs=20)
        assert np.isnan(trend[:19]).all()
        assert np.isfinite(trend[19:]).all()


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
