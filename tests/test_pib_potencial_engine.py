"""Tests para el motor de PIB Potencial alineado con v3 (2026-09-10).

Ver docs/integracion_v3.md para el mapeo completo de la metodología. Cubre:

  TestBusinessCycle          — algoritmo BBQ y variables de ciclo (rampas-meseta)
  TestFactorsUtils           — quarter_label, jornada_legal (Ley 2101/2021)
  TestCalcularMercadoLaboral — TGP, TD, brechas de desempleo/capital
  TestCalcularCapitalDane    — interpolación PCHIP del stock DANE
  TestComputePibPotencial    — fórmulas de PIB potencial y brechas (sintético)
  TestAgainstV3Baseline      — regresión numérica contra la línea base congelada
                                de legacy/pib_potencial_integrado_v3.py
                                (tests/fixtures/baseline_v3/), corriendo el
                                pipeline REAL del repo contra los datos REALES.
                                Se salta si faltan datos/paquetes (statsmodels,
                                scipy) — es un test de integración, no unitario.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.production import business_cycle, factors

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "baseline_v3"
REPO_ROOT = Path(__file__).parent.parent


# ── TestBusinessCycle ───────────────────────────────────────────────────────

class TestBusinessCycle:
    def test_bbq_detects_a_single_clean_peak(self):
        # Serie con un pico claro en t=10 (sube, baja), suficientemente
        # larga para pasar los filtros de fase/ciclo mínimos (BBQ_MIN_PHASE=2,
        # BBQ_MIN_CYCLE=5).
        n = 24
        t = np.arange(n)
        y = -((t - 10) ** 2).astype(float)  # máximo único en t=10
        turns = business_cycle.bbq_turning_points(y)
        picos = [i for i, tipo in turns if tipo == "P"]
        assert picos == [10]

    def test_bbq_ignores_extrema_too_close_to_the_edges(self):
        # Un extremo en los primeros/últimos BBQ_K_WINDOW puntos se censura
        # (no hay margen para confirmarlo como giro real).
        n = 10
        y = np.zeros(n)
        y[0] = 100.0  # candidato a pico, pero en el borde
        turns = business_cycle.bbq_turning_points(y)
        assert 0 not in [i for i, _ in turns]

    def test_construir_variables_ciclo_ramp_then_plateau(self):
        dates = pd.date_range("2020-01-01", periods=8, freq="QS")
        # Un solo pico en la 3ra fecha (idx=2): quarters_elapsed(d, pico) es
        # NEGATIVO antes del pico (se recorta a 0) y POSITIVO después -- la
        # rampa representa la expansión ACUMULADA DESDE el pico, no hacia él.
        # Al ser el único/último pico, no hay meseta: sigue creciendo sin cota.
        picos = [dates[2]]
        out = business_cycle.construir_variables_ciclo(dates, picos)
        assert list(out.columns) == ["ciclo_1"]
        vals = out["ciclo_1"].to_numpy()
        # Antes/en el pico (índices 0-2): recortado a 0 (quarters_elapsed<=0)
        np.testing.assert_allclose(vals[:3], [0.0, 0.0, 0.0])
        # Después del pico: crece 0.25 por trimestre, sin meseta (único pico)
        np.testing.assert_allclose(vals[3:], 0.25 * np.arange(1, 6))
        assert np.all(np.diff(vals[2:]) >= 0)  # no decrece nunca desde el pico

    def test_construir_variables_ciclo_freezes_at_next_peak(self):
        dates = pd.date_range("2020-01-01", periods=8, freq="QS")
        picos = [dates[1], dates[4]]  # dos picos: el primero SÍ tiene meseta
        out = business_cycle.construir_variables_ciclo(dates, picos)
        c1 = out["ciclo_1"].to_numpy()
        # Tras el segundo pico (índice >= 4), ciclo_1 debe quedar congelado
        # (meseta) en 0.25 * quarters_elapsed(picos[1], picos[0]) = 0.25*3=0.75
        assert c1[4] == pytest.approx(0.75)
        assert c1[5] == pytest.approx(0.75)
        assert c1[7] == pytest.approx(0.75)

    def test_detect_bbq_peaks_wrapper_matches_raw_algorithm(self):
        n = 24
        t = np.arange(n)
        y = -((t - 10) ** 2).astype(float)
        dates = pd.date_range("2005-01-01", periods=n, freq="QS")
        df = pd.DataFrame({"date": dates, "V_pib": np.exp(y)})
        picos = business_cycle.detect_bbq_peaks(df)
        assert picos == [dates[10]]


# ── TestFactorsUtils ─────────────────────────────────────────────────────────

class TestFactorsUtils:
    @pytest.mark.parametrize("date_str,expected", [
        ("2020-01-15", "2020-03-01"),
        ("2020-03-01", "2020-03-01"),
        ("2020-04-01", "2020-06-01"),
        ("2020-12-31", "2020-12-01"),
    ])
    def test_quarter_label(self, date_str, expected):
        got = factors.quarter_label(pd.Timestamp(date_str))
        assert got == pd.Timestamp(expected)

    @pytest.mark.parametrize("date_str,expected_horas", [
        ("2023-01-01", 48),   # antes de la Ley 2101 (jornada base)
        ("2023-09-01", 47),
        ("2024-08-31", 47),
        ("2024-09-01", 46),
        ("2025-09-01", 44),
        ("2026-09-01", 42),
        ("2030-01-01", 42),   # se mantiene en el último escalón
    ])
    def test_jornada_legal(self, date_str, expected_horas):
        assert factors.jornada_legal(pd.Timestamp(date_str)) == expected_horas


# ── TestCalcularMercadoLaboral ───────────────────────────────────────────────

class TestCalcularMercadoLaboral:
    def test_tgp_td_and_gaps_formulas(self):
        df = pd.DataFrame({
            "date": pd.date_range("2020-01-01", periods=4, freq="QS"),
            "pet":  [100.0, 100.0, 100.0, 100.0],
            "fl":   [60.0, 62.0, 61.0, 63.0],
            "ocup": [54.0, 55.0, 56.0, 57.0],
            "nairu": [9.0, 9.0, 9.0, 9.0],
            "icu":  [70.0, 71.0, 72.0, 73.0],
            "naicu": [75.0, 75.0, 75.0, 75.0],
        })
        out = factors.calcular_mercado_laboral(df, ma_window=2)
        np.testing.assert_allclose(out["tgp"], 100 * df["fl"] / df["pet"])
        np.testing.assert_allclose(out["td"], 100 * (df["fl"] - df["ocup"]) / df["fl"])
        np.testing.assert_allclose(out["brecha_u"], out["td"] - df["nairu"])
        np.testing.assert_allclose(out["brecha_icu"], df["icu"] - df["naicu"])
        # media móvil de ventana 2: primer valor NaN, resto = promedio de a pares
        assert pd.isna(out["ma_brecha_u"].iloc[0])
        expected_ma1 = (out["brecha_u"].iloc[0] + out["brecha_u"].iloc[1]) / 2
        assert out["ma_brecha_u"].iloc[1] == pytest.approx(expected_ma1)


# ── TestCalcularCapitalDane ──────────────────────────────────────────────────

class TestCalcularCapitalDane:
    def test_quarterly_average_reproduces_annual_level_away_from_edges(self):
        # Serie anual con crecimiento suave (exponencial) para que la
        # interpolación PCHIP sea casi lineal en el interior de la muestra.
        years = np.arange(2000, 2011)
        K = 1000.0 * (1.03 ** (years - 2000))
        dane_capital = pd.DataFrame({"year": years, "K_prod_mmp": K})

        # Trimestres de un año "interior" (lejos de los bordes de la muestra,
        # donde el spline PCHIP es más fiel a una interpolación suave)
        dates = pd.date_range("2005-01-01", periods=4, freq="QS").map(factors.quarter_label)
        df = pd.DataFrame({"date": dates})
        out = factors.calcular_capital_dane(df, dane_capital)

        promedio_trimestral = out["K"].mean()
        nivel_anual = K[years == 2005][0]
        assert promedio_trimestral == pytest.approx(nivel_anual, rel=1e-3)

    def test_extrapolates_beyond_last_observed_year_with_positive_growth(self):
        years = np.arange(2000, 2011)
        K = 1000.0 * (1.03 ** (years - 2000))
        dane_capital = pd.DataFrame({"year": years, "K_prod_mmp": K})

        dates = pd.date_range("2015-01-01", periods=4, freq="QS").map(factors.quarter_label)
        df = pd.DataFrame({"date": dates})
        out = factors.calcular_capital_dane(df, dane_capital)

        assert out["K"].notna().all()
        assert out["K"].mean() > K[-1]  # sigue creciendo más allá de 2010


# ── TestComputePibPotencial (sintético) ──────────────────────────────────────

class TestComputePibPotencial:
    def test_brecha_cd_matches_idx_pib_over_pib_pot_formula(self):
        from src.production.pib_potencial import compute_pib_potencial

        base_quarter = pd.Timestamp("2010-03-01")
        T = pd.Timestamp("2012-12-01")
        # freq="3MS" (no "QS"): avanza exactamente 3 meses desde base_quarter,
        # manteniendo la convención de etiqueta de v3 (día 1 del mes final del
        # trimestre: mar/jun/sep/dic) en vez de reanclar a inicios de trimestre
        # calendario como haría "QS".
        dates = pd.date_range(base_quarter, T, freq="3MS")
        n = len(dates)
        rng = np.random.default_rng(0)

        df = pd.DataFrame({
            "date": dates,
            "idx_pib":    100 * (1.01 ** np.arange(n)),
            "idx_K_star": 100 * (1.005 ** np.arange(n)),
            "idx_LH_star": 100 * (1.002 ** np.arange(n)),
            "alpha": 0.45,
            "ptf_star": 1.0 + 0.001 * np.arange(n),
            "ptf_hp":   1.0 + 0.0012 * np.arange(n),
            "W_pib_ann": 500_000 * (1.01 ** np.arange(n)),
        })
        out = compute_pib_potencial(df, T=T, base_quarter=base_quarter)

        expected_pib_pot = out["ptf_star"] * (out["idx_K_star"] ** 0.45) * (out["idx_LH_star"] ** 0.55)
        np.testing.assert_allclose(out["pib_pot"], expected_pib_pot, rtol=1e-12)

        expected_brecha_cd = 100.0 * (out["idx_pib"] / expected_pib_pot - 1.0)
        np.testing.assert_allclose(out["Brecha_CD"], expected_brecha_cd, rtol=1e-12)

        # Alias de compatibilidad
        np.testing.assert_allclose(out["PIB_pot"], out["pib_pot"])
        np.testing.assert_allclose(out["Brecha_CD"], 100.0 * out["brecha_pot"])

    def test_raises_on_missing_columns(self):
        from src.production.pib_potencial import compute_pib_potencial
        df = pd.DataFrame({"date": [pd.Timestamp("2020-01-01")], "idx_pib": [100.0]})
        with pytest.raises(KeyError):
            compute_pib_potencial(df, T=pd.Timestamp("2020-01-01"), base_quarter=pd.Timestamp("2020-01-01"))


# ── TestAgainstV3Baseline ─────────────────────────────────────────────────────

def _fixture_available() -> bool:
    return (FIXTURE_DIR / "v3_CONFIG_V2_baseline_series.csv").exists()


def _real_data_available() -> bool:
    required = [
        REPO_ROOT / "data" / "processed" / "dane_gdp_colombia.csv",
        REPO_ROOT / "data" / "processed" / "dane_labor_colombia.csv",
        REPO_ROOT / "data" / "processed" / "pwt_colombia.csv",
        REPO_ROOT / "data" / "processed" / "dane_gdp_income_colombia.csv",
        REPO_ROOT / "outputs" / "nairu" / "nairu_colombia.csv",
        REPO_ROOT / "data" / "inputs" / "alt_capital" / "dane_stock_capital_productivo.csv",
        REPO_ROOT / "data" / "inputs" / "festivos_efectivos_colombia_2001_2026.xlsx",
    ]
    return all(p.exists() for p in required)


@pytest.mark.skipif(not _fixture_available(), reason="falta tests/fixtures/baseline_v3/ (línea base congelada)")
@pytest.mark.skipif(not _real_data_available(), reason="faltan datos reales de data/processed, outputs/nairu o data/inputs")
class TestAgainstV3Baseline:
    """Compara la salida del pipeline REAL del repo contra la línea base
    congelada de v3 (Fase 0, ver docs/integracion_v3.md). No es un test
    unitario: corre el pipeline completo contra los datos reales del repo,
    así que su resultado depende de qué datos haya descargados en el momento
    -- se salta automáticamente si faltan.

    Tolerancia: rtol=1e-6 (no 1e-12) porque las dos corridas parten de rutas
    de código distintas (v3 monolítico vs. motor modular) que pueden diferir
    en el ORDEN de operaciones de punto flotante -- ya verificado que en la
    práctica concuerdan a ~1e-13, ver docs/integracion_v3.md.
    """

    @classmethod
    @pytest.fixture(scope="class")
    def resultado(cls):
        pytest.importorskip("statsmodels")
        pytest.importorskip("scipy")
        from src.pipelines import run_pib_potencial
        return run_pib_potencial.run()

    @classmethod
    @pytest.fixture(scope="class")
    def baseline(cls):
        return pd.read_csv(FIXTURE_DIR / "v3_CONFIG_V2_baseline_series.csv", parse_dates=["date"])

    @pytest.mark.parametrize("col", [
        "idx_pib", "idx_K", "idx_K_star", "idx_L", "idx_L_star",
        "idx_LH", "idx_LH_star", "idx_hc", "alpha", "alpha_t",
        "ptf", "ptf_star", "pib_pot", "brecha_pot", "tgp", "tgp_star",
        "brecha_u", "brecha_icu", "idx_trend", "brecha_hp",
    ])
    def test_column_matches_baseline(self, resultado, baseline, col):
        m = resultado[["date", col]].merge(
            baseline[["date", col]], on="date", suffixes=("_new", "_base"),
        )
        a = pd.to_numeric(m[f"{col}_new"], errors="coerce")
        b = pd.to_numeric(m[f"{col}_base"], errors="coerce")
        both = a.notna() & b.notna()
        # alpha_t solo está definida en la ventana de estimación de alpha
        # (2016-Q1 -> T, ~41 trimestres), mucho más corta que el resto de las
        # columnas (definidas desde BASE_QUARTER, ~74-75 trimestres).
        min_esperado = 30 if col == "alpha_t" else 50
        assert both.sum() > min_esperado, f"muy pocos valores comparables para {col} ({both.sum()})"
        np.testing.assert_allclose(a[both], b[both], rtol=1e-6, atol=1e-9)

    def test_picos_bbq_match(self, resultado):
        from src.production.business_cycle import detect_bbq_peaks
        # Recalcular directamente sobre el df de salida no es posible (V_pib
        # anualizado ya no está en cols de salida crudas simples), así que
        # verificamos contra los picos conocidos del Fase 0 (ver
        # docs/integracion_v3.md): 2019-12-01 y 2023-03-01.
        picos_esperados = [pd.Timestamp("2019-12-01"), pd.Timestamp("2023-03-01")]
        from src.pipelines.run_pib_potencial import _load_quarterly_national_accounts
        from src.config import PROCESSED_DIR
        quarterly = _load_quarterly_national_accounts(PROCESSED_DIR)
        picos = detect_bbq_peaks(quarterly)
        assert picos == picos_esperados
