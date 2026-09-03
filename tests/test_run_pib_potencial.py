"""Tests para src/pipelines/run_pib_potencial.py.

Antes de 2026-09-01 este módulo no tenía ningún test. Estos cubren el
ítem de Fase 1 del plan de limpieza (hallazgo #5 de la auditoría
2026-08-21, decisión del usuario 2026-09-01): K_0 (capital inicial de
estado estacionario) se ancla en 2005-Q1 y su peso decae solo con la
depreciación acumulada -- (1−δ_q)^84 ≈ 0,50 hacia 2026 -- sin ninguna
tabla que cuantifique ese riesgo. Se agregó ``k0_multiplier`` a
``_build_capital_quarterly``/``load_and_align_sources`` y una nueva
función ``compute_k0_sensitivity`` que corre el pipeline completo con
K_0 perturbado ±10 %/±20 % y mide el efecto en PIB_pot/Brecha_CD.

Clases:
  TestBuildCapitalQuarterly  — k0_multiplier escala K linealmente y decae
                               geométricamente con (1-delta_q) en el tiempo
  TestComputeK0Sensitivity   — validación de contrato (no requiere statsmodels)
                               + integración completa (requiere statsmodels,
                               solo corre si está instalado)

Nota de entorno: ``compute_k0_sensitivity`` importa (indirectamente, vía
``src.production.tfp``) ``statsmodels`` para el filtro BHP -- no
disponible en todos los entornos de desarrollo (bloqueado por PyPI 403 en
el contenedor de Cowork en la nube). Las pruebas que lo requieren usan
``pytest.importorskip("statsmodels")``; ``_build_capital_quarterly`` en
sí NO lo necesita (solo pandas/numpy), así que esas pruebas corren en
cualquier entorno.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.pipelines.run_pib_potencial import (
    K0_SENSITIVITY_MULTIPLIERS,
    _build_capital_quarterly,
    compute_k0_sensitivity,
)

try:
    import statsmodels  # noqa: F401
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False

_NEEDS_STATSMODELS = pytest.mark.skipif(
    not HAS_STATSMODELS,
    reason="requiere statsmodels (compute_tfp -> filtro BHP)",
)


# ── Helpers ──────────────────────────────────────────────────────────────

def _write_capital_sources(
    processed_dir: Path,
    n_quarters: int = 12,
    investment: float = 100.0,
    depreciation_rate: float = 0.04,
    human_capital: float = 1.30,
) -> None:
    """Escribe versiones sintéticas y mínimas de las dos fuentes que
    ``_build_capital_quarterly`` necesita: FBKF trimestral DANE (inversión
    CONSTANTE, para que g_q salga exactamente 0 y la aritmética sea
    predecible) y un único año PWT (depreciación/capital humano)."""
    dates = pd.date_range("2005-01-01", periods=n_quarters, freq="QS")
    inv_df = pd.DataFrame({
        "date": dates,
        "year": dates.year,
        "quarter": dates.quarter,
        "investment": investment,
        "source": "sintético",
        "download_date": "2026-09-01",
    })
    inv_df.to_csv(processed_dir / "dane_gdp_expenditure_colombia.csv", index=False)

    pwt_df = pd.DataFrame({
        "date": ["2005-01-01"],
        "year": [2005],
        "month": [1],
        "capital_stock_real": [1_000_000.0],
        "depreciation_rate": [depreciation_rate],
        "human_capital": [human_capital],
        "source": "sintético",
        "download_date": "2026-09-01",
    })
    pwt_df.to_csv(processed_dir / "pwt_colombia.csv", index=False)


def _expected_k0_base(investment: float, depreciation_rate: float) -> tuple[float, float]:
    """Reproduce la fórmula de K_0 (Harberger) para una inversión CONSTANTE:
    g_q=0 (pct_change de una serie constante es 0 en todos los periodos
    después del primero), i0=investment, delta_q=1-(1-delta)^0.25.
    Devuelve (k0_base, delta_q)."""
    delta_q = 1.0 - (1.0 - depreciation_rate) ** 0.25
    k0_base = investment / (0.0 + delta_q)
    return k0_base, delta_q


# ── TestBuildCapitalQuarterly ─────────────────────────────────────────────

class TestBuildCapitalQuarterly:
    """k0_multiplier debe escalar linealmente el K_0 de estado estacionario
    y esa perturbación debe decaer geométricamente con (1-delta_q), igual
    que cualquier otro término de la recursión del inventario permanente."""

    def test_default_multiplier_is_1_0(self, tmp_path):
        _write_capital_sources(tmp_path)
        sin_arg = _build_capital_quarterly(tmp_path)
        explicito = _build_capital_quarterly(tmp_path, k0_multiplier=1.0)
        pd.testing.assert_series_equal(sin_arg["K"], explicito["K"])

    def test_first_period_scales_exactly_with_multiplier(self, tmp_path):
        investment, depreciation_rate = 100.0, 0.04
        _write_capital_sources(tmp_path, investment=investment, depreciation_rate=depreciation_rate)
        k0_base, delta_q = _expected_k0_base(investment, depreciation_rate)

        base = _build_capital_quarterly(tmp_path, k0_multiplier=1.0)
        alto = _build_capital_quarterly(tmp_path, k0_multiplier=1.2)

        k_primero_esperado_base = k0_base * (1.0 - delta_q) + investment
        k_primero_esperado_alto = (k0_base * 1.2) * (1.0 - delta_q) + investment

        assert base["K"].iloc[0] == pytest.approx(k_primero_esperado_base)
        assert alto["K"].iloc[0] == pytest.approx(k_primero_esperado_alto)
        # Y por lo tanto NO es un +20% plano en el primer periodo reportado
        # (ya sufrió un paso de depreciación + dilución por la inversión).
        pct_real = (alto["K"].iloc[0] / base["K"].iloc[0] - 1.0) * 100.0
        assert 0.0 < pct_real < 20.0

    def test_perturbation_decays_geometrically_with_1_minus_delta_q(self, tmp_path):
        # La inversión es IDÉNTICA entre corridas (no depende de k0_multiplier),
        # así que la diferencia K_alto - K_base en cada periodo es simplemente
        # la del periodo anterior multiplicada por (1-delta_q) -- I_t se cancela
        # en la resta porque es el mismo en ambas corridas.
        investment, depreciation_rate = 100.0, 0.04
        _write_capital_sources(tmp_path, n_quarters=8, investment=investment,
                                depreciation_rate=depreciation_rate)
        _, delta_q = _expected_k0_base(investment, depreciation_rate)

        base = _build_capital_quarterly(tmp_path, k0_multiplier=1.0)
        alto = _build_capital_quarterly(tmp_path, k0_multiplier=1.2)
        diffs = (alto["K"] - base["K"]).to_numpy()

        ratios = diffs[1:] / diffs[:-1]
        np.testing.assert_allclose(ratios, 1.0 - delta_q, rtol=1e-9)

    def test_multiplier_1_0_is_a_true_no_op_regardless_of_value(self, tmp_path):
        _write_capital_sources(tmp_path)
        r1 = _build_capital_quarterly(tmp_path, k0_multiplier=1.0)
        r2 = _build_capital_quarterly(tmp_path, k0_multiplier=1.0)
        pd.testing.assert_frame_equal(r1, r2)


# ── TestComputeK0Sensitivity ───────────────────────────────────────────────

class TestComputeK0Sensitivity:
    """compute_k0_sensitivity: contrato de la función + integración completa."""

    def test_requires_1_0_in_multipliers(self):
        # No necesita statsmodels: el ValueError se lanza antes de tocar
        # nada del pipeline (primera línea de la función).
        with pytest.raises(ValueError, match="1.0"):
            compute_k0_sensitivity(multipliers=(0.9, 1.1))

    def test_default_multipliers_include_1_0(self):
        assert 1.0 in K0_SENSITIVITY_MULTIPLIERS

    @_NEEDS_STATSMODELS
    def test_delta_columns_are_zero_at_baseline_multiplier(self):
        # Integración completa con los datos reales del repo (requiere
        # statsmodels -- solo corre si está instalado, p.ej. en el Mac).
        # El trimestre más reciente puede venir con PIB/insumos incompletos
        # (NaN, no fabricado -- ver Fase 1 de este mismo plan de limpieza)
        # y por lo tanto también NaN en las columnas delta_*; se excluye
        # con dropna, no es lo que este test verifica.
        sens = compute_k0_sensitivity()
        base = sens[sens["k0_multiplier"] == 1.0].dropna(subset=["delta_K_pot_pct"])
        assert len(base) > 0
        assert (base["delta_K_pot_pct"] == 0.0).all()
        assert (base["delta_PIB_pot_pct"] == 0.0).all()
        assert (base["delta_Brecha_CD_pp"] == 0.0).all()

    @_NEEDS_STATSMODELS
    def test_larger_k0_error_has_smaller_effect_far_from_2005(self):
        # Verificación de la propiedad central del hallazgo: el efecto de
        # una perturbación de K_0 sobre Brecha_CD debe DECAER con el tiempo
        # (la depreciación acumulada diluye el error de K_0 -- ver también
        # TestBuildCapitalQuarterly.test_perturbation_decays_geometrically).
        sens = compute_k0_sensitivity()
        extremo = (
            sens[sens["k0_multiplier"] == sens["k0_multiplier"].max()]
            .dropna(subset=["delta_Brecha_CD_pp"])
            .sort_values("date")
        )
        efecto_abs = extremo["delta_Brecha_CD_pp"].abs().to_numpy()
        # No monótono estricto (el BHP puede introducir ruido de alta
        # frecuencia), pero el efecto cerca del final debe ser claramente
        # menor que al principio de la serie.
        assert efecto_abs[-5:].mean() < efecto_abs[:5].mean()
