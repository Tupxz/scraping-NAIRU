"""Tests para src/pipelines/build_production_function_dataset.py.

Antes de 2026-09-01 este módulo no tenía NINGÚN test (confirmado por grep
en toda la auditoría de src/, 2026-08-21). Estos tests cubren uno de los
arreglos de Fase 1 del plan de limpieza (ver plan_limpieza_2026-09-01 en
la memoria del proyecto):

  _pct_change() llamaba a pd.Series.pct_change() sin fijar fill_method,
  heredando el default legacy de pandas 2.1-2.x (fill_method='pad'), que
  rellena huecos hacia adelante ANTES de calcular la variación %,
  fabricando variaciones trimestre-a-trimestre que nunca ocurrieron. Es
  el mismo bug arreglado en src/merge.py, src/quality_checks.py,
  src/sources/viog/viog.py y src/pipelines/run_pib_potencial.py (ver
  tests/test_merge_derived.py::TestIpcYoyInternalGap para el caso
  mensual). Aquí se cubre la variante trimestral usada en el dataset de
  función de producción.

Clases:
  TestPctChange — comportamiento correcto de _pct_change() y control
                  negativo confirmando que el default viejo fabricaba datos.
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from src.pipelines.build_production_function_dataset import _pct_change


# ═══════════════════════════════════════════════════════════════════════
# TestPctChange
# ═══════════════════════════════════════════════════════════════════════

class TestPctChange:
    """_pct_change() debe delegar en pct_change(fill_method=None)."""

    def test_no_gap_matches_plain_percentage_change(self) -> None:
        """Sin huecos, el resultado es la variación % trimestre a trimestre normal."""
        s = pd.Series([100.0, 105.0, 110.25, 115.7625])
        result = _pct_change(s)

        assert pd.isna(result.iloc[0])
        np.testing.assert_allclose(
            result.iloc[1:].to_numpy(dtype=float),
            [0.05, 0.05, 0.05],
            rtol=1e-9,
        )

    def test_internal_gap_propagates_as_nan_not_fabricated(self) -> None:
        """Fix 2026-09-01: un hueco interior debe propagar NaN en vez de
        fabricar una variación contra un valor rellenado hacia adelante."""
        s = pd.Series([100.0, None, 102.0, 104.04])
        result = _pct_change(s)

        assert pd.isna(result.iloc[0]), "sin dato previo -> NaN (esperado siempre)"
        assert pd.isna(result.iloc[1]), "el propio dato del trimestre falta"
        assert pd.isna(result.iloc[2]), "el trimestre anterior falta -> no hay base real"
        assert result.iloc[3] == pytest.approx(0.02), (
            "trimestre siguiente: ambos datos son reales, no debe verse afectado"
        )

    def test_no_future_warning_raised(self) -> None:
        """fill_method=None explícito no debe emitir FutureWarning (pandas 2.1-2.x)."""
        s = pd.Series([100.0, 105.0, 110.25])
        with warnings.catch_warnings():
            warnings.simplefilter("error", FutureWarning)
            _pct_change(s)  # no debe lanzar

    def test_old_pad_default_would_have_fabricated_two_values(self) -> None:
        """Control negativo: confirma que el default legacy (fill_method='pad',
        vigente antes del fix) fabricaba DOS variaciones falsas donde debía
        haber NaN, para la misma serie con un hueco interior."""
        s = pd.Series([100.0, None, 102.0, 104.04])

        with pytest.warns(FutureWarning):
            old_behavior = s.pct_change(fill_method="pad")

        # Comportamiento viejo (incorrecto): rellena 100.0 hacia adelante en
        # el hueco y calcula variación contra ese valor fabricado.
        assert old_behavior.iloc[1] == pytest.approx(0.0)
        assert old_behavior.iloc[2] == pytest.approx(0.02)

        # El fix (_pct_change) da NaN en ambas posiciones: ninguna estaba
        # respaldada por un dato real.
        fixed = _pct_change(s)
        assert pd.isna(fixed.iloc[1])
        assert pd.isna(fixed.iloc[2])
