"""Tests del meta.json del tablero (export_web_data).

Regresión del bug 2026-06: ``latest_brecha_cd`` salía NaN porque el último
trimestre tiene PIB observado pero aún no potencial Cobb-Douglas (FBKF
rezagado). El meta debe usar el último valor *válido* y serializar JSON
válido (nunca el literal ``NaN``).
"""

import json
import math

import numpy as np
import pandas as pd
import pytest

from src.pipelines.export_web_data import _last_valid


class TestLastValid:
    def test_ultimo_valor_cuando_no_hay_nan(self):
        df = pd.DataFrame({"brecha_cd": [1.0, 2.0, 3.456]})
        assert _last_valid(df, "brecha_cd") == 3.46

    def test_salta_nan_final(self):
        """Caso real: último trimestre sin potencial CD → usar el anterior."""
        df = pd.DataFrame({"brecha_cd": [1.0, 0.4596, np.nan]})
        assert _last_valid(df, "brecha_cd") == 0.46

    def test_columna_toda_nan_devuelve_none(self):
        df = pd.DataFrame({"brecha_cd": [np.nan, np.nan]})
        assert _last_valid(df, "brecha_cd") is None

    def test_columna_ausente_devuelve_none(self):
        df = pd.DataFrame({"otra": [1.0]})
        assert _last_valid(df, "brecha_cd") is None

    def test_df_none_devuelve_none(self):
        assert _last_valid(None, "brecha_cd") is None

    def test_valores_no_numericos_se_ignoran(self):
        df = pd.DataFrame({"brecha_cd": ["1.5", "texto", ""]})
        assert _last_valid(df, "brecha_cd") == 1.5

    def test_resultado_serializa_json_valido(self):
        """None → null; nunca debe emitirse el literal NaN."""
        df = pd.DataFrame({"x": [np.nan]})
        meta = {"latest_brecha_cd": _last_valid(df, "x")}
        texto = json.dumps(meta)
        assert "NaN" not in texto
        assert json.loads(texto)["latest_brecha_cd"] is None

    def test_float_devuelto_no_es_nan(self):
        df = pd.DataFrame({"x": [2.0, np.nan, 5.0, np.nan, np.nan]})
        v = _last_valid(df, "x")
        assert v == 5.0 and not math.isnan(v)


class TestMetaJsonPublicado:
    """El meta.json versionado en docs/data debe ser JSON válido y coherente."""

    @pytest.fixture()
    def docs_data(self):
        from pathlib import Path

        d = Path(__file__).resolve().parents[1] / "docs" / "data"
        if not (d / "meta.json").exists():
            pytest.skip("docs/data/meta.json no existe en este checkout")
        return d

    def test_meta_es_json_valido(self, docs_data):
        meta = json.loads((docs_data / "meta.json").read_text(encoding="utf-8"))
        assert "last_updated" in meta

    def test_brecha_cd_coherente_con_csv(self, docs_data):
        meta = json.loads((docs_data / "meta.json").read_text(encoding="utf-8"))
        pib = pd.read_csv(docs_data / "pib_trimestral.csv")
        esperado = _last_valid(pib, "brecha_cd")
        assert meta.get("latest_brecha_cd") == esperado
