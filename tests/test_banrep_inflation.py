"""Tests para el pipeline de inflación BANREP/SUAMECA.

Verifica:
- Parseo de datos epoch_ms → date mensual
- Parsing de series individuales
- Merge de las 3 series (Inf_Goal, Inf_Rate, Core_Inf)
- Manejo de Inf_Goal anual expandida a meses
- Esquema final y orden temporal
- Validaciones de calidad
- Manejo de datos faltantes y series parciales
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from src.config import BANREP_PROCESSED_COLUMNS, BanrepInflationConfig
from src.sources.banrep.inflation import (
    clean_banrep_data,
    epoch_ms_to_date,
    merge_series,
    parse_series_data,
)
from src.quality_checks import (
    QualityCheckError,
    run_banrep_checks,
)


# ═══════════════════════════════════════════════════════════════════════
# Fixtures JSON que simulan la respuesta de SUAMECA
# ═══════════════════════════════════════════════════════════════════════

# Epoch milisegundos para fechas de prueba (end-of-month, como SUAMECA)
# 2023-01-31 = 1675123200000 (UTC)
# 2023-02-28 = 1677542400000
# 2023-03-31 = 1680220800000
# 2023-04-30 = 1682812800000
# 2023-05-31 = 1685491200000
# 2023-06-30 = 1688083200000

EPOCH_2023_01 = 1675123200000
EPOCH_2023_02 = 1677542400000
EPOCH_2023_03 = 1680220800000
EPOCH_2023_04 = 1682812800000
EPOCH_2023_05 = 1685491200000
EPOCH_2023_06 = 1688083200000

# 2024-01-31 = 1706659200000
# 2024-02-29 = 1709164800000
EPOCH_2024_01 = 1706659200000
EPOCH_2024_02 = 1709164800000


def _make_sample_raw() -> dict[str, list[list]]:
    """Crea datos crudos simulados para las 3 series (6 meses de 2023)."""
    return {
        "Inf_Goal": [
            [EPOCH_2023_01, 3.0],
            [EPOCH_2023_02, 3.0],
            [EPOCH_2023_03, 3.0],
            [EPOCH_2023_04, 3.0],
            [EPOCH_2023_05, 3.0],
            [EPOCH_2023_06, 3.0],
        ],
        "Inf_Rate": [
            [EPOCH_2023_01, 13.25],
            [EPOCH_2023_02, 13.28],
            [EPOCH_2023_03, 13.34],
            [EPOCH_2023_04, 12.82],
            [EPOCH_2023_05, 12.36],
            [EPOCH_2023_06, 12.13],
        ],
        "Core_Inf": [
            [EPOCH_2023_01, 11.41],
            [EPOCH_2023_02, 11.26],
            [EPOCH_2023_03, 11.44],
            [EPOCH_2023_04, 11.38],
            [EPOCH_2023_05, 11.04],
            [EPOCH_2023_06, 10.51],
        ],
    }


def _make_multi_year_raw() -> dict[str, list[list]]:
    """Datos crudos con 2 años de meta y datos mensuales parciales."""
    return {
        "Inf_Goal": [
            [EPOCH_2023_01, 3.0],
            [EPOCH_2023_02, 3.0],
            [EPOCH_2024_01, 3.0],
            [EPOCH_2024_02, 3.0],
        ],
        "Inf_Rate": [
            [EPOCH_2023_01, 13.25],
            [EPOCH_2023_02, 13.28],
            [EPOCH_2024_01, 7.50],
            [EPOCH_2024_02, 7.30],
        ],
        "Core_Inf": [
            [EPOCH_2023_01, 11.41],
            [EPOCH_2023_02, 11.26],
            [EPOCH_2024_01, 6.80],
            [EPOCH_2024_02, 6.50],
        ],
    }


# ═══════════════════════════════════════════════════════════════════════
# Tests de parseo epoch → date
# ═══════════════════════════════════════════════════════════════════════


class TestEpochConversion:
    """Tests de conversión epoch_ms → date."""

    def test_epoch_to_date_jan_2023(self) -> None:
        """2023-01-31 epoch → date(2023, 1, 1)."""
        d = epoch_ms_to_date(EPOCH_2023_01)
        assert d == date(2023, 1, 1)

    def test_epoch_to_date_feb_2023(self) -> None:
        """2023-02-28 epoch → date(2023, 2, 1)."""
        d = epoch_ms_to_date(EPOCH_2023_02)
        assert d == date(2023, 2, 1)

    def test_epoch_to_date_normalizes_to_first(self) -> None:
        """Siempre normaliza al día 1 del mes."""
        d = epoch_ms_to_date(EPOCH_2023_06)
        assert d.day == 1
        assert d.month == 6
        assert d.year == 2023

    def test_epoch_to_date_2024_leap(self) -> None:
        """Feb 2024 (bisiesto) se normaliza correctamente."""
        d = epoch_ms_to_date(EPOCH_2024_02)
        assert d == date(2024, 2, 1)


# ═══════════════════════════════════════════════════════════════════════
# Tests de parsing de series individuales
# ═══════════════════════════════════════════════════════════════════════


class TestParseSeriesData:
    """Tests de parse_series_data (lista de [epoch, valor] → DataFrame)."""

    def test_basic_parsing(self) -> None:
        """Parsea datos correctamente."""
        raw = [
            [EPOCH_2023_01, 13.25],
            [EPOCH_2023_02, 13.28],
            [EPOCH_2023_03, 13.34],
        ]
        df = parse_series_data(raw, "Inf_Rate")
        assert len(df) == 3
        assert list(df.columns) == ["date", "Inf_Rate"]

    def test_skips_null_values(self) -> None:
        """Omite items con valor None."""
        raw = [
            [EPOCH_2023_01, 13.25],
            [EPOCH_2023_02, None],
            [EPOCH_2023_03, 13.34],
        ]
        df = parse_series_data(raw, "Inf_Rate")
        assert len(df) == 2

    def test_skips_malformed_items(self) -> None:
        """Omite items con menos de 2 elementos."""
        raw = [
            [EPOCH_2023_01, 13.25],
            [EPOCH_2023_02],  # malformed
            [EPOCH_2023_03, 13.34],
        ]
        df = parse_series_data(raw, "test_col")
        assert len(df) == 2

    def test_deduplicates_by_date(self) -> None:
        """Si hay fechas duplicadas, se queda con la última."""
        raw = [
            [EPOCH_2023_01, 13.25],
            [EPOCH_2023_01, 99.99],  # duplicada
        ]
        df = parse_series_data(raw, "test_col")
        assert len(df) == 1
        assert df.iloc[0]["test_col"] == 99.99

    def test_sorted_by_date(self) -> None:
        """El resultado está ordenado por fecha."""
        raw = [
            [EPOCH_2023_03, 1.0],
            [EPOCH_2023_01, 3.0],
            [EPOCH_2023_02, 2.0],
        ]
        df = parse_series_data(raw, "val")
        assert df["date"].is_monotonic_increasing

    def test_empty_input(self) -> None:
        """Lista vacía produce DataFrame vacío."""
        df = parse_series_data([], "col")
        assert df.empty


# ═══════════════════════════════════════════════════════════════════════
# Tests de merge de series
# ═══════════════════════════════════════════════════════════════════════


class TestMergeSeries:
    """Tests de merge_series (unión de múltiples series por fecha)."""

    def test_merge_three_series(self) -> None:
        """Une correctamente las 3 series por fecha."""
        raw = _make_sample_raw()
        df = merge_series(raw)
        assert len(df) == 6
        assert "Inf_Goal" in df.columns
        assert "Inf_Rate" in df.columns
        assert "Core_Inf" in df.columns

    def test_merge_has_year_month(self) -> None:
        """El resultado tiene columnas year y month."""
        raw = _make_sample_raw()
        df = merge_series(raw)
        assert "year" in df.columns
        assert "month" in df.columns
        assert df["year"].iloc[0] == 2023
        assert df["month"].iloc[0] == 1

    def test_merge_sorted_by_date(self) -> None:
        """El resultado está ordenado por fecha."""
        raw = _make_sample_raw()
        df = merge_series(raw)
        assert df["date"].is_monotonic_increasing

    def test_merge_multi_year(self) -> None:
        """Merge funciona con datos de múltiples años."""
        raw = _make_multi_year_raw()
        df = merge_series(raw)
        assert len(df) == 4
        years = df["year"].unique()
        assert 2023 in years
        assert 2024 in years

    def test_merge_partial_series(self) -> None:
        """Si una serie tiene menos meses, hace outer join con NaN."""
        raw = {
            "Inf_Rate": [
                [EPOCH_2023_01, 13.25],
                [EPOCH_2023_02, 13.28],
                [EPOCH_2023_03, 13.34],
            ],
            "Core_Inf": [
                [EPOCH_2023_01, 11.41],
                # Feb faltante
                [EPOCH_2023_03, 11.44],
            ],
        }
        df = merge_series(raw)
        assert len(df) == 3
        # Feb Core_Inf debe ser NaN
        feb = df[df["month"] == 2]
        assert feb["Core_Inf"].isna().iloc[0]

    def test_merge_raises_if_all_empty(self) -> None:
        """Error si todas las series están vacías."""
        raw = {"Inf_Rate": [], "Core_Inf": []}
        with pytest.raises(ValueError, match="No se obtuvieron datos"):
            merge_series(raw)


# ═══════════════════════════════════════════════════════════════════════
# Tests de Inf_Goal (meta de inflación)
# ═══════════════════════════════════════════════════════════════════════


class TestInflationGoal:
    """Tests de la meta de inflación en el merge."""

    def test_goal_constant_within_year(self) -> None:
        """Inf_Goal es constante dentro de un año."""
        raw = _make_sample_raw()
        df = merge_series(raw)
        goals_2023 = df[df["year"] == 2023]["Inf_Goal"]
        assert (goals_2023 == 3.0).all()

    def test_goal_present_in_all_months(self) -> None:
        """Inf_Goal tiene valor para todos los meses del merge."""
        raw = _make_sample_raw()
        df = merge_series(raw)
        # Todos los meses tienen Inf_Goal porque BANREP la expande
        assert df["Inf_Goal"].notna().all()

    def test_goal_multi_year(self) -> None:
        """Inf_Goal funciona con múltiples años."""
        raw = _make_multi_year_raw()
        df = merge_series(raw)
        goals = df.groupby("year")["Inf_Goal"].first()
        assert goals[2023] == 3.0
        assert goals[2024] == 3.0


# ═══════════════════════════════════════════════════════════════════════
# Tests del pipeline completo (clean_banrep_data)
# ═══════════════════════════════════════════════════════════════════════


class TestCleanBanrepData:
    """Tests del pipeline de limpieza completo."""

    def test_produces_final_columns(self) -> None:
        """Produce el esquema completo con source y download_date."""
        raw = _make_sample_raw()
        df = clean_banrep_data(raw)
        assert list(df.columns) == BANREP_PROCESSED_COLUMNS

    def test_source_is_banrep(self) -> None:
        """Todos los registros tienen source='BANREP_SUAMECA'."""
        raw = _make_sample_raw()
        df = clean_banrep_data(raw)
        assert (df["source"] == "BANREP_SUAMECA").all()

    def test_download_date_is_today(self) -> None:
        """download_date es la fecha de hoy."""
        raw = _make_sample_raw()
        df = clean_banrep_data(raw)
        assert (df["download_date"] == date.today().isoformat()).all()

    def test_no_duplicate_dates(self) -> None:
        """No hay fechas duplicadas."""
        raw = _make_sample_raw()
        df = clean_banrep_data(raw)
        assert df["date"].is_unique

    def test_sorted_by_date(self) -> None:
        """Resultado ordenado por fecha."""
        raw = _make_sample_raw()
        df = clean_banrep_data(raw)
        assert df["date"].is_monotonic_increasing

    def test_row_count(self) -> None:
        """6 meses de datos → 6 filas."""
        raw = _make_sample_raw()
        df = clean_banrep_data(raw)
        assert len(df) == 6

    def test_year_month_types(self) -> None:
        """year y month son enteros."""
        raw = _make_sample_raw()
        df = clean_banrep_data(raw)
        assert df["year"].dtype == int
        assert df["month"].dtype == int


# ═══════════════════════════════════════════════════════════════════════
# Tests de calidad
# ═══════════════════════════════════════════════════════════════════════


class TestBanrepQuality:
    """Validaciones de calidad sobre el dataset BANREP procesado."""

    def test_run_banrep_checks_pass(self) -> None:
        """El dataset procesado pasa todas las validaciones."""
        raw = _make_sample_raw()
        df = clean_banrep_data(raw)
        assert run_banrep_checks(df) is True

    def test_rates_in_valid_range(self) -> None:
        """Las tasas de inflación están en rango razonable."""
        raw = _make_sample_raw()
        df = clean_banrep_data(raw)
        assert (df["Inf_Rate"].dropna() >= -5).all()
        assert (df["Inf_Rate"].dropna() <= 40).all()

    def test_goal_in_valid_range(self) -> None:
        """La meta de inflación está en rango razonable."""
        raw = _make_sample_raw()
        df = clean_banrep_data(raw)
        assert (df["Inf_Goal"].dropna() >= 1).all()
        assert (df["Inf_Goal"].dropna() <= 30).all()
