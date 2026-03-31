"""Tests para el pipeline de Brent (FRED/EIA).

Verifica:
- Construcción de URL de descarga
- Parseo del CSV de FRED (DATE + POILBREUSDM)
- Manejo de valores faltantes (".")
- Manejo de campo ``observation_date`` alternativo
- Filtro por rango de fechas
- Agregación diaria → mensual (promedio)
- Esquema final y orden temporal
- Validaciones de calidad
- Integración (clean_brent_data)
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from src.config import BRENT_PROCESSED_COLUMNS, BrentConfig
from src.sources.fred.brent import (
    aggregate_monthly,
    build_source_url,
    clean_brent_data,
    filter_by_date,
    parse_fred_csv,
)
from src.quality_checks import (
    QualityCheckError,
    run_brent_checks,
)


# ═══════════════════════════════════════════════════════════════════════
# Fixtures: CSV de FRED simulado
# ═══════════════════════════════════════════════════════════════════════

SAMPLE_CSV = """\
DATE,POILBREUSDM
2023-01-01,84.12
2023-02-01,83.54
2023-03-01,78.45
2023-04-01,81.32
2023-05-01,75.61
2023-06-01,74.83
"""

SAMPLE_CSV_WITH_MISSING = """\
DATE,POILBREUSDM
2023-01-01,84.12
2023-02-01,.
2023-03-01,78.45
2023-04-01,
2023-05-01,75.61
2023-06-01,74.83
"""

SAMPLE_CSV_OBS_DATE = """\
observation_date,POILBREUSDM
2023-01-01,84.12
2023-02-01,83.54
2023-03-01,78.45
"""

SAMPLE_CSV_DAILY = """\
DATE,POILBREUSDM
2023-01-02,84.00
2023-01-03,84.50
2023-01-04,83.80
2023-02-01,82.10
2023-02-02,82.50
2023-02-03,83.00
2023-03-01,78.00
2023-03-02,79.00
"""

SAMPLE_CSV_BAD_SCHEMA = """\
Fecha,Precio
2023-01-01,84.12
"""

SAMPLE_CSV_MULTI_YEAR = """\
DATE,POILBREUSDM
2001-01-01,25.50
2001-02-01,27.30
2010-06-01,75.20
2010-07-01,76.40
2023-12-01,78.90
2024-01-01,80.10
"""


# ═══════════════════════════════════════════════════════════════════════
# TestBuildSourceUrl
# ═══════════════════════════════════════════════════════════════════════


class TestBuildSourceUrl:
    """Tests para la construcción de URL de descarga."""

    def test_default_config(self):
        url = build_source_url(end_date=date(2024, 6, 15))
        assert "id=POILBREUSDM" in url
        assert "cosd=2001-01-01" in url
        assert "coed=2024-06-15" in url

    def test_custom_config(self):
        config = BrentConfig(
            series_id="TEST_SERIES",
            start_date="2010-01-01",
        )
        url = build_source_url(config=config, end_date=date(2024, 1, 1))
        assert "id=TEST_SERIES" in url
        assert "cosd=2010-01-01" in url

    def test_url_base(self):
        url = build_source_url(end_date=date(2024, 1, 1))
        assert url.startswith("https://fred.stlouisfed.org/graph/fredgraph.csv?")


# ═══════════════════════════════════════════════════════════════════════
# TestParseFredCsv
# ═══════════════════════════════════════════════════════════════════════


class TestParseFredCsv:
    """Tests para el parsing del CSV de FRED."""

    def test_basic_parse(self):
        df = parse_fred_csv(SAMPLE_CSV)
        assert len(df) == 6
        assert "date" in df.columns
        assert "brent_usd_per_barrel" in df.columns

    def test_values_correct(self):
        df = parse_fred_csv(SAMPLE_CSV)
        assert df.iloc[0]["brent_usd_per_barrel"] == pytest.approx(84.12)
        assert df.iloc[-1]["brent_usd_per_barrel"] == pytest.approx(74.83)

    def test_dates_sorted(self):
        df = parse_fred_csv(SAMPLE_CSV)
        dates = df["date"].tolist()
        assert dates == sorted(dates)

    def test_missing_values_skipped(self):
        """Valores '.' y vacíos se ignoran."""
        df = parse_fred_csv(SAMPLE_CSV_WITH_MISSING)
        assert len(df) == 4  # 6 filas - 2 faltantes

    def test_observation_date_field(self):
        """Campo alternativo 'observation_date' soportado."""
        df = parse_fred_csv(SAMPLE_CSV_OBS_DATE)
        assert len(df) == 3

    def test_bad_schema_raises(self):
        """CSV sin campo de fecha ni series_id lanza error."""
        with pytest.raises(ValueError, match="campo de fecha"):
            parse_fred_csv(SAMPLE_CSV_BAD_SCHEMA)

    def test_missing_series_column_raises(self):
        csv_text = "DATE,OTHER_SERIES\n2023-01-01,100\n"
        with pytest.raises(ValueError, match="POILBREUSDM"):
            parse_fred_csv(csv_text)

    def test_empty_csv_raises(self):
        csv_text = "DATE,POILBREUSDM\n"
        with pytest.raises(ValueError, match="No se obtuvieron"):
            parse_fred_csv(csv_text)

    def test_all_missing_raises(self):
        csv_text = "DATE,POILBREUSDM\n2023-01-01,.\n2023-02-01,.\n"
        with pytest.raises(ValueError, match="No se obtuvieron"):
            parse_fred_csv(csv_text)

    def test_date_dtype(self):
        df = parse_fred_csv(SAMPLE_CSV)
        assert pd.api.types.is_datetime64_any_dtype(df["date"])


# ═══════════════════════════════════════════════════════════════════════
# TestFilterByDate
# ═══════════════════════════════════════════════════════════════════════


class TestFilterByDate:
    """Tests para el filtro por rango de fechas."""

    def test_filter_no_change(self):
        df = parse_fred_csv(SAMPLE_CSV)
        filtered = filter_by_date(df, start_date="2023-01-01")
        assert len(filtered) == len(df)

    def test_filter_start_date(self):
        df = parse_fred_csv(SAMPLE_CSV)
        filtered = filter_by_date(df, start_date="2023-03-01")
        assert len(filtered) == 4  # Mar, Apr, May, Jun
        assert filtered["date"].min().date() == date(2023, 3, 1)

    def test_filter_end_date(self):
        df = parse_fred_csv(SAMPLE_CSV)
        filtered = filter_by_date(
            df, start_date="2023-01-01", end_date=date(2023, 3, 1)
        )
        assert len(filtered) == 3

    def test_filter_string_start(self):
        df = parse_fred_csv(SAMPLE_CSV)
        filtered = filter_by_date(df, start_date="2023-04-01")
        assert len(filtered) == 3

    def test_filter_date_object_start(self):
        df = parse_fred_csv(SAMPLE_CSV)
        filtered = filter_by_date(df, start_date=date(2023, 5, 1))
        assert len(filtered) == 2


# ═══════════════════════════════════════════════════════════════════════
# TestAggregateMonthly
# ═══════════════════════════════════════════════════════════════════════


class TestAggregateMonthly:
    """Tests para la agregación diaria → mensual."""

    def test_daily_to_monthly(self):
        df = parse_fred_csv(SAMPLE_CSV_DAILY)
        monthly = aggregate_monthly(df)
        assert len(monthly) == 3  # Ene, Feb, Mar 2023

    def test_monthly_average(self):
        df = parse_fred_csv(SAMPLE_CSV_DAILY)
        monthly = aggregate_monthly(df)
        # Enero: (84.00 + 84.50 + 83.80) / 3 = 84.10
        jan = monthly[monthly["month"] == 1].iloc[0]
        assert jan["brent_usd_per_barrel"] == pytest.approx(84.10, abs=0.01)

    def test_monthly_has_year_month(self):
        df = parse_fred_csv(SAMPLE_CSV_DAILY)
        monthly = aggregate_monthly(df)
        assert "year" in monthly.columns
        assert "month" in monthly.columns

    def test_date_is_first_of_month(self):
        df = parse_fred_csv(SAMPLE_CSV_DAILY)
        monthly = aggregate_monthly(df)
        for _, row in monthly.iterrows():
            assert row["date"].day == 1

    def test_sorted_by_date(self):
        df = parse_fred_csv(SAMPLE_CSV_DAILY)
        monthly = aggregate_monthly(df)
        dates = monthly["date"].tolist()
        assert dates == sorted(dates)

    def test_already_monthly_passthrough(self):
        """Si los datos ya son mensuales, no cambia la cantidad."""
        df = parse_fred_csv(SAMPLE_CSV)
        monthly = aggregate_monthly(df)
        assert len(monthly) == 6

    def test_multi_year(self):
        df = parse_fred_csv(SAMPLE_CSV_MULTI_YEAR)
        monthly = aggregate_monthly(df)
        assert len(monthly) == 6
        years = monthly["year"].unique()
        assert 2001 in years
        assert 2024 in years


# ═══════════════════════════════════════════════════════════════════════
# TestCleanBrentData
# ═══════════════════════════════════════════════════════════════════════


class TestCleanBrentData:
    """Tests para la transformación final del dataset."""

    def _make_monthly_df(self) -> pd.DataFrame:
        df = parse_fred_csv(SAMPLE_CSV)
        return aggregate_monthly(df)

    def test_final_columns(self):
        monthly = self._make_monthly_df()
        clean = clean_brent_data(monthly)
        assert list(clean.columns) == BRENT_PROCESSED_COLUMNS

    def test_source_field(self):
        monthly = self._make_monthly_df()
        clean = clean_brent_data(monthly)
        assert (clean["source"] == "FRED_EIA").all()

    def test_download_date_present(self):
        monthly = self._make_monthly_df()
        clean = clean_brent_data(monthly)
        assert clean["download_date"].notna().all()
        # Debe ser fecha ISO válida
        for d in clean["download_date"]:
            assert len(d) == 10  # "YYYY-MM-DD"

    def test_no_duplicates(self):
        monthly = self._make_monthly_df()
        clean = clean_brent_data(monthly)
        assert clean.duplicated(subset=["date"]).sum() == 0

    def test_sorted_by_date(self):
        monthly = self._make_monthly_df()
        clean = clean_brent_data(monthly)
        dates = clean["date"].tolist()
        assert dates == sorted(dates)

    def test_row_count(self):
        monthly = self._make_monthly_df()
        clean = clean_brent_data(monthly)
        assert len(clean) == 6

    def test_types(self):
        monthly = self._make_monthly_df()
        clean = clean_brent_data(monthly)
        assert clean["year"].dtype in ("int64", "int32")
        assert clean["month"].dtype in ("int64", "int32")
        assert pd.api.types.is_float_dtype(clean["brent_usd_per_barrel"])


# ═══════════════════════════════════════════════════════════════════════
# TestBrentQuality
# ═══════════════════════════════════════════════════════════════════════


class TestBrentQuality:
    """Tests para las validaciones de calidad de Brent."""

    def _make_clean_df(self) -> pd.DataFrame:
        df = parse_fred_csv(SAMPLE_CSV)
        monthly = aggregate_monthly(df)
        return clean_brent_data(monthly)

    def test_all_checks_pass(self):
        df = self._make_clean_df()
        assert run_brent_checks(df) is True

    def test_price_range_too_high(self):
        df = self._make_clean_df()
        df.loc[0, "brent_usd_per_barrel"] = 250.0
        with pytest.raises(QualityCheckError, match="fuera de rango"):
            run_brent_checks(df)

    def test_price_range_negative(self):
        df = self._make_clean_df()
        df.loc[0, "brent_usd_per_barrel"] = -5.0
        with pytest.raises(QualityCheckError, match="fuera de rango"):
            run_brent_checks(df)

    def test_missing_column(self):
        df = self._make_clean_df()
        df = df.drop(columns=["brent_usd_per_barrel"])
        with pytest.raises(QualityCheckError, match="faltantes"):
            run_brent_checks(df)

    def test_null_price(self):
        df = self._make_clean_df()
        df.loc[0, "brent_usd_per_barrel"] = None
        with pytest.raises(QualityCheckError, match="nulos"):
            run_brent_checks(df)

    def test_duplicate_date(self):
        df = self._make_clean_df()
        dup = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        with pytest.raises(QualityCheckError, match="duplicadas"):
            run_brent_checks(dup)
