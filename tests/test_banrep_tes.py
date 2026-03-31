"""Tests para el pipeline de TES Cero Cupón BANREP/SUAMECA.

Verifica:
- Parseo de datos epoch_ms → date diaria (sin normalización a inicio de mes)
- Parsing de series individuales diarias
- Merge y agregación diaria → mensual (último valor del mes)
- Esquema final y orden temporal
- Validaciones de calidad
- Manejo de datos faltantes y series parciales
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from src.config import BANREP_TES_PROCESSED_COLUMNS, BanrepTESConfig
from src.sources.banrep.tes import (
    aggregate_daily_to_monthly,
    clean_banrep_tes_data,
    epoch_ms_to_date,
    parse_series_data,
)
from src.quality_checks import (
    QualityCheckError,
    run_banrep_tes_checks,
)


# ═══════════════════════════════════════════════════════════════════════
# Fixtures — Epoch milisegundos para fechas diarias de prueba
# ═══════════════════════════════════════════════════════════════════════

# Enero 2024 (algunos días hábiles)
# 2024-01-02 = 1704153600000
# 2024-01-15 = 1705276800000
# 2024-01-31 = 1706659200000
EPOCH_2024_01_02 = 1704153600000
EPOCH_2024_01_15 = 1705276800000
EPOCH_2024_01_31 = 1706659200000

# Febrero 2024
# 2024-02-01 = 1706745600000
# 2024-02-15 = 1707955200000
# 2024-02-29 = 1709164800000  (bisiesto)
EPOCH_2024_02_01 = 1706745600000
EPOCH_2024_02_15 = 1707955200000
EPOCH_2024_02_29 = 1709164800000

# Marzo 2024
# 2024-03-01 = 1709251200000
# 2024-03-15 = 1710460800000
# 2024-03-29 = 1711670400000
EPOCH_2024_03_01 = 1709251200000
EPOCH_2024_03_15 = 1710460800000
EPOCH_2024_03_29 = 1711670400000

# Abril 2024
# 2024-04-01 = 1711929600000
# 2024-04-15 = 1713139200000
# 2024-04-30 = 1714435200000
EPOCH_2024_04_01 = 1711929600000
EPOCH_2024_04_15 = 1713139200000
EPOCH_2024_04_30 = 1714435200000

# Diciembre 2023 (para tests multi-año)
# 2023-12-15 = 1702598400000
# 2023-12-29 = 1703808000000
EPOCH_2023_12_15 = 1702598400000
EPOCH_2023_12_29 = 1703808000000


def _make_daily_raw() -> dict[str, list[list]]:
    """Datos diarios simulados para 2 series TES (ene-abr 2024, 3 obs/mes)."""
    return {
        "TES_UVR_1Y": [
            [EPOCH_2024_01_02, 2.50],
            [EPOCH_2024_01_15, 2.55],
            [EPOCH_2024_01_31, 2.60],   # ← último de enero
            [EPOCH_2024_02_01, 2.58],
            [EPOCH_2024_02_15, 2.62],
            [EPOCH_2024_02_29, 2.65],   # ← último de febrero
            [EPOCH_2024_03_01, 2.70],
            [EPOCH_2024_03_15, 2.72],
            [EPOCH_2024_03_29, 2.75],   # ← último de marzo
            [EPOCH_2024_04_01, 2.80],
            [EPOCH_2024_04_15, 2.82],
            [EPOCH_2024_04_30, 2.85],   # ← último de abril
        ],
        "TES_PESOS_1Y": [
            [EPOCH_2024_01_02, 10.00],
            [EPOCH_2024_01_15, 10.10],
            [EPOCH_2024_01_31, 10.20],  # ← último de enero
            [EPOCH_2024_02_01, 10.15],
            [EPOCH_2024_02_15, 10.25],
            [EPOCH_2024_02_29, 10.30],  # ← último de febrero
            [EPOCH_2024_03_01, 10.35],
            [EPOCH_2024_03_15, 10.40],
            [EPOCH_2024_03_29, 10.45],  # ← último de marzo
            [EPOCH_2024_04_01, 10.50],
            [EPOCH_2024_04_15, 10.55],
            [EPOCH_2024_04_30, 10.60],  # ← último de abril
        ],
    }


def _make_multi_year_raw() -> dict[str, list[list]]:
    """Datos que cruzan 2023 y 2024 para test multi-año."""
    return {
        "TES_UVR_1Y": [
            [EPOCH_2023_12_15, 3.10],
            [EPOCH_2023_12_29, 3.15],   # ← último dic 2023
            [EPOCH_2024_01_02, 2.50],
            [EPOCH_2024_01_31, 2.60],   # ← último ene 2024
        ],
        "TES_PESOS_1Y": [
            [EPOCH_2023_12_15, 11.00],
            [EPOCH_2023_12_29, 11.10],  # ← último dic 2023
            [EPOCH_2024_01_02, 10.00],
            [EPOCH_2024_01_31, 10.20],  # ← último ene 2024
        ],
    }


def _make_partial_raw() -> dict[str, list[list]]:
    """Una serie con más meses que la otra (test de outer join)."""
    return {
        "TES_UVR_1Y": [
            [EPOCH_2024_01_15, 2.55],
            [EPOCH_2024_01_31, 2.60],
            [EPOCH_2024_02_15, 2.62],
            [EPOCH_2024_02_29, 2.65],
            [EPOCH_2024_03_15, 2.72],
            [EPOCH_2024_03_29, 2.75],
        ],
        "TES_PESOS_1Y": [
            [EPOCH_2024_01_15, 10.10],
            [EPOCH_2024_01_31, 10.20],
            # febrero faltante
            [EPOCH_2024_03_15, 10.40],
            [EPOCH_2024_03_29, 10.45],
        ],
    }


# ═══════════════════════════════════════════════════════════════════════
# Tests de parseo epoch → date (diaria, NO normalizada)
# ═══════════════════════════════════════════════════════════════════════


class TestEpochConversionTES:
    """Tests de conversión epoch_ms → date (conserva día real)."""

    def test_epoch_to_date_preserves_day(self) -> None:
        """epoch de 2024-01-15 → date(2024, 1, 15)."""
        d = epoch_ms_to_date(EPOCH_2024_01_15)
        assert d == date(2024, 1, 15)

    def test_epoch_to_date_jan_2(self) -> None:
        """epoch de 2024-01-02 → date(2024, 1, 2)."""
        d = epoch_ms_to_date(EPOCH_2024_01_02)
        assert d == date(2024, 1, 2)

    def test_epoch_to_date_end_of_month(self) -> None:
        """epoch de 2024-01-31 → date(2024, 1, 31)."""
        d = epoch_ms_to_date(EPOCH_2024_01_31)
        assert d == date(2024, 1, 31)

    def test_epoch_to_date_leap_feb(self) -> None:
        """Feb 2024 (bisiesto) día 29 se parsea correctamente."""
        d = epoch_ms_to_date(EPOCH_2024_02_29)
        assert d == date(2024, 2, 29)

    def test_epoch_to_date_dec_2023(self) -> None:
        """Diciembre 2023 se parsea correctamente."""
        d = epoch_ms_to_date(EPOCH_2023_12_29)
        assert d == date(2023, 12, 29)

    def test_epoch_preserves_exact_day_not_first(self) -> None:
        """A diferencia del pipeline de inflación, NO normaliza al día 1."""
        d = epoch_ms_to_date(EPOCH_2024_03_15)
        assert d.day == 15  # NO 1


# ═══════════════════════════════════════════════════════════════════════
# Tests de parsing de series individuales (diarias)
# ═══════════════════════════════════════════════════════════════════════


class TestParseSeriesDataTES:
    """Tests de parse_series_data (lista de [epoch, valor] → DataFrame diario)."""

    def test_basic_parsing(self) -> None:
        """Parsea datos diarios correctamente."""
        raw = [
            [EPOCH_2024_01_02, 2.50],
            [EPOCH_2024_01_15, 2.55],
            [EPOCH_2024_01_31, 2.60],
        ]
        df = parse_series_data(raw, "TES_UVR_1Y")
        assert len(df) == 3
        assert list(df.columns) == ["date", "TES_UVR_1Y"]

    def test_skips_null_values(self) -> None:
        """Omite items con valor None."""
        raw = [
            [EPOCH_2024_01_02, 2.50],
            [EPOCH_2024_01_15, None],
            [EPOCH_2024_01_31, 2.60],
        ]
        df = parse_series_data(raw, "TES_UVR_1Y")
        assert len(df) == 2

    def test_skips_malformed_items(self) -> None:
        """Omite items con menos de 2 elementos."""
        raw = [
            [EPOCH_2024_01_02, 2.50],
            [EPOCH_2024_01_15],  # malformed
            [EPOCH_2024_01_31, 2.60],
        ]
        df = parse_series_data(raw, "test_col")
        assert len(df) == 2

    def test_deduplicates_by_date(self) -> None:
        """Si hay fechas duplicadas, se queda con la última."""
        raw = [
            [EPOCH_2024_01_15, 2.50],
            [EPOCH_2024_01_15, 9.99],  # duplicada
        ]
        df = parse_series_data(raw, "test_col")
        assert len(df) == 1
        assert df.iloc[0]["test_col"] == 9.99

    def test_sorted_by_date(self) -> None:
        """El resultado está ordenado por fecha."""
        raw = [
            [EPOCH_2024_03_01, 1.0],
            [EPOCH_2024_01_02, 3.0],
            [EPOCH_2024_02_15, 2.0],
        ]
        df = parse_series_data(raw, "val")
        assert df["date"].is_monotonic_increasing

    def test_empty_input(self) -> None:
        """Lista vacía produce DataFrame vacío."""
        df = parse_series_data([], "col")
        assert df.empty


# ═══════════════════════════════════════════════════════════════════════
# Tests de agregación diaria → mensual
# ═══════════════════════════════════════════════════════════════════════


class TestAggregateMonthly:
    """Tests de aggregate_daily_to_monthly (último valor del mes)."""

    def test_aggregation_row_count(self) -> None:
        """12 obs diarias × 2 series en 4 meses → 4 filas mensuales."""
        raw = _make_daily_raw()
        df = aggregate_daily_to_monthly(raw)
        assert len(df) == 4

    def test_last_value_per_month_uvr(self) -> None:
        """TES_UVR_1Y: el valor mensual es el ÚLTIMO del mes."""
        raw = _make_daily_raw()
        df = aggregate_daily_to_monthly(raw)
        jan = df[df["month"] == 1].iloc[0]
        assert jan["TES_UVR_1Y"] == 2.60   # último de enero

        feb = df[df["month"] == 2].iloc[0]
        assert feb["TES_UVR_1Y"] == 2.65   # último de febrero

    def test_last_value_per_month_pesos(self) -> None:
        """TES_PESOS_1Y: el valor mensual es el ÚLTIMO del mes."""
        raw = _make_daily_raw()
        df = aggregate_daily_to_monthly(raw)
        mar = df[df["month"] == 3].iloc[0]
        assert mar["TES_PESOS_1Y"] == 10.45  # último de marzo

        apr = df[df["month"] == 4].iloc[0]
        assert apr["TES_PESOS_1Y"] == 10.60  # último de abril

    def test_has_year_month_columns(self) -> None:
        """El resultado tiene columnas year y month."""
        raw = _make_daily_raw()
        df = aggregate_daily_to_monthly(raw)
        assert "year" in df.columns
        assert "month" in df.columns
        assert df["year"].iloc[0] == 2024
        assert df["month"].iloc[0] == 1

    def test_date_is_first_of_month(self) -> None:
        """La fecha mensual es el primer día del mes."""
        raw = _make_daily_raw()
        df = aggregate_daily_to_monthly(raw)
        for _, row in df.iterrows():
            assert pd.Timestamp(row["date"]).day == 1

    def test_sorted_by_date(self) -> None:
        """El resultado está ordenado por fecha."""
        raw = _make_daily_raw()
        df = aggregate_daily_to_monthly(raw)
        assert df["date"].is_monotonic_increasing

    def test_multi_year(self) -> None:
        """Agregación funciona con datos de múltiples años."""
        raw = _make_multi_year_raw()
        df = aggregate_daily_to_monthly(raw)
        assert len(df) == 2  # dic 2023 + ene 2024
        years = df["year"].unique()
        assert 2023 in years
        assert 2024 in years
        # Verificar último valor de diciembre 2023
        dec = df[df["month"] == 12].iloc[0]
        assert dec["TES_UVR_1Y"] == 3.15

    def test_partial_series_outer_join(self) -> None:
        """Si una serie falta en un mes, el merge deja NaN."""
        raw = _make_partial_raw()
        df = aggregate_daily_to_monthly(raw)
        assert len(df) == 3  # ene, feb, mar
        # Febrero TES_PESOS_1Y debe ser NaN (no hay datos en la serie parcial)
        feb = df[df["month"] == 2]
        assert feb["TES_PESOS_1Y"].isna().iloc[0]
        # Pero TES_UVR_1Y tiene dato en febrero
        assert feb["TES_UVR_1Y"].notna().iloc[0]

    def test_raises_if_all_empty(self) -> None:
        """Error si todas las series están vacías."""
        raw = {"TES_UVR_1Y": [], "TES_PESOS_1Y": []}
        with pytest.raises(ValueError, match="No se obtuvieron datos"):
            aggregate_daily_to_monthly(raw)

    def test_single_obs_per_month(self) -> None:
        """Si solo hay 1 observación en un mes, ese es el 'último' valor."""
        raw = {
            "TES_UVR_1Y": [
                [EPOCH_2024_01_15, 2.55],
            ],
            "TES_PESOS_1Y": [
                [EPOCH_2024_01_15, 10.10],
            ],
        }
        df = aggregate_daily_to_monthly(raw)
        assert len(df) == 1
        assert df.iloc[0]["TES_UVR_1Y"] == 2.55
        assert df.iloc[0]["TES_PESOS_1Y"] == 10.10


# ═══════════════════════════════════════════════════════════════════════
# Tests del pipeline completo (clean_banrep_tes_data)
# ═══════════════════════════════════════════════════════════════════════


class TestCleanBanrepTESData:
    """Tests del pipeline de limpieza completo."""

    def test_produces_final_columns(self) -> None:
        """Produce el esquema completo con source y download_date."""
        raw = _make_daily_raw()
        df = clean_banrep_tes_data(raw)
        assert list(df.columns) == BANREP_TES_PROCESSED_COLUMNS

    def test_source_is_banrep(self) -> None:
        """Todos los registros tienen source='BANREP_SUAMECA'."""
        raw = _make_daily_raw()
        df = clean_banrep_tes_data(raw)
        assert (df["source"] == "BANREP_SUAMECA").all()

    def test_download_date_is_today(self) -> None:
        """download_date es la fecha de hoy."""
        raw = _make_daily_raw()
        df = clean_banrep_tes_data(raw)
        assert (df["download_date"] == date.today().isoformat()).all()

    def test_no_duplicate_dates(self) -> None:
        """No hay fechas duplicadas."""
        raw = _make_daily_raw()
        df = clean_banrep_tes_data(raw)
        assert df["date"].is_unique

    def test_sorted_by_date(self) -> None:
        """Resultado ordenado por fecha."""
        raw = _make_daily_raw()
        df = clean_banrep_tes_data(raw)
        assert df["date"].is_monotonic_increasing

    def test_row_count(self) -> None:
        """4 meses de datos diarios → 4 filas mensuales."""
        raw = _make_daily_raw()
        df = clean_banrep_tes_data(raw)
        assert len(df) == 4

    def test_year_month_types(self) -> None:
        """year y month son enteros."""
        raw = _make_daily_raw()
        df = clean_banrep_tes_data(raw)
        assert df["year"].dtype == int
        assert df["month"].dtype == int

    def test_values_are_last_of_month(self) -> None:
        """Los valores corresponden al último dato diario del mes."""
        raw = _make_daily_raw()
        df = clean_banrep_tes_data(raw)
        jan = df[df["month"] == 1].iloc[0]
        assert jan["TES_UVR_1Y"] == 2.60
        assert jan["TES_PESOS_1Y"] == 10.20


# ═══════════════════════════════════════════════════════════════════════
# Tests de BanrepTESConfig
# ═══════════════════════════════════════════════════════════════════════


class TestBanrepTESConfig:
    """Tests de la configuración TES."""

    def test_default_config(self) -> None:
        """La config por defecto tiene los campos esperados."""
        cfg = BanrepTESConfig()
        assert "TES_UVR_1Y" in cfg.series_map
        assert "TES_PESOS_1Y" in cfg.series_map
        assert cfg.tipo_dato == 1
        assert cfg.cant_datos >= 8000

    def test_series_ids(self) -> None:
        """Los IDs de serie son correctos."""
        cfg = BanrepTESConfig()
        assert cfg.series_map["TES_UVR_1Y"] == 15275
        assert cfg.series_map["TES_PESOS_1Y"] == 15272

    def test_warmup_url(self) -> None:
        """warmup_url se construye correctamente."""
        cfg = BanrepTESConfig()
        assert cfg.warmup_url.startswith("https://suameca.banrep.gov.co")
        assert "informacionSerie" in cfg.warmup_url

    def test_endpoint_url(self) -> None:
        """endpoint_url se construye correctamente."""
        cfg = BanrepTESConfig()
        assert "consultaInformacionSerieXTipoDato" in cfg.endpoint_url

    def test_config_is_frozen(self) -> None:
        """La config es inmutable."""
        cfg = BanrepTESConfig()
        with pytest.raises(AttributeError):
            cfg.tipo_dato = 9  # type: ignore[misc]


# ═══════════════════════════════════════════════════════════════════════
# Tests de calidad
# ═══════════════════════════════════════════════════════════════════════


class TestBanrepTESQuality:
    """Validaciones de calidad sobre el dataset TES procesado."""

    def test_run_tes_checks_pass(self) -> None:
        """El dataset procesado pasa todas las validaciones."""
        raw = _make_daily_raw()
        df = clean_banrep_tes_data(raw)
        assert run_banrep_tes_checks(df) is True

    def test_rates_in_valid_range(self) -> None:
        """Las tasas TES están en rango razonable."""
        raw = _make_daily_raw()
        df = clean_banrep_tes_data(raw)
        for col in ("TES_UVR_1Y", "TES_PESOS_1Y"):
            vals = df[col].dropna()
            assert (vals >= -5).all()
            assert (vals <= 40).all()

    def test_quality_rejects_out_of_range(self) -> None:
        """El check de calidad rechaza valores fuera de rango."""
        raw = _make_daily_raw()
        df = clean_banrep_tes_data(raw)
        # Inyectar valor fuera de rango
        df.loc[0, "TES_PESOS_1Y"] = 50.0
        with pytest.raises(QualityCheckError, match="fuera de rango"):
            run_banrep_tes_checks(df)

    def test_quality_rejects_wrong_columns(self) -> None:
        """El check de calidad rechaza columnas incorrectas."""
        df = pd.DataFrame({"date": [date.today()], "bad_col": [1.0]})
        with pytest.raises(QualityCheckError, match="faltantes"):
            run_banrep_tes_checks(df)

    def test_multi_year_passes_checks(self) -> None:
        """Datos multi-año pasan las validaciones."""
        raw = _make_multi_year_raw()
        df = clean_banrep_tes_data(raw)
        assert run_banrep_tes_checks(df) is True
