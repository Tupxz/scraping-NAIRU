"""Tests para el pipeline PWT 11.0 (Stock de Capital, Depreciación y Capital Humano).

Verifica:
- Parseo del CSV crudo con filtro por countrycode
- Columnas y tipos de datos correctos del DataFrame resultante
- Filtro exclusivo para Colombia (COL)
- Ausencia de NaN en capital_stock_real
- Validaciones de calidad (pass y fail)
- Detección de valores fuera de rango

Todos los tests usan fixtures con datos sintéticos — no se realizan
llamadas a la red.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pandas as pd
import pytest

from src.config import PWT_PROCESSED_COLUMNS, PWTConfig
from src.quality_checks import QualityCheckError, run_pwt_checks
from src.sources.pwt.pwt import parse_pwt_csv


# ═══════════════════════════════════════════════════════════════════════
# Fixtures: CSV de PWT simulado
# ═══════════════════════════════════════════════════════════════════════

# CSV con datos válidos para COL y otro país (MEX) para probar el filtro.
# Valores realistas PWT 11.0: rnna en millones COP 2017 (Colombia ≈ 0.9–2.7M),
# delta ≈ 0.047–0.048, hc ≈ 1.5–2.6.
SAMPLE_PWT_CSV = textwrap.dedent("""\
    countrycode,year,rnna,delta,hc
    COL,1950,900000.0,0.0478,1.45
    COL,1951,910000.0,0.0479,1.47
    COL,1952,920000.0,0.0480,1.50
    MEX,1950,1500000.0,0.0500,1.60
    MEX,1951,1520000.0,0.0501,1.62
""")

# CSV con filas de COL con rnna nulo (deben descartarse)
SAMPLE_PWT_WITH_NULLS = textwrap.dedent("""\
    countrycode,year,rnna,delta,hc
    COL,1950,900000.0,0.0478,1.45
    COL,1951,,0.0479,1.47
    COL,1952,920000.0,,1.50
    COL,1953,930000.0,0.0481,1.52
""")

# CSV con 70 filas de COL para test de mínimo de filas
SAMPLE_PWT_LARGE = "\n".join(
    ["countrycode,year,rnna,delta,hc"]
    + [f"COL,{1950 + i},{900_000.0 + i * 5_000:.1f},{0.0478 + i * 0.00001:.5f},{1.40 + i * 0.01:.2f}"
       for i in range(70)]
)

# CSV sin la columna requerida countrycode
SAMPLE_PWT_BAD_SCHEMA = textwrap.dedent("""\
    country,year,rnna,delta,hc
    COL,1950,900000.0,0.0478,1.45
""")

# CSV con país inexistente
SAMPLE_PWT_WRONG_COUNTRY = textwrap.dedent("""\
    countrycode,year,rnna,delta,hc
    USA,1950,5000000.0,0.0500,3.20
    DEU,1950,3000000.0,0.0500,2.90
""")


@pytest.fixture
def tmp_pwt_csv(tmp_path: Path) -> Path:
    """Escribe el CSV de ejemplo en un archivo temporal y retorna su ruta."""
    p = tmp_path / "pwt_raw.csv"
    p.write_text(SAMPLE_PWT_CSV, encoding="utf-8")
    return p


@pytest.fixture
def tmp_pwt_nulls_csv(tmp_path: Path) -> Path:
    """CSV con filas que tienen rnna nulo."""
    p = tmp_path / "pwt_nulls.csv"
    p.write_text(SAMPLE_PWT_WITH_NULLS, encoding="utf-8")
    return p


@pytest.fixture
def tmp_pwt_large_csv(tmp_path: Path) -> Path:
    """CSV con 70 filas para COL."""
    p = tmp_path / "pwt_large.csv"
    p.write_text(SAMPLE_PWT_LARGE, encoding="utf-8")
    return p


@pytest.fixture
def tmp_pwt_bad_schema_csv(tmp_path: Path) -> Path:
    """CSV con esquema incorrecto (falta countrycode)."""
    p = tmp_path / "pwt_bad.csv"
    p.write_text(SAMPLE_PWT_BAD_SCHEMA, encoding="utf-8")
    return p


@pytest.fixture
def tmp_pwt_wrong_country_csv(tmp_path: Path) -> Path:
    """CSV sin datos de Colombia."""
    p = tmp_path / "pwt_no_col.csv"
    p.write_text(SAMPLE_PWT_WRONG_COUNTRY, encoding="utf-8")
    return p


# ═══════════════════════════════════════════════════════════════════════
# test_parse_pwt_csv_basic
# ═══════════════════════════════════════════════════════════════════════


class TestParsePwtCsvBasic:
    """Tests básicos de parseo: columnas y tipos de datos."""

    def test_returns_dataframe(self, tmp_pwt_csv: Path) -> None:
        """parse_pwt_csv retorna un DataFrame."""
        df = parse_pwt_csv(tmp_pwt_csv)
        assert isinstance(df, pd.DataFrame)

    def test_columns_match_standard(self, tmp_pwt_csv: Path) -> None:
        """El DataFrame tiene exactamente las columnas del estándar PWT."""
        df = parse_pwt_csv(tmp_pwt_csv)
        assert list(df.columns) == PWT_PROCESSED_COLUMNS

    def test_date_dtype_is_datetime(self, tmp_pwt_csv: Path) -> None:
        """La columna date es de tipo datetime64."""
        df = parse_pwt_csv(tmp_pwt_csv)
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_year_dtype_is_integer(self, tmp_pwt_csv: Path) -> None:
        """La columna year es entera."""
        df = parse_pwt_csv(tmp_pwt_csv)
        assert df["year"].dtype in ("int64", "int32", "int16")

    def test_month_is_always_1(self, tmp_pwt_csv: Path) -> None:
        """month siempre es 1 (datos anuales)."""
        df = parse_pwt_csv(tmp_pwt_csv)
        assert (df["month"] == 1).all()

    def test_date_is_first_of_year(self, tmp_pwt_csv: Path) -> None:
        """date es el primer día del año (01-01)."""
        df = parse_pwt_csv(tmp_pwt_csv)
        assert (df["date"].dt.month == 1).all()
        assert (df["date"].dt.day == 1).all()

    def test_source_field(self, tmp_pwt_csv: Path) -> None:
        """El campo source es 'PWT 11.0'."""
        df = parse_pwt_csv(tmp_pwt_csv)
        assert (df["source"] == "PWT 11.0").all()

    def test_download_date_present(self, tmp_pwt_csv: Path) -> None:
        """download_date está presente y tiene formato ISO (YYYY-MM-DD)."""
        df = parse_pwt_csv(tmp_pwt_csv)
        assert df["download_date"].notna().all()
        for d in df["download_date"]:
            assert len(d) == 10  # "YYYY-MM-DD"

    def test_capital_stock_real_is_float(self, tmp_pwt_csv: Path) -> None:
        """capital_stock_real es numérico (float)."""
        df = parse_pwt_csv(tmp_pwt_csv)
        assert pd.api.types.is_float_dtype(df["capital_stock_real"])

    def test_depreciation_rate_is_float(self, tmp_pwt_csv: Path) -> None:
        """depreciation_rate es numérico (float)."""
        df = parse_pwt_csv(tmp_pwt_csv)
        assert pd.api.types.is_float_dtype(df["depreciation_rate"])

    def test_human_capital_is_float(self, tmp_pwt_csv: Path) -> None:
        """human_capital es numérico (float)."""
        df = parse_pwt_csv(tmp_pwt_csv)
        assert pd.api.types.is_float_dtype(df["human_capital"])

    def test_sorted_by_date(self, tmp_pwt_csv: Path) -> None:
        """El DataFrame está ordenado por fecha ascendente."""
        df = parse_pwt_csv(tmp_pwt_csv)
        dates = df["date"].tolist()
        assert dates == sorted(dates)

    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        """Si el archivo no existe, se lanza FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            parse_pwt_csv(tmp_path / "inexistente.csv")

    def test_bad_schema_raises(self, tmp_pwt_bad_schema_csv: Path) -> None:
        """CSV sin columnas requeridas lanza ValueError."""
        with pytest.raises(ValueError, match="ausentes"):
            parse_pwt_csv(tmp_pwt_bad_schema_csv)


# ═══════════════════════════════════════════════════════════════════════
# test_parse_pwt_col_filter
# ═══════════════════════════════════════════════════════════════════════


class TestParsePwtColFilter:
    """Tests que verifican que solo se retornan filas de Colombia."""

    def test_only_col_rows(self, tmp_pwt_csv: Path) -> None:
        """El DataFrame resultante contiene solo datos de COL."""
        df = parse_pwt_csv(tmp_pwt_csv)
        # No hay columna countrycode en el output (se usa solo para filtrar)
        assert "countrycode" not in df.columns

    def test_correct_row_count(self, tmp_pwt_csv: Path) -> None:
        """Solo se retornan las 3 filas de COL (MEX excluido)."""
        df = parse_pwt_csv(tmp_pwt_csv)
        assert len(df) == 3

    def test_years_are_col_years(self, tmp_pwt_csv: Path) -> None:
        """Los años corresponden a COL (1950, 1951, 1952)."""
        df = parse_pwt_csv(tmp_pwt_csv)
        assert set(df["year"].tolist()) == {1950, 1951, 1952}

    def test_custom_country_code(self, tmp_pwt_csv: Path) -> None:
        """Con country_code='MEX' se filtran solo filas de México."""
        config = PWTConfig(country_code="MEX")
        df = parse_pwt_csv(tmp_pwt_csv, config=config)
        assert len(df) == 2
        assert (df["year"].isin([1950, 1951])).all()

    def test_wrong_country_raises(self, tmp_pwt_wrong_country_csv: Path) -> None:
        """Si no hay datos para el país, se lanza ValueError."""
        with pytest.raises(ValueError, match="COL"):
            parse_pwt_csv(tmp_pwt_wrong_country_csv)


# ═══════════════════════════════════════════════════════════════════════
# test_parse_pwt_no_nulls_rnna
# ═══════════════════════════════════════════════════════════════════════


class TestParsePwtNoNullsRnna:
    """Tests que verifican que no haya NaN en capital_stock_real (rnna)."""

    def test_no_nulls_in_capital_stock_real(self, tmp_pwt_nulls_csv: Path) -> None:
        """capital_stock_real no tiene NaN tras el parseo."""
        df = parse_pwt_csv(tmp_pwt_nulls_csv)
        assert df["capital_stock_real"].isna().sum() == 0

    def test_row_with_null_rnna_is_dropped(self, tmp_pwt_nulls_csv: Path) -> None:
        """La fila con rnna nulo (1951) se elimina; quedan 3 filas."""
        df = parse_pwt_csv(tmp_pwt_nulls_csv)
        assert len(df) == 3
        assert 1951 not in df["year"].values

    def test_null_delta_is_kept(self, tmp_pwt_nulls_csv: Path) -> None:
        """Filas con delta nulo (pero rnna presente) se conservan."""
        df = parse_pwt_csv(tmp_pwt_nulls_csv)
        # El año 1952 tiene delta nulo pero rnna válido → debe estar
        assert 1952 in df["year"].values


# ═══════════════════════════════════════════════════════════════════════
# test_run_pwt_checks_pass
# ═══════════════════════════════════════════════════════════════════════


class TestRunPwtChecksPass:
    """Tests que verifican que un DataFrame válido supera las validaciones."""

    def _make_valid_df(self, n: int = 70) -> pd.DataFrame:
        """Construye un DataFrame PWT válido con n filas.

        Valores realistas Colombia (PWT 11.0):
          - capital_stock_real ≈ 0.9–2.7 millones (millones COP 2017)
          - depreciation_rate ≈ 0.0478 (4.78%)
          - human_capital ≈ 1.5–2.6 (índice)
        """
        years = list(range(1950, 1950 + n))
        return pd.DataFrame({
            "date": pd.to_datetime([f"{y}-01-01" for y in years]),
            "year": years,
            "month": [1] * n,
            "capital_stock_real": [900_000.0 + i * 5_000 for i in range(n)],
            "depreciation_rate": [0.0478 + i * 0.00001 for i in range(n)],
            "human_capital": [1.5 + i * 0.01 for i in range(n)],
            "source": ["PWT 11.0"] * n,
            "download_date": ["2026-05-11"] * n,
        })

    def test_valid_df_passes(self) -> None:
        """Un DataFrame válido no lanza ninguna excepción."""
        df = self._make_valid_df()
        assert run_pwt_checks(df) is True

    def test_returns_true(self) -> None:
        """run_pwt_checks retorna True cuando todo está OK."""
        df = self._make_valid_df()
        result = run_pwt_checks(df)
        assert result is True

    def test_large_csv_passes(self, tmp_pwt_large_csv: Path) -> None:
        """Un CSV con 70 filas de COL supera las validaciones."""
        from src.sources.pwt.pwt import parse_pwt_csv as _parse
        df = _parse(tmp_pwt_large_csv)
        assert run_pwt_checks(df) is True


# ═══════════════════════════════════════════════════════════════════════
# test_run_pwt_checks_fail_range
# ═══════════════════════════════════════════════════════════════════════


class TestRunPwtChecksFailRange:
    """Tests que verifican que valores fuera de rango lanzan QualityCheckError."""

    def _make_valid_df(self, n: int = 70) -> pd.DataFrame:
        years = list(range(1950, 1950 + n))
        return pd.DataFrame({
            "date": pd.to_datetime([f"{y}-01-01" for y in years]),
            "year": years,
            "month": [1] * n,
            "capital_stock_real": [900_000.0 + i * 5_000 for i in range(n)],
            "depreciation_rate": [0.0478 + i * 0.00001 for i in range(n)],
            "human_capital": [1.5 + i * 0.01 for i in range(n)],
            "source": ["PWT 11.0"] * n,
            "download_date": ["2026-05-11"] * n,
        })

    def test_capital_stock_real_too_high(self) -> None:
        """capital_stock_real > CAPITAL_STOCK_MAX lanza QualityCheckError."""
        df = self._make_valid_df()
        df.loc[0, "capital_stock_real"] = 9_999_999_999.0
        with pytest.raises(QualityCheckError, match="capital_stock_real fuera de rango"):
            run_pwt_checks(df)

    def test_capital_stock_real_negative(self) -> None:
        """capital_stock_real negativo lanza QualityCheckError."""
        df = self._make_valid_df()
        df.loc[0, "capital_stock_real"] = -1.0
        with pytest.raises(QualityCheckError, match="capital_stock_real fuera de rango"):
            run_pwt_checks(df)

    def test_depreciation_rate_too_high(self) -> None:
        """depreciation_rate > DEPRECIATION_RATE_MAX lanza QualityCheckError."""
        df = self._make_valid_df()
        df.loc[0, "depreciation_rate"] = 0.99
        with pytest.raises(QualityCheckError, match="depreciation_rate fuera de rango"):
            run_pwt_checks(df)

    def test_depreciation_rate_too_low(self) -> None:
        """depreciation_rate < DEPRECIATION_RATE_MIN lanza QualityCheckError."""
        df = self._make_valid_df()
        df.loc[0, "depreciation_rate"] = 0.0
        with pytest.raises(QualityCheckError, match="depreciation_rate fuera de rango"):
            run_pwt_checks(df)

    def test_human_capital_too_high(self) -> None:
        """human_capital > HUMAN_CAPITAL_MAX lanza QualityCheckError."""
        df = self._make_valid_df()
        df.loc[0, "human_capital"] = 10.0
        with pytest.raises(QualityCheckError, match="human_capital fuera de rango"):
            run_pwt_checks(df)

    def test_human_capital_too_low(self) -> None:
        """human_capital < HUMAN_CAPITAL_MIN lanza QualityCheckError."""
        df = self._make_valid_df()
        df.loc[0, "human_capital"] = 0.5
        with pytest.raises(QualityCheckError, match="human_capital fuera de rango"):
            run_pwt_checks(df)

    def test_missing_column_raises(self) -> None:
        """DataFrame sin columna capital_stock_real lanza QualityCheckError."""
        df = self._make_valid_df()
        df = df.drop(columns=["capital_stock_real"])
        with pytest.raises(QualityCheckError, match="faltantes"):
            run_pwt_checks(df)

    def test_null_in_capital_stock_raises(self) -> None:
        """NaN en capital_stock_real lanza QualityCheckError."""
        df = self._make_valid_df()
        df.loc[0, "capital_stock_real"] = None
        with pytest.raises(QualityCheckError, match="nulos"):
            run_pwt_checks(df)

    def test_null_in_depreciation_rate_raises(self) -> None:
        """NaN en depreciation_rate lanza QualityCheckError."""
        df = self._make_valid_df()
        df.loc[0, "depreciation_rate"] = None
        with pytest.raises(QualityCheckError, match="nulos"):
            run_pwt_checks(df)

    def test_null_in_human_capital_raises(self) -> None:
        """NaN en human_capital lanza QualityCheckError."""
        df = self._make_valid_df()
        df.loc[0, "human_capital"] = None
        with pytest.raises(QualityCheckError, match="nulos"):
            run_pwt_checks(df)

    def test_duplicate_date_raises(self) -> None:
        """Fecha duplicada lanza QualityCheckError."""
        df = self._make_valid_df()
        dup = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        with pytest.raises(QualityCheckError, match="duplicadas"):
            run_pwt_checks(dup)

    def test_too_few_rows_raises(self) -> None:
        """DataFrame con menos de 50 filas lanza QualityCheckError."""
        df = self._make_valid_df(n=30)
        with pytest.raises(QualityCheckError, match="al menos 50"):
            run_pwt_checks(df)
