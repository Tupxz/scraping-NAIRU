"""Tests para el pipeline de datos NAIRU Colombia.

Verifica que el pipeline produce un dataset procesado con la
estructura, tipos y calidad esperados. Incluye tests para:
- Parser CSV placeholder
- Parser Excel robusto (simula formato DANE)
- Detección automática de encabezados y columnas
- Validaciones de calidad
- Utilidades de I/O
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.config import PROCESSED_COLUMNS, SourceProfile
from src.sources.dane.common import (
    auto_map_columns,
    detect_header_row,
    match_column,
)
from src.sources.dane.unemployment import clean_unemployment_data
from src.io_utils import save_csv, load_csv
from src.quality_checks import (
    QualityCheckError,
    check_columns,
    check_no_duplicates,
    check_no_nulls,
    check_unemployment_rate_range,
    run_all_checks,
)


# ═══════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════


@pytest.fixture()
def placeholder_profile() -> SourceProfile:
    """Perfil placeholder para tests sin red."""
    return SourceProfile(
        name="test_placeholder",
        url="",
        raw_filename="test.csv",
        file_format="csv",
    )


@pytest.fixture()
def dane_xlsx_profile() -> SourceProfile:
    """Perfil Excel para tests del parser DANE."""
    return SourceProfile(
        name="test_dane_xlsx",
        url="",
        raw_filename="test.xlsx",
        file_format="xlsx",
    )


@pytest.fixture()
def sample_raw_csv(tmp_path: Path) -> Path:
    """Crea un CSV crudo que simula el formato placeholder (BLS annual)."""
    csv_content = """\
year,population,labor_force,population_percent,employed_total,employed_percent,agrictulture_ratio,nonagriculture_ratio,unemployed,unemployed_percent,not_in_labor,footnotes
2018,100000,60000,60.0,57000,57.0,5000,52000,3000,3.9,40000,
2019,101000,61000,60.4,57500,56.9,4800,52700,3500,3.7,40000,
2020,102000,60000,58.8,52000,51.0,4500,47500,8000,14.7,42000,
2021,103000,61500,59.7,55000,53.4,4600,50400,6500,10.8,41500,
2022,104000,62000,59.6,58000,55.8,4700,53300,4000,6.0,42000,
"""
    raw_path = tmp_path / "raw.csv"
    raw_path.write_text(csv_content)
    return raw_path


@pytest.fixture()
def sample_dane_xlsx(tmp_path: Path) -> Path:
    """Crea un archivo Excel que simula el formato típico del DANE.

    Estructura simulada:
    - Fila 0: título institucional ("DANE - GEIH ...")
    - Fila 1: subtítulo ("Tasa de desempleo nacional")
    - Fila 2: vacía
    - Fila 3: encabezados reales (Año, Mes, Tasa de desempleo (%))
    - Filas 4+: datos
    """
    # Construir DataFrame con filas basura + encabezados + datos
    rows = [
        ["DANE - Gran Encuesta Integrada de Hogares", None, None],
        ["Tasa de desempleo nacional mensual", None, None],
        [None, None, None],  # fila vacía
        ["Año", "Mes", "Tasa de desempleo (%)"],  # ← encabezado real
        [2020, 1, 13.0],
        [2020, 2, 12.2],
        [2020, 3, 12.6],
        [2020, 4, 19.8],
        [2020, 5, 21.4],
        [2020, 6, 19.8],
        [2021, 1, 17.3],
        [2021, 2, 15.9],
        [2021, 3, 14.2],
    ]
    df = pd.DataFrame(rows)
    xlsx_path = tmp_path / "dane_test.xlsx"
    df.to_excel(xlsx_path, index=False, header=False, engine="openpyxl")
    return xlsx_path


@pytest.fixture()
def sample_processed_df() -> pd.DataFrame:
    """DataFrame procesado de ejemplo con esquema estándar."""
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"]),
            "year": [2020, 2020, 2020],
            "month": [1, 2, 3],
            "unemployment_rate": [3.6, 3.5, 4.4],
            "source": ["DANE", "DANE", "DANE"],
            "download_date": [date.today().isoformat()] * 3,
        }
    )


# ═══════════════════════════════════════════════════════════════════════
# Tests de estructura del dataset procesado
# ═══════════════════════════════════════════════════════════════════════


class TestProcessedDataset:
    """Tests sobre la estructura del dataset procesado."""

    def test_has_expected_columns(self, sample_processed_df: pd.DataFrame) -> None:
        assert list(sample_processed_df.columns) == PROCESSED_COLUMNS

    def test_columns_count(self, sample_processed_df: pd.DataFrame) -> None:
        assert len(sample_processed_df.columns) == len(PROCESSED_COLUMNS)

    def test_no_empty_dataframe(self, sample_processed_df: pd.DataFrame) -> None:
        assert len(sample_processed_df) > 0


# ═══════════════════════════════════════════════════════════════════════
# Tests de limpieza CSV (placeholder)
# ═══════════════════════════════════════════════════════════════════════


class TestCleanPlaceholderData:
    """Tests del parser CSV placeholder."""

    def test_clean_returns_expected_columns(
        self, sample_raw_csv: Path, placeholder_profile: SourceProfile
    ) -> None:
        df = clean_unemployment_data(sample_raw_csv, profile=placeholder_profile)
        assert list(df.columns) == PROCESSED_COLUMNS

    def test_clean_row_count(
        self, sample_raw_csv: Path, placeholder_profile: SourceProfile
    ) -> None:
        df = clean_unemployment_data(sample_raw_csv, profile=placeholder_profile)
        assert len(df) == 5

    def test_clean_correct_types(
        self, sample_raw_csv: Path, placeholder_profile: SourceProfile
    ) -> None:
        df = clean_unemployment_data(sample_raw_csv, profile=placeholder_profile)
        assert pd.api.types.is_numeric_dtype(df["unemployment_rate"])
        assert pd.api.types.is_integer_dtype(df["year"])
        assert pd.api.types.is_integer_dtype(df["month"])

    def test_clean_sorted_by_date(
        self, sample_raw_csv: Path, placeholder_profile: SourceProfile
    ) -> None:
        df = clean_unemployment_data(sample_raw_csv, profile=placeholder_profile)
        dates = pd.to_datetime(df["date"])
        assert dates.is_monotonic_increasing


# ═══════════════════════════════════════════════════════════════════════
# Tests de parsing Excel robusto (simulación DANE)
# ═══════════════════════════════════════════════════════════════════════


class TestExcelParsing:
    """Tests del parser Excel con formato DANE simulado."""

    def test_detect_header_row_finds_correct_row(self) -> None:
        """Detecta la fila de encabezado saltando filas basura."""
        df_raw = pd.DataFrame([
            ["DANE - GEIH", None, None],
            ["Subtítulo", None, None],
            [None, None, None],
            ["Año", "Mes", "Tasa de desempleo"],
            [2020, 1, 13.0],
        ])
        row_idx = detect_header_row(df_raw)
        assert row_idx == 3

    def test_detect_header_row_first_row_when_clean(self) -> None:
        """Si no hay basura, devuelve fila 0."""
        df_raw = pd.DataFrame([
            ["Año", "Mes", "Tasa de desempleo"],
            [2020, 1, 13.0],
            [2020, 2, 12.2],
        ])
        row_idx = detect_header_row(df_raw)
        assert row_idx == 0

    def test_match_column_desempleo_patterns(self) -> None:
        """Los patrones detectan variantes de 'tasa de desempleo'."""
        assert match_column("tasa_de_desempleo", [r"tasa.*desempleo"])
        assert match_column("tasa_desempleo_%", [r"tasa.*desempleo"])
        assert match_column("td", [r"td\b"])
        assert not match_column("total_departamento", [r"td\b"])

    def test_match_column_year_month_patterns(self) -> None:
        """Los patrones detectan año y mes."""
        assert match_column("año", [r"^a[ñn]o$"])
        assert match_column("ano", [r"^a[ñn]o$"])
        assert match_column("mes", [r"^mes$"])

    def test_auto_map_columns_finds_unemployment(self) -> None:
        """auto_map_columns identifica la columna de desempleo."""
        columns = ["ao", "mes", "tasa_de_desempleo", "total_nacional"]
        patterns = {
            "unemployment_rate": [r"tasa.*desempleo"],
            "month": [r"^mes$"],
        }
        mapping = auto_map_columns(columns, patterns)
        assert mapping["tasa_de_desempleo"] == "unemployment_rate"
        assert mapping["mes"] == "month"

    def test_auto_map_columns_no_match_warns(self) -> None:
        """auto_map_columns no falla si no encuentra un campo, solo avisa."""
        columns = ["col_a", "col_b"]
        patterns = {"unemployment_rate": [r"tasa.*desempleo"]}
        mapping = auto_map_columns(columns, patterns)
        assert "unemployment_rate" not in mapping.values() or len(mapping) == 0

    def test_clean_dane_xlsx_produces_valid_output(
        self, sample_dane_xlsx: Path, dane_xlsx_profile: SourceProfile
    ) -> None:
        """El parser Excel produce un dataset con las columnas estándar."""
        df = clean_unemployment_data(sample_dane_xlsx, profile=dane_xlsx_profile)
        assert list(df.columns) == PROCESSED_COLUMNS

    def test_clean_dane_xlsx_correct_row_count(
        self, sample_dane_xlsx: Path, dane_xlsx_profile: SourceProfile
    ) -> None:
        """El parser Excel extrae el número correcto de filas de datos."""
        df = clean_unemployment_data(sample_dane_xlsx, profile=dane_xlsx_profile)
        assert len(df) == 9  # 6 meses de 2020 + 3 de 2021

    def test_clean_dane_xlsx_skips_junk_rows(
        self, sample_dane_xlsx: Path, dane_xlsx_profile: SourceProfile
    ) -> None:
        """Las filas de título/subtítulo no aparecen en el resultado."""
        df = clean_unemployment_data(sample_dane_xlsx, profile=dane_xlsx_profile)
        # Ningún valor de 'year' debería ser NaN o texto
        assert df["year"].notna().all()
        assert (df["year"] >= 2000).all()

    def test_clean_dane_xlsx_unemployment_values(
        self, sample_dane_xlsx: Path, dane_xlsx_profile: SourceProfile
    ) -> None:
        """Los valores de desempleo se leen correctamente."""
        df = clean_unemployment_data(sample_dane_xlsx, profile=dane_xlsx_profile)
        # Primer valor debe ser 13.0 (enero 2020)
        first_rate = df.sort_values("date").iloc[0]["unemployment_rate"]
        assert first_rate == pytest.approx(13.0, abs=0.1)


# ═══════════════════════════════════════════════════════════════════════
# Tests de validaciones de calidad
# ═══════════════════════════════════════════════════════════════════════


class TestQualityChecks:
    """Tests de las validaciones de calidad."""

    def test_check_columns_pass(self, sample_processed_df: pd.DataFrame) -> None:
        check_columns(sample_processed_df)

    def test_check_columns_fail_missing(
        self, sample_processed_df: pd.DataFrame
    ) -> None:
        df_bad = sample_processed_df.drop(columns=["unemployment_rate"])
        with pytest.raises(QualityCheckError, match="Columnas faltantes"):
            check_columns(df_bad)

    def test_check_no_nulls_pass(self, sample_processed_df: pd.DataFrame) -> None:
        check_no_nulls(sample_processed_df)

    def test_check_no_nulls_fail(self, sample_processed_df: pd.DataFrame) -> None:
        df_bad = sample_processed_df.copy()
        df_bad.loc[0, "unemployment_rate"] = None
        with pytest.raises(QualityCheckError, match="Valores nulos"):
            check_no_nulls(df_bad)

    def test_check_rate_range_pass(self, sample_processed_df: pd.DataFrame) -> None:
        check_unemployment_rate_range(sample_processed_df)

    def test_check_rate_range_fail(self, sample_processed_df: pd.DataFrame) -> None:
        df_bad = sample_processed_df.copy()
        df_bad.loc[0, "unemployment_rate"] = 99.0
        with pytest.raises(QualityCheckError, match="fuera de rango"):
            check_unemployment_rate_range(df_bad)

    def test_check_no_duplicates_pass(
        self, sample_processed_df: pd.DataFrame
    ) -> None:
        check_no_duplicates(sample_processed_df)

    def test_check_no_duplicates_fail(
        self, sample_processed_df: pd.DataFrame
    ) -> None:
        df_bad = pd.concat(
            [sample_processed_df, sample_processed_df]
        ).reset_index(drop=True)
        with pytest.raises(QualityCheckError, match="Fechas duplicadas"):
            check_no_duplicates(df_bad)

    def test_run_all_checks_pass(self, sample_processed_df: pd.DataFrame) -> None:
        assert run_all_checks(sample_processed_df) is True


# ═══════════════════════════════════════════════════════════════════════
# Tests de I/O
# ═══════════════════════════════════════════════════════════════════════


class TestIO:
    """Tests de utilidades de entrada/salida."""

    def test_save_and_load_csv(
        self, tmp_path: Path, sample_processed_df: pd.DataFrame
    ) -> None:
        path = tmp_path / "test_output.csv"
        save_csv(sample_processed_df, path)
        df_loaded = load_csv(path)
        df_loaded["date"] = pd.to_datetime(df_loaded["date"])
        pd.testing.assert_frame_equal(
            sample_processed_df.reset_index(drop=True),
            df_loaded.reset_index(drop=True),
            check_dtype=False,
        )

    def test_load_csv_file_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_csv(tmp_path / "no_existe.csv")
