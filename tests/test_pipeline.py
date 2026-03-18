"""Tests para el pipeline de datos NAIRU Colombia.

Verifica que el pipeline produce un dataset procesado con la
estructura, tipos y calidad esperados.
"""

from __future__ import annotations

from datetime import date
from io import StringIO
from pathlib import Path

import pandas as pd
import pytest

from src.config import PROCESSED_COLUMNS, PROCESSED_DIR, DANE_PROCESSED_FILENAME
from src.dane import clean_unemployment_data
from src.io_utils import save_csv, load_csv
from src.quality_checks import (
    QualityCheckError,
    check_columns,
    check_no_duplicates,
    check_no_nulls,
    check_unemployment_rate_range,
    run_all_checks,
)


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture()
def sample_raw_csv(tmp_path: Path) -> Path:
    """Crea un CSV crudo de ejemplo que simula el formato placeholder (BLS annual)."""
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
def sample_processed_df() -> pd.DataFrame:
    """Crea un DataFrame procesado de ejemplo."""
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


# ── Tests de estructura ──────────────────────────────────────────────


class TestProcessedDataset:
    """Tests sobre la estructura del dataset procesado."""

    def test_has_expected_columns(self, sample_processed_df: pd.DataFrame) -> None:
        """El dataset procesado tiene las columnas definidas en config."""
        assert list(sample_processed_df.columns) == PROCESSED_COLUMNS

    def test_columns_count(self, sample_processed_df: pd.DataFrame) -> None:
        """El número de columnas coincide con la especificación."""
        assert len(sample_processed_df.columns) == len(PROCESSED_COLUMNS)

    def test_no_empty_dataframe(self, sample_processed_df: pd.DataFrame) -> None:
        """El dataset no está vacío."""
        assert len(sample_processed_df) > 0


# ── Tests de limpieza ────────────────────────────────────────────────


class TestCleanUnemploymentData:
    """Tests de la función de limpieza."""

    def test_clean_returns_expected_columns(self, sample_raw_csv: Path) -> None:
        """La limpieza produce las columnas estándar."""
        df = clean_unemployment_data(sample_raw_csv)
        assert list(df.columns) == PROCESSED_COLUMNS

    def test_clean_filters_annual_average(self, sample_raw_csv: Path) -> None:
        """La limpieza produce una fila por año en el dataset placeholder."""
        df = clean_unemployment_data(sample_raw_csv)
        assert len(df) == 5  # 5 años en el fixture

    def test_clean_correct_types(self, sample_raw_csv: Path) -> None:
        """Las columnas tienen los tipos correctos."""
        df = clean_unemployment_data(sample_raw_csv)
        assert pd.api.types.is_numeric_dtype(df["unemployment_rate"])
        assert pd.api.types.is_integer_dtype(df["year"])
        assert pd.api.types.is_integer_dtype(df["month"])

    def test_clean_sorted_by_date(self, sample_raw_csv: Path) -> None:
        """El dataset está ordenado por fecha."""
        df = clean_unemployment_data(sample_raw_csv)
        dates = pd.to_datetime(df["date"])
        assert dates.is_monotonic_increasing


# ── Tests de calidad ─────────────────────────────────────────────────


class TestQualityChecks:
    """Tests de las validaciones de calidad."""

    def test_check_columns_pass(self, sample_processed_df: pd.DataFrame) -> None:
        """Pasa con columnas correctas."""
        check_columns(sample_processed_df)  # No debe lanzar excepción

    def test_check_columns_fail_missing(
        self, sample_processed_df: pd.DataFrame
    ) -> None:
        """Falla si faltan columnas."""
        df_bad = sample_processed_df.drop(columns=["unemployment_rate"])
        with pytest.raises(QualityCheckError, match="Columnas faltantes"):
            check_columns(df_bad)

    def test_check_no_nulls_pass(self, sample_processed_df: pd.DataFrame) -> None:
        """Pasa sin valores nulos."""
        check_no_nulls(sample_processed_df)

    def test_check_no_nulls_fail(self, sample_processed_df: pd.DataFrame) -> None:
        """Falla con valores nulos."""
        df_bad = sample_processed_df.copy()
        df_bad.loc[0, "unemployment_rate"] = None
        with pytest.raises(QualityCheckError, match="Valores nulos"):
            check_no_nulls(df_bad)

    def test_check_rate_range_pass(self, sample_processed_df: pd.DataFrame) -> None:
        """Pasa con valores en rango."""
        check_unemployment_rate_range(sample_processed_df)

    def test_check_rate_range_fail(self, sample_processed_df: pd.DataFrame) -> None:
        """Falla con valores fuera de rango."""
        df_bad = sample_processed_df.copy()
        df_bad.loc[0, "unemployment_rate"] = 99.0
        with pytest.raises(QualityCheckError, match="fuera de rango"):
            check_unemployment_rate_range(df_bad)

    def test_check_no_duplicates_pass(
        self, sample_processed_df: pd.DataFrame
    ) -> None:
        """Pasa sin duplicados."""
        check_no_duplicates(sample_processed_df)

    def test_check_no_duplicates_fail(
        self, sample_processed_df: pd.DataFrame
    ) -> None:
        """Falla con fechas duplicadas."""
        df_bad = pd.concat([sample_processed_df, sample_processed_df]).reset_index(
            drop=True
        )
        with pytest.raises(QualityCheckError, match="Fechas duplicadas"):
            check_no_duplicates(df_bad)

    def test_run_all_checks_pass(self, sample_processed_df: pd.DataFrame) -> None:
        """Todas las validaciones pasan con datos correctos."""
        assert run_all_checks(sample_processed_df) is True


# ── Tests de I/O ─────────────────────────────────────────────────────


class TestIO:
    """Tests de utilidades de entrada/salida."""

    def test_save_and_load_csv(
        self, tmp_path: Path, sample_processed_df: pd.DataFrame
    ) -> None:
        """Guardar y leer produce el mismo DataFrame."""
        path = tmp_path / "test_output.csv"
        save_csv(sample_processed_df, path)
        df_loaded = load_csv(path)
        # Parsear fecha después de leer para igualar tipos
        df_loaded["date"] = pd.to_datetime(df_loaded["date"])
        pd.testing.assert_frame_equal(
            sample_processed_df.reset_index(drop=True),
            df_loaded.reset_index(drop=True),
            check_dtype=False,  # CSV pierde algunos tipos numéricos
        )

    def test_load_csv_file_not_found(self, tmp_path: Path) -> None:
        """Lanza error si el archivo no existe."""
        with pytest.raises(FileNotFoundError):
            load_csv(tmp_path / "no_existe.csv")
