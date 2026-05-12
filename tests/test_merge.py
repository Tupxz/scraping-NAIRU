"""Tests para src/merge.py — pipeline de unificación de datasets.

Verifica que merge_all_sources():
  - carga correctamente cada fuente (incluyendo las nuevas: PWT, TGP, PET)
  - produce las columnas esperadas en el orden definido por MERGED_COLUMNS
  - maneja ausencia de fuentes opcionales sin fallar
  - deja NaN en las columnas anuales (PWT) para los meses sin dato
  - elimina duplicados de fecha correctamente

Todos los tests son offline: usan CSVs sintéticos en tmp_path.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.merge import (
    MERGED_COLUMNS,
    MERGED_FILENAME,
    _SOURCES,
    _load_source,
    merge_all_sources,
    run_merge_pipeline,
    save_merged_dataset,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers para crear mini-CSVs sintéticos
# ═══════════════════════════════════════════════════════════════════════

_MONTHLY_DATES = [f"200{y}-0{m}-01" for y in range(4, 7) for m in range(1, 5)]
# 12 fechas mensuales: 2004-01 → 2006-04  (dentro del rango >= 2004-01-01)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _make_labor_csv(processed_dir: Path) -> None:
    """dane_labor_colombia.csv — TD + TGP + PET (mensual)."""
    df = pd.DataFrame({
        "date": pd.to_datetime(_MONTHLY_DATES),
        "year": [d[:4] for d in _MONTHLY_DATES],
        "month": [d[5:7] for d in _MONTHLY_DATES],
        "unemployment_rate": [10.0 + i * 0.1 for i in range(12)],
        "tgp_rate": [63.0 + i * 0.05 for i in range(12)],
        "pet_thousands": [38_000.0 + i * 10 for i in range(12)],
        "source": ["DANE"] * 12,
        "download_date": ["2026-01-01"] * 12,
    })
    _write_csv(processed_dir / "dane_labor_colombia.csv", df)


def _make_ipc_csv(processed_dir: Path) -> None:
    df = pd.DataFrame({
        "date": pd.to_datetime(_MONTHLY_DATES),
        "year": [d[:4] for d in _MONTHLY_DATES],
        "month": [d[5:7] for d in _MONTHLY_DATES],
        "ipc_index": [100.0 + i * 0.2 for i in range(12)],
        "source": ["DANE"] * 12,
        "download_date": ["2026-01-01"] * 12,
    })
    _write_csv(processed_dir / "ipc_colombia.csv", df)


def _make_brent_csv(processed_dir: Path) -> None:
    df = pd.DataFrame({
        "date": pd.to_datetime(_MONTHLY_DATES),
        "year": [d[:4] for d in _MONTHLY_DATES],
        "month": [d[5:7] for d in _MONTHLY_DATES],
        "brent_usd_per_barrel": [50.0 + i for i in range(12)],
        "source": ["EIA"] * 12,
        "download_date": ["2026-01-01"] * 12,
    })
    _write_csv(processed_dir / "brent_colombia.csv", df)


def _make_pwt_csv(processed_dir: Path) -> None:
    """pwt_colombia.csv — anual (solo enero de cada año).

    Valores realistas para Colombia (PWT 11.0):
      - capital_stock_real (rnna) ≈ 0.9–2.7 millones (millones COP 2017)
      - depreciation_rate (delta) ≈ 0.047–0.048 (fracción)
      - human_capital (hc) ≈ 1.9–2.6 (índice)
    """
    annual_dates = ["2004-01-01", "2005-01-01", "2006-01-01"]
    df = pd.DataFrame({
        "date": pd.to_datetime(annual_dates),
        "year": [2004, 2005, 2006],
        "month": [1, 1, 1],
        "capital_stock_real": [900_000.0, 950_000.0, 1_000_000.0],
        "depreciation_rate":  [0.0478, 0.0479, 0.0480],
        "human_capital":      [2.5, 2.6, 2.7],
        "source": ["PWT 11.0"] * 3,
        "download_date": ["2026-01-01"] * 3,
    })
    _write_csv(processed_dir / "pwt_colombia.csv", df)


@pytest.fixture()
def processed_dir_full(tmp_path: Path) -> Path:
    """Directorio tmp con labor + ipc + brent + pwt (sin inflation, andi, tes)."""
    _make_labor_csv(tmp_path)
    _make_ipc_csv(tmp_path)
    _make_brent_csv(tmp_path)
    _make_pwt_csv(tmp_path)
    return tmp_path


@pytest.fixture()
def processed_dir_labor_only(tmp_path: Path) -> Path:
    """Solo dane_labor_colombia.csv (mínimo requerido para que merge no falle)."""
    _make_labor_csv(tmp_path)
    return tmp_path


@pytest.fixture()
def processed_dir_pwt_only(tmp_path: Path) -> Path:
    """Solo pwt_colombia.csv."""
    _make_pwt_csv(tmp_path)
    return tmp_path


# ═══════════════════════════════════════════════════════════════════════
# Tests sobre _SOURCES — estructura del mapeo de fuentes
# ═══════════════════════════════════════════════════════════════════════


class TestSourcesConfig:
    """Verifica que _SOURCES tiene las entradas correctas."""

    def test_unemployment_source_uses_new_filename(self) -> None:
        filename, _ = _SOURCES["unemployment"]
        assert filename == "dane_labor_colombia.csv"

    def test_unemployment_includes_tgp_and_pet(self) -> None:
        _, cols = _SOURCES["unemployment"]
        assert "tgp_rate" in cols
        assert "pet_thousands" in cols
        assert "unemployment_rate" in cols

    def test_pwt_source_exists(self) -> None:
        assert "pwt" in _SOURCES

    def test_pwt_source_columns(self) -> None:
        _, cols = _SOURCES["pwt"]
        assert "capital_stock_real" in cols
        assert "depreciation_rate" in cols
        assert "human_capital" in cols

    def test_pwt_filename(self) -> None:
        filename, _ = _SOURCES["pwt"]
        assert filename == "pwt_colombia.csv"

    def test_viog_us_source_exists(self) -> None:
        assert "viog_us" in _SOURCES
        filename, _ = _SOURCES["viog_us"]
        assert filename == "viog_usa.csv"

    def test_viog_us_renames_to_country_suffix(self) -> None:
        """VIOG-USA renombra columnas con sufijo '_us' para distinguir de Colombia."""
        _, cols = _SOURCES["viog_us"]
        assert isinstance(cols, dict)
        assert cols["gap_viog"] == "gap_viog_us"
        assert cols["gap_inv_viog"] == "gap_inv_viog_us"

    def test_viog_co_source_exists(self) -> None:
        assert "viog_co" in _SOURCES
        filename, _ = _SOURCES["viog_co"]
        assert filename == "viog_colombia.csv"

    def test_viog_co_renames_to_country_suffix(self) -> None:
        """VIOG-Colombia renombra columnas con sufijo '_co'."""
        _, cols = _SOURCES["viog_co"]
        assert isinstance(cols, dict)
        assert cols["gap_viog"] == "gap_viog_co"
        assert cols["gap_inv_viog"] == "gap_inv_viog_co"


# ═══════════════════════════════════════════════════════════════════════
# Tests sobre MERGED_COLUMNS — orden y contenido
# ═══════════════════════════════════════════════════════════════════════


class TestMergedColumnsDefinition:
    """Verifica la definición estática de MERGED_COLUMNS."""

    def test_starts_with_date_year_month(self) -> None:
        assert MERGED_COLUMNS[:3] == ["date", "year", "month"]

    def test_includes_labor_columns(self) -> None:
        assert "unemployment_rate" in MERGED_COLUMNS
        assert "tgp_rate" in MERGED_COLUMNS
        assert "pet_thousands" in MERGED_COLUMNS

    def test_includes_pwt_columns(self) -> None:
        assert "capital_stock_real" in MERGED_COLUMNS
        assert "depreciation_rate" in MERGED_COLUMNS
        assert "human_capital" in MERGED_COLUMNS

    def test_pwt_columns_are_last(self) -> None:
        """Las columnas anuales (PWT) deben ir al final, tras las mensuales."""
        idx_pet = MERGED_COLUMNS.index("pet_thousands")
        idx_k = MERGED_COLUMNS.index("capital_stock_real")
        idx_delta = MERGED_COLUMNS.index("depreciation_rate")
        idx_hc = MERGED_COLUMNS.index("human_capital")
        # PWT va después de las series mensuales
        assert idx_k > idx_pet
        assert idx_delta > idx_pet
        assert idx_hc > idx_pet

    def test_no_duplicate_columns(self) -> None:
        assert len(MERGED_COLUMNS) == len(set(MERGED_COLUMNS))

    def test_total_column_count(self) -> None:
        # date + year + month + 3 labor + ipc + 3 inflation +
        # brent + andi + 2 tes + 3 pwt + 2 viog_us + 2 viog_co = 21
        # (informalidad eliminada en refactor 2026-05-07)
        assert len(MERGED_COLUMNS) == 21


# ═══════════════════════════════════════════════════════════════════════
# Tests sobre merge_all_sources()
# ═══════════════════════════════════════════════════════════════════════


class TestMergeAllSources:
    """Tests de integración sobre merge_all_sources con fixtures."""

    def test_merge_returns_dataframe(self, processed_dir_full: Path) -> None:
        df = merge_all_sources(processed_dir_full)
        assert isinstance(df, pd.DataFrame)

    def test_merge_not_empty(self, processed_dir_full: Path) -> None:
        df = merge_all_sources(processed_dir_full)
        assert len(df) > 0

    def test_merge_includes_labor_columns(self, processed_dir_full: Path) -> None:
        """tgp_rate y pet_thousands deben estar en el resultado."""
        df = merge_all_sources(processed_dir_full)
        assert "tgp_rate" in df.columns
        assert "pet_thousands" in df.columns
        assert "unemployment_rate" in df.columns

    def test_merge_includes_pwt_columns(self, processed_dir_full: Path) -> None:
        """capital_stock_real, depreciation_rate y human_capital deben estar."""
        df = merge_all_sources(processed_dir_full)
        assert "capital_stock_real" in df.columns
        assert "depreciation_rate" in df.columns
        assert "human_capital" in df.columns

    def test_merge_column_order(self, processed_dir_full: Path) -> None:
        """El DataFrame resultante respeta el orden de MERGED_COLUMNS."""
        df = merge_all_sources(processed_dir_full)
        present_expected = [c for c in MERGED_COLUMNS if c in df.columns]
        assert list(df.columns) == present_expected

    def test_merge_date_dtype(self, processed_dir_full: Path) -> None:
        df = merge_all_sources(processed_dir_full)
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_merge_sorted_by_date(self, processed_dir_full: Path) -> None:
        df = merge_all_sources(processed_dir_full)
        assert df["date"].is_monotonic_increasing

    def test_merge_year_month_reconstructed(self, processed_dir_full: Path) -> None:
        """year y month se reconstruyen del date, no del CSV fuente."""
        df = merge_all_sources(processed_dir_full)
        assert (df["year"] == df["date"].dt.year).all()
        assert (df["month"] == df["date"].dt.month).all()

    def test_merge_pwt_annual_nulls(self, processed_dir_full: Path) -> None:
        """Los meses que no son enero deben tener NaN en capital_stock_real.

        PWT es anual (solo enero); los meses feb–dic aparecen en el outer-join
        con NaN para las columnas de capital.
        """
        df = merge_all_sources(processed_dir_full)
        non_january = df[df["month"] != 1]
        if len(non_january) > 0:
            # Para los meses no-enero provenientes de fuentes mensuales,
            # las columnas PWT deben ser NaN
            assert non_january["capital_stock_real"].isna().all()
            assert non_january["depreciation_rate"].isna().all()
            assert non_january["human_capital"].isna().all()

    def test_merge_pwt_january_has_values(self, processed_dir_full: Path) -> None:
        """Los meses de enero (que PWT reporta) deben tener valores no-NaN."""
        df = merge_all_sources(processed_dir_full)
        january_rows = df[
            (df["month"] == 1) & df["date"].dt.year.isin([2004, 2005, 2006])
        ]
        assert january_rows["capital_stock_real"].notna().all()
        assert january_rows["depreciation_rate"].notna().all()
        assert january_rows["human_capital"].notna().all()

    def test_merge_labor_values_correct(self, processed_dir_full: Path) -> None:
        """Verifica que los valores de TGP y PET se preservan tras el merge."""
        df = merge_all_sources(processed_dir_full)
        jan2004 = df[df["date"] == pd.Timestamp("2004-01-01")]
        assert len(jan2004) == 1
        assert jan2004["tgp_rate"].iloc[0] == pytest.approx(63.0, abs=0.01)
        assert jan2004["pet_thousands"].iloc[0] == pytest.approx(38_000.0, abs=1.0)

    def test_merge_missing_source_skipped(self, processed_dir_labor_only: Path) -> None:
        """Si faltan archivos opcionales, el merge no lanza error."""
        df = merge_all_sources(processed_dir_labor_only)
        assert "unemployment_rate" in df.columns
        # Las columnas de fuentes ausentes no están en el resultado
        assert "capital_stock_real" not in df.columns

    def test_merge_no_sources_raises(self, tmp_path: Path) -> None:
        """Si no hay ningún archivo procesado, debe lanzar ValueError."""
        with pytest.raises(ValueError, match="No se encontró"):
            merge_all_sources(tmp_path)


# ═══════════════════════════════════════════════════════════════════════
# Tests sobre save_merged_dataset() y run_merge_pipeline()
# ═══════════════════════════════════════════════════════════════════════


class TestSaveMergedDataset:
    """Tests de guardado del dataset unificado."""

    def test_save_creates_file(self, processed_dir_full: Path) -> None:
        df = merge_all_sources(processed_dir_full)
        path = save_merged_dataset(df, processed_dir_full)
        assert path.exists()
        assert path.name == MERGED_FILENAME

    def test_save_csv_readable(self, processed_dir_full: Path) -> None:
        df = merge_all_sources(processed_dir_full)
        path = save_merged_dataset(df, processed_dir_full)
        df_loaded = pd.read_csv(path, parse_dates=["date"])
        assert len(df_loaded) == len(df)
        assert "capital_stock_real" in df_loaded.columns

    def test_run_merge_pipeline_returns_dataframe(
        self, processed_dir_full: Path, tmp_path: Path
    ) -> None:
        # output_dir distinto de processed_dir para reflejar la separación real
        # processed/ (insumos del merge) vs final/ (dataset consolidado).
        final_dir = tmp_path / "final"
        df = run_merge_pipeline(processed_dir_full, output_dir=final_dir)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert (final_dir / MERGED_FILENAME).exists()


# ═══════════════════════════════════════════════════════════════════════
# Tests sobre _load_source — manejo de renombrado de columnas
# ═══════════════════════════════════════════════════════════════════════


def _make_viog_csv(processed_dir: Path, filename: str, suffix: str) -> None:
    """Genera un viog_*.csv sintético con columnas (date, gap_viog, gap_inv_viog, ...)."""
    quarterly = ["2020-01-01", "2020-04-01", "2020-07-01", "2020-10-01"]
    df = pd.DataFrame({
        "date": pd.to_datetime(quarterly),
        "year": [2020] * 4,
        "quarter": [1, 2, 3, 4],
        "gap_viog": [0.01 * i for i in range(4)],
        "gap_inv_viog": [-0.01 * i for i in range(4)],
        "source": [f"test-{suffix}"] * 4,
    })
    df.to_csv(processed_dir / filename, index=False)


class TestLoadSourceRenaming:
    """Tests para el soporte de renombrado en _load_source."""

    def test_dict_spec_renames_columns(self, tmp_path: Path) -> None:
        _make_viog_csv(tmp_path, "viog_usa.csv", "us")
        df = _load_source(
            "viog_us",
            "viog_usa.csv",
            {"gap_viog": "gap_viog_us", "gap_inv_viog": "gap_inv_viog_us"},
            processed_dir=tmp_path,
        )
        assert df is not None
        assert "gap_viog_us" in df.columns
        assert "gap_inv_viog_us" in df.columns
        assert "gap_viog" not in df.columns
        assert "gap_inv_viog" not in df.columns

    def test_list_spec_keeps_original_names(self, tmp_path: Path) -> None:
        _make_labor_csv(tmp_path)
        df = _load_source(
            "unemployment",
            "dane_labor_colombia.csv",
            ["unemployment_rate", "tgp_rate"],
            processed_dir=tmp_path,
        )
        assert df is not None
        assert "unemployment_rate" in df.columns
        assert "tgp_rate" in df.columns

    def test_returns_none_when_file_missing(self, tmp_path: Path) -> None:
        df = _load_source(
            "viog_co",
            "viog_colombia.csv",  # no existe
            {"gap_viog": "gap_viog_co", "gap_inv_viog": "gap_inv_viog_co"},
            processed_dir=tmp_path,
        )
        assert df is None

    def test_merge_with_both_viogs_creates_4_columns(self, tmp_path: Path) -> None:
        """Si ambos viog_*.csv están presentes, el merge incluye us y co."""
        _make_labor_csv(tmp_path)
        _make_viog_csv(tmp_path, "viog_usa.csv", "us")
        _make_viog_csv(tmp_path, "viog_colombia.csv", "co")

        merged = merge_all_sources(tmp_path)
        for col in ("gap_viog_us", "gap_inv_viog_us", "gap_viog_co", "gap_inv_viog_co"):
            assert col in merged.columns, f"Falta columna {col}"
        # Las viejas (sin sufijo) NO deben existir
        assert "gap_viog" not in merged.columns
        assert "gap_inv_viog" not in merged.columns
