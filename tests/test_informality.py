"""Tests para src/sources/dane/informality.py y src/pipelines/run_informality.py.

Todos los tests son offline: usan fixtures sintéticas en tmp_path
que simulan la estructura real del Excel GEIHEISS del DANE.
"""

from __future__ import annotations

from datetime import date
from io import BytesIO
from pathlib import Path

import pandas as pd
import pytest

from src.config import (
    GEIH_INFORMALITY_CONFIG,
    INFORMALITY_PROCESSED_COLUMNS,
    GEIHInformalityConfig,
)
from src.sources.dane.informality import (
    _parse_trimestre_date,
    extract_informality_xlsx_link,
    parse_informality_excel,
)


# ═══════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════


def _build_synthetic_excel(tmp_path: Path) -> Path:
    """Crea un Excel sintético con la estructura real del GEIHEISS.

    Hoja 'Prop informalidad':
      fila 10 (year_row):      [label, 2021, NaN, NaN, NaN, 2022, NaN, NaN, ...]
      fila 11 (trimestre_row): [NaN, 'Ene - mar', 'Feb - abr', ...,
                                'Nov 21 - ene 22', 'Dic 21 - feb 22',
                                'Ene - mar ', ...]
      fila 13 (13 ciudades):   [label, val, val, ...]
    """
    n_cols = 15  # columna 0 = label, columnas 1-14 = datos

    # Fila 10: años con NaN intermedios
    year_row = [None] * n_cols
    year_row[0] = "Proporción de informalidad (%)"
    year_row[1] = 2021   # cols 1-10 → 2021 (10 trimestres)
    year_row[11] = 2022  # cols 11-12 → inter-año (Nov-ene, Dic-feb)
    year_row[13] = 2022  # col 13 → 2022 "Ene - mar"
    # los demás quedan en None para que se haga ffill

    # Fila 11: trimestres móviles
    trimestre_row = [None] * n_cols
    trimestres = [
        "Ene - mar", "Feb - abr", "Mar - may", "Abr - jun",
        "May - jul", "Jun - ago", "Jul - sep", "Ago - oct",
        "Sep - nov", "Oct - dic",
        "Nov 21 - ene 22", "Dic 21 - feb 22",
        "Ene - mar ", "Feb - abr ",
    ]
    for i, t in enumerate(trimestres, start=1):
        trimestre_row[i] = t

    # Fila 12: Total nacional (relleno)
    total_row = ["Total nacional"] + [60.0 + i * 0.1 for i in range(n_cols - 1)]

    # Fila 13: 13 Ciudades y A.M. — valores sintéticos
    cities_row = ["13 Ciudades y A.M."] + [45.0 + i * 0.1 for i in range(n_cols - 1)]

    # Fila 14: 23 Ciudades y A.M. (relleno)
    cities23_row = ["23 Ciudades y A.M."] + [47.0 + i * 0.1 for i in range(n_cols - 1)]

    # Construir DataFrame con 20 filas (índice 0-19)
    # Usamos un dict para construir las filas con índices explícitos
    all_rows = {}
    for i in range(20):
        all_rows[i] = [""] * n_cols
    all_rows[10] = year_row
    all_rows[11] = trimestre_row
    all_rows[12] = total_row
    all_rows[13] = cities_row
    all_rows[14] = cities23_row

    df_raw = pd.DataFrame([all_rows[i] for i in range(20)])

    path = tmp_path / "geiheiss_test.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df_raw.to_excel(writer, sheet_name="Prop informalidad", index=False, header=False)
    return path


@pytest.fixture()
def synthetic_excel(tmp_path: Path) -> Path:
    return _build_synthetic_excel(tmp_path)


@pytest.fixture()
def config_test() -> GEIHInformalityConfig:
    return GEIH_INFORMALITY_CONFIG


# ═══════════════════════════════════════════════════════════════════════
# Tests: _parse_trimestre_date
# ═══════════════════════════════════════════════════════════════════════


class TestParseTrimestresDate:
    """Verifica la conversión de strings de trimestre → fecha."""

    def test_normal_ene_mar(self) -> None:
        result = _parse_trimestre_date("Ene - mar", 2021)
        assert result == date(2021, 3, 1)

    def test_normal_oct_dic(self) -> None:
        result = _parse_trimestre_date("Oct - dic", 2021)
        assert result == date(2021, 12, 1)

    def test_normal_abr_jun(self) -> None:
        result = _parse_trimestre_date("Abr - jun", 2022)
        assert result == date(2022, 6, 1)

    def test_crossyear_nov_ene(self) -> None:
        """Nov 21 - ene 22 → enero 2022."""
        result = _parse_trimestre_date("Nov 21 - ene 22", 2021)
        assert result == date(2022, 1, 1)

    def test_crossyear_dic_feb(self) -> None:
        """Dic 21 - feb 22 → febrero 2022."""
        result = _parse_trimestre_date("Dic 21 - feb 22", 2021)
        assert result == date(2022, 2, 1)

    def test_trailing_space(self) -> None:
        """Los trimestres del Excel a veces tienen espacios al final."""
        result = _parse_trimestre_date("Ene - mar ", 2022)
        assert result == date(2022, 3, 1)

    def test_nan_returns_none(self) -> None:
        assert _parse_trimestre_date("nan", 2021) is None

    def test_empty_returns_none(self) -> None:
        assert _parse_trimestre_date("", 2021) is None

    def test_lowercase(self) -> None:
        result = _parse_trimestre_date("ene - mar", 2021)
        assert result == date(2021, 3, 1)

    def test_all_months_normal(self) -> None:
        """Verifica que los 10 trimestres dentro de un año se parsean bien."""
        trimestres = [
            ("Ene - mar", 3), ("Feb - abr", 4), ("Mar - may", 5),
            ("Abr - jun", 6), ("May - jul", 7), ("Jun - ago", 8),
            ("Jul - sep", 9), ("Ago - oct", 10), ("Sep - nov", 11),
            ("Oct - dic", 12),
        ]
        for t_str, expected_month in trimestres:
            result = _parse_trimestre_date(t_str, 2023)
            assert result == date(2023, expected_month, 1), f"Fallo en: {t_str!r}"


# ═══════════════════════════════════════════════════════════════════════
# Tests: extract_informality_xlsx_link
# ═══════════════════════════════════════════════════════════════════════


class TestExtractLink:
    """Verifica la extracción del enlace GEIHEISS desde HTML."""

    def _make_html(self, hrefs: list[str]) -> str:
        links = "".join(f'<a href="{h}">Anexos</a>' for h in hrefs)
        return f"<html><body>{links}</body></html>"

    def test_extracts_geiheiss_link(self) -> None:
        html = self._make_html([
            "/files/operaciones/GEIH/anex-GEIHEISS-dic2025-feb2026.xlsx",
        ])
        url = extract_informality_xlsx_link(html)
        assert "GEIHEISS" in url
        assert url.startswith("https://www.dane.gov.co")

    def test_ignores_non_matching(self) -> None:
        html = self._make_html([
            "/files/operaciones/GEIH/anex-GEIH-Desestacionalizado-ene2026.xlsx",
            "/files/operaciones/GEIH/anex-GEIHEISS-dic2025-feb2026.xlsx",
        ])
        url = extract_informality_xlsx_link(html)
        assert "GEIHEISS" in url

    def test_raises_if_no_link(self) -> None:
        html = self._make_html(["/files/otro.xlsx"])
        with pytest.raises(ValueError, match="GEIHEISS"):
            extract_informality_xlsx_link(html)


# ═══════════════════════════════════════════════════════════════════════
# Tests: parse_informality_excel
# ═══════════════════════════════════════════════════════════════════════


class TestParseInformalityExcel:
    """Tests sobre el parser del Excel GEIHEISS."""

    def test_returns_dataframe(self, synthetic_excel: Path) -> None:
        df = parse_informality_excel(synthetic_excel)
        assert isinstance(df, pd.DataFrame)

    def test_not_empty(self, synthetic_excel: Path) -> None:
        df = parse_informality_excel(synthetic_excel)
        assert len(df) > 0

    def test_required_columns(self, synthetic_excel: Path) -> None:
        df = parse_informality_excel(synthetic_excel)
        for col in INFORMALITY_PROCESSED_COLUMNS:
            assert col in df.columns, f"Columna faltante: {col}"

    def test_date_dtype(self, synthetic_excel: Path) -> None:
        df = parse_informality_excel(synthetic_excel)
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_sorted_by_date(self, synthetic_excel: Path) -> None:
        df = parse_informality_excel(synthetic_excel)
        assert df["date"].is_monotonic_increasing

    def test_no_duplicate_dates(self, synthetic_excel: Path) -> None:
        df = parse_informality_excel(synthetic_excel)
        assert df["date"].nunique() == len(df)

    def test_informality_values_in_range(self, synthetic_excel: Path) -> None:
        df = parse_informality_excel(synthetic_excel)
        assert df["informality_rate_13c"].between(0, 100).all()

    def test_year_month_match_date(self, synthetic_excel: Path) -> None:
        df = parse_informality_excel(synthetic_excel)
        assert (df["year"] == df["date"].dt.year).all()
        assert (df["month"] == df["date"].dt.month).all()

    def test_crossyear_dates_parsed(self, synthetic_excel: Path) -> None:
        """Los trimestres inter-año (Nov-ene, Dic-feb) deben dar fechas en el año siguiente."""
        df = parse_informality_excel(synthetic_excel)
        # El trimestre "Nov 21 - ene 22" debe producir 2022-01-01
        jan2022 = df[df["date"] == pd.Timestamp("2022-01-01")]
        assert len(jan2022) == 1, "Falta 2022-01-01 (Nov 21 - ene 22)"
        # El trimestre "Dic 21 - feb 22" debe producir 2022-02-01
        feb2022 = df[df["date"] == pd.Timestamp("2022-02-01")]
        assert len(feb2022) == 1, "Falta 2022-02-01 (Dic 21 - feb 22)"

    def test_source_label(self, synthetic_excel: Path) -> None:
        df = parse_informality_excel(synthetic_excel)
        assert (df["source"] == "DANE GEIH-EISS").all()

    def test_raises_if_wrong_sheet(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.xlsx"
        pd.DataFrame({"A": [1]}).to_excel(path, sheet_name="OtraHoja", index=False)
        with pytest.raises(Exception):
            parse_informality_excel(path)


# ═══════════════════════════════════════════════════════════════════════
# Tests: GEIHInformalityConfig
# ═══════════════════════════════════════════════════════════════════════


class TestInformalityConfig:
    def test_page_url_contains_dane(self) -> None:
        assert "dane.gov.co" in GEIH_INFORMALITY_CONFIG.page_url

    def test_link_pattern_matches_geiheiss(self) -> None:
        import re
        pattern = re.compile(GEIH_INFORMALITY_CONFIG.link_pattern, re.IGNORECASE)
        assert pattern.search(
            "/files/operaciones/GEIH/anex-GEIHEISS-dic2025-feb2026.xlsx"
        )

    def test_processed_filename(self) -> None:
        assert GEIH_INFORMALITY_CONFIG.processed_filename == "dane_informality_colombia.csv"

    def test_sheet_name(self) -> None:
        assert GEIH_INFORMALITY_CONFIG.sheet_name == "Prop informalidad"

    def test_year_row_index(self) -> None:
        assert GEIH_INFORMALITY_CONFIG.year_row == 10

    def test_trimestre_row_index(self) -> None:
        assert GEIH_INFORMALITY_CONFIG.trimestre_row == 11
