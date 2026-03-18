"""Tests para el pipeline de desempleo GEIH del DANE (fuente real).

Verifica:
- Scraping de enlaces GEIH desde HTML simulado
- Selección del enlace correcto (más reciente)
- Extracción de periodo desde el nombre del archivo
- Parsing del Excel pivoteado (conceptos × año·mes → formato largo)
- Detección automática de filas (años, meses, TD)
- Reconstrucción de columnas fecha (forward-fill de años)
- Validaciones de calidad sobre el resultado
- Pipeline completo offline (con fixtures)
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.config import GEIHConfig, PROCESSED_COLUMNS
from src.sources.dane.unemployment import (
    _build_date_columns,
    _detect_month_row,
    _detect_td_row,
    _detect_year_row,
    clean_geih_data,
    extract_geih_xlsx_links,
    load_geih_excel,
    parse_period_from_geih_href,
    select_geih_link,
)
from src.quality_checks import (
    QualityCheckError,
    run_all_checks,
)


# ═══════════════════════════════════════════════════════════════════════
# HTML fixture que simula la página del DANE
# ═══════════════════════════════════════════════════════════════════════

SAMPLE_GEIH_HTML = """
<html><body>
<table>
  <tr>
    <td>Boletín técnico</td><td>27/02/2026</td><td>PDF</td>
    <td><a href="/files/operaciones/GEIH/bol-GEIH-ene2026.pdf">Descargar</a></td>
  </tr>
  <tr>
    <td>Anexos</td><td>27/02/2026</td><td>XLSX</td>
    <td><a href="/files/operaciones/GEIH/anex-GEIH-ene2026.xlsx">Descargar</a></td>
  </tr>
  <tr>
    <td>Anexo desestacionalizado</td><td>27/02/2026</td><td>XLSX</td>
    <td><a href="/files/operaciones/GEIH/anex-GEIH-Desestacionalizado-ene2026.xlsx">Descargar</a></td>
  </tr>
  <tr>
    <td>Acerca de</td><td>27/03/2024</td><td>PDF</td>
    <td><a href="/files/operaciones/GEIH/anex-GEIH-acercade-ene2024.pdf">Descargar</a></td>
  </tr>
</table>
</body></html>
"""

SAMPLE_GEIH_HTML_MULTI = """
<html><body>
<a href="/files/operaciones/GEIH/anex-GEIH-dic2025.xlsx">Descargar</a>
<a href="/files/operaciones/GEIH/anex-GEIH-ene2026.xlsx">Descargar</a>
<a href="/files/operaciones/GEIH/anex-GEIH-nov2025.xlsx">Descargar</a>
</body></html>
"""


# ═══════════════════════════════════════════════════════════════════════
# Excel fixture que simula el formato GEIH "Total nacional"
# ═══════════════════════════════════════════════════════════════════════

def _create_geih_xlsx(path: Path, num_years: int = 3, start_year: int = 2022) -> Path:
    """Crea un Excel que simula la hoja 'Total nacional' del GEIH.

    Layout:
    - Filas 0-10: títulos / basura institucional
    - Fila 11: años  →  [Concepto, 2022, None×11, 2023, None×11, 2024, None×11]
    - Fila 12: meses →  [None, Ene, Feb, ..., Dic, Ene, Feb, ..., Dic, ...]
    - Fila 13: % PET (relleno)
    - Fila 14: TGP (relleno)
    - Fila 15: TO (relleno)
    - Fila 16: TD (Tasa de Desocupación) ← TARGET
    - Filas 17+: más indicadores / notas
    """
    months_abbr = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
                    "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

    total_data_cols = num_years * 12
    total_cols = 1 + total_data_cols  # col 0 = "Concepto"

    rows: list[list] = []

    # Filas basura (0-10)
    for i in range(11):
        row = [None] * total_cols
        if i == 5:
            row[0] = "Gran Encuesta Integrada de Hogares - GEIH"
        elif i == 10:
            row[0] = "Total Nacional"
        rows.append(row)

    # Fila 11: años
    year_row = [None] * total_cols
    year_row[0] = "Concepto"
    for y_idx in range(num_years):
        col = 1 + y_idx * 12
        year_row[col] = start_year + y_idx
    rows.append(year_row)

    # Fila 12: meses
    month_row = [None] * total_cols
    for y_idx in range(num_years):
        for m_idx in range(12):
            col = 1 + y_idx * 12 + m_idx
            month_row[col] = months_abbr[m_idx]
    rows.append(month_row)

    # Fila 13: % PET (relleno)
    pet_row: list = ["% población en edad de trabajar"]
    for _ in range(total_data_cols):
        pet_row.append(75.5)
    rows.append(pet_row)

    # Fila 14: TGP (relleno)
    tgp_row: list = ["Tasa Global de Participación (TGP)"]
    for _ in range(total_data_cols):
        tgp_row.append(63.2)
    rows.append(tgp_row)

    # Fila 15: TO (relleno)
    to_row: list = ["Tasa de Ocupación (TO)"]
    for _ in range(total_data_cols):
        to_row.append(56.8)
    rows.append(to_row)

    # Fila 16: TD (target)
    td_row: list = ["Tasa de Desocupación (TD)"]
    for y_idx in range(num_years):
        for m_idx in range(12):
            # Generar tasas realistas: base 10% con estacionalidad
            base = 10.0 + y_idx * 0.5
            seasonal = 2.0 if m_idx in (0, 1) else -1.0 if m_idx in (5, 6, 7) else 0.0
            td_row.append(round(base + seasonal + m_idx * 0.1, 2))
    rows.append(td_row)

    # Filas 17+: notas
    note_row = [None] * total_cols
    note_row[0] = "Fuente: DANE"
    rows.append(note_row)

    df = pd.DataFrame(rows)
    df.to_excel(path, index=False, header=False, engine="openpyxl",
                sheet_name="Total nacional")
    return path


@pytest.fixture()
def geih_config() -> GEIHConfig:
    """Config GEIH para tests (apunta a fixture local)."""
    return GEIHConfig(
        sheet_name="Total nacional",
        year_row=11,
        month_row=12,
    )


@pytest.fixture()
def geih_config_autodetect() -> GEIHConfig:
    """Config GEIH con auto-detección de filas."""
    return GEIHConfig(
        sheet_name="Total nacional",
        year_row=None,
        month_row=None,
    )


@pytest.fixture()
def sample_geih_xlsx(tmp_path: Path) -> Path:
    """Crea un Excel GEIH simulado con 3 años de datos."""
    return _create_geih_xlsx(tmp_path / "geih_test.xlsx", num_years=3, start_year=2022)


@pytest.fixture()
def sample_geih_xlsx_partial(tmp_path: Path) -> Path:
    """Crea un Excel GEIH simulado con 1 año (parcial: solo 6 meses)."""
    path = tmp_path / "geih_partial.xlsx"
    months_abbr = ["Ene", "Feb", "Mar", "Abr", "May", "Jun"]
    total_cols = 1 + len(months_abbr)

    rows: list[list] = []
    for i in range(11):
        row = [None] * total_cols
        if i == 5:
            row[0] = "Gran Encuesta Integrada de Hogares"
        rows.append(row)

    year_row = [None] * total_cols
    year_row[0] = "Concepto"
    year_row[1] = 2025
    rows.append(year_row)

    month_row = [None] * total_cols
    for j, m in enumerate(months_abbr):
        month_row[1 + j] = m
    rows.append(month_row)

    # PET, TGP, TO
    for label in ["% PET", "TGP", "TO"]:
        r: list = [label] + [50.0] * len(months_abbr)
        rows.append(r)

    # TD
    td: list = ["Tasa de Desocupación (TD)"] + [11.5, 12.0, 10.8, 9.5, 9.2, 8.8]
    rows.append(td)

    rows.append(["Fuente: DANE"] + [None] * len(months_abbr))

    df = pd.DataFrame(rows)
    df.to_excel(path, index=False, header=False, engine="openpyxl",
                sheet_name="Total nacional")
    return path


# ═══════════════════════════════════════════════════════════════════════
# Tests de Scraping
# ═══════════════════════════════════════════════════════════════════════


class TestGEIHScraping:
    """Tests de extracción de enlaces GEIH desde HTML."""

    def test_extract_geih_links_finds_correct_one(self) -> None:
        """Solo extrae el anexo principal (no PDFs, no Desestacionalizado)."""
        links = extract_geih_xlsx_links(SAMPLE_GEIH_HTML)
        assert len(links) == 1
        assert "anex-GEIH-ene2026.xlsx" in links[0]["href"]

    def test_extract_links_empty_html(self) -> None:
        """HTML sin enlaces devuelve lista vacía."""
        links = extract_geih_xlsx_links("<html><body></body></html>")
        assert links == []

    def test_select_geih_link_raises_if_empty(self) -> None:
        """Error si no hay enlaces disponibles."""
        with pytest.raises(ValueError, match="No se encontraron"):
            select_geih_link([])

    def test_select_geih_link_picks_most_recent(self) -> None:
        """Si hay varios enlaces, elige el más reciente."""
        links = extract_geih_xlsx_links(SAMPLE_GEIH_HTML_MULTI)
        selected = select_geih_link(links)
        assert "ene2026" in selected["href"]

    def test_parse_period_from_geih_href(self) -> None:
        """Extrae año y mes del nombre del archivo GEIH."""
        assert parse_period_from_geih_href(
            "/files/operaciones/GEIH/anex-GEIH-ene2026.xlsx"
        ) == (2026, 1)
        assert parse_period_from_geih_href(
            "/files/operaciones/GEIH/anex-GEIH-dic2025.xlsx"
        ) == (2025, 12)
        assert parse_period_from_geih_href("random.pdf") is None

    def test_extract_links_have_absolute_url(self) -> None:
        """Los enlaces extraídos tienen URL absoluta."""
        links = extract_geih_xlsx_links(SAMPLE_GEIH_HTML)
        for lnk in links:
            assert lnk["url"].startswith("https://www.dane.gov.co/")


# ═══════════════════════════════════════════════════════════════════════
# Tests de detección de filas
# ═══════════════════════════════════════════════════════════════════════


class TestGEIHRowDetection:
    """Tests de detección de filas de años, meses y TD."""

    def test_detect_year_row(self, sample_geih_xlsx: Path, geih_config: GEIHConfig) -> None:
        """Detecta la fila de años correctamente."""
        df_raw = pd.read_excel(
            sample_geih_xlsx, sheet_name=geih_config.sheet_name,
            header=None, engine="openpyxl",
        )
        row = _detect_year_row(df_raw)
        assert row == 11

    def test_detect_month_row(self, sample_geih_xlsx: Path, geih_config: GEIHConfig) -> None:
        """Detecta la fila de meses correctamente."""
        df_raw = pd.read_excel(
            sample_geih_xlsx, sheet_name=geih_config.sheet_name,
            header=None, engine="openpyxl",
        )
        row = _detect_month_row(df_raw, year_row=11)
        assert row == 12

    def test_detect_td_row(self, sample_geih_xlsx: Path, geih_config: GEIHConfig) -> None:
        """Detecta la fila de Tasa de Desocupación."""
        df_raw = pd.read_excel(
            sample_geih_xlsx, sheet_name=geih_config.sheet_name,
            header=None, engine="openpyxl",
        )
        row = _detect_td_row(df_raw, geih_config.td_label_pattern, start_row=12)
        assert row == 16

    def test_detect_td_row_raises_if_not_found(self) -> None:
        """Error si no se encuentra la fila TD."""
        df = pd.DataFrame([["Col A"], ["Col B"], ["Col C"]])
        with pytest.raises(ValueError, match="No se encontró fila"):
            _detect_td_row(df, r"Tasa de Desocupación")


# ═══════════════════════════════════════════════════════════════════════
# Tests de reconstrucción de fechas
# ═══════════════════════════════════════════════════════════════════════


class TestGEIHDateColumns:
    """Tests de _build_date_columns (forward-fill de años + meses)."""

    def test_build_date_columns_count(self, sample_geih_xlsx: Path, geih_config: GEIHConfig) -> None:
        """Reconstruye 36 columnas fecha (3 años × 12 meses)."""
        df_raw = pd.read_excel(
            sample_geih_xlsx, sheet_name=geih_config.sheet_name,
            header=None, engine="openpyxl",
        )
        date_cols = _build_date_columns(
            df_raw, year_row=11, month_row=12, month_map=geih_config.month_map,
        )
        assert len(date_cols) == 36

    def test_build_date_columns_first_and_last(self, sample_geih_xlsx: Path, geih_config: GEIHConfig) -> None:
        """Primera fecha = Ene 2022, última = Dic 2024."""
        df_raw = pd.read_excel(
            sample_geih_xlsx, sheet_name=geih_config.sheet_name,
            header=None, engine="openpyxl",
        )
        date_cols = _build_date_columns(
            df_raw, year_row=11, month_row=12, month_map=geih_config.month_map,
        )
        assert date_cols[0]["year"] == 2022
        assert date_cols[0]["month"] == 1
        assert date_cols[-1]["year"] == 2024
        assert date_cols[-1]["month"] == 12

    def test_build_date_columns_forward_fill(self, sample_geih_xlsx: Path, geih_config: GEIHConfig) -> None:
        """Los años se propagan correctamente (forward-fill)."""
        df_raw = pd.read_excel(
            sample_geih_xlsx, sheet_name=geih_config.sheet_name,
            header=None, engine="openpyxl",
        )
        date_cols = _build_date_columns(
            df_raw, year_row=11, month_row=12, month_map=geih_config.month_map,
        )
        # Columna 2 (Feb) debería tener el mismo año que col 1 (Ene)
        assert date_cols[1]["year"] == 2022
        assert date_cols[1]["month"] == 2
        # Col 12 (Ene del segundo año)
        assert date_cols[12]["year"] == 2023
        assert date_cols[12]["month"] == 1


# ═══════════════════════════════════════════════════════════════════════
# Tests de parsing Excel
# ═══════════════════════════════════════════════════════════════════════


class TestGEIHExcelParsing:
    """Tests del parser Excel GEIH pivoteado."""

    def test_load_geih_excel_row_count(self, sample_geih_xlsx: Path, geih_config: GEIHConfig) -> None:
        """Extrae 36 observaciones (3 años × 12 meses)."""
        df = load_geih_excel(sample_geih_xlsx, geih_config)
        assert len(df) == 36

    def test_load_geih_excel_columns(self, sample_geih_xlsx: Path, geih_config: GEIHConfig) -> None:
        """Produce las 4 columnas base: date, year, month, unemployment_rate."""
        df = load_geih_excel(sample_geih_xlsx, geih_config)
        assert list(df.columns) == ["date", "year", "month", "unemployment_rate"]

    def test_load_geih_excel_sorted(self, sample_geih_xlsx: Path, geih_config: GEIHConfig) -> None:
        """Resultado está ordenado por fecha."""
        df = load_geih_excel(sample_geih_xlsx, geih_config)
        assert df["date"].is_monotonic_increasing

    def test_load_geih_excel_first_value(self, sample_geih_xlsx: Path, geih_config: GEIHConfig) -> None:
        """Primer valor de TD es correcto."""
        df = load_geih_excel(sample_geih_xlsx, geih_config)
        first = df.iloc[0]
        assert first["year"] == 2022
        assert first["month"] == 1
        # Valor: base=10.0 + y_idx=0 * 0.5 + seasonal=2.0 + m_idx=0 * 0.1 = 12.0
        assert first["unemployment_rate"] == pytest.approx(12.0, abs=0.01)

    def test_load_geih_excel_autodetect(
        self, sample_geih_xlsx: Path, geih_config_autodetect: GEIHConfig
    ) -> None:
        """Auto-detección de filas produce el mismo resultado."""
        df = load_geih_excel(sample_geih_xlsx, geih_config_autodetect)
        assert len(df) == 36
        assert df.iloc[0]["year"] == 2022

    def test_load_geih_excel_partial_year(
        self, sample_geih_xlsx_partial: Path, geih_config: GEIHConfig
    ) -> None:
        """Funciona con año parcial (menos de 12 meses)."""
        df = load_geih_excel(sample_geih_xlsx_partial, geih_config)
        assert len(df) == 6
        assert df.iloc[0]["unemployment_rate"] == pytest.approx(11.5, abs=0.01)

    def test_clean_geih_data_produces_final_columns(
        self, sample_geih_xlsx: Path, geih_config: GEIHConfig
    ) -> None:
        """clean_geih_data produce el esquema completo con source y download_date."""
        df = clean_geih_data(sample_geih_xlsx, geih_config)
        assert list(df.columns) == PROCESSED_COLUMNS

    def test_clean_geih_data_has_source(
        self, sample_geih_xlsx: Path, geih_config: GEIHConfig
    ) -> None:
        """Todos los registros tienen source='DANE'."""
        df = clean_geih_data(sample_geih_xlsx, geih_config)
        assert (df["source"] == "DANE").all()

    def test_clean_geih_data_has_download_date(
        self, sample_geih_xlsx: Path, geih_config: GEIHConfig
    ) -> None:
        """download_date es la fecha de hoy."""
        df = clean_geih_data(sample_geih_xlsx, geih_config)
        assert (df["download_date"] == date.today().isoformat()).all()


# ═══════════════════════════════════════════════════════════════════════
# Tests de calidad sobre datos GEIH
# ═══════════════════════════════════════════════════════════════════════


class TestGEIHQuality:
    """Validaciones de calidad sobre el dataset GEIH procesado."""

    def test_run_all_checks_pass(
        self, sample_geih_xlsx: Path, geih_config: GEIHConfig
    ) -> None:
        """El dataset procesado pasa todas las validaciones."""
        df = clean_geih_data(sample_geih_xlsx, geih_config)
        assert run_all_checks(df) is True

    def test_no_nulls_in_unemployment_rate(
        self, sample_geih_xlsx: Path, geih_config: GEIHConfig
    ) -> None:
        """No hay nulos en unemployment_rate."""
        df = clean_geih_data(sample_geih_xlsx, geih_config)
        assert df["unemployment_rate"].notna().all()

    def test_no_duplicate_dates(
        self, sample_geih_xlsx: Path, geih_config: GEIHConfig
    ) -> None:
        """No hay fechas duplicadas."""
        df = clean_geih_data(sample_geih_xlsx, geih_config)
        assert df["date"].is_unique

    def test_rates_in_valid_range(
        self, sample_geih_xlsx: Path, geih_config: GEIHConfig
    ) -> None:
        """Todas las tasas están entre 0% y 40%."""
        df = clean_geih_data(sample_geih_xlsx, geih_config)
        assert (df["unemployment_rate"] >= 0).all()
        assert (df["unemployment_rate"] <= 40).all()
