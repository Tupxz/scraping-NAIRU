"""Tests para src/sources/dane/gdp.py — pipeline PIB DANE.

Clases de test:
  - TestExtractGDPLink         — extracción de URLs desde HTML sintético
  - TestParseGDPExcel          — parsing del Cuadro 4 con fixture sintético
  - TestQuarterParsing         — conversión I/II/III/IV → 1..4
  - TestPipelineEndToEnd       — pipeline orquestador con mocks

Todos los tests son offline: no descargan archivos del DANE real.
"""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from src.config import DANE_GDP_CONFIG, DANE_GDP_PROCESSED_COLUMNS
from src.sources.dane.gdp import (
    _parse_quarter,
    extract_gdp_xlsx_link,
    parse_gdp_excel,
    run_dane_gdp_pipeline,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers — fixtures sintéticos
# ═══════════════════════════════════════════════════════════════════════


def _make_synthetic_gdp_excel(tmp_path: Path) -> Path:
    """Construye un Excel con la estructura mínima del Cuadro 4 del DANE.

    Replica exactamente:
      - Filas 0-10: títulos/metadata (pueden ser cualquier cosa)
      - Fila 11 (year_row): años en columnas D, H, L (cada año = 4 cols)
      - Fila 12 (quarter_row): I, II, III, IV repetidos
      - Fila 28: PIB total (col C = "Producto Interno Bruto")
      - Filas extra simulando bloque de variación trimestral (debe
        ignorarse — segundo match no debe usarse)
    """
    # Construir la matriz como lista de listas (None para celdas vacías)
    n_cols = 12  # 9 metadata cols + 3 años × 4 trimestres → simplificamos
    rows: list[list] = []

    # Filas 0-10: metadata variada
    for i in range(11):
        row: list = [None] * n_cols
        if i == 7:
            row[0] = "Datos ajustados por efecto estacional y calendario"
        if i == 8:
            row[0] = "Miles de millones de pesos"
        rows.append(row)

    # Fila 11 (year_row=11): años en columnas 3, 7, 11 (D, H, L)
    year_row: list = [None, None, "Concepto"] + [2005] + [None, None, None] + \
                    [2006] + [None, None, None] + [2007]
    rows.append(year_row)

    # Fila 12 (quarter_row=12): I, II, III, IV repetidos
    quarter_row: list = [None, None, None] + ["I", "II", "III", "IV"] * 3
    # quarter_row tiene 15 elementos, recortar a n_cols
    quarter_row = quarter_row[:n_cols]
    rows.append(quarter_row)

    # Filas 12-27: filler / sectores (no nos importan)
    for _ in range(16):
        rows.append([None] * n_cols)

    # Fila 28: PIB total — primer match ("bloque de niveles")
    pib_levels: list = ["B.1b", "Total economía", "Producto Interno Bruto"] + \
                      [100.0, 102.0, 105.0, 108.0,
                       110.0, 112.0, 115.0, 118.0,
                       120.0]  # Solo 9 valores (3 años × 3-4 trim)
    pib_levels = pib_levels[:n_cols]
    rows.append(pib_levels)

    # Filas 29-50: filler simulando variación trimestral (otro bloque)
    for _ in range(22):
        rows.append([None] * n_cols)

    # Fila ~51: PIB en bloque de variación (NO debe ser elegido — solo
    # tomamos el primer match)
    pib_growth: list = ["B.1b", "Total economía", "Producto Interno Bruto"] + \
                      [0.5, 1.2, 0.8, 1.1, 0.9, 1.5, 0.7, 1.3, 0.6]
    pib_growth = pib_growth[:n_cols]
    rows.append(pib_growth)

    df = pd.DataFrame(rows)
    path = tmp_path / "dane_gdp_test.xlsx"

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Cuadro 4", header=False, index=False)

    return path


# ═══════════════════════════════════════════════════════════════════════
# 1. TestQuarterParsing
# ═══════════════════════════════════════════════════════════════════════


class TestQuarterParsing:
    """Pruebas para _parse_quarter."""

    def test_roman_I(self):
        assert _parse_quarter("I") == 1

    def test_roman_II(self):
        assert _parse_quarter("II") == 2

    def test_roman_III(self):
        assert _parse_quarter("III") == 3

    def test_roman_IV(self):
        assert _parse_quarter("IV") == 4

    def test_lowercase(self):
        assert _parse_quarter("ii") == 2

    def test_with_whitespace(self):
        assert _parse_quarter("  III  ") == 3

    def test_invalid_returns_none(self):
        assert _parse_quarter("V") is None
        assert _parse_quarter("Q1") is None
        assert _parse_quarter("") is None

    def test_non_string_returns_none(self):
        assert _parse_quarter(None) is None
        assert _parse_quarter(123) is None


# ═══════════════════════════════════════════════════════════════════════
# 2. TestExtractGDPLink
# ═══════════════════════════════════════════════════════════════════════


class TestExtractGDPLink:
    """Pruebas para extract_gdp_xlsx_link con HTML sintético."""

    def test_extracts_single_link(self):
        html = """
        <html><body>
        <a href="/files/operaciones/PIB/anex-ProduccionConstantes-IIItrim2025.xlsx">PIB Q3 2025</a>
        </body></html>
        """
        url = extract_gdp_xlsx_link(html)
        assert url.endswith("anex-ProduccionConstantes-IIItrim2025.xlsx")
        assert url.startswith("https://www.dane.gov.co")

    def test_picks_latest_quarter(self):
        """Si hay varios trimestres, debe tomar el más reciente."""
        html = """
        <html><body>
        <a href="/files/operaciones/PIB/anex-ProduccionConstantes-IItrim2025.xlsx">Q2</a>
        <a href="/files/operaciones/PIB/anex-ProduccionConstantes-IVtrim2024.xlsx">Q4 2024</a>
        <a href="/files/operaciones/PIB/anex-ProduccionConstantes-IIItrim2025.xlsx">Q3 2025</a>
        </body></html>
        """
        url = extract_gdp_xlsx_link(html)
        # 2025-Q3 > 2025-Q2 > 2024-Q4
        assert "IIItrim2025" in url

    def test_picks_latest_year_over_older(self):
        """Año más reciente prevalece sobre trimestre alto en año viejo."""
        html = """
        <html><body>
        <a href="/files/operaciones/PIB/anex-ProduccionConstantes-IVtrim2023.xlsx">Old Q4</a>
        <a href="/files/operaciones/PIB/anex-ProduccionConstantes-Itrim2025.xlsx">New Q1</a>
        </body></html>
        """
        url = extract_gdp_xlsx_link(html)
        assert "Itrim2025" in url

    def test_ignores_unrelated_links(self):
        """Otros XLSX (corrientes, gasto) no deben matchear."""
        html = """
        <html><body>
        <a href="/files/operaciones/PIB/anex-ProduccionCorriente-IVtrim2025.xlsx">Corriente</a>
        <a href="/files/operaciones/PIB/anex-GastoConstantes-IVtrim2025.xlsx">Gasto</a>
        <a href="/files/operaciones/PIB/anex-ProduccionConstantes-IIItrim2025.xlsx">Ok</a>
        </body></html>
        """
        url = extract_gdp_xlsx_link(html)
        assert "ProduccionConstantes" in url
        assert "Corriente" not in url
        assert "Gasto" not in url

    def test_raises_when_no_link_found(self):
        html = "<html><body><a href='/foo.html'>nada</a></body></html>"
        with pytest.raises(ValueError, match="ProduccionConstantes"):
            extract_gdp_xlsx_link(html)


# ═══════════════════════════════════════════════════════════════════════
# 3. TestParseGDPExcel
# ═══════════════════════════════════════════════════════════════════════


class TestParseGDPExcel:
    """Pruebas para parse_gdp_excel con fixture sintético."""

    @pytest.fixture
    def synthetic_excel(self, tmp_path):
        return _make_synthetic_gdp_excel(tmp_path)

    def test_returns_dataframe(self, synthetic_excel):
        df = parse_gdp_excel(synthetic_excel)
        assert isinstance(df, pd.DataFrame)

    def test_has_expected_columns(self, synthetic_excel):
        df = parse_gdp_excel(synthetic_excel)
        for col in DANE_GDP_PROCESSED_COLUMNS:
            assert col in df.columns, f"Columna faltante: {col}"

    def test_extracts_correct_number_of_rows(self, synthetic_excel):
        """3 años × 4 trimestres = 12 obs, pero el fixture tiene 9 valores."""
        df = parse_gdp_excel(synthetic_excel)
        assert len(df) == 9

    def test_first_row_is_2005_q1(self, synthetic_excel):
        df = parse_gdp_excel(synthetic_excel)
        first = df.iloc[0]
        assert first["year"] == 2005
        assert first["quarter"] == 1
        # Q1 → enero
        assert first["date"] == pd.Timestamp("2005-01-01")
        assert first["gdp_observed"] == 100.0

    def test_quarter_to_month_mapping(self, synthetic_excel):
        """Q1→Ene, Q2→Abr, Q3→Jul, Q4→Oct."""
        df = parse_gdp_excel(synthetic_excel)
        q_to_month = dict(zip(df["quarter"], df["date"].dt.month))
        # En el fixture aparecen Q1-Q4
        if 1 in q_to_month: assert q_to_month[1] == 1
        if 2 in q_to_month: assert q_to_month[2] == 4
        if 3 in q_to_month: assert q_to_month[3] == 7
        if 4 in q_to_month: assert q_to_month[4] == 10

    def test_date_is_timestamp(self, synthetic_excel):
        df = parse_gdp_excel(synthetic_excel)
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_gdp_observed_numeric(self, synthetic_excel):
        df = parse_gdp_excel(synthetic_excel)
        assert pd.api.types.is_float_dtype(df["gdp_observed"])

    def test_gdp_observed_positive(self, synthetic_excel):
        df = parse_gdp_excel(synthetic_excel)
        assert (df["gdp_observed"] > 0).all()

    def test_uses_first_match_not_growth_block(self, synthetic_excel):
        """Debe usar el bloque de niveles (100.0, 102.0...) NO el de variación (0.5, 1.2...)."""
        df = parse_gdp_excel(synthetic_excel)
        # Si hubiera tomado el bloque de variación, el primer valor sería 0.5
        assert df.iloc[0]["gdp_observed"] >= 100.0

    def test_sorted_by_date(self, synthetic_excel):
        df = parse_gdp_excel(synthetic_excel)
        assert df["date"].is_monotonic_increasing

    def test_no_duplicate_dates(self, synthetic_excel):
        df = parse_gdp_excel(synthetic_excel)
        assert df["date"].nunique() == len(df)

    def test_source_label_set(self, synthetic_excel):
        df = parse_gdp_excel(synthetic_excel)
        assert (df["source"] == DANE_GDP_CONFIG.source_label).all()

    def test_download_date_is_iso(self, synthetic_excel):
        df = parse_gdp_excel(synthetic_excel)
        # Formato YYYY-MM-DD
        first = df["download_date"].iloc[0]
        assert len(first) == 10 and first[4] == "-" and first[7] == "-"

    def test_raises_when_concept_label_missing(self, tmp_path):
        """Si la fila 'Producto Interno Bruto' no existe, debe fallar."""
        df = pd.DataFrame([
            ["foo"] * 5 for _ in range(30)
        ])
        path = tmp_path / "bad.xlsx"
        df.to_excel(path, sheet_name="Cuadro 4", header=False, index=False)

        with pytest.raises(ValueError, match="Producto Interno Bruto"):
            parse_gdp_excel(path)


# ═══════════════════════════════════════════════════════════════════════
# 4. TestPipelineEndToEnd
# ═══════════════════════════════════════════════════════════════════════


class TestPipelineEndToEnd:
    """Pipeline completo con mocks (no toca red real)."""

    def test_run_writes_csv(self, tmp_path):
        # Generamos el "Excel descargado" que el pipeline esperaría
        excel_path = _make_synthetic_gdp_excel(tmp_path)
        excel_bytes = excel_path.read_bytes()

        raw_dir = tmp_path / "raw"
        processed_dir = tmp_path / "processed"

        fake_html = """
        <html><body>
        <a href="/files/operaciones/PIB/anex-ProduccionConstantes-IVtrim2025.xlsx">x</a>
        </body></html>
        """

        # Mock requests.get para devolver primero el HTML, luego los bytes
        responses = [
            type("R", (), {"text": fake_html, "content": fake_html.encode(),
                           "raise_for_status": lambda self: None})(),
            type("R", (), {"text": "", "content": excel_bytes,
                           "raise_for_status": lambda self: None})(),
        ]

        with patch("src.sources.dane.gdp.requests.get", side_effect=responses):
            df = run_dane_gdp_pipeline(
                raw_dir=raw_dir,
                processed_dir=processed_dir,
            )

        assert (processed_dir / DANE_GDP_CONFIG.processed_filename).exists()
        assert len(df) > 0
        assert "gdp_observed" in df.columns
