"""Tests para el pipeline IPC del DANE.

Verifica:
- Scraping de enlaces desde HTML simulado
- Selección del enlace correcto (serie de índices)
- Parsing del Excel pivoteado (meses × años → formato largo)
- Detección automática de encabezado
- Validaciones de calidad IPC
- Pipeline completo offline (con fixtures)
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.config import IPC_PROCESSED_COLUMNS, IPCConfig
from src.sources.dane.ipc import (
    _find_month_column,
    clean_ipc_data,
    detect_header_row_ipc,
    extract_ipc_xlsx_links,
    load_ipc_excel,
    parse_period_from_href,
    select_target_link,
)
from src.quality_checks import (
    QualityCheckError,
    check_ipc_columns,
    check_ipc_index_range,
    run_ipc_checks,
)


# ═══════════════════════════════════════════════════════════════════════
# HTML simulado (estructura real simplificada del DANE)
# ═══════════════════════════════════════════════════════════════════════

SAMPLE_HTML = """
<html><body>
<div class="row">
  <div class="col-sm-4">
    <span><strong>Enlaces destacados</strong></span>
  </div>
  <div class="col-sm-4">
    <p><a href="/files/operaciones/IPC/feb2026/anex-IPC-Indices-feb2026.xlsx">
       Índices - series de empalme - febrero 2026</a></p>
  </div>
  <div class="col-sm-4">
    <p><a href="/files/operaciones/IPC/feb2026/anex-IPC-Variacion-feb2026.xlsx">
       Variaciones porcentuales - febrero 2026</a></p>
  </div>
  <div class="col-sm-4">
    <p><a href="/files/operaciones/IPC/feb2026/anex-IPC-Paag-feb2026.xlsx">
       Variaciones PAAG - febrero 2026</a></p>
  </div>
  <div class="col-sm-4">
    <p><a href="/files/operaciones/IPC/feb2026/anex-IPC-sinAlimentosRegulados-ene2026.xlsx">
       Resultados del IPC sin alimentos ni regulados</a></p>
  </div>
</div>
</body></html>
"""


# ═══════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════


@pytest.fixture()
def ipc_config() -> IPCConfig:
    """Config IPC para tests."""
    return IPCConfig()


@pytest.fixture()
def sample_ipc_xlsx(tmp_path: Path) -> Path:
    """Crea un Excel que simula la estructura real del DANE IPC.

    Estructura:
    - Filas 0-1: vacías
    - Fila 2: título
    - Filas 3-7: vacías/subtítulos
    - Fila 8: encabezados ["Mes", "2023", "2024", "2025"]
    - Filas 9-20: datos (Enero..Diciembre)
    - Filas 21+: notas de pie
    """
    rows: list[list] = []
    # Filas basura (0-7)
    rows.append([None, None, None, None])
    rows.append([None, None, None, None])
    rows.append(["Total, Indice de Precios al Consumidor (IPC)", None, None, None])
    rows.append([None, None, None, None])
    rows.append(["Índices - Serie de empalme\n2023 - 2025", None, None, None])
    rows.append([None, None, None, None])
    rows.append([None, None, None, None])
    rows.append([None, None, None, None])

    # Fila 8: encabezados
    rows.append(["Mes", 2023, 2024, 2025])

    # Filas 9-20: datos mensuales
    month_names = [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
    ]
    # Valores simulados (crecientes, simulando inflación)
    base_2023 = [120.5, 121.2, 121.8, 122.5, 123.1, 123.5,
                 124.0, 124.3, 124.8, 125.2, 125.6, 126.0]
    base_2024 = [127.1, 127.8, 128.3, 128.9, 129.4, 129.8,
                 130.2, 130.5, 131.0, 131.4, 131.8, 132.2]
    base_2025 = [133.0, 133.7, None, None, None, None,
                 None, None, None, None, None, None]  # Solo ene-feb publicados

    for i, month in enumerate(month_names):
        rows.append([month, base_2023[i], base_2024[i], base_2025[i]])

    # Filas pie
    rows.append([None, None, None, None])
    rows.append(["Fuente: DANE.", None, None, None])
    rows.append(["Nota: aproximación y redondeo.", None, None, None])

    df = pd.DataFrame(rows)
    xlsx_path = tmp_path / "ipc_test.xlsx"
    df.to_excel(
        xlsx_path, index=False, header=False,
        sheet_name="IndicesIPC", engine="openpyxl",
    )
    return xlsx_path


@pytest.fixture()
def sample_ipc_processed_df() -> pd.DataFrame:
    """DataFrame IPC procesado de ejemplo."""
    return pd.DataFrame({
        "date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
        "year": [2024, 2024, 2024],
        "month": [1, 2, 3],
        "ipc_index": [127.1, 127.8, 128.3],
        "source": ["DANE", "DANE", "DANE"],
        "download_date": [date.today().isoformat()] * 3,
    })


# ═══════════════════════════════════════════════════════════════════════
# Tests de scraping HTML
# ═══════════════════════════════════════════════════════════════════════


class TestIPCScraping:
    """Tests para la extracción de enlaces desde HTML."""

    def test_extract_all_ipc_links(self, ipc_config: IPCConfig) -> None:
        links = extract_ipc_xlsx_links(SAMPLE_HTML, ipc_config)
        assert len(links) == 4  # 4 enlaces IPC (excluye Transparencia)

    def test_extract_links_have_correct_structure(self, ipc_config: IPCConfig) -> None:
        links = extract_ipc_xlsx_links(SAMPLE_HTML, ipc_config)
        for lnk in links:
            assert "url" in lnk
            assert "text" in lnk
            assert "href" in lnk
            assert lnk["url"].startswith("https://www.dane.gov.co")
            assert lnk["url"].endswith(".xlsx")

    def test_select_target_link_finds_indices(self, ipc_config: IPCConfig) -> None:
        links = extract_ipc_xlsx_links(SAMPLE_HTML, ipc_config)
        target = select_target_link(links, ipc_config)
        assert "Indices" in target["href"]
        assert "feb2026" in target["href"]

    def test_select_target_link_raises_if_not_found(self, ipc_config: IPCConfig) -> None:
        links = [{"url": "https://example.com/other.xlsx", "text": "Otro", "href": "/other.xlsx"}]
        with pytest.raises(ValueError, match="No se encontró enlace IPC"):
            select_target_link(links, ipc_config)

    def test_extract_links_empty_html(self, ipc_config: IPCConfig) -> None:
        links = extract_ipc_xlsx_links("<html><body></body></html>", ipc_config)
        assert len(links) == 0

    def test_parse_period_from_href(self) -> None:
        assert parse_period_from_href("anex-IPC-Indices-feb2026.xlsx") == (2026, 2)
        assert parse_period_from_href("anex-IPC-Indices-dic2025.xlsx") == (2025, 12)
        assert parse_period_from_href("anex-IPC-Indices-ene2024.xlsx") == (2024, 1)
        assert parse_period_from_href("something-else.xlsx") is None


# ═══════════════════════════════════════════════════════════════════════
# Tests de parsing Excel IPC
# ═══════════════════════════════════════════════════════════════════════


class TestIPCExcelParsing:
    """Tests para el parsing del Excel pivoteado del IPC."""

    def test_detect_header_row_finds_mes(self) -> None:
        """Detecta la fila con 'Mes' como encabezado."""
        df_raw = pd.DataFrame([
            [None, None, None],
            ["Título largo del DANE", None, None],
            [None, None, None],
            ["Mes", 2023, 2024],
            ["Enero", 120.5, 127.1],
        ])
        assert detect_header_row_ipc(df_raw) == 3

    def test_detect_header_row_by_years_fallback(self) -> None:
        """Si no encuentra 'Mes', detecta por cantidad de años."""
        df_raw = pd.DataFrame([
            [None, None, None, None, None, None],
            ["Periodo", 2020, 2021, 2022, 2023, 2024],
            ["Ene", 100, 110, 120, 130, 140],
        ])
        # "Periodo" ≠ "Mes", pero hay 5 columnas con años
        assert detect_header_row_ipc(df_raw) == 1

    def test_load_ipc_excel_format_largo(
        self, sample_ipc_xlsx: Path, ipc_config: IPCConfig
    ) -> None:
        """load_ipc_excel convierte el formato pivoteado a largo."""
        df = load_ipc_excel(sample_ipc_xlsx, ipc_config)
        assert "date" in df.columns
        assert "year" in df.columns
        assert "month" in df.columns
        assert "ipc_index" in df.columns

    def test_load_ipc_excel_correct_row_count(
        self, sample_ipc_xlsx: Path, ipc_config: IPCConfig
    ) -> None:
        """Solo incluye filas con valor (excluye meses futuros NaN)."""
        df = load_ipc_excel(sample_ipc_xlsx, ipc_config)
        # 12 meses × 2 años completos + 2 meses de 2025 = 26
        assert len(df) == 26

    def test_load_ipc_excel_excludes_nan_months(
        self, sample_ipc_xlsx: Path, ipc_config: IPCConfig
    ) -> None:
        """Los meses sin valor (NaN) no aparecen en el resultado."""
        df = load_ipc_excel(sample_ipc_xlsx, ipc_config)
        assert df["ipc_index"].notna().all()

    def test_load_ipc_excel_sorted_by_date(
        self, sample_ipc_xlsx: Path, ipc_config: IPCConfig
    ) -> None:
        """El resultado está ordenado cronológicamente."""
        df = load_ipc_excel(sample_ipc_xlsx, ipc_config)
        assert df["date"].is_monotonic_increasing

    def test_load_ipc_excel_first_value(
        self, sample_ipc_xlsx: Path, ipc_config: IPCConfig
    ) -> None:
        """El primer valor es Enero 2023 = 120.5."""
        df = load_ipc_excel(sample_ipc_xlsx, ipc_config)
        first = df.iloc[0]
        assert first["year"] == 2023
        assert first["month"] == 1
        assert first["ipc_index"] == pytest.approx(120.5, abs=0.1)

    def test_load_ipc_excel_last_value(
        self, sample_ipc_xlsx: Path, ipc_config: IPCConfig
    ) -> None:
        """El último valor es Febrero 2025 = 133.7."""
        df = load_ipc_excel(sample_ipc_xlsx, ipc_config)
        last = df.iloc[-1]
        assert last["year"] == 2025
        assert last["month"] == 2
        assert last["ipc_index"] == pytest.approx(133.7, abs=0.1)

    def test_clean_ipc_data_produces_final_columns(
        self, sample_ipc_xlsx: Path, ipc_config: IPCConfig
    ) -> None:
        """clean_ipc_data produce el esquema final con metadatos."""
        df = clean_ipc_data(sample_ipc_xlsx, ipc_config)
        assert list(df.columns) == IPC_PROCESSED_COLUMNS

    def test_clean_ipc_data_has_source(
        self, sample_ipc_xlsx: Path, ipc_config: IPCConfig
    ) -> None:
        """El dataset final tiene la columna 'source' = 'DANE'."""
        df = clean_ipc_data(sample_ipc_xlsx, ipc_config)
        assert (df["source"] == "DANE").all()

    def test_find_month_column_exact_match(self) -> None:
        df = pd.DataFrame({"Mes": [], "2023": [], "2024": []})
        assert _find_month_column(df) == "Mes"

    def test_find_month_column_fallback(self) -> None:
        df = pd.DataFrame({"Periodo": [], "2023": [], "2024": []})
        assert _find_month_column(df) == "Periodo"  # fallback a primera col


# ═══════════════════════════════════════════════════════════════════════
# Tests de validaciones de calidad IPC
# ═══════════════════════════════════════════════════════════════════════


class TestIPCQualityChecks:
    """Tests de las validaciones de calidad IPC."""

    def test_check_ipc_columns_pass(self, sample_ipc_processed_df: pd.DataFrame) -> None:
        check_ipc_columns(sample_ipc_processed_df)

    def test_check_ipc_columns_fail(self, sample_ipc_processed_df: pd.DataFrame) -> None:
        df_bad = sample_ipc_processed_df.drop(columns=["ipc_index"])
        with pytest.raises(QualityCheckError, match="Columnas IPC faltantes"):
            check_ipc_columns(df_bad)

    def test_check_ipc_range_pass(self, sample_ipc_processed_df: pd.DataFrame) -> None:
        check_ipc_index_range(sample_ipc_processed_df)

    def test_check_ipc_range_fail(self, sample_ipc_processed_df: pd.DataFrame) -> None:
        df_bad = sample_ipc_processed_df.copy()
        df_bad.loc[0, "ipc_index"] = 999.0
        with pytest.raises(QualityCheckError, match="fuera de rango"):
            check_ipc_index_range(df_bad)

    def test_run_ipc_checks_pass(self, sample_ipc_processed_df: pd.DataFrame) -> None:
        assert run_ipc_checks(sample_ipc_processed_df) is True
