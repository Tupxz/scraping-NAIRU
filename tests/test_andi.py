"""Tests para el pipeline ANDI EOIC (capacidad instalada).

Fixtures offline que simulan la estructura real de la ANDI:
- Scraping de enlaces EOIC desde HTML mock.
- (La extracción desde los informes y el pipeline nuevo se prueban en
  ``tests/test_andi_icu.py``.)
- Detección de meses faltantes en el reporte.
- Detección de valores sospechosos (fuera de rango, cambios bruscos).
- Validaciones de calidad (columnas, nulos, rangos, duplicados).
- Generación de reporte.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pytest

from src.config import (
    ANDI_CONFIG,
    ANDI_PROCESSED_COLUMNS,
    CAPACITY_UTILIZATION_MAX,
    CAPACITY_UTILIZATION_MAX_CHANGE,
    CAPACITY_UTILIZATION_MIN,
)
from src.quality_checks import (
    QualityCheckError,
    check_andi_columns,
    check_capacity_monthly_change,
    check_capacity_utilization_range,
    check_no_duplicates,
    check_no_nulls_generic,
    run_andi_checks,
)
from src.sources.andi.eoic import (
    ANDIScraper,
    _build_record,
    _load_existing_dates,
    _save_dataframe,
    generate_report,
)

# ═══════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════

SAMPLE_HTML = """
<html><body>
<a href="/uploads/docs/eoic-enero-2024.pdf">EOIC Enero 2024</a>
<a href="/uploads/docs/eoic-febrero-2024.pdf">EOIC Febrero 2024</a>
<a href="/uploads/docs/eoic-marzo-2024.pdf">EOIC Marzo 2024</a>
<a href="/uploads/docs/eoic-mayo-2024.pdf">EOIC Mayo 2024</a>
<a href="/uploads/docs/metodologia-eoic.pdf">Metodología EOIC</a>
<a href="/uploads/docs/presentacion-eoic.pdf">Presentación resultados generales</a>
<a href="/uploads/docs/informe-trimestral.pdf">Informe trimestral industria</a>
</body></html>
"""

SAMPLE_HTML_BROAD = """
<html><body>
<a href="/uploads/docs/encuesta-opinion-industrial-conjunta-2024.pdf">
    Encuesta de Opinión Industrial Conjunta 2024
</a>
</body></html>
"""


@pytest.fixture()
def sample_records() -> list[dict]:
    """Registros de ejemplo con esquema estándar."""
    return [
        _build_record("2024-01", 72.5, "https://andi.com.co/eoic-enero-2024.pdf"),
        _build_record("2024-02", 73.1, "https://andi.com.co/eoic-febrero-2024.pdf"),
        _build_record("2024-03", 74.0, "https://andi.com.co/eoic-marzo-2024.pdf"),
        _build_record("2024-05", 71.8, "https://andi.com.co/eoic-mayo-2024.pdf"),
    ]


@pytest.fixture()
def sample_df(sample_records: list[dict]) -> pd.DataFrame:
    """DataFrame ANDI de ejemplo."""
    df = pd.DataFrame(sample_records)
    df["date"] = pd.to_datetime(df["date"])
    return df[ANDI_PROCESSED_COLUMNS]


@pytest.fixture()
def tmp_csv(tmp_path: Path, sample_records: list[dict]) -> Path:
    """CSV temporal con registros de ejemplo."""
    csv_path = tmp_path / "andi_test.csv"
    df = pd.DataFrame(sample_records)
    df.to_csv(csv_path, index=False)
    return csv_path


# ═══════════════════════════════════════════════════════════════════════
# Tests de scraping (ANDIScraper)
# ═══════════════════════════════════════════════════════════════════════


class TestANDIScraperFindLinks:
    """Tests de identificación de enlaces EOIC."""

    def test_finds_eoic_links(self) -> None:
        """Encuentra enlaces EOIC y excluye metodología/presentaciones."""
        scraper = ANDIScraper.__new__(ANDIScraper)
        links = scraper.find_eoic_links(SAMPLE_HTML)

        urls = [lk["url"] for lk in links]
        # Deben encontrarse los 4 EOIC válidos.
        assert len(links) == 4
        # Metodología y presentación deben estar excluidos.
        assert not any("metodologia" in u for u in urls)
        assert not any("presentacion" in u for u in urls)

    def test_extracts_dates_from_links(self) -> None:
        """Extrae fechas YYYY-MM de los enlaces."""
        scraper = ANDIScraper.__new__(ANDIScraper)
        links = scraper.find_eoic_links(SAMPLE_HTML)
        dates = [lk["date"] for lk in links]
        assert "2024-01" in dates
        assert "2024-02" in dates
        assert "2024-03" in dates
        assert "2024-05" in dates

    def test_sorted_by_date(self) -> None:
        """Los enlaces se devuelven ordenados por fecha."""
        scraper = ANDIScraper.__new__(ANDIScraper)
        links = scraper.find_eoic_links(SAMPLE_HTML)
        dates = [lk["date"] for lk in links if lk["date"]]
        assert dates == sorted(dates)

    def test_excludes_methodology(self) -> None:
        """Excluye enlaces de metodología."""
        assert ANDIScraper._is_eoic_report(
            "/uploads/metodologia-eoic.pdf", "Metodología EOIC",
        ) is False

    def test_excludes_presentations(self) -> None:
        """Excluye presentaciones."""
        assert ANDIScraper._is_eoic_report(
            "/uploads/presentacion-eoic.pdf",
            "Presentación resultados generales",
        ) is False

    def test_accepts_valid_eoic(self) -> None:
        """Acepta un enlace EOIC válido."""
        assert ANDIScraper._is_eoic_report(
            "/uploads/eoic-enero-2024.pdf", "EOIC Enero 2024",
        ) is True

    def test_broad_fallback(self) -> None:
        """Usa filtro amplio si strict no encuentra nada."""
        scraper = ANDIScraper.__new__(ANDIScraper)
        links = scraper.find_eoic_links(SAMPLE_HTML_BROAD)
        assert len(links) >= 1

    def test_deduplicates_urls(self) -> None:
        """Elimina URLs duplicadas."""
        html = """
        <html><body>
        <a href="/uploads/eoic-enero-2024.pdf">EOIC Enero 2024</a>
        <a href="/uploads/eoic-enero-2024.pdf">EOIC Enero 2024 (repetido)</a>
        </body></html>
        """
        scraper = ANDIScraper.__new__(ANDIScraper)
        links = scraper.find_eoic_links(html)
        assert len(links) == 1

    def test_empty_html_returns_empty(self) -> None:
        """Devuelve lista vacía con HTML sin enlaces."""
        scraper = ANDIScraper.__new__(ANDIScraper)
        links = scraper.find_eoic_links("<html><body></body></html>")
        assert links == []


class TestANDIScraperDateExtraction:
    """Tests de extracción de fecha."""

    @pytest.mark.parametrize(
        "url, text, expected",
        [
            ("/eoic-enero-2024.pdf", "EOIC Enero 2024", "2024-01"),
            ("/eoic-diciembre-2023.pdf", "EOIC Diciembre 2023", "2023-12"),
            ("/eoic-2024_06.pdf", "EOIC Junio", "2024-06"),
            ("/report.pdf", "Sin fecha", None),
            # URL codificada de la página real: antes "%202026" daba 2020.
            ("https://www.andi.com.co/Uploads/Informe%20EOIC%20Enero%202026.pdf",
             "Informe EOIC Enero 2026", "2026-01"),
            ("https://www.andi.com.co/Uploads/Informe%20EOIC%20Abril%202026.pdf", "", "2026-04"),
            ("https://www.andi.com.co/Uploads/Informe%20EOIC%20Enero%202025%20MARZO%2028%20VF.pdf",
             "", "2025-01"),
        ],
    )
    def test_extract_date(self, url: str, text: str, expected: str | None) -> None:
        result = ANDIScraper._extract_date(url, text)
        assert result == expected


# ═══════════════════════════════════════════════════════════════════════
# Tests de funciones auxiliares del pipeline
# ═══════════════════════════════════════════════════════════════════════


class TestPipelineHelpers:
    """Tests de helpers de cache, CSV y record building."""

    def test_build_record_schema(self) -> None:
        """El registro tiene el esquema esperado."""
        rec = _build_record("2024-01", 72.5, "https://example.com/eoic.pdf")
        assert rec["date"] == "2024-01-01"
        assert rec["year"] == 2024
        assert rec["month"] == 1
        assert rec["capacity_utilization"] == 72.5
        assert rec["source"] == "https://example.com/eoic.pdf"
        assert "download_date" in rec

    def test_load_existing_dates(self, tmp_csv: Path) -> None:
        """Carga fechas existentes del CSV."""
        dates = _load_existing_dates(tmp_csv)
        assert len(dates) == 4
        assert "2024-01-01" in dates

    def test_load_existing_dates_no_file(self, tmp_path: Path) -> None:
        """Devuelve set vacío si no existe el CSV."""
        dates = _load_existing_dates(tmp_path / "nonexistent.csv")
        assert dates == set()

    def test_save_dataframe(self, tmp_path: Path, sample_records: list[dict]) -> None:
        """Guarda DataFrame con columnas correctas."""
        csv_path = tmp_path / "output.csv"
        df = _save_dataframe(sample_records, csv_path)
        assert csv_path.exists()
        assert list(df.columns) == ANDI_PROCESSED_COLUMNS
        assert len(df) == 4

    def test_save_empty_dataframe(self, tmp_path: Path) -> None:
        """Guarda DataFrame vacío sin error."""
        csv_path = tmp_path / "empty.csv"
        df = _save_dataframe([], csv_path)
        assert df.empty


# ═══════════════════════════════════════════════════════════════════════
# Tests de reporte
# ═══════════════════════════════════════════════════════════════════════


class TestReport:
    """Tests de generación del reporte."""

    def test_report_created(
        self, tmp_path: Path, sample_records: list[dict],
    ) -> None:
        """El reporte se crea correctamente."""
        report_path = tmp_path / "report.txt"
        stats = {"processed": 5, "ok": 4, "failed": 1, "skipped": 0}
        result = generate_report(sample_records, stats, report_path)
        assert result.exists()
        content = report_path.read_text(encoding="utf-8")
        assert "REPORTE ANDI" in content
        assert "PDFs procesados:           5" in content

    def test_report_detects_missing_months(
        self, tmp_path: Path, sample_records: list[dict],
    ) -> None:
        """El reporte detecta meses faltantes (abril falta)."""
        report_path = tmp_path / "report.txt"
        stats = {"processed": 4, "ok": 4, "failed": 0, "skipped": 0}
        generate_report(sample_records, stats, report_path)
        content = report_path.read_text(encoding="utf-8")
        assert "Meses faltantes" in content
        assert "2024-04-01" in content

    def test_report_detects_suspicious_values_out_of_range(
        self, tmp_path: Path,
    ) -> None:
        """El reporte detecta valores fuera de rango."""
        records = [
            _build_record("2024-01", 72.5, "https://example.com/1.pdf"),
            _build_record("2024-02", 25.0, "https://example.com/2.pdf"),  # < 30
        ]
        report_path = tmp_path / "report.txt"
        stats = {"processed": 2, "ok": 2, "failed": 0, "skipped": 0}
        generate_report(records, stats, report_path)
        content = report_path.read_text(encoding="utf-8")
        assert "Valores sospechosos" in content
        assert "FUERA DE RANGO" in content

    def test_report_detects_suspicious_large_change(
        self, tmp_path: Path,
    ) -> None:
        """El reporte detecta cambios mensuales > 20 pp."""
        records = [
            _build_record("2024-01", 72.0, "https://example.com/1.pdf"),
            _build_record("2024-02", 50.0, "https://example.com/2.pdf"),  # Δ=22
        ]
        report_path = tmp_path / "report.txt"
        stats = {"processed": 2, "ok": 2, "failed": 0, "skipped": 0}
        generate_report(records, stats, report_path)
        content = report_path.read_text(encoding="utf-8")
        assert "Valores sospechosos" in content
        assert "Δ=" in content

    def test_report_empty_records(self, tmp_path: Path) -> None:
        """El reporte se genera correctamente con registros vacíos."""
        report_path = tmp_path / "report.txt"
        stats = {"processed": 0, "ok": 0, "failed": 0, "skipped": 0}
        generate_report([], stats, report_path)
        assert report_path.exists()


# ═══════════════════════════════════════════════════════════════════════
# Tests de validaciones de calidad
# ═══════════════════════════════════════════════════════════════════════


class TestAndiQualityChecks:
    """Tests de validaciones de calidad del dataset ANDI."""

    def test_check_andi_columns_pass(self, sample_df: pd.DataFrame) -> None:
        """Pasa con columnas correctas."""
        check_andi_columns(sample_df)

    def test_check_andi_columns_missing(self, sample_df: pd.DataFrame) -> None:
        """Falla si falta una columna."""
        df = sample_df.drop(columns=["capacity_utilization"])
        with pytest.raises(QualityCheckError, match="Columnas ANDI faltantes"):
            check_andi_columns(df)

    def test_check_capacity_range_pass(self, sample_df: pd.DataFrame) -> None:
        """Pasa con valores dentro de rango."""
        check_capacity_utilization_range(sample_df)

    def test_check_capacity_range_below_min(self) -> None:
        """Falla con valores por debajo del mínimo."""
        df = pd.DataFrame({
            "date": ["2024-01-01"],
            "year": [2024],
            "month": [1],
            "capacity_utilization": [15.0],  # < 30
            "source": ["test"],
            "download_date": ["2024-01-01"],
        })
        with pytest.raises(QualityCheckError, match="fuera de rango"):
            check_capacity_utilization_range(df)

    def test_check_capacity_range_above_max(self) -> None:
        """Falla con valores por encima del máximo."""
        df = pd.DataFrame({
            "date": ["2024-01-01"],
            "year": [2024],
            "month": [1],
            "capacity_utilization": [110.0],  # > 100
            "source": ["test"],
            "download_date": ["2024-01-01"],
        })
        with pytest.raises(QualityCheckError, match="fuera de rango"):
            check_capacity_utilization_range(df)

    def test_check_no_duplicates_pass(self, sample_df: pd.DataFrame) -> None:
        """Pasa sin duplicados."""
        check_no_duplicates(sample_df)

    def test_check_no_duplicates_fail(self) -> None:
        """Falla con fechas duplicadas."""
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-01"],
            "year": [2024, 2024],
            "month": [1, 1],
            "capacity_utilization": [72.5, 73.0],
            "source": ["a", "b"],
            "download_date": ["2024-01-01", "2024-01-01"],
        })
        with pytest.raises(QualityCheckError, match="duplicadas"):
            check_no_duplicates(df)

    def test_check_nulls_pass(self, sample_df: pd.DataFrame) -> None:
        """Pasa sin nulos en columnas críticas."""
        check_no_nulls_generic(
            sample_df, ["date", "capacity_utilization", "year", "month"],
        )

    def test_check_nulls_fail(self) -> None:
        """Falla con nulos en columnas críticas."""
        df = pd.DataFrame({
            "date": ["2024-01-01"],
            "year": [2024],
            "month": [1],
            "capacity_utilization": [None],
            "source": ["test"],
            "download_date": ["2024-01-01"],
        })
        with pytest.raises(QualityCheckError, match="nulos"):
            check_no_nulls_generic(df, ["capacity_utilization"])

    def test_check_monthly_change_no_warning(
        self, sample_df: pd.DataFrame, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """No hay warning si los cambios son normales."""
        with caplog.at_level(logging.INFO, logger="nairu_pipeline.quality"):
            check_capacity_monthly_change(sample_df)
        assert "cambios mensuales" not in caplog.text

    def test_check_monthly_change_warning(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Warning si un cambio mensual es > 20 pp."""
        df = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-01", "2024-02-01"]),
            "year": [2024, 2024],
            "month": [1, 2],
            "capacity_utilization": [72.0, 50.0],  # Δ = 22
            "source": ["a", "b"],
            "download_date": ["2024-01-01", "2024-01-01"],
        })
        with caplog.at_level(logging.WARNING, logger="nairu_pipeline.quality"):
            check_capacity_monthly_change(df)
        assert "cambios mensuales" in caplog.text

    def test_run_all_andi_checks_pass(self, sample_df: pd.DataFrame) -> None:
        """run_andi_checks pasa con datos válidos."""
        assert run_andi_checks(sample_df) is True

    def test_run_all_andi_checks_fail(self) -> None:
        """run_andi_checks falla con datos inválidos."""
        df = pd.DataFrame({
            "date": ["2024-01-01"],
            "year": [2024],
            "month": [1],
            "capacity_utilization": [10.0],  # < 30
            "source": ["test"],
            "download_date": ["2024-01-01"],
        })
        with pytest.raises(QualityCheckError):
            run_andi_checks(df)


# ═══════════════════════════════════════════════════════════════════════
# Tests de configuración
# ═══════════════════════════════════════════════════════════════════════


class TestAndiConfig:
    """Tests de la configuración ANDI."""

    def test_config_exists(self) -> None:
        """La configuración ANDI existe."""
        assert ANDI_CONFIG is not None

    def test_config_urls(self) -> None:
        """Las URLs están definidas."""
        assert "andi.com.co" in ANDI_CONFIG.base_url
        assert "andi.com.co" in ANDI_CONFIG.eoic_page_url

    def test_config_thresholds(self) -> None:
        """Los umbrales de extracción son razonables."""
        assert 0 < ANDI_CONFIG.text_similarity_threshold < 1
        assert 0 < ANDI_CONFIG.table_similarity_threshold < 1

    def test_config_capacity_phrases(self) -> None:
        """Tiene frases clave definidas."""
        assert len(ANDI_CONFIG.capacity_phrases) > 5

    def test_config_month_map(self) -> None:
        """El mapeo de meses cubre enero–diciembre."""
        assert len(ANDI_CONFIG.month_map) == 12
        assert ANDI_CONFIG.month_map["enero"] == 1
        assert ANDI_CONFIG.month_map["diciembre"] == 12

    def test_quality_thresholds(self) -> None:
        """Los umbrales de calidad son razonables."""
        assert CAPACITY_UTILIZATION_MIN == 30.0
        assert CAPACITY_UTILIZATION_MAX == 100.0
        assert CAPACITY_UTILIZATION_MAX_CHANGE == 20.0

    def test_processed_columns(self) -> None:
        """Las columnas procesadas están definidas."""
        assert "capacity_utilization" in ANDI_PROCESSED_COLUMNS
        assert "date" in ANDI_PROCESSED_COLUMNS
        assert len(ANDI_PROCESSED_COLUMNS) == 6


# ═══════════════════════════════════════════════════════════════════════
# Histórico ICU (data/inputs/icu_historical.csv)
# ═══════════════════════════════════════════════════════════════════════


class TestLoadIcuHistorical:
    """El histórico de la ANDI arranca en 2001-01 (no en 2004)."""

    def test_conserva_2001_y_descarta_anteriores(self, tmp_path):
        from src.sources.andi.eoic import _load_icu_historical

        p = tmp_path / "icu.csv"
        p.write_text(
            "date,year,month,capacity_utilization,source,download_date\n"
            "2000-12-01,2000,12,70.0,x,2026-11-09\n"
            "2001-01-01,2001,1,63.903334,x,2026-11-09\n"
            "2004-01-01,2004,1,71.9587,x,2026-05-05\n",
            encoding="utf-8",
        )
        fechas = [r["date"] for r in _load_icu_historical(p)]
        assert fechas == ["2001-01-01", "2004-01-01"]

    def test_archivo_del_repo_sin_huecos_2001_2025(self):
        from src.config import INPUTS_DIR

        h = pd.read_csv(INPUTS_DIR / "icu_historical.csv", parse_dates=["date"])
        meses = h["date"].dt.to_period("M")
        assert meses.tolist() == list(pd.period_range("2001-01", "2025-12", freq="M"))
        assert h["capacity_utilization"].notna().all()
