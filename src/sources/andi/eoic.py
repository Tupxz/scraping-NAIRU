"""Scraping y extracción de la EOIC (ANDI).

Módulo de 3 capas:

1. **SCRAPING** — ``ANDIScraper`` navega la página de la ANDI, identifica
   los PDFs de la EOIC y los descarga a ``data/raw/andi/``.
2. **PARSING** — ``EOICParser`` extrae el porcentaje de utilización de
   la capacidad instalada desde cada PDF descargado usando *pdfplumber*
   y 3 estrategias de extracción (fuzzy text, tablas, regex).
3. **PIPELINE** — ``run_andi_pipeline`` orquesta la lógica incremental
   o backfill, produce un DataFrame estándar y genera un reporte.
"""

from __future__ import annotations

import json
import logging
import re
import time
import unicodedata
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import (
    ANDI_CONFIG,
    ANDI_PROCESSED_COLUMNS,
    CAPACITY_UTILIZATION_MAX,
    CAPACITY_UTILIZATION_MAX_CHANGE,
    CAPACITY_UTILIZATION_MIN,
    OUTPUTS_DIR,
    PROCESSED_DIR,
    RAW_ANDI_DIR,
)

logger = logging.getLogger("nairu_pipeline.andi")

# ═══════════════════════════════════════════════════════════════════════
# Constantes locales (derivadas de AndiConfig)
# ═══════════════════════════════════════════════════════════════════════

_EOIC_REQUIRED = re.compile(ANDI_CONFIG.eoic_required_pattern, re.IGNORECASE)
_EOIC_BROAD = re.compile(ANDI_CONFIG.eoic_broad_pattern, re.IGNORECASE)
_EXCLUDE = re.compile(ANDI_CONFIG.eoic_exclude_pattern, re.IGNORECASE)
_PERCENT_RE = re.compile(ANDI_CONFIG.percent_pattern)

_MONTH_MAP: dict[str, int] = ANDI_CONFIG.month_map


# ═══════════════════════════════════════════════════════════════════════
# Capa 1 — Scraping: ANDIScraper
# ═══════════════════════════════════════════════════════════════════════


class ANDIScraper:
    """Descarga PDFs de la EOIC desde la página de la ANDI.

    Parameters
    ----------
    data_dir : Path | None
        Directorio donde guardar los PDFs descargados.
        Por defecto ``RAW_ANDI_DIR``.
    """

    def __init__(self, data_dir: Path | None = None) -> None:
        self.data_dir = data_dir or RAW_ANDI_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        retry = Retry(
            total=ANDI_CONFIG.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))
        self.session.headers.update(ANDI_CONFIG.http_headers)

    # ── Obtención de HTML ─────────────────────────────────────────

    def get_page_content(self, url: str) -> str | None:
        """Descarga el HTML de *url* con reintentos.

        Returns
        -------
        str | None
            HTML como texto, o ``None`` si falla.
        """
        for attempt in range(1, ANDI_CONFIG.max_retries + 1):
            try:
                resp = self.session.get(
                    url, timeout=ANDI_CONFIG.timeout,
                )
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as exc:
                logger.warning(
                    "Intento %d/%d fallido para %s: %s",
                    attempt, ANDI_CONFIG.max_retries, url, exc,
                )
                if attempt < ANDI_CONFIG.max_retries:
                    time.sleep(2 * attempt)
        return None

    # ── Identificación de enlaces EOIC ────────────────────────────

    @staticmethod
    def _is_eoic_report(url: str, text: str) -> bool:
        """Determina si un enlace corresponde a un informe EOIC."""
        combined = f"{url} {text}".lower()
        if _EXCLUDE.search(combined):
            return False
        return bool(
            _EOIC_REQUIRED.search(combined) or _EOIC_BROAD.search(combined)
        )

    @staticmethod
    def _extract_date(url: str, text: str) -> str | None:
        """Extrae fecha YYYY-MM de la URL o texto del enlace.

        Returns
        -------
        str | None
            Fecha en formato ``"YYYY-MM"`` o ``None``.
        """
        combined = f"{url} {text}".lower()
        year_match = re.search(r"20[12]\d", combined)
        if not year_match:
            return None
        year = year_match.group()

        for name, num in _MONTH_MAP.items():
            if name in combined:
                return f"{year}-{num:02d}"

        # Intentar mes numérico en la URL.
        m = re.search(r"(\d{4})[_/-](\d{2})", combined)
        if m and 1 <= int(m.group(2)) <= 12:
            return f"{m.group(1)}-{m.group(2)}"

        return None

    def find_eoic_links(
        self, html: str, *, strict: bool = True,
    ) -> list[dict[str, str | None]]:
        """Extrae enlaces a PDFs de la EOIC desde el HTML.

        Parameters
        ----------
        html : str
            Contenido HTML de la página.
        strict : bool
            Si ``True`` aplica filtros estrictos primero; si no encuentra
            nada, relaja a filtro amplio.

        Returns
        -------
        list[dict]
            Lista de dicts con claves ``url``, ``text``, ``date``, ``href``.
        """
        soup = BeautifulSoup(html, "lxml")
        results: list[dict[str, str | None]] = []
        seen_urls: set[str] = set()

        for a_tag in soup.find_all("a", href=True):
            href: str = a_tag["href"]
            text: str = a_tag.get_text(strip=True)

            # Construir URL absoluta.
            if href.startswith("/"):
                full_url = ANDI_CONFIG.base_url + href
            elif href.startswith("http"):
                full_url = href
            else:
                continue

            if not self._is_eoic_report(full_url, text):
                continue

            norm_url = full_url.split("?")[0].lower()
            if norm_url in seen_urls:
                continue
            seen_urls.add(norm_url)

            results.append({
                "url": full_url,
                "text": text,
                "date": self._extract_date(full_url, text),
                "href": href,
            })

        # Fallback amplio si strict no encontró nada.
        if strict and not results:
            logger.info("Búsqueda estricta vacía; intentando filtro amplio.")
            return self.find_eoic_links(html, strict=False)

        return sorted(results, key=lambda d: d.get("date") or "")

    def get_all_eoic_pdfs(self) -> list[dict[str, str | None]]:
        """Obtiene todos los enlaces EOIC (para backfill).

        Returns
        -------
        list[dict]
            Enlaces ordenados por fecha (más antiguo primero).
        """
        html = self.get_page_content(ANDI_CONFIG.eoic_page_url)
        if not html:
            logger.error("No se pudo obtener la página de la ANDI.")
            return []
        return self.find_eoic_links(html)

    def get_latest_eoic_pdf(self) -> dict[str, str | None] | None:
        """Obtiene el enlace EOIC más reciente (para incremental).

        Returns
        -------
        dict | None
            Dict del enlace más reciente, o ``None``.
        """
        pdfs = self.get_all_eoic_pdfs()
        return pdfs[-1] if pdfs else None

    def download_pdf(self, url: str) -> Path | None:
        """Descarga un PDF a ``self.data_dir``.

        Si el archivo ya existe, no lo vuelve a descargar.

        Returns
        -------
        Path | None
            Ruta al archivo descargado, o ``None`` si falla.
        """
        filename = url.split("/")[-1].split("?")[0]
        if not filename.endswith(".pdf"):
            filename += ".pdf"
        local_path = self.data_dir / filename

        if local_path.exists():
            logger.debug("Ya existe: %s", local_path.name)
            return local_path

        try:
            resp = self.session.get(url, timeout=ANDI_CONFIG.timeout)
            resp.raise_for_status()
            local_path.write_bytes(resp.content)
            logger.info("Descargado: %s (%.1f KB)", filename, len(resp.content) / 1024)
            return local_path
        except requests.RequestException as exc:
            logger.error("Error descargando %s: %s", url, exc)
            return None


# ═══════════════════════════════════════════════════════════════════════
# Capa 2 — Parsing: EOICParser
# ═══════════════════════════════════════════════════════════════════════


class EOICParser:
    """Extrae el porcentaje de utilización de capacidad instalada de un PDF.

    Usa 3 estrategias de extracción en orden de prioridad:

    1. **Fuzzy text** — Busca frases similares a las claves en el texto
       completo y captura el porcentaje más cercano.
    2. **Tablas** — Escanea tablas extraídas por *pdfplumber* buscando
       celdas con etiquetas de capacidad y valores porcentuales.
    3. **Regex** — Busca porcentajes precedidos de palabras clave de
       capacidad en un radio de 200 caracteres.

    Parameters
    ----------
    pdf_path : Path
        Ruta al archivo PDF.
    """

    VALUE_MIN: float = CAPACITY_UTILIZATION_MIN
    VALUE_MAX: float = CAPACITY_UTILIZATION_MAX

    def __init__(self, pdf_path: Path) -> None:
        self.pdf_path = pdf_path
        self._text: str | None = None
        self._pages: list[Any] | None = None

    # ── Helpers ────────────────────────────────────────────────────

    @staticmethod
    def _normalize(text: str) -> str:
        """Normaliza texto: quita acentos y pasa a minúsculas."""
        nfkd = unicodedata.normalize("NFKD", text)
        return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()

    @staticmethod
    def _similarity(a: str, b: str) -> float:
        """Ratio de similitud entre dos cadenas."""
        return SequenceMatcher(None, a, b).ratio()

    @classmethod
    def _parse_percent(cls, raw: str) -> float | None:
        """Parsea un string porcentual y valida rango."""
        try:
            value = float(raw.replace(",", "."))
        except (ValueError, TypeError):
            return None
        if cls.VALUE_MIN <= value <= cls.VALUE_MAX:
            return value
        return None

    def _get_text(self) -> str:
        """Extrae texto completo del PDF (con caché)."""
        if self._text is None:
            import pdfplumber

            text_parts: list[str] = []
            with pdfplumber.open(self.pdf_path) as pdf:
                self._pages = pdf.pages
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    text_parts.append(page_text)
            self._text = "\n".join(text_parts)
        return self._text

    # ── Estrategia 1: Fuzzy text ──────────────────────────────────

    def _strategy_text(self) -> tuple[float, str] | None:
        """Busca la utilización de capacidad con fuzzy matching en texto."""
        text = self._get_text()
        norm_text = self._normalize(text)

        threshold = ANDI_CONFIG.text_similarity_threshold
        best_score = 0.0
        best_pos = -1
        best_phrase = ""

        for phrase in ANDI_CONFIG.capacity_phrases:
            norm_phrase = self._normalize(phrase)
            window = len(norm_phrase)
            for i in range(len(norm_text) - window + 1):
                chunk = norm_text[i : i + window]
                score = self._similarity(norm_phrase, chunk)
                if score > best_score:
                    best_score = score
                    best_pos = i
                    best_phrase = phrase

        if best_score < threshold or best_pos < 0:
            return None

        # Buscar porcentaje cercano a la posición encontrada.
        search_start = max(0, best_pos - 50)
        search_end = min(len(text), best_pos + 300)
        region = text[search_start:search_end]

        for m in _PERCENT_RE.finditer(region):
            val = self._parse_percent(m.group(1))
            if val is not None:
                context = region[max(0, m.start() - 40) : m.end() + 20].strip()
                return val, f"[text|{best_score:.2f}] {best_phrase!r}: {context}"

        return None

    # ── Estrategia 2: Tablas ──────────────────────────────────────

    def _strategy_tables(self) -> tuple[float, str] | None:
        """Busca la utilización de capacidad en tablas del PDF."""
        import pdfplumber

        threshold = ANDI_CONFIG.table_similarity_threshold

        with pdfplumber.open(self.pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                tables = page.extract_tables() or []
                for table in tables:
                    if not table:
                        continue
                    for row in table:
                        if not row:
                            continue
                        row_text = " ".join(
                            str(cell) for cell in row if cell
                        )
                        norm_row = self._normalize(row_text)

                        for phrase in ANDI_CONFIG.capacity_phrases:
                            norm_phrase = self._normalize(phrase)
                            if self._similarity(norm_row, norm_phrase) >= threshold or \
                               norm_phrase in norm_row:
                                # Buscar valores en las celdas de la fila.
                                for cell in row:
                                    if cell is None:
                                        continue
                                    for m in _PERCENT_RE.finditer(str(cell)):
                                        val = self._parse_percent(m.group(1))
                                        if val is not None:
                                            return val, (
                                                f"[table|p{page_num}] "
                                                f"{row_text[:80]}"
                                            )
        return None

    # ── Estrategia 3: Regex con proximidad ────────────────────────

    def _strategy_regex(self) -> tuple[float, str] | None:
        """Busca porcentajes precedidos de keywords de capacidad."""
        text = self._get_text()
        norm_text = self._normalize(text)

        keywords = ["capacidad instalada", "capacidad", "utilizacion"]
        radius = 200

        for kw in keywords:
            for m in re.finditer(re.escape(kw), norm_text):
                start = max(0, m.start() - radius)
                end = min(len(text), m.end() + radius)
                region = text[start:end]
                for pm in _PERCENT_RE.finditer(region):
                    val = self._parse_percent(pm.group(1))
                    if val is not None:
                        ctx = region[
                            max(0, pm.start() - 30) : pm.end() + 20
                        ].strip()
                        return val, f"[regex] {kw!r}: {ctx}"
        return None

    # ── Método principal ──────────────────────────────────────────

    def extract_capacity_utilization(self) -> tuple[float, str] | None:
        """Extrae el porcentaje de utilización de capacidad instalada.

        Ejecuta las 3 estrategias en orden de prioridad.

        Returns
        -------
        tuple[float, str] | None
            ``(valor, contexto)`` o ``None`` si no se pudo extraer.
        """
        for strategy in (
            self._strategy_text,
            self._strategy_tables,
            self._strategy_regex,
        ):
            try:
                result = strategy()
                if result is not None:
                    return result
            except Exception as exc:
                logger.debug(
                    "Estrategia %s falló: %s",
                    strategy.__name__, exc,
                )
        return None

    def extract_date_from_content(self) -> str | None:
        """Intenta inferir la fecha YYYY-MM desde el contenido del PDF.

        Analiza las primeras 3 páginas buscando patrones de mes + año.

        Returns
        -------
        str | None
            Fecha en formato ``"YYYY-MM"`` o ``None``.
        """
        import pdfplumber

        with pdfplumber.open(self.pdf_path) as pdf:
            pages_to_check = pdf.pages[:3]
            for page in pages_to_check:
                text = (page.extract_text() or "").lower()
                for name, num in _MONTH_MAP.items():
                    if name in text:
                        year_match = re.search(r"20[12]\d", text)
                        if year_match:
                            return f"{year_match.group()}-{num:02d}"
        return None


# ═══════════════════════════════════════════════════════════════════════
# Capa 3 — Pipeline
# ═══════════════════════════════════════════════════════════════════════

# ── Cache helpers ─────────────────────────────────────────────────────


def _load_cache(cache_path: Path) -> dict[str, Any]:
    """Carga el cache JSON (clave = URL del PDF)."""
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("Cache corrupto; se ignora.")
    return {}


def _save_cache(cache: dict[str, Any], cache_path: Path) -> None:
    """Persiste el cache JSON."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(cache, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# ── CSV helpers ───────────────────────────────────────────────────────


def _load_existing_dates(csv_path: Path) -> set[str]:
    """Devuelve el set de fechas ya presentes en el CSV."""
    if not csv_path.exists():
        return set()
    try:
        df = pd.read_csv(csv_path, usecols=["date"])
        return set(df["date"].astype(str))
    except Exception:
        return set()


def _build_record(
    date_str: str,
    value: float,
    source_url: str,
) -> dict[str, Any]:
    """Construye un registro con el esquema estándar del proyecto."""
    parts = date_str.split("-")
    return {
        "date": f"{date_str}-01",
        "year": int(parts[0]),
        "month": int(parts[1]),
        "capacity_utilization": value,
        "source": source_url,
        "download_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }


def _save_dataframe(records: list[dict[str, Any]], csv_path: Path) -> pd.DataFrame:
    """Construye, ordena y guarda el DataFrame procesado."""
    if not records:
        return pd.DataFrame(columns=ANDI_PROCESSED_COLUMNS)
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df = df[ANDI_PROCESSED_COLUMNS]
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    logger.info("Guardado: %s (%d filas)", csv_path.name, len(df))
    return df


# ── Procesamiento unitario ────────────────────────────────────────────


def process_one_pdf(
    pdf_info: dict[str, str | None],
    scraper: ANDIScraper,
    cache: dict[str, Any],
    existing_dates: set[str],
    *,
    force: bool = False,
) -> dict[str, Any] | None:
    """Descarga y procesa un solo PDF de la EOIC.

    Parameters
    ----------
    pdf_info : dict
        Dict con claves ``url``, ``text``, ``date``.
    scraper : ANDIScraper
        Instancia del scraper para descargar PDFs.
    cache : dict
        Cache de PDFs ya procesados.
    existing_dates : set[str]
        Fechas ya presentes en el CSV.
    force : bool
        Si ``True``, reprocesa aunque ya exista en cache.

    Returns
    -------
    dict | None
        Registro (date, value, source) o ``None`` si falla/existe.
    """
    url = pdf_info.get("url")
    if not url:
        return None

    # Revisar cache (a menos que sea forzado).
    if not force and url in cache:
        logger.debug("En cache: %s", url)
        return None

    # Revisar si la fecha ya existe.
    date_from_link = pdf_info.get("date")
    if not force and date_from_link and date_from_link in existing_dates:
        logger.debug("Fecha ya existe: %s", date_from_link)
        return None

    # Descargar.
    local_path = scraper.download_pdf(url)
    if not local_path:
        return None

    # Parsear.
    parser = EOICParser(local_path)
    result = parser.extract_capacity_utilization()

    if result is None:
        logger.warning("No se pudo extraer datos de: %s", local_path.name)
        cache[url] = {"status": "failed", "date": date_from_link}
        return None

    value, context = result

    # Determinar la fecha final (preferir la del enlace, fallback al PDF).
    date_str = date_from_link or parser.extract_date_from_content()
    if not date_str:
        logger.warning("No se pudo determinar la fecha de: %s", local_path.name)
        cache[url] = {"status": "no_date", "value": value}
        return None

    logger.info(
        "Extraído: %s → %.1f%% (%s)",
        date_str, value, context[:60],
    )

    cache[url] = {
        "status": "ok",
        "date": date_str,
        "value": value,
        "context": context,
    }

    return _build_record(date_str, value, url)


# ── Reporte ───────────────────────────────────────────────────────────


def generate_report(
    records: list[dict[str, Any]],
    stats: dict[str, Any],
    report_path: Path,
) -> Path:
    """Genera un reporte de texto con el resumen del pipeline.

    Parameters
    ----------
    records : list[dict]
        Registros procesados exitosamente.
    stats : dict
        Estadísticas del pipeline (processed, ok, failed, skipped).
    report_path : Path
        Ruta de salida del reporte.

    Returns
    -------
    Path
        Ruta al reporte generado.
    """
    report_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = [
        "=" * 60,
        "REPORTE ANDI — EOIC Capacidad Instalada",
        f"Generado: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "=" * 60,
        "",
        "── Resumen ──",
        f"  PDFs procesados:           {stats.get('processed', 0)}",
        f"  Extracciones exitosas:     {stats.get('ok', 0)}",
        f"  Extracciones fallidas:     {stats.get('failed', 0)}",
        f"  Omitidos (ya existían):    {stats.get('skipped', 0)}",
        "",
    ]

    # Meses faltantes.
    if records:
        dates = sorted(r["date"] for r in records)
        all_months = pd.date_range(
            start=dates[0], end=dates[-1], freq="MS",
        )
        existing = set(dates)
        missing = [
            d.strftime("%Y-%m-%d")
            for d in all_months if d.strftime("%Y-%m-%d") not in existing
        ]
        if missing:
            lines.append("── Meses faltantes ──")
            for m in missing:
                lines.append(f"  {m}")
            lines.append("")

    # Valores sospechosos.
    suspicious: list[str] = []
    sorted_records = sorted(records, key=lambda r: r["date"])
    for i, rec in enumerate(sorted_records):
        val = rec["capacity_utilization"]
        if val < CAPACITY_UTILIZATION_MIN or val > CAPACITY_UTILIZATION_MAX:
            suspicious.append(
                f"  {rec['date']}  val={val:.1f}%  "
                f"FUERA DE RANGO [{CAPACITY_UTILIZATION_MIN}, "
                f"{CAPACITY_UTILIZATION_MAX}]  src={rec['source']}"
            )
        if i > 0:
            prev_val = sorted_records[i - 1]["capacity_utilization"]
            change = abs(val - prev_val)
            if change > CAPACITY_UTILIZATION_MAX_CHANGE:
                suspicious.append(
                    f"  {rec['date']}  val={val:.1f}%  "
                    f"Δ={change:.1f}pp (prev={prev_val:.1f}%)  "
                    f"src={rec['source']}"
                )

    if suspicious:
        lines.append("── Valores sospechosos ──")
        lines.extend(suspicious)
        lines.append("")

    lines.append("═" * 60)

    report_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Reporte generado: %s", report_path)
    return report_path


# ── Función principal del pipeline ────────────────────────────────────


def run_andi_pipeline(*, backfill: bool = False) -> pd.DataFrame:
    """Ejecuta el pipeline ANDI EOIC.

    Parameters
    ----------
    backfill : bool
        Si ``True``, procesa todos los PDFs disponibles.
        Si ``False`` (default), solo procesa el más reciente.

    Returns
    -------
    pd.DataFrame
        DataFrame con los datos procesados.
    """
    csv_path = PROCESSED_DIR / ANDI_CONFIG.processed_filename
    cache_path = RAW_ANDI_DIR / ANDI_CONFIG.cache_filename
    report_path = OUTPUTS_DIR / ANDI_CONFIG.report_filename

    scraper = ANDIScraper()
    cache = _load_cache(cache_path)
    existing_dates = _load_existing_dates(csv_path)

    # Obtener PDFs.
    if backfill:
        pdf_list = scraper.get_all_eoic_pdfs()
        logger.info("Backfill: %d PDFs encontrados.", len(pdf_list))
    else:
        latest = scraper.get_latest_eoic_pdf()
        pdf_list = [latest] if latest else []
        logger.info(
            "Incremental: %s",
            f"PDF más reciente → {latest.get('date', '?')}" if latest else "sin PDFs",
        )

    # Procesar.
    new_records: list[dict[str, Any]] = []
    stats: dict[str, int] = {"processed": 0, "ok": 0, "failed": 0, "skipped": 0}

    for pdf_info in pdf_list:
        stats["processed"] += 1
        record = process_one_pdf(
            pdf_info, scraper, cache, existing_dates,
        )
        if record:
            new_records.append(record)
            existing_dates.add(record["date"])
            stats["ok"] += 1
        else:
            # Distinguir skip vs fail.
            url = pdf_info.get("url", "")
            entry = cache.get(url, {})
            if entry.get("status") == "failed" or entry.get("status") == "no_date":
                stats["failed"] += 1
            else:
                stats["skipped"] += 1

    # Persistir cache.
    _save_cache(cache, cache_path)

    # Combinar con registros existentes.
    all_records: list[dict[str, Any]] = []
    if csv_path.exists():
        existing_df = pd.read_csv(csv_path)
        all_records = existing_df.to_dict("records")
    all_records.extend(new_records)

    # Guardar DataFrame final.
    df = _save_dataframe(all_records, csv_path)

    # Generar reporte (solo en backfill o si hay nuevos registros).
    if backfill or new_records:
        generate_report(all_records, stats, report_path)

    logger.info(
        "ANDI EOIC: %d nuevos, %d total, %d fallidos, %d omitidos",
        stats["ok"], len(df), stats["failed"], stats["skipped"],
    )

    return df
