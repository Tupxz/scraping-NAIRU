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

        Busca co-ocurrencias ``mes + año`` próximas en el texto combinado
        para evitar falsos positivos cuando el texto menciona varios meses
        (ej. comparaciones interanuales).

        Returns
        -------
        str | None
            Fecha en formato ``"YYYY-MM"`` o ``None``.
        """
        combined = f"{url} {text}".lower()

        # Estrategia 1: buscar patrones "mes (de) año" o "año ... mes"
        # cercanos entre sí.
        best: tuple[int, str] | None = None  # (pos, "YYYY-MM")
        for name, num in _MONTH_MAP.items():
            for m in re.finditer(re.escape(name), combined):
                # Buscar un año 20xx en un radio de 30 chars alrededor.
                start = max(0, m.start() - 30)
                end = min(len(combined), m.end() + 30)
                region = combined[start:end]
                year_m = re.search(r"20[12]\d", region)
                if year_m:
                    pos = m.start()
                    candidate = f"{year_m.group()}-{num:02d}"
                    if best is None or pos < best[0]:
                        best = (pos, candidate)

        if best is not None:
            return best[1]

        # Estrategia 2: mes numérico en la URL.
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

    # Rango "típico" de capacidad instalada industrial colombiana.
    _TYPICAL_MIN: float = 65.0
    _TYPICAL_MAX: float = 90.0

    # Patrones que preceden al valor real de utilización de capacidad.
    _INDICATOR_RE = re.compile(
        r"(?:se\s+situ[oó]|se\s+ubic[oó]|fue\s+de|registr[oó]"
        r"|pas[oó]\s+(?:a|de)|alcanz[oó])\s+(?:en\s+)?",
        re.IGNORECASE,
    )

    @classmethod
    def _rank_candidate(
        cls,
        value: float,
        region: str,
        match_start: int,
        anchor_pos: int,
    ) -> float:
        """Asigna un puntaje a un candidato de porcentaje.

        Criterios (mayor = mejor):
        * +3 si el valor está en el rango típico [65, 90].
        * +2 si el porcentaje está *después* de la posición ancla.
        * +2 si está precedido por un verbo indicador ("se situó en").
        * -1 si el valor está fuera del rango típico.

        Parameters
        ----------
        value : float
            Valor porcentual extraído.
        region : str
            Fragmento de texto donde se encontró el porcentaje.
        match_start : int
            Posición del match dentro de *region*.
        anchor_pos : int
            Posición de la frase ancla de capacidad dentro de *region*.
        """
        score = 0.0
        if cls._TYPICAL_MIN <= value <= cls._TYPICAL_MAX:
            score += 3.0
        else:
            score -= 1.0
        if match_start >= anchor_pos:
            score += 2.0
        # Mirar los 60 chars antes del match para buscar verbo indicador.
        pre_start = max(0, match_start - 60)
        prefix = region[pre_start:match_start]
        if cls._INDICATOR_RE.search(prefix):
            score += 2.0
        return score

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

    # ── Helpers para estrategia 1 (regex rápido) ─────────────────

    @staticmethod
    def _phrase_to_regex(phrase: str) -> re.Pattern[str]:
        """Convierte una frase de búsqueda en un regex flexible.

        Cada palabra de la frase se une con ``\\s+`` para tolerar
        saltos de línea y espacios múltiples.  Las tildes se
        manejan con clases de caracteres (``[aá]``, ``[oó]``, etc.).
        El resultado es ~100× más rápido que ventana deslizante +
        SequenceMatcher.
        """
        accent_map = {
            "a": "[aá]", "e": "[eé]", "i": "[ií]",
            "o": "[oó]", "u": "[uú]", "n": "[nñ]",
        }
        words = phrase.lower().split()
        regex_words: list[str] = []
        for word in words:
            escaped = ""
            for ch in word:
                escaped += accent_map.get(ch, re.escape(ch))
            regex_words.append(escaped)
        pattern = r"\s+".join(regex_words)
        return re.compile(pattern, re.IGNORECASE)

    _phrase_regex_cache: dict[str, re.Pattern[str]] = {}

    @classmethod
    def _get_phrase_regex(cls, phrase: str) -> re.Pattern[str]:
        """Obtiene (con caché) el regex compilado para una frase."""
        if phrase not in cls._phrase_regex_cache:
            cls._phrase_regex_cache[phrase] = cls._phrase_to_regex(phrase)
        return cls._phrase_regex_cache[phrase]

    # ── Estrategia 1: Regex rápido ────────────────────────────────

    def _strategy_text(self) -> tuple[float, str] | None:
        """Busca la utilización de capacidad con regex rápido.

        Convierte cada *capacity_phrase* en un regex flexible que
        tolera tildes y espacios variables.  Para cada coincidencia
        busca porcentajes cercanos rankeados con ``_rank_candidate``.

        Es ~100× más rápido que la versión anterior (ventana
        deslizante + ``SequenceMatcher``), al evitar O(n×k×m²)
        comparaciones en favor de un paso O(n) por regex.
        """
        text = self._get_text()

        # Recopilar TODAS las coincidencias regex.
        matches: list[tuple[float, int, str]] = []  # (score, pos, phrase)
        for phrase in ANDI_CONFIG.capacity_phrases:
            rgx = self._get_phrase_regex(phrase)
            for m in rgx.finditer(text):
                # Score = 1.0 para coincidencia exacta; ajustar si
                # el match tiene chars extra (ratio rápido).
                matched_text = self._normalize(m.group())
                norm_phrase = self._normalize(phrase)
                if matched_text == norm_phrase:
                    score = 1.0
                else:
                    # Ratio simplificado: len overlap / max len.
                    score = min(len(norm_phrase), len(matched_text)) / max(
                        len(norm_phrase), len(matched_text)
                    )
                matches.append((score, m.start(), phrase))

        if not matches:
            return None

        # Para cada coincidencia, buscar porcentajes cercanos.
        candidates: list[tuple[float, float, str, float]] = []
        for match_score, pos, phrase in matches:
            search_start = max(0, pos - 50)
            search_end = min(len(text), pos + 300)
            region = text[search_start:search_end]
            anchor_offset = pos - search_start

            for m in _PERCENT_RE.finditer(region):
                val = self._parse_percent(m.group(1))
                if val is not None:
                    rank = self._rank_candidate(
                        val, region, m.start(), anchor_offset,
                    )
                    # Bonus pequeño por score de similitud.
                    rank += match_score * 0.5
                    context = region[max(0, m.start() - 40) : m.end() + 20].strip()
                    candidates.append((rank, val, context, match_score))

        if candidates:
            candidates.sort(key=lambda c: c[0], reverse=True)
            _, val, context, ms = candidates[0]
            return val, f"[text|{ms:.2f}] {phrase!r}: {context}"

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

        candidates: list[tuple[float, float, str]] = []
        for kw in keywords:
            for m in re.finditer(re.escape(kw), norm_text):
                start = max(0, m.start() - radius)
                end = min(len(text), m.end() + radius)
                region = text[start:end]
                anchor_offset = m.start() - start
                for pm in _PERCENT_RE.finditer(region):
                    val = self._parse_percent(pm.group(1))
                    if val is not None:
                        rank = self._rank_candidate(
                            val, region, pm.start(), anchor_offset,
                        )
                        ctx = region[
                            max(0, pm.start() - 30) : pm.end() + 20
                        ].strip()
                        candidates.append((rank, val, f"[regex] {kw!r}: {ctx}"))

        if candidates:
            candidates.sort(key=lambda c: c[0], reverse=True)
            _, val, ctx = candidates[0]
            return val, ctx
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

    def extract_date_from_content(
        self, *, hint_month: int | None = None,
    ) -> str | None:
        """Intenta inferir la fecha YYYY-MM desde el contenido del PDF.

        Busca co-ocurrencias ``mes + año`` próximas entre sí en las
        primeras 3 páginas.  Prioriza la primera co-ocurrencia encontrada
        para evitar que meses de comparaciones interanuales (ej. "en
        enero comparado con …") ganen sobre el mes del informe.

        Parameters
        ----------
        hint_month : int | None
            Si se conoce el mes (ej. del nombre del archivo),
            prioriza coincidencias de ese mes.

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
                # Buscar todas las co-ocurrencias mes+año y devolver la
                # primera (más temprana en el texto).
                best: tuple[int, str] | None = None
                hint_best: tuple[int, str] | None = None
                for name, num in _MONTH_MAP.items():
                    for m in re.finditer(re.escape(name), text):
                        start = max(0, m.start() - 30)
                        end = min(len(text), m.end() + 30)
                        region = text[start:end]
                        year_m = re.search(r"20[12]\d", region)
                        if year_m:
                            pos = m.start()
                            candidate = f"{year_m.group()}-{num:02d}"
                            if best is None or pos < best[0]:
                                best = (pos, candidate)
                            if hint_month and num == hint_month:
                                if hint_best is None or pos < hint_best[0]:
                                    hint_best = (pos, candidate)
                # Preferir coincidencia con el hint_month.
                result = hint_best or best
                if result is not None:
                    return result[1]
        return None

    @staticmethod
    def extract_date_from_filename(filename: str) -> str | None:
        """Infiere la fecha YYYY-MM a partir del nombre del archivo.

        Usa la misma lógica de co-ocurrencia ``mes + año`` que los demás
        extractores.

        Parameters
        ----------
        filename : str
            Nombre del archivo PDF (sin ruta).

        Returns
        -------
        str | None
            Fecha ``"YYYY-MM"`` o ``None``.
        """
        text = filename.lower()
        best: tuple[int, str] | None = None
        for name, num in _MONTH_MAP.items():
            for m in re.finditer(re.escape(name), text):
                start = max(0, m.start() - 30)
                end = min(len(text), m.end() + 30)
                region = text[start:end]
                year_m = re.search(r"20[12]\d", region)
                if year_m:
                    pos = m.start()
                    candidate = f"{year_m.group()}-{num:02d}"
                    if best is None or pos < best[0]:
                        best = (pos, candidate)
        return best[1] if best else None


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
    """Construye, ordena, deduplica y guarda el DataFrame procesado."""
    if not records:
        return pd.DataFrame(columns=ANDI_PROCESSED_COLUMNS)
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates(subset="date", keep="last")
    df = df.reset_index(drop=True)
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


# ── Reprocesamiento de PDFs locales huérfanos ─────────────────────────


def reprocess_local_pdfs() -> pd.DataFrame:
    """Reprocesa PDFs locales que no están en el CSV final.

    Busca todos los PDFs en ``RAW_ANDI_DIR``, identifica cuáles no
    tienen una entrada con valor en el cache (o cuya fecha no aparece
    en el CSV), e intenta extraer el dato de capacidad instalada.

    La fecha se determina en este orden de prioridad:

    1. Nombre del archivo (``extract_date_from_filename``).
    2. Contenido del PDF (``extract_date_from_content``).

    Returns
    -------
    pd.DataFrame
        DataFrame actualizado con todos los registros.
    """
    csv_path = PROCESSED_DIR / ANDI_CONFIG.processed_filename
    cache_path = RAW_ANDI_DIR / ANDI_CONFIG.cache_filename

    cache = _load_cache(cache_path)
    existing_dates = _load_existing_dates(csv_path)

    # Construir set de fechas que YA tienen valor en el cache.
    cached_dates: set[str] = set()
    for entry in cache.values():
        d = entry.get("date")
        v = entry.get("value")
        if d and v is not None:
            cached_dates.add(d)

    # Encontrar PDFs locales.
    local_pdfs = sorted(
        p for p in RAW_ANDI_DIR.glob("*.pdf") if p.is_file()
    )
    logger.info(
        "Reprocess: %d PDFs locales, %d en cache, %d en CSV.",
        len(local_pdfs), len(cached_dates), len(existing_dates),
    )

    new_records: list[dict[str, Any]] = []
    stats: dict[str, int] = {"processed": 0, "ok": 0, "failed": 0, "skipped": 0}

    for pdf_path in local_pdfs:
        # Intentar fecha desde el nombre del archivo.
        date_str = EOICParser.extract_date_from_filename(pdf_path.name)

        # Si la fecha ya existe en CSV y cache, omitir.
        if date_str and date_str in existing_dates and date_str in cached_dates:
            stats["skipped"] += 1
            continue

        stats["processed"] += 1

        # Parsear valor.
        parser = EOICParser(pdf_path)
        result = parser.extract_capacity_utilization()

        if result is None:
            logger.warning("Reprocess FAIL: %s", pdf_path.name)
            stats["failed"] += 1
            continue

        value, context = result

        # Fecha: prioridad filename > content.
        if not date_str:
            # Si el filename tiene mes pero no año, usar como pista.
            hint_month: int | None = None
            fname_lower = pdf_path.name.lower()
            for mname, mnum in _MONTH_MAP.items():
                if mname in fname_lower:
                    hint_month = mnum
                    break
            date_str = parser.extract_date_from_content(
                hint_month=hint_month,
            )
        if not date_str:
            logger.warning(
                "Reprocess: sin fecha para %s (valor=%.1f%%)",
                pdf_path.name, value,
            )
            stats["failed"] += 1
            continue

        # Si la fecha ya existe en el CSV, omitir (podría tener un valor
        # distinto obtenido por otra vía).
        if date_str in existing_dates:
            stats["skipped"] += 1
            continue

        logger.info(
            "Reprocess OK: %s → %s = %.1f%% (%s)",
            pdf_path.name, date_str, value, context[:60],
        )

        # Usar ruta local como clave de cache (sin URL remota).
        cache_key = f"local://{pdf_path.name}"
        cache[cache_key] = {
            "status": "ok",
            "date": date_str,
            "value": value,
            "context": context,
        }

        record = _build_record(date_str, value, f"local://{pdf_path.name}")
        new_records.append(record)
        existing_dates.add(date_str)
        stats["ok"] += 1

    # Persistir cache.
    _save_cache(cache, cache_path)

    # Combinar con registros existentes.
    all_records: list[dict[str, Any]] = []
    if csv_path.exists():
        existing_df = pd.read_csv(csv_path)
        all_records = existing_df.to_dict("records")
    all_records.extend(new_records)

    # Guardar.
    df = _save_dataframe(all_records, csv_path)

    # Reporte.
    report_path = OUTPUTS_DIR / "andi_reprocess_report.txt"
    if new_records:
        generate_report(all_records, stats, report_path)

    logger.info(
        "Reprocess ANDI: %d nuevos, %d total, %d fallidos, %d omitidos",
        stats["ok"], len(df), stats["failed"], stats["skipped"],
    )

    return df
