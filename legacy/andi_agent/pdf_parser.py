"""
PDF Parser for EOIC reports.
Extracts the monthly capacity utilization rate using three progressive strategies:
  1. Fuzzy full-text search (sliding-window similarity)
  2. Structured table scan
  3. Direct regex fallback

Also exposes extract_date_from_content() for PDFs whose filenames carry no date.

Designed to handle formatting changes across report editions.
"""

import logging
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pdfplumber

# ---------------------------------------------------------------------------
# Month map (mirrors scraper.MONTH_MAP; kept here for standalone use)
# ---------------------------------------------------------------------------
MONTH_MAP: Dict[str, int] = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Search vocabulary (handles accent/no-accent variants and abbreviations)
# ---------------------------------------------------------------------------

CAPACITY_PHRASES: List[str] = [
    "utilización de capacidad instalada",
    "utilizacion de capacidad instalada",
    "utilización de la capacidad instalada",
    "utilizacion de la capacidad instalada",
    "capacidad instalada utilizada",
    "uso de la capacidad instalada",
    "tasa de utilización de capacidad",
    "tasa de utilizacion de capacidad",
    "grado de utilización de la capacidad",
    "nivel de utilización de la capacidad",
    "capacidad utilizada",
    "utilización de capacidad",
    "utilizacion de capacidad",
    "uci",  # common abbreviation in EOIC tables
]

# Percentages: handles 78.5% / 78,5% / 78 %
PERCENT_RE = re.compile(r"(\d{1,3}[.,]\d{1,2})\s*%|\b(\d{2,3})\s*%")

# Plausible range for industrial capacity utilization (%)
VALUE_MIN, VALUE_MAX = 30.0, 100.0

# Characters of surrounding text captured as evidence
CONTEXT_WINDOW = 250


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize(text: str) -> str:
    """Lowercase + strip accents/tildes for accent-insensitive comparison."""
    accent_map = str.maketrans(
        "áéíóúÁÉÍÓÚñÑ",
        "aeiouAEIOUnN",
    )
    return text.translate(accent_map).lower()


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, _normalize(a), _normalize(b)).ratio()


def _parse_percent(raw: str) -> Optional[float]:
    """Parse a raw string like '78,5' or '78.5' to a float; return None if out of range."""
    try:
        value = float(raw.replace(",", "."))
    except ValueError:
        return None
    return value if VALUE_MIN <= value <= VALUE_MAX else None


def _nearest_percent(text: str, anchor: int, window: int = 350) -> Optional[float]:
    """Find the first plausible percentage value within `window` chars of `anchor`."""
    snippet = text[max(0, anchor - 50): min(len(text), anchor + window)]
    for m in PERCENT_RE.finditer(snippet):
        raw = m.group(1) or m.group(2) or ""
        value = _parse_percent(raw)
        if value is not None:
            return value
    return None


# ---------------------------------------------------------------------------
# Parser class
# ---------------------------------------------------------------------------

class EOICParser:
    """
    Extracts the capacity utilization rate from an EOIC PDF.

    Usage:
        parser = EOICParser(Path("data/eoic_2025_03.pdf"))
        result = parser.extract_capacity_utilization()
        if result:
            value, context = result
            print(f"{value}%")
    """

    def __init__(self, pdf_path: Path):
        self.pdf_path = Path(pdf_path)
        self._pages: List[str] = []
        self._full_text: str = ""

    # ------------------------------------------------------------------
    # Internal: PDF loading
    # ------------------------------------------------------------------

    def _load(self) -> None:
        if self._pages:
            return

        logger.info(f"Opening PDF: {self.pdf_path}")
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                for i, page in enumerate(pdf.pages):
                    txt = page.extract_text() or ""
                    self._pages.append(txt)
                    logger.debug(f"  Page {i + 1}: {len(txt)} chars")

            self._full_text = "\n".join(self._pages)
            logger.info(
                f"PDF loaded: {len(self._pages)} pages, "
                f"{len(self._full_text):,} total chars"
            )
        except Exception as exc:
            logger.error(f"Failed to open PDF: {exc}")
            raise

    # ------------------------------------------------------------------
    # Strategy 1: Fuzzy full-text search
    # ------------------------------------------------------------------

    def _strategy_text(self) -> Optional[Tuple[float, str]]:
        """Sliding-window fuzzy match across the full text corpus."""
        text = self._full_text
        text_norm = _normalize(text)

        best_pos: Optional[int] = None
        best_sim = 0.0

        for phrase in CAPACITY_PHRASES:
            norm_phrase = _normalize(phrase)
            plen = len(norm_phrase)
            if plen > len(text_norm):
                continue

            for i in range(0, len(text_norm) - plen + 1, max(1, plen // 4)):
                sim = _similarity(norm_phrase, text_norm[i: i + plen])
                if sim > best_sim and sim >= 0.82:
                    best_sim = sim
                    best_pos = i

        if best_pos is None:
            logger.debug("Strategy 1: no phrase match at threshold ≥ 0.82")
            return None

        value = _nearest_percent(text, best_pos)
        if value is None:
            logger.debug(f"Strategy 1: phrase found (sim={best_sim:.2f}) but no valid % nearby")
            return None

        snippet = text[max(0, best_pos - 30): min(len(text), best_pos + CONTEXT_WINDOW)]
        context = " ".join(snippet.split())
        logger.info(f"Strategy 1 — value: {value}%  (similarity={best_sim:.2f})")
        return value, context

    # ------------------------------------------------------------------
    # Strategy 2: Structured table scan
    # ------------------------------------------------------------------

    def _strategy_tables(self) -> Optional[Tuple[float, str]]:
        """Scan all PDF tables for a row whose label matches capacity utilization."""
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, start=1):
                    for table in page.extract_tables() or []:
                        result = self._scan_table(table, page_num)
                        if result:
                            return result
        except Exception as exc:
            logger.warning(f"Strategy 2 (tables) error: {exc}")
        return None

    def _scan_table(self, table: List[List], page_num: int) -> Optional[Tuple[float, str]]:
        for row in table:
            if not row:
                continue
            cells = [str(c).strip() for c in row if c is not None]
            if not cells:
                continue

            label = cells[0]
            row_text = " | ".join(cells)

            for phrase in CAPACITY_PHRASES:
                if _similarity(phrase, label) >= 0.75 or _normalize(phrase) in _normalize(label):
                    # Found the label row — look for numeric value in remaining cells
                    for cell in cells[1:]:
                        for m in PERCENT_RE.finditer(cell):
                            raw = m.group(1) or m.group(2) or ""
                            value = _parse_percent(raw)
                            if value is not None:
                                context = f"Table p.{page_num}: {row_text}"
                                logger.info(f"Strategy 2 — value: {value}%  (table, page {page_num})")
                                return value, context

                        # Bare number without % sign
                        num_m = re.search(r"(\d{2,3}[.,]\d{1,2})", cell)
                        if num_m:
                            value = _parse_percent(num_m.group(1))
                            if value is not None:
                                context = f"Table p.{page_num}: {row_text}"
                                logger.info(f"Strategy 2 — value: {value}%  (table/no-sign, page {page_num})")
                                return value, context
        return None

    # ------------------------------------------------------------------
    # Strategy 3: Direct regex
    # ------------------------------------------------------------------

    def _strategy_regex(self) -> Optional[Tuple[float, str]]:
        """Direct regex combining keyword proximity + percentage in one pass."""
        text = self._full_text

        patterns = [
            r"capacidad\s+instalada[^%\n]{0,120}?(\d{1,3}[.,]\d{1,2})\s*%",
            r"utilizaci[oó]n[^%\n]{0,80}?(\d{1,3}[.,]\d{1,2})\s*%",
            r"(\d{1,3}[.,]\d{1,2})\s*%[^.\n]{0,80}?capacidad\s+instalada",
            # Bare percentage after the phrase (no % sign if the column header carries it)
            r"capacidad\s+instalada[^.\n]{0,60}?(\d{2,3}[.,]\d{1,2})\b",
        ]

        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if m:
                raw = m.group(1)
                value = _parse_percent(raw)
                if value is not None:
                    start = max(0, m.start() - 60)
                    end = min(len(text), m.end() + 60)
                    context = " ".join(text[start:end].split())
                    logger.info(f"Strategy 3 (regex) — value: {value}%")
                    return value, context

        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_capacity_utilization(self) -> Optional[Tuple[float, str]]:
        """
        Run all extraction strategies in order of confidence.
        Returns (value_float, context_snippet) or None.
        """
        self._load()

        for label, strategy in [
            ("fuzzy text search", self._strategy_text),
            ("table scan", self._strategy_tables),
            ("direct regex", self._strategy_regex),
        ]:
            logger.info(f"Trying strategy: {label}...")
            result = strategy()
            if result:
                return result

        logger.error("All strategies exhausted — value not found.")
        return None

    def get_all_percentages(self) -> List[Tuple[float, str]]:
        """
        Debug helper: return every percentage in the document that falls in the
        plausible range [VALUE_MIN, VALUE_MAX], with surrounding context.
        Useful when automated extraction fails.
        """
        self._load()
        results: List[Tuple[float, str]] = []
        for m in PERCENT_RE.finditer(self._full_text):
            raw = m.group(1) or m.group(2) or ""
            value = _parse_percent(raw)
            if value is None:
                continue
            start = max(0, m.start() - 100)
            end = min(len(self._full_text), m.end() + 100)
            context = " ".join(self._full_text[start:end].split())
            results.append((value, context))
        return results

    def extract_date_from_content(self) -> Optional[str]:
        """
        Infer the report's YYYY-MM from the PDF's own text (first 3 pages).
        Looks for patterns like:
          "Encuesta de Opinión Industrial — Enero de 2025"
          "EOIC Febrero 2024"
          "Resultados marzo 2023"
        Returns "YYYY-MM" or None.
        """
        self._load()
        text = _normalize("\n".join(self._pages[:3]))

        # Find year
        year_m = re.search(r"\b(20\d{2})\b", text)
        if not year_m:
            return None
        year = year_m.group(1)
        year_pos = year_m.start()

        # Find closest month name to the year token
        best_month: Optional[int] = None
        best_dist = float("inf")
        for name, num in MONTH_MAP.items():
            for m in re.finditer(rf"\b{re.escape(name)}\b", text):
                dist = abs(m.start() - year_pos)
                if dist < best_dist:
                    best_dist = dist
                    best_month = num

        if best_month and best_dist < 120:  # month must be within ~2 lines of the year
            result = f"{year}-{best_month:02d}"
            logger.info(f"Date from PDF content: {result} (dist={best_dist})")
            return result

        return None

    def extract_indicator(self, phrases: List[str], label: str = "indicator") -> Optional[Tuple[float, str]]:
        """
        Generic extraction entry-point for other economic indicators.
        Pass a list of target phrases and a human-readable label.
        Returns (value, context) or None.
        """
        original_phrases = CAPACITY_PHRASES[:]
        CAPACITY_PHRASES.clear()
        CAPACITY_PHRASES.extend(phrases)
        try:
            self._load()
            result = self.extract_capacity_utilization()
        finally:
            CAPACITY_PHRASES.clear()
            CAPACITY_PHRASES.extend(original_phrases)

        if result:
            logger.info(f"Generic extraction '{label}': {result[0]}%")
        return result
