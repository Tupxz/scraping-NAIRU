"""
ANDI EOIC Web Scraper
Finds and downloads EOIC PDF reports from ANDI's website.

Supports:
  - get_latest_eoic_pdf()  → most recent report (incremental mode)
  - get_all_eoic_pdfs()    → full historical list, oldest first (backfill mode)
"""

import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

ANDI_BASE_URL = "https://www.andi.com.co"
ANDI_EOIC_PAGE = "https://www.andi.com.co/Home/Pagina/3-desarrollo-economico-y-competitividad"

# Spanish month names → month numbers (ordered longest-first to avoid prefix collisions)
MONTH_MAP: Dict[str, int] = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

# ---------------------------------------------------------------------------
# EOIC identification rules
# ---------------------------------------------------------------------------

# Primary: URL or link text must contain one of these to be considered EOIC.
# "eoic" alone is the strongest signal (it is ANDI's own acronym for this survey).
_EOIC_REQUIRED = re.compile(r"\beoic\b", re.IGNORECASE)

# Secondary: broad candidates when primary signal is absent
_EOIC_BROAD = re.compile(
    r"encuesta.*opini[oó]n.*industrial|opini[oó]n.*industrial.*conjunta",
    re.IGNORECASE,
)

# Exclusions: file types or keywords that signal it is NOT a monthly report
_EXCLUDE = re.compile(
    r"metodolog[íi]a|presentaci[oó]n|anexo.tecnico|ficha.tecnica|manual|instructivo",
    re.IGNORECASE,
)

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-CO,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/",
}


class ANDIScraper:
    """Scrapes the ANDI website for EOIC PDF reports."""

    def __init__(self, data_dir: Path = Path("data")):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)

    # ------------------------------------------------------------------
    # Page fetching
    # ------------------------------------------------------------------

    def get_page_content(self, url: str) -> Optional[str]:
        """Fetch raw HTML with retry logic; falls back to Playwright if needed."""
        for attempt in range(3):
            try:
                logger.info(f"Fetching page (attempt {attempt + 1}): {url}")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                logger.info(f"Page fetched: {len(response.text):,} chars")
                return response.text
            except requests.RequestException as exc:
                logger.warning(f"Attempt {attempt + 1} failed: {exc}")
                if attempt < 2:
                    time.sleep(2 ** attempt)

        return self._fetch_with_playwright(url)

    def _fetch_with_playwright(self, url: str) -> Optional[str]:
        """Fallback renderer for JavaScript-heavy pages."""
        try:
            from playwright.sync_api import sync_playwright  # type: ignore

            logger.info("Falling back to Playwright for dynamic content...")
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url, wait_until="networkidle", timeout=30_000)
                content = page.content()
                browser.close()
            logger.info("Playwright fetch successful")
            return content
        except ImportError:
            logger.warning(
                "Playwright not installed. "
                "Run: pip install playwright && playwright install chromium"
            )
            return None
        except Exception as exc:
            logger.error(f"Playwright error: {exc}")
            return None

    # ------------------------------------------------------------------
    # EOIC identification
    # ------------------------------------------------------------------

    def _is_eoic_report(self, url: str, text: str) -> bool:
        """
        Strict check: is this link an EOIC monthly report?
        Requires the EOIC acronym or the full survey name, and rejects
        known non-report documents (methodology, presentations, annexes).
        """
        combined = f"{url} {text}"
        if _EXCLUDE.search(combined):
            return False
        return bool(_EOIC_REQUIRED.search(combined) or _EOIC_BROAD.search(combined))

    # ------------------------------------------------------------------
    # Link discovery
    # ------------------------------------------------------------------

    def find_eoic_links(self, html: str, strict: bool = True) -> List[Dict]:
        """
        Parse HTML and return EOIC PDF candidates.

        strict=True  → only links that pass _is_eoic_report() (default)
        strict=False → all PDF links on the page (broad fallback)
        """
        soup = BeautifulSoup(html, "lxml")
        candidates: List[Dict] = []

        for tag in soup.find_all("a", href=True):
            href: str = tag["href"]
            text: str = tag.get_text(strip=True)

            is_pdf = (
                href.lower().endswith(".pdf")
                or ".pdf" in href.lower()
                or "pdf" in tag.get("type", "").lower()
            )
            if not is_pdf:
                continue

            if strict and not self._is_eoic_report(href, text):
                continue

            full_url = href if href.startswith("http") else urljoin(ANDI_BASE_URL, href)
            candidates.append(
                {
                    "url": full_url,
                    "text": text,
                    "date": self._extract_date(href, text),
                    "href": href,
                }
            )

        if not candidates and strict:
            logger.warning(
                "No EOIC-specific PDFs found with strict filter; "
                "broadening to all PDFs on the page."
            )
            return self.find_eoic_links(html, strict=False)

        logger.info(
            f"Found {len(candidates)} EOIC PDF candidate(s) "
            f"({'strict' if strict else 'broad'} filter)"
        )
        return candidates

    # ------------------------------------------------------------------
    # Date extraction
    # ------------------------------------------------------------------

    def _extract_date(self, url: str, text: str) -> Optional[str]:
        """
        Infer YYYY-MM from a PDF URL + link text.

        Strategy:
          1. URL-decode the href so %2028 → ' 28' (avoids false year hits).
          2. Find the 4-digit year with \b boundary.
          3. Find every Spanish month name; pick the one closest to the year.
          4. Fallback: look for numeric mm in URL separators.
        """
        combined = f"{unquote(url)} {text}".lower()

        # Year
        year_m = re.search(r"\b(20\d{2})\b", combined)
        year = year_m.group(1) if year_m else None
        year_pos = year_m.start() if year_m else None

        # Month — pick the occurrence closest to the year token
        best_month: Optional[int] = None
        best_dist = float("inf")

        for name, num in MONTH_MAP.items():
            for m in re.finditer(rf"\b{re.escape(name)}\b", combined):
                dist = abs(m.start() - year_pos) if year_pos is not None else m.start()
                if dist < best_dist:
                    best_dist = dist
                    best_month = num

        # Numeric month in URL separators (e.g. _01_, -03-)
        if best_month is None:
            num_m = re.search(r"[-_](0[1-9]|1[0-2])[-_.]", combined)
            if num_m:
                best_month = int(num_m.group(1))

        if year and best_month:
            return f"{year}-{best_month:02d}"
        if year:
            return year
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_all_eoic_pdfs(self) -> List[Dict]:
        """
        Return ALL EOIC PDF entries found on the ANDI page,
        deduplicated and sorted oldest-first.
        Used by backfill mode.
        """
        html = self.get_page_content(ANDI_EOIC_PAGE)
        if not html:
            logger.error("Could not fetch ANDI page")
            return []

        candidates = self.find_eoic_links(html, strict=True)

        # Deduplicate by normalised URL
        seen: set = set()
        unique: List[Dict] = []
        for c in candidates:
            key = c["url"].lower().strip()
            if key not in seen:
                seen.add(key)
                unique.append(c)

        with_date = sorted(
            [c for c in unique if c.get("date") and len(c["date"]) == 7],  # YYYY-MM only
            key=lambda x: x["date"],
        )
        without_date = [c for c in unique if not (c.get("date") and len(c["date"]) == 7)]

        total = len(with_date) + len(without_date)
        logger.info(
            f"Total unique EOIC PDFs: {total} "
            f"({len(with_date)} with date, {len(without_date)} without date)"
        )
        return with_date + without_date  # oldest first

    def get_latest_eoic_pdf(self) -> Optional[Dict]:
        """
        Return metadata for the most recent EOIC PDF.
        Used by incremental (default) mode.
        """
        html = self.get_page_content(ANDI_EOIC_PAGE)
        if not html:
            logger.error("Could not fetch ANDI page")
            return None

        candidates = self.find_eoic_links(html, strict=True)
        if not candidates:
            logger.error("No EOIC PDF candidates found on page")
            return None

        dated = [c for c in candidates if c.get("date") and len(c["date"]) == 7]
        if dated:
            dated.sort(key=lambda x: x["date"], reverse=True)
            best = dated[0]
        else:
            best = candidates[0]

        logger.info(f"Latest PDF: {best['url']}  (date={best.get('date', 'unknown')})")
        return best

    def download_pdf(self, url: str) -> Path:
        """
        Download the PDF to the data directory.
        Skips if the file is already on disk.
        """
        raw_name = unquote(url.split("/")[-1].split("?")[0])
        filename = raw_name if raw_name.lower().endswith(".pdf") else \
            f"eoic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"

        dest = self.data_dir / filename
        if dest.exists():
            logger.info(f"PDF already on disk: {dest.name}")
            return dest

        logger.info(f"Downloading: {url}")
        response = self.session.get(url, stream=True, timeout=60)
        response.raise_for_status()

        with open(dest, "wb") as fh:
            for chunk in response.iter_content(chunk_size=8_192):
                fh.write(chunk)

        size_kb = dest.stat().st_size / 1_024
        logger.info(f"Saved: {dest.name} ({size_kb:.1f} KB)")
        return dest
