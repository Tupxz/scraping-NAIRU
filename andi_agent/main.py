"""
ANDI EOIC — Capacity Utilization Rate Collector
================================================
Automated agent with two operating modes:

  DEFAULT (incremental):
      python main.py
      Fetches the latest EOIC report, skips if already stored.

  BACKFILL (historical):
      python main.py --backfill
      Scrapes ALL available EOIC reports, processes every new one,
      and builds a complete chronological CSV.

  FLAGS:
      --backfill   Full historical extraction
      --force      Re-extract even if date already in CSV
      --debug      Enable DEBUG-level logging
"""

import argparse
import csv
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Project layout
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).parent
DATA_DIR = ROOT_DIR / "data"
LOGS_DIR = ROOT_DIR / "logs"
OUTPUTS_DIR = ROOT_DIR / "outputs"
CSV_PATH = OUTPUTS_DIR / "capacidad_instalada.csv"
CACHE_PATH = DATA_DIR / "processed_cache.json"

CSV_FIELDNAMES = ["date", "value", "source_url", "extracted_at"]

for _d in [DATA_DIR, LOGS_DIR, OUTPUTS_DIR]:
    _d.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(debug: bool = False) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOGS_DIR / f"andi_agent_{timestamp}.log"

    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return log_file


# ---------------------------------------------------------------------------
# Processing cache  (data/processed_cache.json)
# Keyed by PDF URL → {date, value, extracted_at}
# Avoids re-downloading and re-parsing PDFs that were already processed.
# ---------------------------------------------------------------------------

def load_cache() -> Dict:
    if CACHE_PATH.exists():
        try:
            with open(CACHE_PATH, encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            logging.getLogger(__name__).warning("Cache file corrupted; starting fresh.")
    return {}


def save_cache(cache: Dict) -> None:
    with open(CACHE_PATH, "w", encoding="utf-8") as fh:
        json.dump(cache, fh, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _load_existing_dates() -> Set[str]:
    if not CSV_PATH.exists():
        return set()
    with open(CSV_PATH, newline="", encoding="utf-8") as fh:
        return {row.get("date", "") for row in csv.DictReader(fh)}


def _write_all_records(records: List[Dict]) -> None:
    """Overwrite the CSV with a sorted, deduplicated list of records."""
    seen: Set[str] = set()
    unique = []
    for r in records:
        if r["date"] not in seen:
            seen.add(r["date"])
            unique.append(r)

    unique.sort(key=lambda r: r.get("date", ""))

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(unique)


def _read_all_records() -> List[Dict]:
    if not CSV_PATH.exists():
        return []
    with open(CSV_PATH, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def append_record(date: str, value: float, source_url: str, force: bool = False) -> bool:
    """
    Append one row to the CSV (sorted insert).
    Returns True if a new record was actually written.
    """
    logger = logging.getLogger(__name__)
    existing = _load_existing_dates()

    if date in existing and not force:
        logger.info(f"Record for {date} already in CSV — skipping")
        return False

    records = _read_all_records()
    # Remove stale entry if force-replacing
    if force:
        records = [r for r in records if r.get("date") != date]

    records.append(
        {
            "date": date,
            "value": str(value),
            "source_url": source_url,
            "extracted_at": datetime.now().isoformat(timespec="seconds"),
        }
    )
    _write_all_records(records)
    return True


def sort_csv() -> None:
    """Re-sort the CSV by date in ascending order."""
    records = _read_all_records()
    if records:
        _write_all_records(records)
        logging.getLogger(__name__).info(f"CSV sorted: {len(records)} record(s) in {CSV_PATH}")


# ---------------------------------------------------------------------------
# Core processing: one PDF
# ---------------------------------------------------------------------------

def _process_one(
    pdf_info: Dict,
    scraper,
    cache: Dict,
    existing_dates: Set[str],
    force: bool,
) -> Optional[Tuple[str, float, str]]:
    """
    Download and extract a single EOIC PDF.
    Returns (date, value, url) on success, None on failure.
    Updates the cache in place.
    """
    from pdf_parser import EOICParser

    logger = logging.getLogger(__name__)
    url = pdf_info["url"]

    # ── Cache hit ──────────────────────────────────────────────────────
    if url in cache and not force:
        cached = cache[url]
        date = cached.get("date", "")
        value = cached.get("value")
        if date and value is not None:
            logger.info(f"  Cache hit → {value}% ({date})")
            return date, float(value), url

    # ── Download ───────────────────────────────────────────────────────
    try:
        pdf_path = scraper.download_pdf(url)
    except Exception as exc:
        logger.error(f"  Download failed: {exc}")
        return None

    # ── Extract ────────────────────────────────────────────────────────
    try:
        parser = EOICParser(pdf_path)
        result = parser.extract_capacity_utilization()
    except Exception as exc:
        logger.error(f"  Extraction error: {exc}")
        return None

    if result is None:
        # Try to show candidate percentages to aid manual debugging
        try:
            candidates = parser.get_all_percentages()
            if candidates:
                logger.warning(
                    f"  Extraction failed. Top candidates: "
                    + ", ".join(f"{v:.1f}%" for v, _ in candidates[:5])
                )
        except Exception:
            pass
        return None

    value, _ = result

    # ── Resolve date ───────────────────────────────────────────────────
    date = pdf_info.get("date") or ""
    if not date or len(date) != 7:
        try:
            date = parser.extract_date_from_content() or date
        except Exception:
            pass

    if not date:
        logger.warning(f"  Could not determine date for {url} — skipping")
        return None

    # ── Update cache ───────────────────────────────────────────────────
    cache[url] = {
        "date": date,
        "value": value,
        "extracted_at": datetime.now().isoformat(timespec="seconds"),
    }

    return date, value, url


# ---------------------------------------------------------------------------
# Mode: incremental (default)
# ---------------------------------------------------------------------------

def run_incremental(scraper, args) -> int:
    logger = logging.getLogger(__name__)
    force = getattr(args, "force", False)
    cache = load_cache()
    existing_dates = _load_existing_dates()

    logger.info("[1/4] Searching for latest EOIC PDF...")
    pdf_info = scraper.get_latest_eoic_pdf()
    if not pdf_info:
        logger.error("No EOIC PDF found. Aborting.")
        return 1

    logger.info(f"      URL  : {pdf_info['url']}")
    logger.info(f"      Date : {pdf_info.get('date', 'unknown')}")
    logger.info(f"      Title: {pdf_info.get('text', '')[:80]}")

    # Early exit
    candidate_date = pdf_info.get("date", "")
    if not force and candidate_date in existing_dates:
        logger.info(f"Record for {candidate_date} already stored. Nothing to do.")
        print(f"\n[OK] Data for {candidate_date} already in {CSV_PATH}. Use --force to refresh.\n")
        return 0

    logger.info("[2/4] Downloading PDF...")
    logger.info("[3/4] Extracting capacity utilization rate...")
    result = _process_one(pdf_info, scraper, cache, existing_dates, force)
    save_cache(cache)

    if result is None:
        logger.error("Extraction failed. See log for candidate values.")
        return 1

    date, value, url = result
    logger.info(f"[4/4] Saving record (date={date}, value={value})...")
    added = append_record(date, value, url, force=force)

    bar = "=" * 56
    print(f"\n{bar}")
    print(f"  EOIC — UTILIZACIÓN DE CAPACIDAD INSTALADA")
    print(bar)
    print(f"  Periodo  : {date}")
    print(f"  Valor    : {value:.1f}%")
    print(f"  Fuente   : {url}")
    print(f"  CSV      : {CSV_PATH}")
    print(f"  Nuevo    : {'Sí' if added else 'No (ya existía)'}")
    print(f"{bar}\n")

    logger.info("Incremental run completed.")
    return 0


# ---------------------------------------------------------------------------
# Mode: backfill (historical)
# ---------------------------------------------------------------------------

def run_backfill(scraper, args) -> int:
    logger = logging.getLogger(__name__)
    force = getattr(args, "force", False)
    cache = load_cache()
    existing_dates = _load_existing_dates()

    logger.info("BACKFILL MODE — fetching full list of EOIC PDFs from ANDI...")
    all_pdfs = scraper.get_all_eoic_pdfs()

    if not all_pdfs:
        logger.error("No EOIC PDFs found on ANDI page. Aborting.")
        return 1

    total = len(all_pdfs)
    logger.info(f"Found {total} EOIC PDF(s). Starting extraction...")

    new_records: List[Tuple[str, float, str]] = []
    skipped = 0
    failed = 0

    for i, pdf_info in enumerate(all_pdfs, 1):
        url = pdf_info["url"]
        label = pdf_info.get("text") or pdf_info.get("date") or url.split("/")[-1][:50]
        logger.info(f"[{i:>3}/{total}] {label}")

        # Skip if already in CSV and not forcing
        candidate_date = pdf_info.get("date", "")
        if not force and candidate_date and candidate_date in existing_dates:
            logger.info(f"        Already in CSV ({candidate_date}) — skipping")
            skipped += 1
            continue

        result = _process_one(pdf_info, scraper, cache, existing_dates, force)
        save_cache(cache)  # Persist after every PDF in case of interruption

        if result is None:
            failed += 1
            continue

        date, value, src_url = result
        logger.info(f"        → {value:.1f}% ({date})")

        if date not in existing_dates or force:
            new_records.append((date, value, src_url))
            existing_dates.add(date)
        else:
            logger.info(f"        Date {date} already in CSV — not adding duplicate")
            skipped += 1

    # Bulk write + sort
    if new_records:
        records = _read_all_records()
        if force:
            force_dates = {r[0] for r in new_records}
            records = [r for r in records if r.get("date") not in force_dates]
        for date, value, url in new_records:
            records.append(
                {
                    "date": date,
                    "value": str(value),
                    "source_url": url,
                    "extracted_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
        _write_all_records(records)

    # Summary
    bar = "=" * 60
    print(f"\n{bar}")
    print(f"  BACKFILL COMPLETE — EOIC Capacidad Instalada")
    print(bar)
    print(f"  PDFs encontrados   : {total}")
    print(f"  Nuevos registros   : {len(new_records)}")
    print(f"  Ya existían (skip) : {skipped}")
    print(f"  Fallos extracción  : {failed}")
    print(f"  CSV                : {CSV_PATH}")
    print(f"{bar}\n")

    logger.info(
        f"Backfill done — found={total}, new={len(new_records)}, "
        f"skipped={skipped}, failed={failed}"
    )
    return 0 if failed < total else 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="ANDI EOIC Capacity Utilization Collector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python main.py                  # incremental: latest report only\n"
            "  python main.py --backfill       # full historical extraction\n"
            "  python main.py --backfill --force  # re-extract everything\n"
            "  python main.py --debug          # verbose logging\n"
        ),
    )
    parser.add_argument("--backfill", action="store_true", help="Extract full historical series")
    parser.add_argument("--force", action="store_true", help="Re-extract even if already stored")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG logging")
    args = parser.parse_args()

    log_file = setup_logging(debug=args.debug)
    logger = logging.getLogger(__name__)
    mode = "BACKFILL" if args.backfill else "INCREMENTAL"

    logger.info("=" * 62)
    logger.info(f"  ANDI EOIC Collection Agent — {mode}")
    logger.info(f"  {datetime.now().isoformat(timespec='seconds')}")
    logger.info(f"  Log → {log_file}")
    logger.info("=" * 62)

    try:
        from scraper import ANDIScraper

        scraper = ANDIScraper(data_dir=DATA_DIR)

        if args.backfill:
            return run_backfill(scraper, args)
        else:
            return run_incremental(scraper, args)

    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Interrupted by user.")
        return 130
    except Exception:
        logging.getLogger(__name__).exception("Unhandled error — agent terminated.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
