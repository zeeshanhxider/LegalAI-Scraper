#!/usr/bin/env python3
"""
Requirements implemented:

1) Save HTML + PNG in SAME folder as PDF:
   downloads/published_opinions/download/<case_number>/

2) Do NOT save listing page (courts.ca.gov/opinions/publishedcitable-opinions)
   Only save the case page opened by title <a> link (appellatecases.courtinfo.ca.gov)

3) For each case:
   - Find the title <a> on listing page
   - Get href
   - Open that href (case detail page)
   - Save HTML and PNG from THAT page
"""

from __future__ import annotations

import argparse
import csv
import datetime
import logging
import re
import time
from pathlib import Path
from urllib.parse import urlencode, urljoin, urlparse, parse_qsl

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError  # type: ignore

SEARCH_BASE_URL = "https://courts.ca.gov/opinions/publishedcitable-opinions"
CASE_BASE_URL = "https://appellatecases.courtinfo.ca.gov"

ROOT_DIR = Path(__file__).resolve().parent
LOG_DIR = ROOT_DIR / "logs"
METADATA_DIR = ROOT_DIR / "downloads" / "published_opinions"
PDF_DOWNLOAD_BASE = METADATA_DIR / "download"  # ✅ HTML/PNG will also go here

DEFAULT_LOG_PATH = LOG_DIR / f"published-opinions-{datetime.date.today():%Y%m%d}.log"

METADATA_FIELDS = [
    "case_number",
    "date",
    "court",
    "opinion_type",
    "title",
    "case_info_url",
    "pdf_url",
    "pdf_filename",
    "download_status",
    "case_html_filename",
    "case_png_filename",
    "case_page_status",
]

logger = logging.getLogger("published_opinions")


# -----------------------
# Utilities
# -----------------------

def configure_logging(level: str, log_path: Path) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("[%(levelname)s] %(asctime)s %(message)s", "%Y-%m-%d %H:%M:%S")

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.info("Logging to %s", log_path)


def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": "LegalAI-Scraper/PublishedOpinions",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return session


def sanitize_filename(value: str) -> str:
    cleaned = re.sub(r"[^\w\s-]", "", value or "")
    cleaned = re.sub(r"\s+", "_", cleaned).strip("_")
    return cleaned[:220] if cleaned else "file"


def metadata_csv_path() -> Path:
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    return METADATA_DIR / f"{datetime.date.today():%Y%m%d}-metadata.csv"


def load_existing_case_numbers(path: Path) -> set[str]:
    if not path.exists():
        return set()
    seen: set[str] = set()
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cn = (row.get("case_number") or "").strip()
            if cn:
                seen.add(cn)
    return seen


def case_folder(case_number: str) -> Path:
    # ✅ This is the SAME folder where PDF is stored
    d = PDF_DOWNLOAD_BASE / sanitize_filename(case_number)
    d.mkdir(parents=True, exist_ok=True)
    return d


def fetch_listing_page(session: requests.Session, page: int) -> str:
    url = f"{SEARCH_BASE_URL}?{urlencode({'page': str(page)})}"
    logger.info("Fetching listing page %d: %s", page, url)
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def parse_pagination(html: str) -> tuple[int, int, int]:
    m = re.search(r"(\d+)\s*-\s*(\d+)\s*of\s*([0-9,]+)\s*results", html, re.IGNORECASE)
    if not m:
        return 0, 0, 0
    start = int(m.group(1))
    end = int(m.group(2))
    total = int(m.group(3).replace(",", ""))
    return start, end, total


def extract_case_number_from_url(url: str) -> str:
    parsed = urlparse(url)
    params = dict(parse_qsl(parsed.query))
    return params.get("query_caseNumber") or params.get("caseNumber") or ""


def parse_entries(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    entries: list[dict[str, str]] = []

    for card in soup.select("div.result-excerpt"):
        title_anchor = card.select_one("div.result-excerpt__title h2 a")
        if not title_anchor or not title_anchor.get("href"):
            continue

        title = title_anchor.get_text(strip=True)
        case_info_url = urljoin(CASE_BASE_URL, title_anchor["href"])

        case_number_element = card.select_one(".result-excerpt__brow-primary")
        date_element = card.select_one(".result-excerpt__brow-secondary")
        notation_element = card.select_one(".result-excerpt__brow-notation")

        case_number = case_number_element.get_text(strip=True) if case_number_element else ""
        if not case_number:
            case_number = extract_case_number_from_url(case_info_url)
        if not case_number:
            continue

        case_date = date_element.get_text(strip=True) if date_element else ""
        court = ""
        opinion_type = "Published Opinion"

        if notation_element:
            notation = notation_element.get_text(" ", strip=True)
            if "•" in notation:
                parts = [p.strip() for p in notation.split("•", 1)]
                court = parts[0]
                if len(parts) > 1:
                    opinion_type = parts[1]
            else:
                court = notation

        pdf_url = ""
        pdf_anchor = card.select_one("a.button.file")
        if pdf_anchor and pdf_anchor.get("href"):
            pdf_url = urljoin("https://www.courts.ca.gov", pdf_anchor["href"])
        else:
            for a in card.select("a[href]"):
                href = (a.get("href") or "").strip()
                txt = (a.get_text(" ", strip=True) or "").lower()
                if ".pdf" in href.lower() or txt.startswith("pdf"):
                    pdf_url = urljoin("https://www.courts.ca.gov", href)
                    break

        entries.append(
            {
                "case_number": case_number,
                "date": case_date,
                "court": court,
                "opinion_type": opinion_type,
                "title": title,
                "case_info_url": case_info_url,  # ✅ from the <a> href
                "pdf_url": pdf_url,
            }
        )

    return entries


def download_pdf(session: requests.Session, pdf_url: str, case_number: str) -> tuple[str, str]:
    if not pdf_url:
        return "", "missing_pdf"

    folder = case_folder(case_number)
    parsed = urlparse(pdf_url)
    filename = Path(parsed.path).name or f"{sanitize_filename(case_number)}.pdf"
    path = folder / filename

    if path.exists():
        return filename, "cached"

    logger.info("Downloading PDF for %s: %s", case_number, pdf_url)
    try:
        r = session.get(pdf_url, stream=True, timeout=60)
        r.raise_for_status()
        with path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=32768):
                if chunk:
                    f.write(chunk)
        return filename, "downloaded"
    except Exception as e:
        logger.warning("PDF download failed for %s: %s", case_number, e)
        return "", "download_error"


def _looks_blocked(html: str) -> bool:
    h = (html or "").lower()
    return ("request rejected" in h) or ("support id" in h and "rejected" in h)


# -----------------------
# ✅ Core: open the title <a> link and save THAT page only
# -----------------------

def save_case_html_png(case_info_url: str, case_number: str, title: str, headless: bool) -> tuple[str, str, str]:
    """
    ✅ Saves HTML+PNG for the CASE PAGE (appellatecases...), NOT the listing.

    Strategy:
    - Open listing page in Playwright
    - Locate the result card by case_number
    - Grab the title <a> and click it
      - If it opens a new tab -> use popup page
      - Else -> use same page after navigation
    - Save HTML first, then screenshot with fallbacks
    - Save into SAME folder as PDF: downloads/published_opinions/download/<case_number>/
    """
    folder = case_folder(case_number)
    base = sanitize_filename(title) or sanitize_filename(case_number)

    html_filename = f"{base}.html"
    png_filename = f"{base}.png"
    html_path = folder / html_filename
    png_path = folder / png_filename

    if html_path.exists() and png_path.exists():
        return html_filename, png_filename, "cached"

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-software-rasterizer",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
            ),
        )
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")

        page = context.new_page()
        page.goto(SEARCH_BASE_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(800)

        # Find the exact card by case number, then the title link inside it
        locator = page.get_by_text(case_number, exact=False)
        if locator.count() == 0:
            browser.close()
            return "", "", "not_found_on_listing"

        card = locator.first.locator("xpath=ancestor::div[contains(@class,'result-excerpt')]")
        link = card.locator("css=div.result-excerpt__title h2 a").first
        if link.count() == 0:
            browser.close()
            return "", "", "title_link_not_found"

        # Confirm href matches what we parsed (optional safety)
        href = link.get_attribute("href") or ""
        opened_url = urljoin(CASE_BASE_URL, href) if href else case_info_url

        # Click and capture popup if it opens a new tab
        case_page = None
        try:
            with page.expect_popup(timeout=5000) as pop:
                link.click()
            case_page = pop.value
        except Exception:
            # No popup -> navigation in same tab
            link.click()
            case_page = page

        # Wait for case page load
        try:
            case_page.wait_for_load_state("domcontentloaded", timeout=60000)
            case_page.wait_for_load_state("networkidle", timeout=60000)
        except PlaywrightTimeoutError:
            pass

        # ✅ IMPORTANT: ensure we are on case_info_url domain
        final_url = case_page.url
        if "appellatecases.courtinfo.ca.gov" not in final_url:
            # Sometimes it doesn't navigate; force open the case url in the SAME context (keeps cookies)
            case_page.goto(opened_url, wait_until="domcontentloaded", timeout=60000)
            try:
                case_page.wait_for_load_state("networkidle", timeout=60000)
            except PlaywrightTimeoutError:
                pass

        # ✅ Save HTML FIRST (even if screenshot fails)
        html = case_page.content()
        html_path.write_text(html, encoding="utf-8")

        if _looks_blocked(html):
            browser.close()
            # HTML saved, but it is blocked page
            return html_filename, "", "blocked_html_saved"

        # Screenshot with fallback
        try:
            case_page.screenshot(path=str(png_path), full_page=True)
            browser.close()
            return html_filename, png_filename, "saved"
        except Exception:
            try:
                case_page.screenshot(path=str(png_path), full_page=False)
                browser.close()
                return html_filename, png_filename, "saved_viewport_only"
            except Exception as e:
                browser.close()
                return html_filename, "", f"html_saved_png_failed:{type(e).__name__}"


# -----------------------
# Main
# -----------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--start-page", type=int, default=0)
    parser.add_argument("--max-pages", type=int, default=0, help="0 = no limit")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay between cases (seconds)")
    parser.add_argument("--headless", action="store_true", help="Headless mode (may block more often)")
    args = parser.parse_args()

    configure_logging(args.log_level, DEFAULT_LOG_PATH)
    PDF_DOWNLOAD_BASE.mkdir(parents=True, exist_ok=True)

    session = create_session()
    csv_path = metadata_csv_path()
    seen = load_existing_case_numbers(csv_path)
    file_exists = csv_path.exists()

    processed = 0
    current_page = args.start_page
    total_results = None
    per_page = 50

    try:
        with csv_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=METADATA_FIELDS)
            if not file_exists:
                writer.writeheader()

            while True:
                listing_html = fetch_listing_page(session, current_page)
                start, end, total = parse_pagination(listing_html)
                if total_results is None and total:
                    total_results = total
                if end and start:
                    per_page = max(per_page, (end - start + 1))

                entries = parse_entries(listing_html)
                logger.info("Page %d: %d entries", current_page, len(entries))

                for entry in entries:
                    case_number = entry["case_number"]
                    if not case_number or case_number in seen:
                        continue

                    title = entry.get("title", "")
                    pdf_url = entry.get("pdf_url", "")
                    case_info_url = entry.get("case_info_url", "")

                    # 1) PDF
                    pdf_filename, download_status = download_pdf(session, pdf_url, case_number)

                    # 2) ✅ CASE PAGE HTML + PNG (NOT listing)
                    html_name, png_name, page_status = ("", "", "skipped")
                    if case_info_url:
                        html_name, png_name, page_status = save_case_html_png(
                            case_info_url=case_info_url,
                            case_number=case_number,
                            title=title,
                            headless=args.headless,
                        )

                    row = {
                        "case_number": case_number,
                        "date": entry.get("date", ""),
                        "court": entry.get("court", ""),
                        "opinion_type": entry.get("opinion_type", ""),
                        "title": title,
                        "case_info_url": case_info_url,
                        "pdf_url": pdf_url,
                        "pdf_filename": pdf_filename,
                        "download_status": download_status,
                        "case_html_filename": html_name,
                        "case_png_filename": png_name,
                        "case_page_status": page_status,
                    }

                    writer.writerow(row)
                    f.flush()
                    seen.add(case_number)
                    processed += 1

                    time.sleep(max(0.0, args.delay))

                # stop
                if total_results is not None and (current_page + 1) * per_page >= total_results:
                    break
                current_page += 1
                if args.max_pages and (current_page - args.start_page) >= args.max_pages:
                    break

    except Exception as e:
        logger.exception("Fatal error: %s", e)
        return 1

    logger.info("Done. New cases processed: %d", processed)
    logger.info("PDF + HTML + PNG folders are under: %s", PDF_DOWNLOAD_BASE.resolve())
    logger.info("CSV: %s", csv_path.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
