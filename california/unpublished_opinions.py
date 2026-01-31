#!/usr/bin/env python3

import argparse
import csv
import datetime
import logging
import re
import time
from pathlib import Path
from urllib.parse import urlencode, urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError  # type: ignore


SEARCH_BASE_URL = "https://courts.ca.gov/opinions/unpublishednon-citable-opinions"
CASE_BASE_URL = "https://appellatecases.courtinfo.ca.gov"

ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / "logs"
DOWNLOAD_ROOT = ROOT / "downloads" / "unpublished_opinions" / "download"
META_DIR = ROOT / "downloads" / "unpublished_opinions"

LOG_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / f"unpublished-opinions-{datetime.date.today():%Y%m%d}.log"
CSV_FILE = META_DIR / f"{datetime.date.today():%Y%m%d}-metadata.csv"

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
log = logging.getLogger("scraper")


# ✅ CSV fields exactly as you requested
CSV_FIELDS = [
    "case_number",
    "date",
    "court",
    "opinion_type",
    "title",
    "case_info_url",
    "pdf_url",
    "pdf_filename",
    "download_status",
    "tabs_status",
]


# -----------------------------
# HTTP helpers
# -----------------------------

def create_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.6, status_forcelist=(500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return s


def case_folder(case_number: str) -> Path:
    p = DOWNLOAD_ROOT / case_number
    p.mkdir(parents=True, exist_ok=True)
    return p


def fetch_listing_page(session: requests.Session, page: int) -> str:
    url = f"{SEARCH_BASE_URL}?{urlencode({'page': str(page)})}"
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def parse_total_results(listing_html: str) -> int:
    m = re.search(r"of\s*([0-9,]+)\s*results", listing_html, re.IGNORECASE)
    if not m:
        return 0
    return int(m.group(1).replace(",", ""))


def extract_case_number_from_case_url(case_url: str) -> str:
    qs = parse_qs(urlparse(case_url).query)
    doc_no = (qs.get("doc_no") or [""])[0].strip()
    if doc_no:
        return doc_no
    return ""


def _find_pdf_url_in_card(card) -> str:
    for a in card.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        hl = href.lower()
        if "/opinions/documents/" in hl and ".pdf" in hl:
            return urljoin("https://www.courts.ca.gov", href)
        if hl.endswith(".pdf"):
            return urljoin("https://www.courts.ca.gov", href)
    return ""


# -----------------------------
# ✅ Parsing listing entries incl date/court/opinion_type
# -----------------------------

def parse_entries(listing_html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(listing_html, "lxml")
    rows: list[dict[str, str]] = []

    for card in soup.select("div.result-excerpt"):
        a = card.select_one("div.result-excerpt__title h2 a")
        if not a or not a.get("href"):
            continue

        title = a.get_text(strip=True)
        case_url = urljoin(CASE_BASE_URL, a.get("href"))

        # case_number
        num_el = card.select_one(".result-excerpt__brow-primary")
        case_number = num_el.get_text(strip=True) if num_el else ""
        if not case_number:
            case_number = extract_case_number_from_case_url(case_url)
        if not case_number:
            log.warning("Skipping entry with no case number: %s (%s)", title, case_url)
            continue

        # ✅ date
        date_el = card.select_one(".result-excerpt__brow-secondary")
        date_str = date_el.get_text(strip=True) if date_el else ""

        # ✅ court + opinion_type from "court • opinion"
        court = ""
        opinion_type = ""
        notation_el = card.select_one(".result-excerpt__brow-notation")
        if notation_el:
            notation = notation_el.get_text(" ", strip=True)
            # example: "6th District Court of Appeal • Published Opinion"
            if "•" in notation:
                parts = [p.strip() for p in notation.split("•", 1)]
                court = parts[0] if len(parts) > 0 else ""
                opinion_type = parts[1] if len(parts) > 1 else ""
            else:
                court = notation.strip()

        pdf_url = _find_pdf_url_in_card(card)

        rows.append(
            {
                "case_number": case_number,
                "date": date_str,
                "court": court,
                "opinion_type": opinion_type,
                "title": title,
                "case_info_url": case_url,
                "pdf_url": pdf_url,
            }
        )

    return rows


# -----------------------------
# PDF download
# -----------------------------

def download_pdf(session: requests.Session, pdf_url: str, case_number: str) -> tuple[str, str]:
    if not pdf_url:
        return "", "missing_pdf"

    folder = case_folder(case_number)
    filename = Path(urlparse(pdf_url).path).name or f"{case_number}.PDF"
    path = folder / filename

    if path.exists():
        return filename, "cached"

    log.info("Downloading PDF for %s: %s", case_number, pdf_url)
    try:
        r = session.get(pdf_url, stream=True, timeout=60)
        r.raise_for_status()
        with open(path, "wb") as f:
            for c in r.iter_content(32768):
                if c:
                    f.write(c)
        return filename, "downloaded"
    except Exception as e:
        log.warning("PDF download failed %s : %s", case_number, e)
        return "", "download_error"


# -----------------------------
# Playwright save tabs
# -----------------------------

def looks_blocked(html: str) -> bool:
    h = (html or "").lower()
    return "request rejected" in h or ("support id" in h and "rejected" in h)


def _save_current_page(case_page, out_dir: Path, suffix: str) -> str:
    html_path = out_dir / f"{suffix}.html"
    png_path = out_dir / f"{suffix}.png"

    html = case_page.content()
    html_path.write_text(html, encoding="utf-8")

    if looks_blocked(html):
        return "blocked_html_saved"

    return "saved"
    try:
        case_page.screenshot(path=str(png_path), full_page=True)
        return "saved"
    except Exception:
        try:
            case_page.screenshot(path=str(png_path), full_page=False)
            return "saved_viewport_only"
        except Exception as e:
            return f"html_saved_png_failed:{type(e).__name__}"


def save_all_tabs_for_case(context, case_number: str, case_info_url: str, out_dir: Path) -> dict:
    tabs = [
        ("docket", "Docket"),
        ("briefs", "Briefs"),
        ("scheduled_actions", "Scheduled Actions"),
        ("disposition", "Disposition"),
        ("parties_and_attorneys", "Parties and Attorneys"),
        ("trial_court", "Trial Court"),
    ]

    result = {}

    page = context.new_page()
    page.goto(SEARCH_BASE_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(600)

    locator = page.get_by_text(case_number, exact=False)
    if locator.count() == 0:
        page.close()
        return {"error": "case_not_found_on_listing"}

    card = locator.first.locator("xpath=ancestor::div[contains(@class,'result-excerpt')]")
    link = card.locator("css=div.result-excerpt__title h2 a").first

    try:
        with page.expect_popup(timeout=5000) as pop:
            link.click()
        case_page = pop.value
    except Exception:
        link.click()
        case_page = page

    try:
        case_page.wait_for_load_state("domcontentloaded", timeout=60000)
        case_page.wait_for_load_state("networkidle", timeout=60000)
    except PlaywrightTimeoutError:
        pass

    if "appellatecases.courtinfo.ca.gov" not in case_page.url:
        case_page.goto(case_info_url, wait_until="domcontentloaded", timeout=60000)
        try:
            case_page.wait_for_load_state("networkidle", timeout=60000)
        except PlaywrightTimeoutError:
            pass

    # save default page
    result["case_summary"] = _save_current_page(case_page, out_dir, "case_summary")

    # click tabs
    for suffix, label in tabs:
        try:
            case_page.get_by_text(label, exact=True).click(timeout=8000)
            case_page.wait_for_timeout(350)
            try:
                case_page.wait_for_load_state("networkidle", timeout=8000)
            except PlaywrightTimeoutError:
                pass
            result[suffix] = _save_current_page(case_page, out_dir, suffix)
        except Exception:
            result[suffix] = "tab_click_failed"

    return result


# -----------------------------
# CSV helpers
# -----------------------------

def load_seen_cases(csv_path: Path) -> set[str]:
    if not csv_path.exists():
        return set()
    seen = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            cn = (row.get("case_number") or "").strip()
            if cn:
                seen.add(cn)
    return seen


# -----------------------------
# Main
# -----------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=1.5)
    parser.add_argument("--max-pages", type=int, default=0, help="0 = all pages")
    parser.add_argument("--headless", dest="headless", action="store_true", default=True,
                        help="default True (background)")
    parser.add_argument("--no-headless", dest="headless", action="store_false",
                        help="show browser window")
    args = parser.parse_args()

    session = create_session()
    seen = load_seen_cases(CSV_FILE)
    first_write = not CSV_FILE.exists()

    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if first_write:
            writer.writeheader()

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=args.headless,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )
            context = browser.new_context(
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            )
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")

            page_no = 0
            total_results = None

            while True:
                listing_html = fetch_listing_page(session, page_no)

                if total_results is None:
                    total_results = parse_total_results(listing_html)
                    log.info("Total results reported by site: %s", total_results)

                entries = parse_entries(listing_html)
                log.info("Listing page %d -> parsed %d cases", page_no, len(entries))

                if not entries:
                    break

                for e in entries:
                    case_number = e["case_number"]
                    if case_number in seen:
                        continue

                    folder = case_folder(case_number)

                    pdf_file, pdf_status = download_pdf(session, e.get("pdf_url", ""), case_number)

                    tabs_status = {}
                    try:
                        tabs_status = save_all_tabs_for_case(
                            context=context,
                            case_number=case_number,
                            case_info_url=e["case_info_url"],
                            out_dir=folder,
                        )
                    except Exception as ex:
                        log.warning("Tabs failed for %s : %s", case_number, ex)
                        tabs_status = {"error": str(ex)}

                    writer.writerow(
                        {
                            "case_number": case_number,
                            "date": e.get("date", ""),
                            "court": e.get("court", ""),
                            "opinion_type": e.get("opinion_type", ""),
                            "title": e.get("title", ""),
                            "case_info_url": e.get("case_info_url", ""),
                            "pdf_url": e.get("pdf_url", ""),
                            "pdf_filename": pdf_file,
                            "download_status": pdf_status,
                            "tabs_status": str(tabs_status),
                        }
                    )
                    f.flush()
                    seen.add(case_number)

                    time.sleep(max(0.0, args.delay))

                page_no += 1

                if args.max_pages and page_no >= args.max_pages:
                    break

                # stop automatically when passed total results
                if total_results and page_no * 50 >= total_results:
                    break

            browser.close()

    log.info("DONE. CSV file: %s", CSV_FILE)


if __name__ == "__main__":
    main()
