#!/usr/bin/env python3
#d
from __future__ import annotations

import argparse
import csv
import datetime as dt
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable
import threading
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError  # type: ignore


BASE_URL = "https://appellatecases.courtinfo.ca.gov"
SEARCH_URL = f"{BASE_URL}/search.cfm"

ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / "logs"
DOWNLOAD_ROOT = ROOT / "downloads" / "court_opinions" / "search"
META_DIR = DOWNLOAD_ROOT / "metadata"

LOG_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / f"case-number-search-{dt.date.today():%Y%m%d}.log"
LOG_FORMATTER = logging.Formatter("[%(levelname)s] %(asctime)s %(message)s")
_META_LOCK = threading.Lock()


def configure_logger() -> logging.Logger:
    logger = logging.getLogger("case-number-search")
    if getattr(logger, "_configured", False):
        return logger
    logger.setLevel(logging.INFO)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(LOG_FORMATTER)
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(LOG_FORMATTER)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    logger.propagate = False
    logger._configured = True  # type: ignore[attr-defined]
    return logger


log = configure_logger()


def add_log_file_handler(logger: logging.Logger, path: str) -> None:
    path_obj = Path(path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path_obj, encoding="utf-8")
    handler.setFormatter(LOG_FORMATTER)
    logger.addHandler(handler)

COURTS = {
    "0": "Supreme",
    "1": "District 1",
    "2": "District 2",
    "3": "District 3",
    "41": "District 4 Div 1",
    "42": "District 4 Div 2",
    "43": "District 4 Div 3",
    "5": "District 5",
    "6": "District 6",
}

TAB_SPECS = {
    "case_summary": {
        "filename": "case_summary",
        "labels": ("case summary",),
        "path": "mainCaseScreen.cfm",
    },
    "docket": {
        "filename": "docket",
        "labels": ("docket", "scheduled actions"),
        "path": "dockets.cfm",
    },
    "briefs": {
        "filename": "briefs",
        "labels": ("briefs", "briefing"),
        "path": "briefing.cfm",
    },
    "disposition": {
        "filename": "disposition",
        "labels": ("disposition",),
        "path": "disposition.cfm",
    },
    "parties_and_attorneys": {
        "filename": "parties_and_attorneys",
        "labels": ("parties", "attorneys"),
        "path": "partiesAndAttorneys.cfm",
    },
    "lower_court": {
        "filename": "lower_court",
        "labels": ("trial court", "lower court"),
        "path": "trialCourt.cfm",
    },
}

STATUS_COLUMNS = [
    "status",
    "case_summary_status",
    "docket_status",
    "briefs_status",
    "disposition_status",
    "parties_and_attorneys_status",
    "lower_court_status",
    "pdf_status",
    "error",
    "scraped_at",
]

META_FIELDS = [
    "case_number",
    "case_title",
    "case_url",
    "doc_id",
    "doc_no",
    "request_token",
    "dist",
    "court",
    "source_csv",
    "pdf_url",
    "pdf_filename",
    "case_summary_status",
    "docket_status",
    "briefs_status",
    "disposition_status",
    "parties_and_attorneys_status",
    "lower_court_status",
    "pdf_status",
    "error",
    "scraped_at",
]

PDF_TEMPLATES = [
    "https://www.courts.ca.gov/opinions/documents/{case}.PDF",
    "https://www.courts.ca.gov/opinions/archive/{case}.PDF",
    "https://www.courts.ca.gov/opinions/revnppub/{case}.PDF",
]


def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.6, status_forcelist=(500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return session


def sanitize_filename(text: str) -> str:
    cleaned = re.sub(r"[^\w\s-]", "", text or "")
    cleaned = re.sub(r"\s+", "_", cleaned).strip("_")
    return cleaned[:200] if cleaned else "document"


def sanitize_folder_name(text: str) -> str:
    cleaned = re.sub(r"[\\\\/:*?\"<>|]+", "", text or "").strip()
    return cleaned if cleaned else "Unknown"


def case_dir(case_number: str, court_label: str) -> Path:
    court_folder = sanitize_folder_name(court_label)
    target = DOWNLOAD_ROOT / court_folder / sanitize_filename(case_number)
    target.mkdir(parents=True, exist_ok=True)
    return target


def save_html(path: Path, html: str, force: bool) -> str:
    if path.exists() and not force:
        return "cached"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(html, encoding="utf-8")
        return "saved"
    except OSError as exc:
        log.warning("Failed to write HTML %s: %s", path, exc)
        return "write_error"


def looks_like_bot_challenge(html: str) -> bool:
    h = (html or "").lower()
    return "istl-infinite-loop" in h or "from-page-runscript" in h or "initcustomevent" in h


def looks_like_session_expired(html: str) -> bool:
    h = (html or "").lower()
    return "session has expired" in h or "start a new case search" in h


def is_case_summary_page(html: str) -> bool:
    if not html:
        return False
    if "Case Summary" not in html:
        return False
    return "caseDetails" in html or "mainCaseScreen.cfm" in html


def extract_case_link(html: str, case_number: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for a in soup.select("a[href]"):
        if a.get_text(strip=True).upper() == case_number.upper():
            return a.get("href") or ""
    return ""


def extract_case_title(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for row in soup.select("div.row"):
        label = row.select_one("div.col-xs-5")
        if not label:
            continue
        if "Case Caption" in label.get_text(" ", strip=True):
            value = row.select_one("div.col-xs-7")
            if value:
                return value.get_text(" ", strip=True)
    return ""


def extract_court_label(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    header = soup.select_one("div.floatLeft h2")
    if header:
        return header.get_text(strip=True)
    return ""


def extract_tab_links(html: str, base_url: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "lxml")
    scope = soup.select_one("#caseDetails") or soup
    tab_links: dict[str, str] = {}
    for a in scope.select("a[href]"):
        text = a.get_text(" ", strip=True).lower()
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full_url = urljoin(base_url, href)
        for key, spec in TAB_SPECS.items():
            if key in tab_links:
                continue
            if any(label in text for label in spec["labels"]):
                tab_links[key] = full_url
    return tab_links


def fallback_tab_links(case_url: str, tab_links: dict[str, str]) -> dict[str, str]:
    if "mainCaseScreen.cfm" not in case_url:
        return tab_links
    for key, spec in TAB_SPECS.items():
        if key in tab_links:
            continue
        tab_links[key] = case_url.replace("mainCaseScreen.cfm", spec["path"])
    return tab_links


def extract_pdf_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    urls: list[str] = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if ".pdf" in href.lower():
            urls.append(urljoin(base_url, href))
    return urls


def build_pdf_filename(case_number: str, case_title: str, pdf_url: str) -> str:
    title_part = sanitize_filename(case_title)
    if title_part and title_part.lower() not in ("document",):
        return f"{sanitize_filename(case_number)}_{title_part}.pdf"
    basename = Path(urlparse(pdf_url).path).name
    if basename:
        return basename
    return f"{sanitize_filename(case_number)}.pdf"


def download_pdf(
    session: requests.Session,
    pdf_url: str,
    case_number: str,
    case_title: str,
    target_dir: Path,
    force: bool,
) -> tuple[str, str]:
    if not pdf_url:
        return "", "missing"
    filename = build_pdf_filename(case_number, case_title, pdf_url)
    path = target_dir / filename
    if path.exists() and not force:
        return filename, "cached"
    try:
        response = session.get(pdf_url, stream=True, timeout=60)
        if response.status_code == 404:
            response.close()
            return "", "missing"
        response.raise_for_status()
        content_type = response.headers.get("content-type", "").lower()
        iterator = response.iter_content(chunk_size=32768)
        first_chunk = next(iterator, b"")
        if not first_chunk:
            response.close()
            return "", "missing"
        if "pdf" not in content_type and not first_chunk.startswith(b"%PDF"):
            response.close()
            return "", "missing"
        with path.open("wb") as out:
            out.write(first_chunk)
            for chunk in iterator:
                if chunk:
                    out.write(chunk)
        return filename, "downloaded"
    except requests.RequestException as exc:
        log.warning("Failed to download PDF %s: %s", pdf_url, exc)
        return "", "download_error"


def download_pdf_candidates(
    session: requests.Session,
    candidates: Iterable[str],
    case_number: str,
    case_title: str,
    target_dir: Path,
    force: bool,
) -> tuple[str, str, str]:
    for url in candidates:
        filename, status = download_pdf(session, url, case_number, case_title, target_dir, force)
        if status in {"downloaded", "cached"}:
            return url, filename, status
    return "", "", "missing"


def infer_dist_from_path(path: Path) -> str:
    match = re.match(r"(\d+)_", path.name)
    if match:
        return match.group(1)
    return "0"


def iter_csv_paths(csv_path: str | None, csv_dir: str | None) -> list[Path]:
    paths: list[Path] = []
    if csv_path:
        paths.append(Path(csv_path))
    if csv_dir:
        paths.extend(sorted(Path(csv_dir).rglob("*.csv")))
    return paths


class Browser:
    def __init__(self, headless: bool, slow_mo: int | None) -> None:
        self.headless = headless
        self.slow_mo = slow_mo
        self._playwright = None
        self._browser = None
        self._context = None

    def start(self) -> None:
        if self._context is not None:
            return
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=self.headless,
            slow_mo=self.slow_mo,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        self._context = self._browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
            ),
        )
        self._context.set_default_timeout(60000)
        self._context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

    @property
    def context(self):
        self.start()
        return self._context

    def new_page(self):
        return self.context.new_page()

    def close(self) -> None:
        if self._browser is not None:
            self._browser.close()
        if self._playwright is not None:
            self._playwright.stop()
        self._context = None
        self._browser = None
        self._playwright = None


def wait_for_page_ready(page, case_number: str | None = None) -> str:
    try:
        page.wait_for_selector("#mainContent", timeout=15000)
    except PlaywrightTimeoutError:
        pass
    if case_number:
        try:
            page.wait_for_function(
                "caseNumber => document.body && document.body.innerText.includes(caseNumber)",
                arg=case_number,
                timeout=15000,
            )
        except PlaywrightTimeoutError:
            pass
    html = page.content()
    return html


def open_case_from_search(page, case_number: str, dist: str, retries: int = 2) -> tuple[str, str, str | None]:
    search_url = f"{SEARCH_URL}?dist={dist}"
    for attempt in range(1, retries + 2):
        try:
            page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_selector("#query_caseNumber", timeout=20000)
            page.fill("#query_caseNumber", case_number)
            page.click("#caseNumberSubmit")
            page.wait_for_load_state("domcontentloaded")
            html = wait_for_page_ready(page, case_number=case_number)
        except PlaywrightTimeoutError:
            log.warning("Timeout opening search page for %s (attempt %d/%d)", case_number, attempt, retries + 1)
            if attempt < retries + 1:
                page.wait_for_timeout(2000)
                continue
            return page.url, "", "search_timeout"

        if looks_like_bot_challenge(html) or looks_like_session_expired(html):
            log.warning("Session issue for %s (attempt %d/%d)", case_number, attempt, retries + 1)
            page.wait_for_timeout(2000)
            continue

        if is_case_summary_page(html):
            return page.url, html, None

        link = extract_case_link(html, case_number)
        if link:
            clicked = False
            locator = page.locator("a", has_text=case_number)
            if locator.count() > 0:
                try:
                    locator.first.click(timeout=15000)
                    page.wait_for_load_state("domcontentloaded")
                    clicked = True
                except PlaywrightTimeoutError:
                    clicked = False
            if not clicked:
                page.goto(urljoin(BASE_URL, link), wait_until="domcontentloaded", timeout=60000)
            html = wait_for_page_ready(page, case_number=case_number)
            if looks_like_bot_challenge(html) or looks_like_session_expired(html):
                log.warning("Session issue after click for %s (attempt %d/%d)", case_number, attempt, retries + 1)
                page.wait_for_timeout(2000)
                continue
            if is_case_summary_page(html):
                return page.url, html, None

        if attempt < retries + 1:
            log.warning("Retrying case %s (attempt %d/%d)", case_number, attempt, retries + 1)
            page.wait_for_timeout(2000)
            continue

        return page.url, html, "case_not_opened"
    return page.url, "", "case_not_opened"


def update_row_fields(row: dict[str, str], fieldnames: list[str]) -> dict[str, str]:
    for field in fieldnames:
        row.setdefault(field, "")
    return row


def append_metadata(meta_path: Path, data: dict[str, str]) -> None:
    with _META_LOCK:
        write_header = not meta_path.exists() or meta_path.stat().st_size == 0
        with meta_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=META_FIELDS)
            if write_header:
                writer.writeheader()
            writer.writerow({field: data.get(field, "") for field in META_FIELDS})


def scrape_case(
    page,
    session: requests.Session,
    case_number: str,
    dist: str,
    source_csv: Path,
    force: bool,
) -> dict[str, str]:
    result: dict[str, str] = {
        "status": "error",
        "case_summary_status": "",
        "docket_status": "",
        "briefs_status": "",
        "disposition_status": "",
        "parties_and_attorneys_status": "",
        "lower_court_status": "",
        "pdf_status": "",
        "error": "",
    }
    case_number = case_number.strip()
    case_url = ""
    case_html = ""
    case_title = ""
    court_label = COURTS.get(dist, dist)
    pdf_url = ""
    pdf_filename = ""

    def finalize_metadata() -> None:
        scraped_at = dt.datetime.utcnow().isoformat()
        meta_row = {
            "case_number": case_number,
            "case_title": case_title,
            "case_url": case_url,
            "doc_id": "",
            "doc_no": "",
            "request_token": "",
            "dist": dist,
            "court": court_label,
            "source_csv": str(source_csv),
            "pdf_url": pdf_url,
            "pdf_filename": pdf_filename,
            "case_summary_status": result["case_summary_status"],
            "docket_status": result["docket_status"],
            "briefs_status": result["briefs_status"],
            "disposition_status": result["disposition_status"],
            "parties_and_attorneys_status": result["parties_and_attorneys_status"],
            "lower_court_status": result["lower_court_status"],
            "pdf_status": result["pdf_status"],
            "error": result["error"],
            "scraped_at": scraped_at,
        }

        if case_url:
            parsed = urlparse(case_url)
            qs = parse_qs(parsed.query)
            meta_row["doc_id"] = (qs.get("doc_id") or [""])[0]
            meta_row["doc_no"] = (qs.get("doc_no") or [""])[0]
            meta_row["request_token"] = (qs.get("request_token") or [""])[0]

        meta_path = META_DIR / f"{dt.date.today():%Y%m%d}-metadata.csv"
        append_metadata(meta_path, meta_row)
        result["scraped_at"] = scraped_at

    if not case_number:
        result["error"] = "missing_case_number"
        result["status"] = "error"
        for key in (
            "case_summary_status",
            "docket_status",
            "briefs_status",
            "disposition_status",
            "parties_and_attorneys_status",
            "lower_court_status",
            "pdf_status",
        ):
            result[key] = "missing_case_number"
        finalize_metadata()
        return result
    try:
        case_url, case_html, err = open_case_from_search(page, case_number, dist)
        if err:
            result["error"] = err
            result["status"] = "error"
            for key in (
                "case_summary_status",
                "docket_status",
                "briefs_status",
                "disposition_status",
                "parties_and_attorneys_status",
                "lower_court_status",
                "pdf_status",
            ):
                result[key] = err
            finalize_metadata()
            return result

        case_title = extract_case_title(case_html)
        court_display = extract_court_label(case_html)
        court_label = COURTS.get(dist, court_display or dist)

        case_folder = case_dir(case_number, court_label)
        case_summary_path = case_folder / f"{case_number}__{TAB_SPECS['case_summary']['filename']}.html"
        result["case_summary_status"] = save_html(case_summary_path, case_html, force)

        tab_links = extract_tab_links(case_html, case_url)
        tab_links = fallback_tab_links(case_url, tab_links)

        pdf_candidates = extract_pdf_links(case_html, case_url)

        for key, spec in TAB_SPECS.items():
            if key == "case_summary":
                continue
            tab_url = tab_links.get(key, "")
            if not tab_url:
                result[f"{key}_status"] = "missing"
                continue
            tab_html = ""
            try:
                page.goto(tab_url, wait_until="domcontentloaded", timeout=60000)
                tab_html = wait_for_page_ready(page, case_number=case_number)
            except PlaywrightTimeoutError:
                log.warning("Timeout loading %s for %s, retrying once", key, case_number)
                try:
                    page.goto(tab_url, wait_until="domcontentloaded", timeout=60000)
                    tab_html = wait_for_page_ready(page, case_number=case_number)
                except PlaywrightTimeoutError:
                    result[f"{key}_status"] = "timeout"
                    continue
            if looks_like_bot_challenge(tab_html) or looks_like_session_expired(tab_html):
                result[f"{key}_status"] = "session_error"
                continue
            tab_path = case_folder / f"{case_number}__{spec['filename']}.html"
            result[f"{key}_status"] = save_html(tab_path, tab_html, force)
            if not pdf_candidates:
                pdf_candidates = extract_pdf_links(tab_html, tab_url)

        if not pdf_candidates:
            pdf_candidates = [template.format(case=case_number) for template in PDF_TEMPLATES]

        pdf_url, pdf_filename, pdf_status = download_pdf_candidates(
            session,
            pdf_candidates,
            case_number,
            case_title,
            case_folder,
            force,
        )
        result["pdf_status"] = pdf_status

        result["status"] = "done"
        result["error"] = ""

        finalize_metadata()
        return result
    except Exception as exc:
        log.exception("Unhandled error while scraping %s", case_number)
        result["error"] = f"exception:{exc.__class__.__name__}"
        result["status"] = "error"
        for key in (
            "case_summary_status",
            "docket_status",
            "briefs_status",
            "disposition_status",
            "parties_and_attorneys_status",
            "lower_court_status",
            "pdf_status",
        ):
            if not result.get(key):
                result[key] = "exception"
        finalize_metadata()
        return result


def load_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames[:] if reader.fieldnames else []
        rows = [row for row in reader]
    for col in STATUS_COLUMNS:
        if col not in fieldnames:
            fieldnames.append(col)
    return rows, fieldnames


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    tmp_path = path.with_suffix(".tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
    tmp_path.replace(path)


def extract_case_number(row: dict[str, str], fieldnames: list[str]) -> str:
    for key in ("case_number", "Case Number", "CaseNumber", "case no", "case_no"):
        if key in row and row[key]:
            return row[key].strip()
    for key in fieldnames:
        if row.get(key):
            return row[key].strip()
    return ""


def csv_has_pending(rows: list[dict[str, str]], fieldnames: list[str]) -> bool:
    for row in rows:
        case_number = extract_case_number(row, fieldnames)
        if not case_number:
            continue
        status = (row.get("status") or "").strip().lower()
        if status not in {"done", "error"}:
            return True
    return False


def process_csv(csv_path: Path, args: argparse.Namespace) -> None:
    if not csv_path.exists():
        log.warning("CSV not found: %s", csv_path)
        return
    rows: list[dict[str, str]] = []
    fieldnames: list[str] = []
    try:
        rows, fieldnames = load_csv(csv_path)
        if args.skip_complete and not csv_has_pending(rows, fieldnames):
            log.info("Skipping %s (all rows done/error)", csv_path)
            return
        session = create_session()
        browser = Browser(headless=not args.headed, slow_mo=args.slowmo)
        page = browser.new_page()
        dist = args.dist or infer_dist_from_path(csv_path)
        court_label = COURTS.get(dist, dist)
        log.info("Processing %s (dist=%s, court=%s)", csv_path, dist, court_label)

        processed = 0
        for row in rows:
            row = update_row_fields(row, fieldnames)
            case_number = extract_case_number(row, fieldnames)
            if not case_number:
                row["status"] = "error"
                row["error"] = "missing_case_number"
                continue
            if row.get("status") == "done" and not args.force:
                continue

            log.info("Scraping case %s (dist=%s)", case_number, dist)
            result = scrape_case(page, session, case_number, dist, csv_path, args.force)
            row.update(result)
            if args.delay:
                time.sleep(args.delay)

            processed += 1
            if args.max_cases and processed >= args.max_cases:
                break
    except Exception:
        log.exception("Failed processing %s", csv_path)
    finally:
        if rows and fieldnames:
            try:
                write_csv(csv_path, rows, fieldnames)
            except Exception:
                log.exception("Failed writing CSV %s", csv_path)
        if "browser" in locals():
            browser.close()
        if "session" in locals():
            session.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape case detail pages by case number search.")
    parser.add_argument("--csv", help="CSV file of case numbers (default: Supreme/0_AA.csv)")
    parser.add_argument("--csv-dir", help="Process all CSV files in this directory recursively")
    parser.add_argument("--dist", help="Court dist value (overrides inference from filename)")
    parser.add_argument("--headed", action="store_true", help="Run browser in headed mode")
    parser.add_argument("--slowmo", type=int, help="Slow down Playwright actions (ms)")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay between cases (seconds)")
    parser.add_argument("--force", action="store_true", help="Re-save HTML/PDF even if files exist")
    parser.add_argument("--max-cases", type=int, help="Limit number of cases per CSV")
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel CSV workers (default: 1)",
    )
    parser.add_argument(
        "--skip-complete",
        action="store_true",
        help="Skip CSVs where all rows are done or error",
    )
    parser.add_argument(
        "--extra-log-file",
        help="Write logs to an additional file path",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)
        for handler in log.handlers:
            handler.setLevel(logging.DEBUG)
    if args.extra_log_file:
        add_log_file_handler(log, args.extra_log_file)

    default_dir = ROOT / "downloads" / "court_opinions" / "case_numbers"
    default_csv = default_dir / "Supreme" / "0_AA.csv"
    if not args.csv and not args.csv_dir and default_dir.exists():
        args.csv_dir = str(default_dir)

    csv_paths = iter_csv_paths(
        args.csv or (str(default_csv) if default_csv.exists() else None),
        args.csv_dir,
    )
    if not csv_paths:
        log.error("No CSV files found. Provide --csv or --csv-dir.")
        return 1

    if args.headed and args.workers > 1:
        log.warning("Headed mode does not support multiple workers; forcing workers=1.")
        args.workers = 1

    if args.workers <= 1:
        for csv_path in csv_paths:
            process_csv(csv_path, args)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(process_csv, csv_path, args): csv_path for csv_path in csv_paths}
            for fut in as_completed(futures):
                csv_path = futures[fut]
                try:
                    fut.result()
                except Exception:
                    log.exception("Worker failed processing %s", csv_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
