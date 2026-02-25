#!/usr/bin/env python3
"""
Iowa Court of Appeals Opinions Scraper (2018 to current year)

Install dependencies (Python 3.10+):
    pip install selenium webdriver-manager beautifulsoup4 requests python-dateutil

Run:
    python court-of-appeals.py

Outputs are created under:
    Iowa/
      downloads/court-of-appeals/CSV/iowa_court_of_appeals_opinions.csv
      downloads/court-of-appeals/PDF/*.pdf
      Log/court-of-appeals/iowa-court-of-appeals-YYYY-MM-DD.log
"""

from __future__ import annotations

import csv
import logging
import re
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from requests import Response
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

BASE_SITE = "https://www.iowacourts.gov"
START_YEAR = 2018
CURRENT_YEAR = datetime.now().year
ARCHIVE_URL_TEMPLATE = (
    "https://www.iowacourts.gov/iowa-courts/court-of-appeals/"
    "court-of-appeals-court-opinions/archive/{year}/page/1"
)
CASE_PATH_FRAGMENT = "/court-of-appeals-court-opinions/case/"

CSV_COLUMNS = [
    "case_no",
    "case_caption",
    "filed_date",
    "opinion_no",
    "case_detail_url",
    "pdf_url",
    "pdf_local_path",
]

REQUEST_TIMEOUT = 45
PDF_DOWNLOAD_RETRIES = 3


def normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def safe_filename_part(value: str) -> str:
    value = normalize_spaces(value)
    value = value.replace("/", "-")
    value = re.sub(r"[^A-Za-z0-9._-]+", "-", value)
    value = value.strip("-._")
    return value or "unknown"


def parse_case_no(text: str) -> str:
    text = normalize_spaces(text)
    match = re.search(r"Case\s*No\.?\s*([^:]+)", text, flags=re.IGNORECASE)
    return normalize_spaces(match.group(1)) if match else ""


def parse_case_no_from_url(case_detail_url: str) -> str:
    match = re.search(r"/case/([^/?#]+)", case_detail_url)
    return normalize_spaces(match.group(1)) if match else ""


def parse_case_caption(anchor_tag) -> str:
    em = anchor_tag.find("em")
    if em:
        return normalize_spaces(em.get_text(" ", strip=True))

    text = normalize_spaces(anchor_tag.get_text(" ", strip=True))
    match = re.search(r":\s*(.+)$", text)
    return normalize_spaces(match.group(1)) if match else ""


def parse_filed_date(text: str) -> str:
    text = normalize_spaces(text)
    match = re.search(r"\bFiled\b\s*(.+)$", text, flags=re.IGNORECASE)
    return normalize_spaces(match.group(1)) if match else ""


def parse_opinion_no(text: str) -> str:
    text = normalize_spaces(text)
    match = re.search(r"Opinion\s*No\.?\s*([A-Za-z0-9-]+)", text, flags=re.IGNORECASE)
    return normalize_spaces(match.group(1)) if match else ""


def filed_date_to_iso_or_unknown(filed_date: str) -> str:
    if not filed_date:
        return "unknown-date"
    try:
        dt = date_parser.parse(filed_date, fuzzy=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return "unknown-date"


def build_record_key(case_no: str, opinion_no: str, case_detail_url: str = "") -> str:
    case_no = normalize_spaces(case_no)
    opinion_no = normalize_spaces(opinion_no)
    if case_no and opinion_no:
        return f"{case_no}|{opinion_no}"
    if case_no:
        return case_no
    if case_detail_url:
        return f"url|{normalize_spaces(case_detail_url)}"
    return ""


def build_pdf_relative_path(case_no: str, opinion_no: str, filed_date: str) -> str:
    id_part = opinion_no if normalize_spaces(opinion_no) else case_no
    id_part = safe_filename_part(id_part or "unknown-case")
    filed_iso = filed_date_to_iso_or_unknown(filed_date)
    filename = f"{id_part}__{filed_iso}.pdf"
    return str(Path("downloads") / "court-of-appeals" / "PDF" / filename).replace("\\", "/")


def ensure_directories(base_dir: Path) -> Tuple[Path, Path, Path]:
    downloads_csv_dir = base_dir / "downloads" / "court-of-appeals" / "CSV"
    downloads_pdf_dir = base_dir / "downloads" / "court-of-appeals" / "PDF"
    log_dir = base_dir / "Log" / "court-of-appeals"

    downloads_csv_dir.mkdir(parents=True, exist_ok=True)
    downloads_pdf_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    return downloads_csv_dir, downloads_pdf_dir, log_dir


def setup_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("iowa_court_of_appeals_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def init_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "--user-agent="
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    return driver


def init_requests_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return session


def sync_cookies_from_driver(driver: webdriver.Chrome, session: requests.Session) -> None:
    try:
        for cookie in driver.get_cookies():
            session.cookies.set(
                cookie.get("name", ""),
                cookie.get("value", ""),
                domain=cookie.get("domain"),
                path=cookie.get("path", "/"),
            )
    except WebDriverException:
        pass


def load_existing_csv(csv_path: Path) -> Tuple[Dict[str, Dict[str, str]], List[str]]:
    rows_by_key: Dict[str, Dict[str, str]] = {}
    key_order: List[str] = []

    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return rows_by_key, key_order

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            normalized = {col: normalize_spaces(row.get(col, "")) for col in CSV_COLUMNS}
            key = build_record_key(
                normalized.get("case_no", ""),
                normalized.get("opinion_no", ""),
                normalized.get("case_detail_url", ""),
            )
            if not key:
                continue
            if key not in rows_by_key:
                rows_by_key[key] = normalized
                key_order.append(key)

    return rows_by_key, key_order


def write_csv(csv_path: Path, rows_by_key: Dict[str, Dict[str, str]], key_order: List[str]) -> None:
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for key in key_order:
            row = rows_by_key.get(key, {})
            writer.writerow({col: normalize_spaces(row.get(col, "")) for col in CSV_COLUMNS})


def extract_metadata_after_heading(anchor_tag, page_url: str) -> Tuple[str, str, str]:
    """
    Extract filed_date, opinion_no, listing_view_url from sibling nodes after the
    case heading (<h3>) until next <h3>.
    """
    h3 = anchor_tag.find_parent("h3")
    if not h3:
        return "", "", ""

    block_parts: List[str] = []
    for sibling in h3.next_siblings:
        sibling_name = getattr(sibling, "name", None)
        if sibling_name == "h3":
            break
        block_parts.append(str(sibling))

    if not block_parts:
        return "", "", ""

    block_soup = BeautifulSoup("".join(block_parts), "html.parser")

    filed_date = ""
    opinion_no = ""
    listing_view_url = ""

    for p_tag in block_soup.find_all("p"):
        p_text = normalize_spaces(p_tag.get_text(" ", strip=True))
        if "filed" in p_text.lower():
            filed_date = parse_filed_date(p_text)
            if filed_date:
                break

    for link in block_soup.find_all("a", href=True):
        link_text = normalize_spaces(link.get_text(" ", strip=True))
        if "view opinion" in link_text.lower():
            listing_view_url = urljoin(page_url, link["href"])
            parsed_opinion = parse_opinion_no(link_text)
            if parsed_opinion:
                opinion_no = parsed_opinion
            break

    return filed_date, opinion_no, listing_view_url


def extract_listing_records(soup: BeautifulSoup, page_url: str) -> List[Dict[str, str]]:
    records: List[Dict[str, str]] = []
    seen_detail_urls: Set[str] = set()

    for anchor in soup.select("h3 a[href]"):
        case_detail_url = urljoin(page_url, normalize_spaces(anchor.get("href", "")))
        if CASE_PATH_FRAGMENT not in case_detail_url:
            continue
        if case_detail_url in seen_detail_urls:
            continue
        seen_detail_urls.add(case_detail_url)

        text = normalize_spaces(anchor.get_text(" ", strip=True))
        case_no = parse_case_no(text)
        if not case_no:
            case_no = parse_case_no_from_url(case_detail_url)

        case_caption = parse_case_caption(anchor)
        filed_date, opinion_no, listing_view_url = extract_metadata_after_heading(anchor, page_url)

        if not opinion_no:
            maybe_opinion = parse_opinion_no(text)
            if maybe_opinion:
                opinion_no = maybe_opinion

        records.append(
            {
                "case_no": case_no,
                "case_caption": case_caption,
                "filed_date": filed_date,
                "opinion_no": opinion_no,
                "case_detail_url": case_detail_url,
                "listing_view_url": listing_view_url,
            }
        )

    return records


def extract_next_page_url_for_year(soup: BeautifulSoup, current_url: str, year: int) -> Optional[str]:
    listing_re = re.compile(
        rf"/iowa-courts/court-of-appeals/court-of-appeals-court-opinions/archive/{year}/page/(\d+)(?:/)?(?:[?#].*)?$",
        flags=re.IGNORECASE,
    )

    def page_no_from_url(url: str) -> Optional[int]:
        match = listing_re.search(url)
        if not match:
            return None
        try:
            return int(match.group(1))
        except ValueError:
            return None

    current_page_no = page_no_from_url(current_url)
    candidates: List[Tuple[int, int, str]] = []

    for anchor in soup.find_all("a", href=True):
        abs_url = urljoin(current_url, anchor["href"])
        page_no = page_no_from_url(abs_url)
        if page_no is None:
            continue

        if current_page_no is not None and page_no <= current_page_no:
            continue

        anchor_classes = " ".join(anchor.get("class", [])).lower()
        parent_classes = " ".join(anchor.parent.get("class", [])).lower() if anchor.parent else ""
        if "disabled" in anchor_classes or "disabled" in parent_classes:
            continue

        rel_values = anchor.get("rel", [])
        rel_text = rel_values.lower() if isinstance(rel_values, str) else " ".join(rel_values).lower()
        text = normalize_spaces(anchor.get_text(" ", strip=True)).lower()

        score = 0
        if "next" in rel_text:
            score += 3
        if "next" in text:
            score += 2
        if any(token in anchor_classes for token in ("next", "pagination", "pager")):
            score += 1
        if "next" in parent_classes:
            score += 1
        if current_page_no is not None and page_no == current_page_no + 1:
            score += 2

        candidates.append((page_no, -score, abs_url))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2]


def looks_like_pdf_url(url: str) -> bool:
    clean = (url or "").lower().split("?")[0]
    return clean.endswith(".pdf")


def response_is_pdf(response: Response) -> bool:
    content_type = response.headers.get("Content-Type", "").lower()
    if "application/pdf" in content_type:
        return True
    try:
        prefix = response.content[:4]
    except Exception:
        prefix = b""
    return prefix == b"%PDF"


def extract_candidates_from_soup(soup: BeautifulSoup, base_url: str) -> List[str]:
    candidates: List[str] = []

    for tag_name, attr in (("iframe", "src"), ("embed", "src"), ("object", "data"), ("source", "src")):
        for tag in soup.find_all(tag_name):
            raw = normalize_spaces(tag.get(attr, ""))
            if raw and not raw.lower().startswith("javascript:"):
                candidates.append(urljoin(base_url, raw))

    for link in soup.find_all("a", href=True):
        raw_href = normalize_spaces(link.get("href", ""))
        text = normalize_spaces(link.get_text(" ", strip=True)).lower()
        href_lc = raw_href.lower()

        if not raw_href or href_lc.startswith("javascript:"):
            continue

        if (
            ".pdf" in href_lc
            or "courtcases" in href_lc
            or "courtofappealsopinion" in href_lc
            or "supremecourtopinion" in href_lc
            or "view opinion" in text
            or "opinion" in text
        ):
            candidates.append(urljoin(base_url, raw_href))

    unique: List[str] = []
    seen: Set[str] = set()
    for candidate in candidates:
        if candidate not in seen:
            seen.add(candidate)
            unique.append(candidate)
    return unique


def resolve_pdf_via_requests(
    start_urls: List[str],
    session: requests.Session,
    logger: logging.Logger,
    max_depth: int = 3,
) -> Optional[str]:
    queue = deque((url, 0) for url in start_urls if url)
    seen: Set[str] = set()

    while queue:
        current_url, depth = queue.popleft()
        if current_url in seen:
            continue
        seen.add(current_url)

        try:
            response = session.get(current_url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        except requests.RequestException as exc:
            logger.debug("Request failed for %s: %s", current_url, exc)
            continue

        final_url = response.url
        if response_is_pdf(response) or looks_like_pdf_url(final_url):
            return final_url

        if depth >= max_depth:
            continue

        content_type = response.headers.get("Content-Type", "").lower()
        if "html" not in content_type and "xml" not in content_type:
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        for candidate in extract_candidates_from_soup(soup, final_url):
            if candidate not in seen:
                queue.append((candidate, depth + 1))

    return None


def resolve_pdf_via_selenium(
    url: str,
    driver: webdriver.Chrome,
    session: requests.Session,
    logger: logging.Logger,
) -> Optional[str]:
    if not url:
        return None

    try:
        driver.get(url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except TimeoutException:
        logger.debug("Selenium timeout while opening %s", url)
    except WebDriverException as exc:
        logger.debug("Selenium failed for %s: %s", url, exc)
        return None

    sync_cookies_from_driver(driver, session)

    current_url = driver.current_url
    if looks_like_pdf_url(current_url):
        return current_url

    soup = BeautifulSoup(driver.page_source, "html.parser")
    candidates = extract_candidates_from_soup(soup, current_url)
    if candidates:
        resolved = resolve_pdf_via_requests(candidates, session, logger, max_depth=2)
        if resolved:
            return resolved

    return None


def resolve_pdf_from_case_detail(
    case_detail_url: str,
    driver: webdriver.Chrome,
    session: requests.Session,
    logger: logging.Logger,
) -> Optional[str]:
    if not case_detail_url:
        return None

    sync_cookies_from_driver(driver, session)

    try:
        detail_response = session.get(case_detail_url, timeout=REQUEST_TIMEOUT)
        detail_response.raise_for_status()
        soup = BeautifulSoup(detail_response.text, "html.parser")
        candidates = extract_candidates_from_soup(soup, detail_response.url)
        if candidates:
            resolved = resolve_pdf_via_requests(candidates, session, logger, max_depth=3)
            if resolved:
                return resolved
    except requests.RequestException as exc:
        logger.debug("Case detail request failed for %s: %s", case_detail_url, exc)

    return resolve_pdf_via_selenium(case_detail_url, driver, session, logger)


def resolve_pdf_for_record(
    record: Dict[str, str],
    driver: webdriver.Chrome,
    session: requests.Session,
    logger: logging.Logger,
) -> Optional[str]:
    listing_view_url = record.get("listing_view_url", "")
    case_detail_url = record.get("case_detail_url", "")

    sync_cookies_from_driver(driver, session)

    if listing_view_url:
        resolved = resolve_pdf_via_requests([listing_view_url], session, logger, max_depth=3)
        if resolved:
            return resolved

        resolved = resolve_pdf_via_selenium(listing_view_url, driver, session, logger)
        if resolved:
            return resolved

    return resolve_pdf_from_case_detail(case_detail_url, driver, session, logger)


def download_pdf(
    pdf_url: str,
    target_path: Path,
    session: requests.Session,
    driver: webdriver.Chrome,
    logger: logging.Logger,
    retries: int = PDF_DOWNLOAD_RETRIES,
) -> Tuple[bool, str]:
    if not pdf_url:
        return False, ""

    for attempt in range(1, retries + 1):
        sync_cookies_from_driver(driver, session)
        tmp_path = target_path.with_suffix(target_path.suffix + ".part")

        try:
            with session.get(pdf_url, timeout=REQUEST_TIMEOUT, stream=True, allow_redirects=True) as response:
                response.raise_for_status()
                final_url = response.url
                content_type = response.headers.get("Content-Type", "").lower()

                if "pdf" not in content_type and not looks_like_pdf_url(final_url):
                    chunk = next(response.iter_content(chunk_size=4096), b"")
                    if not chunk.startswith(b"%PDF"):
                        raise ValueError(f"Response does not look like PDF (Content-Type: {content_type})")

                    with tmp_path.open("wb") as f:
                        f.write(chunk)
                        for data in response.iter_content(chunk_size=65536):
                            if data:
                                f.write(data)
                else:
                    with tmp_path.open("wb") as f:
                        for data in response.iter_content(chunk_size=65536):
                            if data:
                                f.write(data)

            tmp_path.replace(target_path)
            return True, final_url
        except Exception as exc:
            logger.warning(
                "PDF download attempt %d/%d failed for %s: %s",
                attempt,
                retries,
                pdf_url,
                exc,
            )
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

    return False, ""


def reconcile_key_if_needed(
    case_no: str,
    opinion_no: str,
    case_detail_url: str,
    rows_by_key: Dict[str, Dict[str, str]],
    key_order: List[str],
) -> str:
    target_key = build_record_key(case_no, opinion_no, case_detail_url)

    if not case_no or not opinion_no:
        return target_key

    case_only_key = build_record_key(case_no, "", case_detail_url)
    if target_key in rows_by_key:
        return target_key

    old_row = rows_by_key.get(case_only_key)
    if not old_row:
        return target_key

    if normalize_spaces(old_row.get("opinion_no", "")):
        return target_key

    rows_by_key[target_key] = old_row
    del rows_by_key[case_only_key]
    for idx, value in enumerate(key_order):
        if value == case_only_key:
            key_order[idx] = target_key
            break

    return target_key


def process_record(
    record: Dict[str, str],
    rows_by_key: Dict[str, Dict[str, str]],
    key_order: List[str],
    base_dir: Path,
    driver: webdriver.Chrome,
    session: requests.Session,
    logger: logging.Logger,
) -> Tuple[bool, bool, bool, bool]:
    """
    Returns:
        (is_new_row_added, is_duplicate_skipped, is_pdf_downloaded, is_pdf_already_exists)
    """
    case_no = normalize_spaces(record.get("case_no", ""))
    opinion_no = normalize_spaces(record.get("opinion_no", ""))
    case_detail_url = normalize_spaces(record.get("case_detail_url", ""))

    key = reconcile_key_if_needed(case_no, opinion_no, case_detail_url, rows_by_key, key_order)
    if not key:
        logger.warning("Skipping record with empty key: detail_url=%s", case_detail_url)
        return False, False, False, False

    is_new = key not in rows_by_key
    if is_new:
        rows_by_key[key] = {col: "" for col in CSV_COLUMNS}
        key_order.append(key)

    row = rows_by_key[key]
    row["case_no"] = case_no
    row["case_caption"] = normalize_spaces(record.get("case_caption", ""))
    row["filed_date"] = normalize_spaces(record.get("filed_date", ""))
    row["opinion_no"] = opinion_no
    row["case_detail_url"] = case_detail_url

    desired_pdf_rel = build_pdf_relative_path(case_no, opinion_no, row["filed_date"])
    desired_pdf_abs = base_dir / desired_pdf_rel

    existing_rel = normalize_spaces(row.get("pdf_local_path", ""))
    existing_abs = base_dir / existing_rel if existing_rel else None

    if existing_rel and existing_abs and existing_abs.exists():
        if existing_rel != desired_pdf_rel:
            try:
                desired_pdf_abs.parent.mkdir(parents=True, exist_ok=True)
                if not desired_pdf_abs.exists():
                    existing_abs.rename(desired_pdf_abs)
                    logger.info("Renamed PDF path: %s -> %s", existing_rel, desired_pdf_rel)
                row["pdf_local_path"] = desired_pdf_rel
            except Exception as exc:
                logger.warning("Could not rename PDF %s -> %s: %s", existing_rel, desired_pdf_rel, exc)
                row["pdf_local_path"] = existing_rel
        else:
            row["pdf_local_path"] = existing_rel

        logger.info("Skipped duplicate (already in CSV + PDF exists): %s", key)
        return is_new, True, False, False

    resolved_pdf_url = resolve_pdf_for_record(record, driver, session, logger)
    if resolved_pdf_url:
        row["pdf_url"] = resolved_pdf_url
    else:
        row["pdf_url"] = normalize_spaces(row.get("pdf_url", ""))

    if not row["pdf_url"]:
        logger.warning("No PDF URL resolved for case_no=%s", case_no or "unknown")
        return is_new, False, False, False

    row["pdf_local_path"] = desired_pdf_rel

    if desired_pdf_abs.exists():
        logger.info("PDF already exists: %s", desired_pdf_rel)
        return is_new, False, False, True

    ok, final_pdf_url = download_pdf(row["pdf_url"], desired_pdf_abs, session, driver, logger)
    if ok:
        if final_pdf_url:
            row["pdf_url"] = final_pdf_url
        logger.info("PDF downloaded: %s", desired_pdf_rel)
        return is_new, False, True, False

    logger.error("Failed to download PDF for case_no=%s | url=%s", case_no or "unknown", row["pdf_url"])
    return is_new, False, False, False


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    output_base = script_dir

    csv_dir, _pdf_dir, log_dir = ensure_directories(output_base)
    csv_path = csv_dir / "iowa_court_of_appeals_opinions.csv"
    log_path = log_dir / f"iowa-court-of-appeals-{datetime.now().strftime('%Y-%m-%d')}.log"

    logger = setup_logger(log_path)
    logger.info("Scraper start")
    logger.info("Year range: %d-%d", START_YEAR, CURRENT_YEAR)

    rows_by_key, key_order = load_existing_csv(csv_path)
    logger.info("Loaded existing CSV rows: %d", len(rows_by_key))

    driver: Optional[webdriver.Chrome] = None
    session = init_requests_session()

    pages_visited = 0
    total_records_seen = 0
    new_rows_added = 0
    skipped_duplicates = 0
    pdf_downloaded = 0
    pdf_already_exists = 0
    record_errors = 0

    try:
        driver = init_driver()

        for year in range(START_YEAR, CURRENT_YEAR + 1):
            year_pages = 0
            year_records = 0
            year_start_url = ARCHIVE_URL_TEMPLATE.format(year=year)
            next_page_url: Optional[str] = year_start_url
            visited_year_pages: Set[str] = set()

            logger.info("Starting year %d: %s", year, year_start_url)

            while next_page_url and next_page_url not in visited_year_pages:
                visited_year_pages.add(next_page_url)
                pages_visited += 1
                year_pages += 1
                logger.info("Visiting year %d page %d: %s", year, year_pages, next_page_url)

                driver.get(next_page_url)
                try:
                    WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                except TimeoutException:
                    logger.warning("Timeout waiting for page body: %s", next_page_url)

                sync_cookies_from_driver(driver, session)
                soup = BeautifulSoup(driver.page_source, "html.parser")

                records = extract_listing_records(soup, next_page_url)
                logger.info("Records found for year %d page %d: %d", year, year_pages, len(records))

                total_records_seen += len(records)
                year_records += len(records)

                for record in records:
                    case_no = record.get("case_no", "") or "unknown-case"
                    try:
                        is_new, is_skipped_dup, is_pdf_downloaded, is_pdf_exists = process_record(
                            record=record,
                            rows_by_key=rows_by_key,
                            key_order=key_order,
                            base_dir=output_base,
                            driver=driver,
                            session=session,
                            logger=logger,
                        )

                        if is_new:
                            new_rows_added += 1
                        if is_skipped_dup:
                            skipped_duplicates += 1
                        if is_pdf_downloaded:
                            pdf_downloaded += 1
                        if is_pdf_exists:
                            pdf_already_exists += 1

                    except Exception as exc:
                        record_errors += 1
                        logger.exception("Error processing case_no=%s: %s", case_no, exc)
                    finally:
                        # Persist progress one-by-one.
                        write_csv(csv_path, rows_by_key, key_order)

                next_page_url = extract_next_page_url_for_year(soup, next_page_url, year)

            logger.info("Year %d summary: pages=%d records=%d", year, year_pages, year_records)

    except Exception as exc:
        logger.exception("Fatal scraper error: %s", exc)
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

        write_csv(csv_path, rows_by_key, key_order)
        logger.info("CSV written: %s", csv_path)

        logger.info("Run summary")
        logger.info("Total pages visited: %d", pages_visited)
        logger.info("Total records found: %d", total_records_seen)
        logger.info("New rows added: %d", new_rows_added)
        logger.info("Skipped duplicates: %d", skipped_duplicates)
        logger.info("PDF downloaded: %d", pdf_downloaded)
        logger.info("PDF already exists: %d", pdf_already_exists)
        logger.info("Record errors: %d", record_errors)
        logger.info("Scraper finished")


if __name__ == "__main__":
    main()
