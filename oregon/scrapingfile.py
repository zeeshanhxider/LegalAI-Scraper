#!/usr/bin/env python3
"""
How to run:
  python scrapingfile.py
  python scrapingfile.py --start-page 1 --max-pages 5
  python scrapingfile.py --headful --csv-name case.csv
  python scrapingfile.py --repair-missing --start-page 1 --max-pages 1
"""

import argparse
import csv
import logging
import os
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

try:
    from webdriver_manager.chrome import ChromeDriverManager
except Exception:
    ChromeDriverManager = None


DEFAULT_BASE_URL = (
    "https://cdm17027.contentdm.oclc.org/digital/search/collection/"
    "p17027coll8!p17027coll7/order/dated/ad/desc/page/1"
)
BASE_DOMAIN = "https://cdm17027.contentdm.oclc.org"

CSV_HEADERS = [
    "Briefs",
    "Title",
    "Official case name",
    "Case number",
    "Type",
    "Date decided",
    "Citation",
    "Title",
    "PDF url",
    "Donwload PDF path",
]

IDX_PDF_URL = 8
IDX_DOWNLOAD_PATH = 9
IDX_BRIEFS = 0
IDX_TITLE_PRIMARY = 1
IDX_OFFICIAL_CASE = 2
IDX_CASE_NUMBER = 3
IDX_TYPE = 4
IDX_DATE_DECIDED = 5
IDX_CITATION = 6
IDX_TITLE_SECONDARY = 7
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"scrape-{datetime.now().strftime('%Y-%m-%d')}.log"

    logger = logging.getLogger("contentdm_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def ensure_folders(base_dir: Path) -> Dict[str, Path]:
    downloads_dir = base_dir / "downloads"
    csv_dir = downloads_dir / "CSV"
    pdf_dir = downloads_dir / "PDF"
    log_dir = base_dir / "Log"

    csv_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    return {
        "downloads": downloads_dir,
        "csv": csv_dir,
        "pdf": pdf_dir,
        "log": log_dir,
    }


def setup_driver(headful: bool) -> webdriver.Chrome:
    options = Options()
    if not headful:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"--user-agent={USER_AGENT}")

    if ChromeDriverManager is not None:
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)
    return webdriver.Chrome(options=options)


def setup_requests_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        read=3,
        connect=3,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def random_delay(min_s: float = 0.5, max_s: float = 1.5) -> None:
    time.sleep(random.uniform(min_s, max_s))


def build_page_url(base_url: str, page_num: int) -> str:
    if re.search(r"/page/\d+", base_url):
        return re.sub(r"/page/\d+", f"/page/{page_num}", base_url, count=1)
    if base_url.endswith("/"):
        return f"{base_url}page/{page_num}"
    return f"{base_url}/page/{page_num}"


def extract_item_id_and_collection(item_url: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.search(r"/collection/([^/]+)/id/(\d+)", item_url)
    if not m:
        return None, None
    return m.group(2), m.group(1)


def predicted_pdf_url(collection: Optional[str], item_id: Optional[str]) -> Optional[str]:
    if not collection or not item_id:
        return None
    return f"{BASE_DOMAIN}/digital/api/collection/{collection}/id/{item_id}/download"


def ensure_csv_with_header(csv_path: Path) -> None:
    if csv_path.exists() and csv_path.stat().st_size > 0:
        return
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)


def extract_id_from_path(path_str: str) -> Optional[str]:
    if not path_str:
        return None
    basename = os.path.basename(path_str.strip())
    m = re.match(r"(\d+)\.pdf$", basename)
    return m.group(1) if m else None


def extract_id_from_pdf_url(pdf_url: str) -> Optional[str]:
    if not pdf_url:
        return None
    m = re.search(r"/id/(\d+)/download", pdf_url)
    return m.group(1) if m else None


def extract_collection_from_pdf_url(pdf_url: str) -> Optional[str]:
    if not pdf_url:
        return None
    m = re.search(r"/collection/([^/]+)/id/\d+/download", pdf_url)
    return m.group(1) if m else None


def load_existing_index(csv_path: Path) -> Tuple[Set[str], Set[str]]:
    done_ids: Set[str] = set()
    done_pdf_urls: Set[str] = set()

    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return done_ids, done_pdf_urls

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row_num, row in enumerate(reader, start=1):
            if not row:
                continue
            if row_num == 1 and row[0].strip().lower().startswith("briefs"):
                continue

            pdf_url = row[IDX_PDF_URL].strip() if len(row) > IDX_PDF_URL else ""
            download_path = row[IDX_DOWNLOAD_PATH].strip() if len(row) > IDX_DOWNLOAD_PATH else ""

            if pdf_url:
                done_pdf_urls.add(pdf_url)
                url_id = extract_id_from_pdf_url(pdf_url)
                if url_id:
                    done_ids.add(url_id)

            path_id = extract_id_from_path(download_path)
            if path_id:
                done_ids.add(path_id)

    return done_ids, done_pdf_urls


def append_csv_row(csv_path: Path, row_values: List[str]) -> None:
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row_values)


def normalize_row_length(row: List[str], target_len: int = len(CSV_HEADERS)) -> List[str]:
    if len(row) < target_len:
        return row + ([""] * (target_len - len(row)))
    if len(row) > target_len:
        return row[:target_len]
    return row


def get_text_safe(element) -> str:
    try:
        return element.text.strip()
    except Exception:
        return ""


def normalize_key(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "").strip())
    return cleaned.rstrip(":").lower()


def parse_case_number_and_type(title_text: str) -> Tuple[str, str]:
    title_text = (title_text or "").strip()
    m = re.match(r"^\s*([A-Za-z]\d+)\s*,\s*(.+?)\s*$", title_text)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return "", ""


def page_load_with_retry(
    driver: webdriver.Chrome,
    url: str,
    wait: WebDriverWait,
    wait_selector: str,
    logger: logging.Logger,
    retries: int = 2,
) -> bool:
    attempts = retries + 1
    for attempt in range(1, attempts + 1):
        try:
            driver.get(url)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector)))
            return True
        except (TimeoutException, WebDriverException) as exc:
            logger.warning(
                "Load failed (%s/%s): %s | %s", attempt, attempts, url, str(exc)
            )
            if attempt >= attempts:
                return False
            random_delay(0.8, 1.8)
    return False


def extract_metadata_from_table(table_element) -> Dict[str, str]:
    data: Dict[str, str] = {}
    rows = table_element.find_elements(By.CSS_SELECTOR, "tr")
    for row in rows:
        cells = row.find_elements(By.CSS_SELECTOR, "th,td")
        key = ""
        value = ""

        if len(cells) >= 2:
            key = get_text_safe(cells[0])
            parts = [get_text_safe(c) for c in cells[1:]]
            value = " ".join([p for p in parts if p]).strip()
        elif len(cells) == 1:
            single = get_text_safe(cells[0])
            chunks = [x.strip() for x in single.split("\n") if x.strip()]
            if len(chunks) >= 2:
                key = chunks[0]
                value = " ".join(chunks[1:]).strip()
        else:
            row_text = get_text_safe(row)
            chunks = [x.strip() for x in row_text.split("\n") if x.strip()]
            if len(chunks) >= 2:
                key = chunks[0]
                value = " ".join(chunks[1:]).strip()

        if key:
            data[normalize_key(key)] = value
    return data


def find_value_near_label(container, label_text: str) -> str:
    target = normalize_key(label_text)
    if not target:
        return ""

    label_nodes = container.find_elements(
        By.XPATH,
        ".//*[self::th or self::td or self::dt or self::div or self::span "
        "or self::strong or self::label]",
    )

    for node in label_nodes:
        node_text = normalize_key(get_text_safe(node))
        if node_text != target:
            continue

        # Direct siblings in the same row/block.
        for sibling in node.find_elements(By.XPATH, "following-sibling::*"):
            value = get_text_safe(sibling)
            if value and normalize_key(value) != target:
                return value

        # Table row fallback.
        try:
            row = node.find_element(By.XPATH, "ancestor::tr[1]")
            row_cells = row.find_elements(By.XPATH, "./th|./td")
            if len(row_cells) >= 2:
                first_cell = normalize_key(get_text_safe(row_cells[0]))
                if first_cell == target:
                    parts = [get_text_safe(c) for c in row_cells[1:]]
                    value = " ".join([p for p in parts if p]).strip()
                    if value:
                        return value
        except Exception:
            pass

        # Generic container fallback.
        try:
            parent = node.find_element(By.XPATH, "./..")
            children = parent.find_elements(By.XPATH, "./*")
            if len(children) >= 2:
                for child in children[1:]:
                    value = get_text_safe(child)
                    if value and normalize_key(value) != target:
                        return value
        except Exception:
            pass

        # Last fallback: nearest following value-like element.
        following = node.find_elements(
            By.XPATH,
            "following::*[self::td or self::dd or self::div or self::span][1]",
        )
        if following:
            value = get_text_safe(following[0])
            if value and normalize_key(value) != target:
                return value

    return ""


def find_value_in_roots(roots: List, labels: List[str]) -> str:
    for label in labels:
        for root in roots:
            try:
                value = find_value_near_label(root, label)
            except Exception:
                value = ""
            if value:
                return value
    return ""


def clean_briefs_value(raw_text: str) -> str:
    text = (raw_text or "").strip()
    if not text:
        return ""

    noise_tokens = [
        "transcript",
        "object description",
        "item description",
        "search this record",
        "powered by contentdm",
        "homebrowseaboutcontact",
    ]

    candidates = []
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    candidates.extend(lines)
    candidates.append(re.sub(r"\s+", " ", text).strip())

    for candidate in candidates:
        value = re.sub(r"\s+", " ", candidate).strip()
        if not value:
            continue

        lowered = value.lower()
        if any(token in lowered for token in noise_tokens):
            continue

        if "--" in value and "briefs" in lowered:
            value = value.split("--", 1)[1].strip()
            lowered = value.lower()

        value = re.sub(r"(?i)^briefs\s*[:\-]*\s*", "", value).strip()
        lowered = value.lower()
        if not value:
            continue

        if len(value) > 120:
            continue

        # Most records in this run should resolve to Oregon Supreme Court.
        if "oregon supreme court" in lowered:
            return "Oregon Supreme Court"
        return value

    return ""


def extract_briefs_value(driver: webdriver.Chrome, listing_briefs: str = "") -> str:
    selectors = [
        ".BreadCrumbs a",
        ".breadcrumbs a",
        ".breadcrumb a",
        "[aria-label='breadcrumb'] a",
    ]
    for selector in selectors:
        links = driver.find_elements(By.CSS_SELECTOR, selector)
        for link in links:
            txt = get_text_safe(link)
            if not txt:
                continue
            if "briefs --" in txt.lower() or "oregon supreme court" in txt.lower():
                cleaned = clean_briefs_value(txt)
                if cleaned:
                    return cleaned

    xpath_candidates = [
        "//a[contains(normalize-space(),'Briefs --')]",
        "//a[contains(normalize-space(),'Oregon Supreme Court')]",
        "//li[contains(@class,'breadcrumb')]//a[contains(normalize-space(),'Briefs')]",
    ]
    for xpath_query in xpath_candidates:
        nodes = driver.find_elements(By.XPATH, xpath_query)
        for node in nodes:
            cleaned = clean_briefs_value(get_text_safe(node))
            if cleaned:
                return cleaned

    return clean_briefs_value(listing_briefs)


def briefs_value_is_valid(value: str) -> bool:
    raw = (value or "").strip()
    if not raw:
        return False
    cleaned = clean_briefs_value(raw)
    if not cleaned:
        return False
    raw_single_line = re.sub(r"\s+", " ", raw).strip()
    return raw_single_line == cleaned and len(cleaned) <= 120


def extract_item_fields(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    item_url: str,
    logger: logging.Logger,
    listing_hint: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, str]]:
    if not page_load_with_retry(driver, item_url, wait, "h1.ItemTitle-primaryTitle", logger, retries=2):
        return None

    listing_hint = listing_hint or {}

    try:
        wait.until(
            lambda d: len(
                d.find_elements(
                    By.CSS_SELECTOR,
                    "table.ItemView-itemMetadata, .ItemView-itemMetadata",
                )
            )
            > 0
        )
    except TimeoutException:
        pass

    result = {
        "briefs": "",
        "title_primary": "",
        "official_case_name": "",
        "case_number": "",
        "type": "",
        "date_decided": "",
        "citation": "",
        "title_secondary": "",
        "pdf_url": "",
    }

    result["briefs"] = extract_briefs_value(driver, listing_hint.get("briefs", ""))

    title_el = driver.find_elements(By.CSS_SELECTOR, "h1.ItemTitle-primaryTitle")
    if title_el:
        result["title_primary"] = get_text_safe(title_el[0])
    if not result["title_primary"]:
        result["title_primary"] = listing_hint.get("title", "")

    object_tables = driver.find_elements(
        By.CSS_SELECTOR, "table.ItemView-itemMetadata.object-description"
    )
    object_blocks = driver.find_elements(
        By.CSS_SELECTOR,
        ".ItemView-itemMetadata.object-description, .object-description",
    )
    object_roots = object_tables + object_blocks
    if not object_roots:
        object_roots = driver.find_elements(By.CSS_SELECTOR, "table.ItemView-itemMetadata, .ItemView-itemMetadata")

    merged_object_meta: Dict[str, str] = {}
    if object_tables:
        for table in object_tables:
            merged_object_meta.update(extract_metadata_from_table(table))

    result["official_case_name"] = (
        merged_object_meta.get("official case name", "")
        or find_value_in_roots(object_roots, ["Official case name"])
    )
    result["case_number"] = (
        merged_object_meta.get("case number", "")
        or find_value_in_roots(object_roots, ["Case number"])
    )
    result["type"] = (
        merged_object_meta.get("type", "")
        or find_value_in_roots(object_roots, ["Type"])
    )
    result["date_decided"] = (
        merged_object_meta.get("date decided", "")
        or find_value_in_roots(object_roots, ["Date decided"])
    )
    result["citation"] = (
        merged_object_meta.get("citation", "")
        or find_value_in_roots(object_roots, ["Citation"])
    )

    item_tables = driver.find_elements(
        By.CSS_SELECTOR, "table.ItemView-itemMetadata.item-description"
    )
    item_blocks = driver.find_elements(
        By.CSS_SELECTOR,
        ".ItemView-itemMetadata.item-description, .item-description",
    )
    item_roots = item_tables + item_blocks
    if not item_roots:
        item_roots = object_roots

    if item_tables:
        merged_item_meta: Dict[str, str] = {}
        for table in item_tables:
            merged_item_meta.update(extract_metadata_from_table(table))
        result["title_secondary"] = merged_item_meta.get("title", "")
    if not result["title_secondary"]:
        result["title_secondary"] = find_value_in_roots(item_roots, ["Title"])
    if not result["title_secondary"]:
        result["title_secondary"] = listing_hint.get("title", "")

    if not result["official_case_name"]:
        result["official_case_name"] = listing_hint.get("official_case_name", "")
    if not result["date_decided"]:
        result["date_decided"] = listing_hint.get("date_decided", "")
    if not result["briefs"]:
        result["briefs"] = clean_briefs_value(listing_hint.get("briefs", ""))

    if not result["case_number"] or not result["type"]:
        parsed_case_number, parsed_type = parse_case_number_and_type(result["title_primary"])
        if not result["case_number"]:
            result["case_number"] = parsed_case_number
        if not result["type"]:
            result["type"] = parsed_type

    pdf_links = driver.find_elements(
        By.XPATH,
        "//a[contains(@href,'/digital/api/collection') and contains(@href,'/download') and normalize-space()='All']",
    )
    if not pdf_links:
        pdf_links = driver.find_elements(
            By.XPATH,
            "//a[contains(@href,'/digital/api/collection') and contains(@href,'/download')]",
        )

    if pdf_links:
        href = pdf_links[0].get_attribute("href") or ""
        if href.startswith("http://") or href.startswith("https://"):
            result["pdf_url"] = href
        else:
            result["pdf_url"] = urljoin(BASE_DOMAIN, href)

    return result


def validate_pdf_signature_or_type(content_type: str, first_bytes: bytes) -> bool:
    ctype = (content_type or "").lower()
    return ("application/pdf" in ctype) or first_bytes.startswith(b"%PDF")


def download_pdf(
    session: requests.Session,
    pdf_url: str,
    output_path: Path,
    logger: logging.Logger,
) -> str:
    if output_path.exists() and output_path.stat().st_size > 0:
        return "PDF_EXISTS"

    tmp_path = output_path.with_suffix(".pdf.part")
    first_bytes = b""

    try:
        with session.get(pdf_url, stream=True, timeout=(15, 90)) as resp:
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")

            with tmp_path.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    if len(first_bytes) < 8:
                        first_bytes += chunk[: 8 - len(first_bytes)]
                    f.write(chunk)

            if not validate_pdf_signature_or_type(content_type, first_bytes):
                if tmp_path.exists():
                    tmp_path.unlink()
                logger.error("Invalid PDF response for URL: %s (Content-Type: %s)", pdf_url, content_type)
                return "PDF_INVALID"

            tmp_path.replace(output_path)
            return "PDF_DOWNLOADED"

    except Exception as exc:
        if tmp_path.exists():
            tmp_path.unlink()
        logger.error("PDF download failed: %s | %s", pdf_url, str(exc))
        return "PDF_ERROR"


def parse_page_num_from_url(url: str) -> Optional[int]:
    m = re.search(r"/page/(\d+)", url or "")
    return int(m.group(1)) if m else None


def detect_last_page(driver: webdriver.Chrome) -> Optional[int]:
    candidates: Set[int] = set()

    # Direct page numbers from href.
    for link in driver.find_elements(By.CSS_SELECTOR, "a[href*='/page/']"):
        href = (link.get_attribute("href") or "").strip()
        m = re.search(r"/page/(\d+)", href)
        if m:
            candidates.add(int(m.group(1)))

    # Page numbers from pagination widgets.
    pager_roots = driver.find_elements(
        By.XPATH,
        "//*[contains(@class,'pagination') or contains(@class,'pager') "
        "or contains(normalize-space(),'Go to page')]",
    )
    for root in pager_roots:
        nodes = root.find_elements(By.XPATH, ".//*[self::a or self::button or self::span]")
        for node in nodes:
            txt = (get_text_safe(node) or "").strip()
            if re.fullmatch(r"\d{1,4}", txt):
                candidates.add(int(txt))

    # "Go to page" input sometimes exposes max page.
    page_inputs = driver.find_elements(By.XPATH, "//input[@max]")
    for inp in page_inputs:
        max_attr = (inp.get_attribute("max") or "").strip()
        if re.fullmatch(r"\d{1,4}", max_attr):
            candidates.add(int(max_attr))

    valid = [x for x in candidates if x >= 1]
    return max(valid) if valid else None


def page_signature_from_records(item_records: List[Dict[str, str]]) -> str:
    if not item_records:
        return ""
    first_urls = [r.get("url", "") for r in item_records[:5]]
    return "|".join(first_urls)


def collect_item_records(driver: webdriver.Chrome) -> List[Dict[str, str]]:
    links: List[Dict[str, str]] = []
    for elem in driver.find_elements(By.CSS_SELECTOR, "a.SearchResult-container"):
        href = (elem.get_attribute("href") or "").strip()
        if not href:
            continue
        if href.startswith("/"):
            href = urljoin(BASE_DOMAIN, href)
        lines = [line.strip() for line in (elem.text or "").splitlines() if line.strip()]
        listing_title = lines[0] if len(lines) >= 1 else ""
        listing_case_name = lines[1] if len(lines) >= 2 else ""
        listing_date = lines[2] if len(lines) >= 3 else ""
        listing_briefs = lines[3] if len(lines) >= 4 else ""
        links.append(
            {
                "url": href,
                "title": listing_title,
                "official_case_name": listing_case_name,
                "date_decided": listing_date,
                "briefs": clean_briefs_value(listing_briefs),
            }
        )

    deduped: List[Dict[str, str]] = []
    seen = set()
    for record in links:
        url = record.get("url", "")
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(record)
    return deduped


def row_needs_repair(row: List[str]) -> bool:
    row = normalize_row_length(row)
    if not briefs_value_is_valid(row[IDX_BRIEFS]):
        return True
    fields_to_check = [
        IDX_OFFICIAL_CASE,
        IDX_CASE_NUMBER,
        IDX_TYPE,
        IDX_DATE_DECIDED,
        IDX_CITATION,
        IDX_TITLE_SECONDARY,
    ]
    return any(not row[idx].strip() for idx in fields_to_check)


def repair_missing_rows_in_csv(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    csv_path: Path,
    logger: logging.Logger,
) -> Tuple[int, int]:
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return 0, 0

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))

    if not rows:
        return 0, 0

    header = normalize_row_length(rows[0])
    data_rows = [normalize_row_length(r) for r in rows[1:]]

    repaired = 0
    errors = 0

    for idx, row in enumerate(data_rows):
        if not row or not any(cell.strip() for cell in row):
            continue
        if not row_needs_repair(row):
            continue

        pdf_url = row[IDX_PDF_URL].strip()
        item_id = extract_id_from_pdf_url(pdf_url) or extract_id_from_path(row[IDX_DOWNLOAD_PATH].strip())
        collection = extract_collection_from_pdf_url(pdf_url)
        if not item_id or not collection:
            continue

        item_url = f"{BASE_DOMAIN}/digital/collection/{collection}/id/{item_id}/rec/1"
        listing_hint = {
            "title": row[IDX_TITLE_PRIMARY],
            "official_case_name": row[IDX_OFFICIAL_CASE],
            "date_decided": row[IDX_DATE_DECIDED],
            "briefs": row[IDX_BRIEFS],
        }

        try:
            fields = extract_item_fields(
                driver,
                wait,
                item_url,
                logger,
                listing_hint=listing_hint,
            )
            if not fields:
                errors += 1
                logger.error("Record %s | REPAIR_ERROR extracting fields", item_id)
                random_delay()
                continue

            repaired_briefs = clean_briefs_value(fields.get("briefs", "") or row[IDX_BRIEFS])
            if repaired_briefs:
                row[IDX_BRIEFS] = repaired_briefs
            row[IDX_TITLE_PRIMARY] = fields.get("title_primary", "") or row[IDX_TITLE_PRIMARY]
            row[IDX_OFFICIAL_CASE] = fields.get("official_case_name", "") or row[IDX_OFFICIAL_CASE]
            row[IDX_CASE_NUMBER] = fields.get("case_number", "") or row[IDX_CASE_NUMBER]
            row[IDX_TYPE] = fields.get("type", "") or row[IDX_TYPE]
            row[IDX_DATE_DECIDED] = fields.get("date_decided", "") or row[IDX_DATE_DECIDED]
            row[IDX_CITATION] = fields.get("citation", "") or row[IDX_CITATION]
            row[IDX_TITLE_SECONDARY] = fields.get("title_secondary", "") or row[IDX_TITLE_SECONDARY]

            data_rows[idx] = row
            repaired += 1
            logger.info("Record %s | REPAIRED", item_id)
            random_delay()
        except Exception as exc:
            errors += 1
            logger.error("Record %s | REPAIR_ERROR %s", item_id, str(exc))
            random_delay()

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS if header[0].strip().lower() != "briefs" else header)
        for row in data_rows:
            writer.writerow(normalize_row_length(row))

    return repaired, errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape ContentDM case records and PDFs.")
    parser.add_argument("--start-page", type=int, default=1, help="Page number to start from.")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum number of pages to scrape from start page.",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        default=False,
        help="Run Chrome in visible mode (default is headless).",
    )
    parser.add_argument(
        "--csv-name",
        type=str,
        default="case.csv",
        help="CSV case under downloads/CSV/",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=DEFAULT_BASE_URL,
        help="Base search URL that includes /page/1.",
    )
    parser.add_argument(
        "--repair-missing",
        action="store_true",
        default=False,
        help="Re-open existing CSV records with blank metadata fields and update rows in place.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    base_dir = Path(__file__).resolve().parent
    paths = ensure_folders(base_dir)

    logger = setup_logging(paths["log"])
    logger.info("=" * 80)
    logger.info("Scrape run started")

    csv_path = paths["csv"] / args.csv_name
    ensure_csv_with_header(csv_path)

    done_ids, done_pdf_urls = load_existing_index(csv_path)
    logger.info("Loaded existing index: %d ids, %d pdf urls", len(done_ids), len(done_pdf_urls))

    driver = None
    session = setup_requests_session()
    wait_timeout = 25

    processed_this_run = 0
    skipped_duplicates = 0
    errors = 0

    try:
        driver = setup_driver(args.headful)
        wait = WebDriverWait(driver, wait_timeout)

        if args.repair_missing:
            logger.info("Repair mode enabled: updating rows with missing metadata in %s", csv_path)
            repaired_count, repair_errors = repair_missing_rows_in_csv(driver, wait, csv_path, logger)
            errors += repair_errors
            logger.info(
                "Repair summary | repaired rows: %d | repair errors: %d",
                repaired_count,
                repair_errors,
            )
            done_ids, done_pdf_urls = load_existing_index(csv_path)
            logger.info(
                "Reloaded existing index after repair: %d ids, %d pdf urls",
                len(done_ids),
                len(done_pdf_urls),
            )

        current_page = max(1, args.start_page)
        pages_processed = 0
        detected_last_page: Optional[int] = None
        seen_page_signatures: Set[str] = set()

        while True:
            if args.max_pages is not None and pages_processed >= args.max_pages:
                logger.info("Reached --max-pages limit (%s)", args.max_pages)
                break

            page_url = build_page_url(args.base_url, current_page)
            logger.info("Current page number: %d | URL: %s", current_page, page_url)

            loaded = page_load_with_retry(driver, page_url, wait, "body", logger, retries=2)
            if not loaded:
                logger.error("Failed to load search page %d after retries", current_page)
                errors += 1
                break

            try:
                wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.SearchResult-container")))
            except TimeoutException:
                pass

            item_records = collect_item_records(driver)
            if not item_records:
                logger.info("No search results found on page %d; stopping.", current_page)
                break

            current_signature = page_signature_from_records(item_records)
            if current_signature and current_signature in seen_page_signatures:
                logger.info(
                    "Detected repeated page content on page %d; pagination likely ended. Stopping.",
                    current_page,
                )
                break
            if current_signature:
                seen_page_signatures.add(current_signature)

            if detected_last_page is None:
                detected_last_page = detect_last_page(driver)
                if detected_last_page:
                    logger.info("Detected paginator last page: %d", detected_last_page)

            for item_record in item_records:
                item_url = item_record.get("url", "")
                if not item_url:
                    continue
                item_id, collection = extract_item_id_and_collection(item_url)
                if not item_id:
                    logger.error("Could not parse item id from URL: %s", item_url)
                    errors += 1
                    continue

                pred_pdf_url = predicted_pdf_url(collection, item_id)
                if item_id in done_ids or (pred_pdf_url and pred_pdf_url in done_pdf_urls):
                    skipped_duplicates += 1
                    logger.info("Record %s | SKIPPED", item_id)
                    random_delay()
                    continue

                fields = extract_item_fields(
                    driver,
                    wait,
                    item_url,
                    logger,
                    listing_hint=item_record,
                )
                if not fields:
                    errors += 1
                    logger.error("Record %s | ERROR extracting fields", item_id)
                    random_delay()
                    continue

                pdf_url = fields.get("pdf_url") or (pred_pdf_url or "")
                if not pdf_url:
                    errors += 1
                    logger.error("Record %s | ERROR missing PDF url", item_id)
                    random_delay()
                    continue

                local_pdf_path = paths["pdf"] / f"{item_id}.pdf"
                csv_pdf_path = str(Path("downloads") / "PDF" / f"{item_id}.pdf")
                pdf_status = download_pdf(session, pdf_url, local_pdf_path, logger)

                if pdf_status in {"PDF_ERROR", "PDF_INVALID"}:
                    errors += 1
                    logger.error("Record %s | ERROR %s", item_id, pdf_status)
                    random_delay()
                    continue

                row = [
                    clean_briefs_value(fields.get("briefs", "")),
                    fields.get("title_primary", ""),
                    fields.get("official_case_name", ""),
                    fields.get("case_number", ""),
                    fields.get("type", ""),
                    fields.get("date_decided", ""),
                    fields.get("citation", ""),
                    fields.get("title_secondary", ""),
                    pdf_url,
                    csv_pdf_path,
                ]

                append_csv_row(csv_path, row)

                done_ids.add(item_id)
                done_pdf_urls.add(pdf_url)
                processed_this_run += 1

                logger.info("Record %s | SCRAPED | %s", item_id, pdf_status)
                random_delay()

            pages_processed += 1

            if args.max_pages is not None and pages_processed >= args.max_pages:
                logger.info("Reached --max-pages limit (%s)", args.max_pages)
                break

            loaded_page_num = parse_page_num_from_url(driver.current_url or "")
            if loaded_page_num and loaded_page_num != current_page:
                logger.warning(
                    "Loaded URL page (%d) differs from target page (%d): %s",
                    loaded_page_num,
                    current_page,
                    driver.current_url,
                )

            if detected_last_page is not None and current_page >= detected_last_page:
                logger.info(
                    "Reached detected last page (%d); stopping.",
                    detected_last_page,
                )
                break

            current_page += 1
            random_delay()

    except Exception as exc:
        errors += 1
        logger.exception("Fatal error: %s", str(exc))
    finally:
        if driver is not None:
            driver.quit()
        session.close()

        logger.info("Scrape run ended")
        logger.info(
            "Run totals | processed this run: %d | skipped duplicates: %d | errors: %d",
            processed_this_run,
            skipped_duplicates,
            errors,
        )
        logger.info("=" * 80)


if __name__ == "__main__":
    main()
