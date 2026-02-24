#!/usr/bin/env python3
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
from urllib.parse import unquote, urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from urllib3.util.retry import Retry

BASE_URL_TEMPLATE = (
    "https://www.lasc.org/search?query=Kg%3D%3D&cat=All&sort=relevance&page={page}&pagesize={page_size}/"
)
OUT_CSV_FILENAME = "lasc_cases.csv"
HEADLESS = True
PAGE_SIZE = 20
START_PAGE = 1
CSV_COLUMNS = [
    "listing_title",
    "listing_url",
    "published_date",
    "snippet",
    "pdf_url",
    "pdf_local_path",
]

LISTING_WAIT_SECONDS = 20
DETAIL_WAIT_SECONDS = 15
PAGE_LOAD_RETRIES = 3
DOWNLOAD_RETRIES = 3
POLITE_DELAY_RANGE = (0.4, 1.1)

CANONICAL_FIELDS = [
    "listing_title",
    "listing_url",
    "published_date",
    "snippet",
    "pdf_url",
    "pdf_local_path",
]

FIELD_ALIASES = {
    "listing_title": [
        "title",
        "case_title",
        "opinion_title",
        "record_title",
        "name",
        "caption",
    ],
    "listing_url": [
        "url",
        "source_url",
        "detail_url",
        "record_url",
        "case_url",
        "opinion_url",
        "action_url",
        "link",
    ],
    "published_date": [
        "published",
        "publish_date",
        "published_on",
        "filed_date",
        "date",
        "posted_date",
    ],
    "snippet": [
        "summary",
        "excerpt",
        "description",
        "details",
    ],
    "pdf_url": [
        "case_pdf_url",
        "opinion_pdf_url",
        "document_url",
        "pdf_link",
        "download_url",
    ],
    "pdf_local_path": [
        "pdf_path",
        "file_path",
        "local_pdf_path",
        "downloaded_file",
        "local_path",
    ],
}


def normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")


def normalize_key(value: str) -> str:
    return (value or "").strip()


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^\w.\-]+", "_", (name or "").strip())
    cleaned = cleaned.strip("._")
    if not cleaned:
        cleaned = f"document_{int(time.time())}.pdf"
    if not cleaned.lower().endswith(".pdf"):
        cleaned = f"{cleaned}.pdf"
    return cleaned


def is_pdf_link(url: str) -> bool:
    if not url:
        return False
    lower = url.lower()
    path = urlparse(lower).path
    return path.endswith(".pdf") or ".pdf" in path or lower.endswith(".pdf")


def looks_like_case_opinion_action(title: str, snippet: str, url: str) -> bool:
    if is_pdf_link(url):
        return True

    path = urlparse((url or "").lower()).path
    if "/opinions" in path or "/actions" in path:
        return True

    blob = f"{title} {snippet}".lower()
    keywords = [
        "opinion",
        "action",
        "case",
        "docket",
        "writ",
        "judgment",
        "per curiam",
    ]
    if any(keyword in blob for keyword in keywords):
        return True
    if re.search(r"\b\d{2,4}[-/]\d{1,6}\b", blob):
        return True
    return False


def tokenize(text: str) -> Set[str]:
    return {t for t in re.findall(r"[a-z0-9]+", (text or "").lower()) if len(t) > 1}


def extract_case_tokens(text: str) -> Set[str]:
    tokens = set(re.findall(r"\b\d{2,4}[-_]\d{1,6}\b", (text or "").lower()))
    tokens.update(re.findall(r"\b\d{2,4}\s*-\s*\d{1,6}\b", (text or "").lower()))
    return {re.sub(r"\s+", "", t) for t in tokens}


def should_map_by_heuristic(canonical: str, normalized_header: str) -> bool:
    tokens = set(filter(None, normalized_header.split("_")))

    if canonical == "listing_title":
        return bool(tokens & {"title", "name", "caption"})
    if canonical == "listing_url":
        has_url_token = bool(tokens & {"url", "link", "href"})
        mentions_pdf = "pdf" in tokens
        context = bool(tokens & {"source", "listing", "detail", "record", "case", "opinion", "action"})
        return has_url_token and not mentions_pdf and (context or normalized_header in {"url", "link"})
    if canonical == "published_date":
        return bool(tokens & {"date", "published", "filed", "posted"})
    if canonical == "snippet":
        return bool(tokens & {"snippet", "summary", "excerpt", "description", "details"})
    if canonical == "pdf_url":
        return "pdf" in tokens and bool(tokens & {"url", "link", "href", "download"})
    if canonical == "pdf_local_path":
        return bool(tokens & {"path", "file", "filename", "local"}) and bool(tokens & {"pdf", "download", "path"})
    return False


def build_field_mapping(headers: List[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    used_headers: Set[str] = set()
    normalized_headers = {header: normalize_header(header) for header in headers}

    for canonical in CANONICAL_FIELDS:
        alias_norms = {normalize_header(canonical)}
        alias_norms.update(normalize_header(a) for a in FIELD_ALIASES.get(canonical, []))
        for header in headers:
            if header in used_headers:
                continue
            if normalized_headers[header] in alias_norms:
                mapping[canonical] = header
                used_headers.add(header)
                break

    for canonical in CANONICAL_FIELDS:
        if canonical in mapping:
            continue
        for header in headers:
            if header in used_headers:
                continue
            if should_map_by_heuristic(canonical, normalized_headers[header]):
                mapping[canonical] = header
                used_headers.add(header)
                break

    return mapping


def get_row_value(row: Dict[str, str], mapping: Dict[str, str], canonical: str) -> str:
    mapped_header = mapping.get(canonical)
    if mapped_header:
        return (row.get(mapped_header) or "").strip()

    alias_norms = {normalize_header(canonical)}
    alias_norms.update(normalize_header(a) for a in FIELD_ALIASES.get(canonical, []))
    for header, value in row.items():
        if normalize_header(header) in alias_norms:
            return (value or "").strip()
    for header, value in row.items():
        if should_map_by_heuristic(canonical, normalize_header(header)):
            return (value or "").strip()
    return ""


def set_row_value(
    row_data: Dict[str, str], mapping: Dict[str, str], canonical: str, value: str
) -> None:
    header = mapping.get(canonical)
    if header:
        row_data[header] = (value or "").strip()


def ensure_directories(base_dir: Path) -> Dict[str, Path]:
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


def setup_logger(log_dir: Path) -> logging.Logger:
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"log-{today}.log"

    logger = logging.getLogger("lasc_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def load_or_create_csv(csv_path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if csv_path.exists() and csv_path.stat().st_size > 0:
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames[:] if reader.fieldnames else CSV_COLUMNS[:]
            rows = list(reader)
        return headers, rows

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
    return CSV_COLUMNS[:], []


def build_dedup_sets(
    rows: List[Dict[str, str]], mapping: Dict[str, str]
) -> Tuple[Set[str], Set[str], Set[str]]:
    existing_keys: Set[str] = set()
    existing_pdf_urls: Set[str] = set()
    existing_listing_urls: Set[str] = set()

    for row in rows:
        pdf_url = normalize_key(get_row_value(row, mapping, "pdf_url"))
        listing_url = normalize_key(get_row_value(row, mapping, "listing_url"))
        unique_key = pdf_url if pdf_url else listing_url

        if unique_key:
            existing_keys.add(unique_key)
        if pdf_url:
            existing_pdf_urls.add(pdf_url)
        if listing_url:
            existing_listing_urls.add(listing_url)

    return existing_keys, existing_pdf_urls, existing_listing_urls


def build_http_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=DOWNLOAD_RETRIES,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        }
    )
    return session


def init_driver(headless: bool) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1600,1200")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(45)
    return driver


def load_page_with_retries(
    driver: webdriver.Chrome,
    url: str,
    timeout: int,
    logger: logging.Logger,
    expect_articles: bool = False,
) -> bool:
    for attempt in range(1, PAGE_LOAD_RETRIES + 1):
        try:
            driver.get(url)
            if expect_articles:
                WebDriverWait(driver, timeout).until(
                    lambda d: (
                        len(d.find_elements(By.CSS_SELECTOR, "div.container.search-results")) > 0
                        or len(d.find_elements(By.CSS_SELECTOR, "article.post")) > 0
                        or "no results" in d.page_source.lower()
                    )
                )
            else:
                WebDriverWait(driver, timeout).until(
                    lambda d: len(d.find_elements(By.TAG_NAME, "body")) > 0
                )
            return True
        except (TimeoutException, WebDriverException) as exc:
            logger.warning("Load failed (attempt %s/%s): %s | %s", attempt, PAGE_LOAD_RETRIES, url, exc)
            if attempt < PAGE_LOAD_RETRIES:
                time.sleep(1.2 * attempt)
    return False


def extract_listing_records(driver: webdriver.Chrome, page_url: str) -> List[Dict[str, str]]:
    records: List[Dict[str, str]] = []
    articles = driver.find_elements(By.CSS_SELECTOR, "div.container.search-results article.post")
    for article in articles:
        try:
            link_el = article.find_element(By.CSS_SELECTOR, "h4 a")
            title = (link_el.text or "").strip()
            href = (link_el.get_attribute("href") or "").strip()
            listing_url = urljoin(page_url, href) if href else ""

            published_date = ""
            time_elements = article.find_elements(By.CSS_SELECTOR, "time")
            if time_elements:
                published_date = (time_elements[0].get_attribute("datetime") or time_elements[0].text or "").strip()

            snippet = ""
            snippet_elements = article.find_elements(By.CSS_SELECTOR, "p span")
            snippet_texts = [s.text.strip() for s in snippet_elements if s.text and s.text.strip()]
            if snippet_texts:
                snippet = " ".join(snippet_texts)
            else:
                p_elements = article.find_elements(By.CSS_SELECTOR, "p")
                if p_elements:
                    snippet = (p_elements[0].text or "").strip()

            if listing_url:
                records.append(
                    {
                        "listing_title": title,
                        "listing_url": listing_url,
                        "published_date": published_date,
                        "snippet": snippet,
                    }
                )
        except Exception:
            continue
    return records


def score_pdf_candidate(
    driver: webdriver.Chrome,
    anchor,
    href: str,
    link_text: str,
    listing_title: str,
    case_tokens: Set[str],
) -> int:
    score = 0
    path = urlparse(href).path.lower()
    filename = unquote(os.path.basename(path)).lower()
    combined = f"{link_text} {filename}".lower()

    if path.endswith(".pdf"):
        score += 10
    if "opinion" in combined or "opn" in combined:
        score += 2

    title_tokens = tokenize(listing_title)
    combined_tokens = tokenize(combined)
    overlap = len(title_tokens & combined_tokens)
    score += min(overlap * 2, 8)

    for token in case_tokens:
        if token in re.sub(r"\s+", "", combined):
            score += 4

    try:
        in_main = bool(
            driver.execute_script(
                "return !!arguments[0].closest('main, article, #content, .content, .main-content, .post-content, .entry-content');",
                anchor,
            )
        )
        if in_main:
            score += 5
    except Exception:
        pass

    return score


def discover_pdf_on_detail_page(
    driver: webdriver.Chrome, detail_url: str, listing_title: str, logger: logging.Logger
) -> str:
    ok = load_page_with_retries(
        driver, detail_url, timeout=DETAIL_WAIT_SECONDS, logger=logger, expect_articles=False
    )
    if not ok:
        logger.error("Could not load detail page: %s", detail_url)
        return ""

    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")
    if not anchors:
        logger.warning("No links found on detail page: %s", detail_url)
        return ""

    candidates: List[Tuple[int, int, str]] = []
    case_tokens = extract_case_tokens(listing_title)

    for idx, anchor in enumerate(anchors):
        try:
            raw_href = (anchor.get_attribute("href") or "").strip()
            if not raw_href:
                continue
            full_href = urljoin(detail_url, raw_href)
            if not is_pdf_link(full_href):
                continue
            link_text = (anchor.text or "").strip()
            score = score_pdf_candidate(
                driver=driver,
                anchor=anchor,
                href=full_href,
                link_text=link_text,
                listing_title=listing_title,
                case_tokens=case_tokens,
            )
            candidates.append((score, idx, full_href))
        except Exception:
            continue

    if not candidates:
        logger.warning("No PDF found on detail page: %s", detail_url)
        return ""

    candidates.sort(key=lambda item: (-item[0], item[1]))
    return candidates[0][2]


def download_pdf(
    session: requests.Session,
    pdf_url: str,
    pdf_dir: Path,
    listing_title: str,
    logger: logging.Logger,
) -> Tuple[str, bool]:
    parsed = urlparse(pdf_url)
    filename = unquote(os.path.basename(parsed.path))
    if not filename:
        filename = re.sub(r"\s+", "_", listing_title.strip())[:120]
    filename = sanitize_filename(filename)

    local_path = pdf_dir / filename
    relative_path = f"downloads/PDF/{filename}"

    if local_path.exists():
        logger.info("PDF already exists: %s", local_path)
        return relative_path, False

    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            with session.get(pdf_url, stream=True, timeout=(20, 90)) as response:
                response.raise_for_status()
                with local_path.open("wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            logger.info("PDF downloaded: %s -> %s", pdf_url, relative_path)
            return relative_path, True
        except Exception as exc:
            if attempt == DOWNLOAD_RETRIES:
                if local_path.exists() and local_path.stat().st_size == 0:
                    local_path.unlink(missing_ok=True)
                raise exc
            time.sleep(1.0 * attempt)

    return "", False


def run() -> None:
    base_dir = Path(__file__).resolve().parent
    dirs = ensure_directories(base_dir)
    csv_path = dirs["csv"] / OUT_CSV_FILENAME

    logger = setup_logger(dirs["log"])
    logger.info("Scrape started")
    logger.info("CSV file: %s", csv_path)

    headers, existing_rows = load_or_create_csv(csv_path)
    mapping = build_field_mapping(headers)
    existing_keys, existing_pdf_urls, existing_listing_urls = build_dedup_sets(existing_rows, mapping)

    logger.info("Existing CSV rows: %s", len(existing_rows))
    logger.info("Resolved CSV column mapping: %s", mapping)

    stats = {
        "pages_visited": 0,
        "total_items_found": 0,
        "duplicates_skipped": 0,
        "downloaded": 0,
        "written_rows": 0,
        "errors": 0,
    }

    session = build_http_session()
    driver: Optional[webdriver.Chrome] = None

    try:
        driver = init_driver(HEADLESS)

        with csv_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            page = START_PAGE

            while True:
                page_url = BASE_URL_TEMPLATE.format(page=page, page_size=PAGE_SIZE)
                logger.info("Navigating to listing page %s: %s", page, page_url)
                ok = load_page_with_retries(
                    driver,
                    page_url,
                    timeout=LISTING_WAIT_SECONDS,
                    logger=logger,
                    expect_articles=True,
                )
                if not ok:
                    logger.error("Failed to load listing page, stopping: %s", page_url)
                    break

                records = extract_listing_records(driver, page_url)
                if not records:
                    logger.info("No article.post records on page %s. Stopping pagination.", page)
                    break

                stats["pages_visited"] += 1
                logger.info("Page %s loaded. article.post count: %s", page, len(records))

                for record in records:
                    listing_title = record.get("listing_title", "")
                    listing_url = record.get("listing_url", "")
                    published_date = record.get("published_date", "")
                    snippet = record.get("snippet", "")

                    if not looks_like_case_opinion_action(listing_title, snippet, listing_url):
                        continue

                    stats["total_items_found"] += 1
                    listing_key = normalize_key(listing_url)

                    if listing_key and listing_key in existing_listing_urls:
                        stats["duplicates_skipped"] += 1
                        logger.info("Duplicate skipped by listing URL: %s", listing_url)
                        continue

                    try:
                        pdf_url = ""
                        if is_pdf_link(listing_url):
                            pdf_url = listing_url
                            logger.info("Direct PDF on listing: %s", pdf_url)
                        else:
                            time.sleep(random.uniform(*POLITE_DELAY_RANGE))
                            pdf_url = discover_pdf_on_detail_page(
                                driver=driver,
                                detail_url=listing_url,
                                listing_title=listing_title,
                                logger=logger,
                            )
                            if pdf_url:
                                logger.info("PDF resolved from detail page: %s -> %s", listing_url, pdf_url)

                        pdf_key = normalize_key(pdf_url)
                        dedupe_key = pdf_key if pdf_key else listing_key

                        if pdf_key and pdf_key in existing_pdf_urls:
                            stats["duplicates_skipped"] += 1
                            logger.info("Duplicate skipped by PDF URL: %s", pdf_url)
                            continue
                        if dedupe_key and dedupe_key in existing_keys:
                            stats["duplicates_skipped"] += 1
                            logger.info("Duplicate skipped by unique key: %s", dedupe_key)
                            continue

                        pdf_local_path = ""
                        if pdf_url:
                            try:
                                pdf_local_path, is_new_download = download_pdf(
                                    session=session,
                                    pdf_url=pdf_url,
                                    pdf_dir=dirs["pdf"],
                                    listing_title=listing_title,
                                    logger=logger,
                                )
                                if is_new_download:
                                    stats["downloaded"] += 1
                                else:
                                    logger.info("PDF not re-downloaded (already exists): %s", pdf_local_path)
                            except Exception as download_exc:
                                stats["errors"] += 1
                                logger.error(
                                    "PDF download error | listing=%s | pdf=%s | error=%s",
                                    listing_url,
                                    pdf_url,
                                    download_exc,
                                )
                        else:
                            logger.warning("No PDF resolved for detail URL: %s", listing_url)

                        row_data = {header: "" for header in headers}
                        set_row_value(row_data, mapping, "listing_title", listing_title)
                        set_row_value(row_data, mapping, "listing_url", listing_url)
                        set_row_value(row_data, mapping, "published_date", published_date)
                        set_row_value(row_data, mapping, "snippet", snippet)
                        set_row_value(row_data, mapping, "pdf_url", pdf_url)
                        set_row_value(row_data, mapping, "pdf_local_path", pdf_local_path)

                        writer.writerow(row_data)
                        f.flush()
                        stats["written_rows"] += 1

                        if listing_key:
                            existing_listing_urls.add(listing_key)
                        if pdf_key:
                            existing_pdf_urls.add(pdf_key)
                        if dedupe_key:
                            existing_keys.add(dedupe_key)
                    except Exception as rec_exc:
                        stats["errors"] += 1
                        logger.error("Record processing error | url=%s | error=%s", listing_url, rec_exc)

                    time.sleep(random.uniform(*POLITE_DELAY_RANGE))

                next_page = page + 1
                logger.info("Moving pagination from page %s to page %s", page, next_page)
                page = next_page
                time.sleep(random.uniform(*POLITE_DELAY_RANGE))

    finally:
        if driver is not None:
            driver.quit()

        logger.info("Scrape finished")
        logger.info("Total items found: %s", stats["total_items_found"])
        logger.info("Items skipped (duplicates): %s", stats["duplicates_skipped"])
        logger.info("Items downloaded: %s", stats["downloaded"])
        logger.info("Rows written: %s", stats["written_rows"])
        logger.info("Errors: %s", stats["errors"])


if __name__ == "__main__":
    run()
