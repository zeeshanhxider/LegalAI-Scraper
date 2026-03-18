#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, unquote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

try:
    from webdriver_manager.chrome import ChromeDriverManager
    WEBDRIVER_MANAGER_AVAILABLE = True
except Exception:
    WEBDRIVER_MANAGER_AVAILABLE = False


BASE_SITE = "https://www.vermontjudiciary.org"
BASE_LIST_URL = (
    "https://www.vermontjudiciary.org/opinions-decisions"
    "?search_api_fulltext=&facet_from_date=&facet_to_date=&page={page}"
)

STATE_NAME = "Vermont"
COURTS_WEBSITE = "https://www.vermontjudiciary.org/opinions-decisions"

CSV_COLUMNS = [
    "State",
    "Opinion folder (PDF full path)",
    "Metadata location (CSV file path)",
    "Lawyer + Side",
    "Firm",
    "Firm Address",
    "Court",
    "Court Level",
    "Date",
    "Case Number",
    "Case Title",
    "Courts Website",
]

REQUEST_TIMEOUT = 120
PAGE_LOAD_TIMEOUT = 120
WAIT_TIME = 25


def setup_logging() -> logging.Logger:
    log_dir = Path("Log")
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"vermont_judiciary_{datetime.now().strftime('%Y-%m-%d')}.log"

    logger = logging.getLogger("vermont_judiciary")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger


def sanitize_filename(value: str, max_len: int = 180) -> str:
    value = (value or "").strip()
    value = re.sub(r'[\\/:*?"<>|]+', "_", value)
    value = re.sub(r"\s+", " ", value).strip()
    value = value.strip(". ")
    if not value:
        value = "file"
    if len(value) > max_len:
        value = value[:max_len].rstrip(" ._")
    return value


def sanitize_case_no(case_no: str) -> str:
    case_no = (case_no or "").strip()
    case_no = re.sub(r'[\\/:*?"<>|]+', "_", case_no)
    case_no = re.sub(r"\s+", " ", case_no).strip()
    case_no = case_no.strip(". ")
    return case_no or "unknown_case_no"


def normalize_court_name(court: str) -> str:
    court = (court or "").strip().lower()
    court = re.sub(r"&", " and ", court)
    court = re.sub(r"[^a-z0-9]+", "_", court)
    court = re.sub(r"_+", "_", court).strip("_")
    return court or "unknown_court"


def na_if_empty(value: Optional[str]) -> str:
    value = (value or "").strip()
    return value if value else "N/A"


def get_year_from_date(date_str: str) -> str:
    date_str = (date_str or "").strip()

    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return str(datetime.strptime(date_str, fmt).year)
        except ValueError:
            pass

    match = re.search(r"(19|20)\d{2}", date_str)
    if match:
        return match.group(0)

    return str(datetime.now().year)


def infer_court_level(court: str) -> str:
    court_lower = (court or "").strip().lower()
    if not court_lower:
        return "N/A"
    if "supreme" in court_lower:
        return "Supreme Court"
    if "environmental" in court_lower:
        return "Environmental"
    if "civil" in court_lower:
        return "Civil"
    if "criminal" in court_lower:
        return "Criminal"
    if "family" in court_lower:
        return "Family"
    if "probate" in court_lower:
        return "Probate"
    return "N/A"


def build_requests_session() -> requests.Session:
    session = requests.Session()

    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return session


def setup_driver(headless: int = 0) -> webdriver.Chrome:
    options = Options()
    if int(headless) == 1:
        options.add_argument("--headless=new")

    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1600,1200")
    options.add_argument("--lang=en-US")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    prefs = {
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "download_restrictions": 0,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
        "plugins.always_open_pdf_externally": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
    }
    options.add_experimental_option("prefs", prefs)

    if WEBDRIVER_MANAGER_AVAILABLE:
        service = Service(ChromeDriverManager().install())
    else:
        service = Service()

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                """
            },
        )
    except Exception:
        pass

    return driver


def transfer_cookies_selenium_to_requests(driver: webdriver.Chrome, session: requests.Session) -> None:
    try:
        for cookie in driver.get_cookies():
            session.cookies.set(
                cookie["name"],
                cookie["value"],
                domain=cookie.get("domain"),
                path=cookie.get("path", "/"),
            )
    except Exception:
        pass


def get_base_download_dir() -> Path:
    base_dir = Path("downloads")
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir


def get_global_csv_path() -> Path:
    csv_dir = Path("downloads") / "CSV"
    csv_dir.mkdir(parents=True, exist_ok=True)
    return csv_dir / "case.csv"


def get_pdf_base_dir_for_court(court: str) -> Path:
    base_dir = get_base_download_dir()
    court_folder = normalize_court_name(court)
    court_dir = base_dir / court_folder
    court_dir.mkdir(parents=True, exist_ok=True)
    return court_dir


def init_csv(csv_path: Path) -> None:
    if not csv_path.exists():
        with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()


def build_composite_key(case_no: str, date: str, title: str) -> str:
    return "||".join(
        [
            (case_no or "").strip().lower(),
            (date or "").strip().lower(),
            re.sub(r"\s+", " ", (title or "").strip().lower()),
        ]
    )


def load_existing_csv_keys(csv_path: Path) -> Tuple[Set[str], Set[str], Set[str]]:
    pdf_path_keys: Set[str] = set()
    composite_keys: Set[str] = set()
    metadata_paths: Set[str] = set()

    if not csv_path.exists():
        return pdf_path_keys, composite_keys, metadata_paths

    with csv_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            opinion_folder = (row.get("Opinion folder (PDF full path)") or "").strip()
            case_no = (row.get("Case Number") or "").strip()
            date = (row.get("Date") or "").strip()
            title = (row.get("Case Title") or "").strip()
            metadata_location = (row.get("Metadata location (CSV file path)") or "").strip()

            if opinion_folder and opinion_folder != "N/A":
                pdf_path_keys.add(opinion_folder)

            composite_keys.add(build_composite_key(case_no, date, title))

            if metadata_location and metadata_location != "N/A":
                metadata_paths.add(metadata_location)

    return pdf_path_keys, composite_keys, metadata_paths


def append_csv_row(csv_path: Path, row: Dict[str, str]) -> None:
    with csv_path.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerow({col: row.get(col, "N/A") for col in CSV_COLUMNS})
        f.flush()
        os.fsync(f.fileno())


def safe_get_text(parent, css: str) -> str:
    try:
        return parent.find_element(By.CSS_SELECTOR, css).text.strip()
    except Exception:
        return ""


def safe_get_attr(parent, css: str, attr: str) -> str:
    try:
        return (parent.find_element(By.CSS_SELECTOR, css).get_attribute(attr) or "").strip()
    except Exception:
        return ""


def extract_rows_from_listing(driver: webdriver.Chrome, logger: logging.Logger) -> List[Dict[str, str]]:
    wait = WebDriverWait(driver, WAIT_TIME)

    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "section.forms-library")))
    except TimeoutException:
        logger.warning("forms-library not found")
        return []

    articles = driver.find_elements(By.CSS_SELECTOR, "article.views-row.media-document")
    results: List[Dict[str, str]] = []

    for idx, article in enumerate(articles, start=1):
        try:
            title = safe_get_text(article, "div.views-field-name a")
            href = safe_get_attr(article, "div.views-field-name a", "href")
            case_url = urljoin(BASE_SITE, href) if href else ""

            date = safe_get_text(article, "div.views-field-field-document-expiration time")

            court = safe_get_text(article, "div.views-field-field-document-court-division .field-content")
            if not court:
                court = safe_get_text(article, "div.views-field-field-document-court-division")

            case_no = safe_get_text(article, "div.views-field-field-document-number .field-content")
            if not case_no:
                case_no = safe_get_text(article, "div.views-field-field-document-number")

            if not title and not case_url:
                continue

            results.append(
                {
                    "Case Number": case_no,
                    "Date": date,
                    "Court": court,
                    "Case Title": title,
                    "case_url": case_url,
                }
            )
        except StaleElementReferenceException:
            logger.warning("stale row skipped")
        except Exception as e:
            logger.warning("row parse error %s: %s", idx, e)

    return results


def resolve_pdf_candidate_via_get(session: requests.Session, url: str) -> Optional[str]:
    resp = None
    try:
        resp = session.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT, stream=True)
        final_url = resp.url
        content_type = (resp.headers.get("Content-Type") or "").lower()

        if "application/pdf" in content_type:
            return final_url

        try:
            first_chunk = next(resp.iter_content(chunk_size=32), b"")
        except StopIteration:
            first_chunk = b""

        if first_chunk.startswith(b"%PDF"):
            return final_url

        if final_url.lower().endswith(".pdf"):
            return final_url

        return None
    finally:
        if resp is not None:
            resp.close()


def resolve_pdf_url_from_case_page(
    driver: webdriver.Chrome,
    session: requests.Session,
    case_url: str,
    logger: logging.Logger,
) -> Optional[str]:
    if not case_url:
        return None

    transfer_cookies_selenium_to_requests(driver, session)

    lower_case_url = case_url.lower()
    if lower_case_url.endswith(".pdf") or "/media/" in lower_case_url:
        candidate = resolve_pdf_candidate_via_get(session, case_url)
        if candidate:
            return candidate

    for attempt in range(1, 4):
        try:
            driver.get(case_url)
            time.sleep(1.5)

            current_url = driver.current_url or case_url
            transfer_cookies_selenium_to_requests(driver, session)

            lower_current_url = current_url.lower()
            if lower_current_url.endswith(".pdf") or "/media/" in lower_current_url:
                candidate = resolve_pdf_candidate_via_get(session, current_url)
                if candidate:
                    return candidate

            anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")
            for a in anchors:
                href = (a.get_attribute("href") or "").strip()
                if not href:
                    continue

                absolute = urljoin(BASE_SITE, href)
                lower_absolute = absolute.lower()

                if (
                    lower_absolute.endswith(".pdf")
                    or "/media/" in lower_absolute
                    or "download" in lower_absolute
                ):
                    candidate = resolve_pdf_candidate_via_get(session, absolute)
                    if candidate:
                        return candidate

            page_source = driver.page_source or ""
            matches = re.findall(r'href="([^"]+)"', page_source, flags=re.I)
            for href in matches:
                absolute = urljoin(BASE_SITE, href)
                lower_absolute = absolute.lower()

                if (
                    lower_absolute.endswith(".pdf")
                    or "/media/" in lower_absolute
                    or "download" in lower_absolute
                ):
                    candidate = resolve_pdf_candidate_via_get(session, absolute)
                    if candidate:
                        return candidate

            return None

        except Exception as e:
            logger.warning("resolve pdf error attempt %s for %s | %s", attempt, case_url, e)
            time.sleep(2)

    return None


def make_pdf_filename(pdf_url: str, title: str, case_no: str) -> str:
    parsed = urlparse(pdf_url)
    name = unquote(Path(parsed.path).name).strip()

    if not name or "." not in name:
        name = f"{sanitize_filename(title or case_no)}.pdf"

    if not name.lower().endswith(".pdf"):
        name += ".pdf"

    return sanitize_filename(name, 220)


def download_pdf(
    session: requests.Session,
    pdf_url: str,
    pdf_path: Path,
    logger: logging.Logger,
) -> bool:
    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    if pdf_path.exists() and pdf_path.stat().st_size > 0:
        logger.info("PDF already exists: %s", pdf_path)
        return True

    temp_path = pdf_path.with_suffix(pdf_path.suffix + ".part")

    for attempt in range(1, 4):
        try:
            with session.get(pdf_url, stream=True, allow_redirects=True, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()

                content_type = (resp.headers.get("Content-Type") or "").lower()
                first_chunk = b""
                wrote_any = False

                with temp_path.open("wb") as f:
                    for i, chunk in enumerate(resp.iter_content(chunk_size=65536)):
                        if not chunk:
                            continue
                        if i == 0:
                            first_chunk = chunk[:16]
                        f.write(chunk)
                        wrote_any = True

                if not wrote_any:
                    raise RuntimeError("empty file")

                valid_pdf = (
                    "application/pdf" in content_type
                    or first_chunk.startswith(b"%PDF")
                    or pdf_url.lower().endswith(".pdf")
                    or "/media/" in pdf_url.lower()
                )

                if not valid_pdf:
                    temp_path.unlink(missing_ok=True)
                    raise RuntimeError(f"not pdf content-type={content_type}")

                temp_path.replace(pdf_path)
                logger.info("Downloaded PDF: %s", pdf_path)
                return True

        except Exception as e:
            logger.warning("download failed attempt %s | %s | %s", attempt, pdf_url, e)
            temp_path.unlink(missing_ok=True)
            time.sleep(2)

    return False


def parse_listing_page(driver: webdriver.Chrome, page_no: int, logger: logging.Logger) -> List[Dict[str, str]]:
    url = BASE_LIST_URL.format(page=page_no)

    for attempt in range(1, 4):
        try:
            logger.info("Opening page %s | %s", page_no, url)
            driver.get(url)
            time.sleep(1.5)
            rows = extract_rows_from_listing(driver, logger)
            logger.info("Page %s rows: %s", page_no, len(rows))
            return rows
        except Exception as e:
            logger.warning("listing page error attempt %s page %s | %s", attempt, page_no, e)
            time.sleep(2)

    return []


def build_csv_row(
    csv_path: Path,
    pdf_path_str: str,
    item: Dict[str, str],
) -> Dict[str, str]:
    court = na_if_empty(item.get("Court"))
    case_number = na_if_empty(item.get("Case Number"))
    case_title = na_if_empty(item.get("Case Title"))
    date = na_if_empty(item.get("Date"))

    return {
        "State": STATE_NAME,
        "Opinion folder (PDF full path)": na_if_empty(pdf_path_str),
        "Metadata location (CSV file path)": str(csv_path.resolve()).replace("\\", "/"),
        "Lawyer + Side": "N/A",
        "Firm": "N/A",
        "Firm Address": "N/A",
        "Court": court,
        "Court Level": infer_court_level(court),
        "Date": date,
        "Case Number": case_number,
        "Case Title": case_title,
        "Courts Website": COURTS_WEBSITE,
    }


def scrape_all_pages(driver: webdriver.Chrome, session: requests.Session, logger: logging.Logger) -> None:
    page_no = 0
    total_saved = 0

    csv_path = get_global_csv_path()
    init_csv(csv_path)

    pdf_path_keys, composite_keys, _ = load_existing_csv_keys(csv_path)

    while True:
        rows = parse_listing_page(driver, page_no, logger)

        if not rows:
            logger.info("No rows found on page %s. Stop.", page_no)
            break

        for item in rows:
            case_no = (item.get("Case Number") or "").strip()
            date = (item.get("Date") or "").strip()
            court = (item.get("Court") or "").strip()
            title = (item.get("Case Title") or "").strip()
            case_url = (item.get("case_url") or "").strip()

            if not court:
                court = "unknown_court"

            composite_key = build_composite_key(case_no, date, title)
            if composite_key in composite_keys:
                logger.info("Skip existing row | court=%s | case_no=%s | title=%s", court, case_no, title)
                continue

            logger.info(
                "Processing | court=%s | case_no=%s | date=%s | title=%s",
                court, case_no, date, title
            )

            transfer_cookies_selenium_to_requests(driver, session)

            pdf_url = None
            if case_url:
                lower_case_url = case_url.lower()
                if lower_case_url.endswith(".pdf") or "/media/" in lower_case_url:
                    pdf_url = resolve_pdf_candidate_via_get(session, case_url)
                else:
                    pdf_url = resolve_pdf_url_from_case_page(driver, session, case_url, logger)

            year = get_year_from_date(date)
            safe_case_no = sanitize_case_no(case_no)
            pdf_filename = make_pdf_filename(pdf_url or "", title, case_no)

            pdf_base_dir = get_pdf_base_dir_for_court(court)
            pdf_path = pdf_base_dir / year / safe_case_no / pdf_filename
            pdf_path_str = str(pdf_path.resolve()).replace("\\", "/")

            if pdf_path_str in pdf_path_keys and pdf_path.exists():
                logger.info("Skip existing saved path | %s", pdf_path_str)
                composite_keys.add(composite_key)
                continue

            download_ok = False
            if pdf_url:
                transfer_cookies_selenium_to_requests(driver, session)
                download_ok = download_pdf(session, pdf_url, pdf_path, logger)
            else:
                logger.warning("No PDF URL resolved | court=%s | case_no=%s | title=%s", court, case_no, title)

            final_pdf_path_str = pdf_path_str if download_ok else "N/A"
            row = build_csv_row(csv_path=csv_path, pdf_path_str=final_pdf_path_str, item=item)

            append_csv_row(csv_path, row)
            total_saved += 1

            composite_keys.add(composite_key)
            if final_pdf_path_str != "N/A":
                pdf_path_keys.add(final_pdf_path_str)

            logger.info("Saved CSV row immediately | total_saved=%s", total_saved)

        page_no += 1

    logger.info("Scraping finished | total_saved=%s", total_saved)


def main() -> None:
    parser = argparse.ArgumentParser(description="Vermont Judiciary scraper")
    parser.add_argument(
        "--headless",
        type=int,
        choices=[0, 1],
        default=1,
        help="0=open Chrome, 1=headless",
    )
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("Run started")
    logger.info("Headless: %s", bool(args.headless))

    driver = None
    try:
        driver = setup_driver(args.headless)
        session = build_requests_session()
        scrape_all_pages(driver, session, logger)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
    except Exception as e:
        logger.exception("Fatal error: %s", e)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        logger.info("Run finished")


if __name__ == "__main__":
    main()