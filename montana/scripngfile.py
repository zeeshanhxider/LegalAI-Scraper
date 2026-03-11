#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

try:
    from webdriver_manager.chrome import ChromeDriverManager
    USE_WDM = True
except Exception:
    USE_WDM = False


BASE_URL = "https://courts.mt.gov/external/orders/dailyorders"

ROOT_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = ROOT_DIR / "downloads"
CSV_DIR = DOWNLOADS_DIR / "CSV"
PDF_DIR = DOWNLOADS_DIR / "PDF"
LOG_DIR = ROOT_DIR / "Log"

CSV_PATH = CSV_DIR / "montana_daily_orders.csv"

# Updated CSV because you now want inner page "Case information" top table also
CSV_COLUMNS = [
    "document_description",
    "file_date",
    "case_number",
    "title",
    "pdf_url",
    "case_info_url",
    "inner_case_number",
    "original_court",
    "case_type",
    "short_title",
    "full_title",
    "case_filing_date",
    "origination_court_case_number",
    "pdf_local_path",
]


def ensure_dirs() -> None:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logging() -> None:
    ensure_dirs()
    log_path = LOG_DIR / f"montana_daily_orders-{datetime.now().strftime('%Y-%m-%d')}.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)


def normalize_text(value: str) -> str:
    if value is None:
        return ""
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def sanitize_filename(value: str, max_len: int = 180) -> str:
    value = unquote(value or "")
    value = normalize_text(value)
    value = value.replace("/", "-").replace("\\", "-")
    value = re.sub(r'[<>:"|?*]', "", value)
    value = value.strip(" .")
    if not value:
        value = "document"
    if len(value) > max_len:
        value = value[:max_len].rstrip(" .")
    return value


def get_doc_id_from_url(url: str) -> str:
    try:
        query = parse_qs(urlparse(url).query)
        return (query.get("DocId") or [""])[0].strip()
    except Exception:
        return ""


def ensure_csv_exists() -> None:
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
        logging.info("Created CSV: %s", CSV_PATH)


def build_unique_key(row: Dict[str, str]) -> str:
    # Same PDF can appear under different case rows, so use composite key
    parts = [
        normalize_text(row.get("document_description", "")),
        normalize_text(row.get("file_date", "")),
        normalize_text(row.get("case_number", "")),
        normalize_text(row.get("title", "")),
        normalize_text(row.get("pdf_url", "")),
        normalize_text(row.get("case_info_url", "")),
    ]
    return " || ".join(parts).lower()


def load_existing_keys() -> Set[str]:
    ensure_csv_exists()
    keys: Set[str] = set()

    with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            keys.add(build_unique_key(row))

    logging.info("Existing rows in CSV: %s", len(keys))
    return keys


def append_one_row_to_csv(row: Dict[str, str]) -> None:
    ensure_csv_exists()
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerow({col: row.get(col, "") for col in CSV_COLUMNS})


def get_requests_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
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


def make_driver(headless: int = 0) -> webdriver.Chrome:
    options = Options()
    if int(headless) == 1:
        options.add_argument("--headless=new")

    options.add_argument("--start-maximized")
    options.add_argument("--window-size=1600,1200")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")

    prefs = {
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True,
    }
    options.add_experimental_option("prefs", prefs)

    if USE_WDM:
        service = ChromeService(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)

    return webdriver.Chrome(options=options)


def load_main_page_source(driver: webdriver.Chrome, url: str) -> str:
    logging.info("Opening URL in Chrome: %s", url)
    driver.get(url)

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        WebDriverWait(driver, 30).until(
            EC.presence_of_all_elements_located((By.XPATH, "//table//tr"))
        )
    except TimeoutException:
        logging.warning("Timed out waiting for table, using current page source anyway")

    time.sleep(2)
    return driver.page_source


def parse_main_rows(html: str, base_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        raise RuntimeError("No table found on main page")

    trs = table.find_all("tr")
    if len(trs) < 2:
        raise RuntimeError("Main table found but no data rows found")

    rows: List[Dict[str, str]] = []

    for tr in trs[1:]:
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue

        desc_td = tds[0]
        date_td = tds[1]
        case_td = tds[2]
        title_td = tds[3]

        desc_a = desc_td.find("a", href=True)
        case_a = case_td.find("a", href=True)

        document_description = normalize_text(desc_td.get_text(" ", strip=True))
        file_date = normalize_text(date_td.get_text(" ", strip=True))
        case_number = normalize_text(case_td.get_text(" ", strip=True))
        title = normalize_text(title_td.get_text(" ", strip=True))

        pdf_url = urljoin(base_url, desc_a["href"].strip()) if desc_a else ""
        case_info_url = urljoin(base_url, case_a["href"].strip()) if case_a else ""

        if not any([document_description, file_date, case_number, title, pdf_url, case_info_url]):
            continue

        rows.append(
            {
                "document_description": document_description,
                "file_date": file_date,
                "case_number": case_number,
                "title": title,
                "pdf_url": pdf_url,
                "case_info_url": case_info_url,
                "inner_case_number": "",
                "original_court": "",
                "case_type": "",
                "short_title": "",
                "full_title": "",
                "case_filing_date": "",
                "origination_court_case_number": "",
                "pdf_local_path": "",
            }
        )

    return rows


def open_case_info_in_tab_and_parse(driver: webdriver.Chrome, case_info_url: str) -> Dict[str, str]:
    result = {
        "inner_case_number": "",
        "original_court": "",
        "case_type": "",
        "short_title": "",
        "full_title": "",
        "case_filing_date": "",
        "origination_court_case_number": "",
    }

    if not case_info_url:
        return result

    main_handle = driver.current_window_handle

    try:
        driver.execute_script("window.open(arguments[0], '_blank');", case_info_url)
        time.sleep(1)

        handles = driver.window_handles
        driver.switch_to.window(handles[-1])

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        time.sleep(1)

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Find "Case information" heading
        heading = None
        for tag in soup.find_all(["h1", "h2", "h3", "h4", "b", "strong"]):
            text = normalize_text(tag.get_text(" ", strip=True)).lower()
            if "case information" in text:
                heading = tag
                break

        case_table = None
        if heading:
            nxt = heading.find_next("table")
            if nxt:
                case_table = nxt

        if case_table is None:
            tables = soup.find_all("table")
            if tables:
                case_table = tables[0]

        if case_table is not None:
            trs = case_table.find_all("tr")
            if len(trs) >= 2:
                headers = [normalize_text(th.get_text(" ", strip=True)) for th in trs[0].find_all(["th", "td"])]
                values = [normalize_text(td.get_text(" ", strip=True)) for td in trs[1].find_all(["th", "td"])]

                mapping = {}
                for i, header in enumerate(headers):
                    value = values[i] if i < len(values) else ""
                    mapping[header.lower()] = value

                result["inner_case_number"] = mapping.get("case number", "")
                result["original_court"] = mapping.get("original court", "")
                result["case_type"] = mapping.get("case type", "")
                result["short_title"] = mapping.get("short title", "")
                result["full_title"] = mapping.get("full title", "")
                result["case_filing_date"] = mapping.get("case filling date", "") or mapping.get("case filing date", "")
                result["origination_court_case_number"] = mapping.get("origination court case number", "")

        driver.close()
        driver.switch_to.window(main_handle)

    except Exception as e:
        logging.warning("Failed to parse inner case info page: %s | %s", case_info_url, e)
        try:
            driver.close()
        except Exception:
            pass
        try:
            driver.switch_to.window(main_handle)
        except Exception:
            pass

    return result


def build_pdf_filename(row: Dict[str, str]) -> str:
    file_date = normalize_text(row.get("file_date", "")).split(" ")[0]
    case_number = sanitize_filename(row.get("case_number", ""))
    title = sanitize_filename(row.get("title", ""))
    doc_id = sanitize_filename(get_doc_id_from_url(row.get("pdf_url", "")))

    parts = [p for p in [file_date, case_number, title, doc_id] if p]
    filename = sanitize_filename(" - ".join(parts)) if parts else "document"
    if not filename.lower().endswith(".pdf"):
        filename += ".pdf"
    return filename


def download_pdf(session: requests.Session, row: Dict[str, str]) -> str:
    pdf_url = row.get("pdf_url", "").strip()
    if not pdf_url:
        return ""

    filename = build_pdf_filename(row)
    pdf_path = PDF_DIR / filename

    if pdf_path.exists() and pdf_path.stat().st_size > 0:
        logging.info("PDF already exists, skipping download: %s", pdf_path.name)
        return str(pdf_path.relative_to(ROOT_DIR))

    try:
        with session.get(pdf_url, timeout=90, stream=True, allow_redirects=True) as response:
            response.raise_for_status()
            content_type = (response.headers.get("Content-Type") or "").lower()

            with pdf_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)

        if not pdf_path.exists() or pdf_path.stat().st_size == 0:
            raise RuntimeError("Downloaded file is empty")

        if "html" in content_type and pdf_path.stat().st_size < 4096:
            try:
                text = pdf_path.read_text(encoding="utf-8", errors="ignore")
                if "<html" in text.lower():
                    pdf_path.unlink(missing_ok=True)
                    raise RuntimeError("Server returned HTML instead of PDF")
            except Exception:
                pass

        logging.info("Downloaded PDF: %s", pdf_path.name)
        return str(pdf_path.relative_to(ROOT_DIR))

    except Exception as e:
        logging.error("Failed to download PDF: %s | %s", pdf_url, e)
        if pdf_path.exists() and pdf_path.stat().st_size == 0:
            pdf_path.unlink(missing_ok=True)
        return ""


def scrape_main_rows(session: requests.Session, driver: webdriver.Chrome) -> List[Dict[str, str]]:
    try:
        html = load_main_page_source(driver, BASE_URL)
        rows = parse_main_rows(html, BASE_URL)
        logging.info("Parsed %s rows from Selenium main page", len(rows))
        if rows:
            return rows
    except Exception as e:
        logging.warning("Selenium main page parse failed: %s", e)

    logging.info("Falling back to requests main page")
    response = session.get(BASE_URL, timeout=60)
    response.raise_for_status()
    rows = parse_main_rows(response.text, BASE_URL)
    logging.info("Parsed %s rows from requests main page", len(rows))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Montana Supreme Court Daily Orders scraper")
    parser.add_argument("--headless", type=int, default=1, choices=[0, 1], help="0=visible Chrome, 1=headless")
    args = parser.parse_args()

    setup_logging()
    ensure_dirs()

    logging.info("Run started")
    logging.info("Base URL: %s", BASE_URL)
    logging.info("Headless: %s", args.headless)

    session = get_requests_session()
    existing_keys = load_existing_keys()
    driver = None

    try:
        driver = make_driver(headless=args.headless)

        rows = scrape_main_rows(session, driver)
        if not rows:
            logging.info("No rows found. Exiting.")
            return

        seen_this_run: Set[str] = set()
        saved_count = 0

        for idx, row in enumerate(rows, start=1):
            unique_key = build_unique_key(row)

            if unique_key in existing_keys or unique_key in seen_this_run:
                logging.info(
                    "Skipping duplicate row %s | %s | %s",
                    idx,
                    row.get("case_number", ""),
                    row.get("title", ""),
                )
                continue

            # 1) inner page top case information
            inner_info = open_case_info_in_tab_and_parse(driver, row.get("case_info_url", ""))
            row.update(inner_info)

            # 2) download PDF from document description link
            row["pdf_local_path"] = download_pdf(session, row)

            # 3) save row immediately one by one
            append_one_row_to_csv(row)

            seen_this_run.add(unique_key)
            existing_keys.add(unique_key)
            saved_count += 1

            logging.info(
                "Saved row %s | case_number=%s | title=%s",
                idx,
                row.get("case_number", ""),
                row.get("title", ""),
            )

        logging.info("Run completed successfully | total_found=%s | saved_now=%s", len(rows), saved_count)

    except Exception as e:
        logging.exception("Run failed: %s", e)
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    main()