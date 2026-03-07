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
from typing import Dict, List, Set, Tuple
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By


YEARLY_URL_TEMPLATE = "https://www.courts.maine.gov/courts/sjc/lawcourt/{year}/index.html"
CURRENT_YEAR_URL = "https://www.courts.maine.gov/courts/sjc/opinions.html"

CSV_PATH = Path("downloads") / "CSV" / "maine_published_opinions.csv"
PDF_DIR = Path("downloads") / "PDF"
LOG_DIR = Path("Log")

CSV_COLUMNS = [
    "opinion_number",
    "case_name",
    "date_filed",
    "pdf_url",
    "pdf_local_path",
]


def setup_dirs() -> None:
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logging() -> logging.Logger:
    log_date = datetime.now().strftime("%Y-%m-%d")
    log_path = LOG_DIR / f"maine_published_opinions_{log_date}.log"

    logger = logging.getLogger("maine_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Maine published opinions")
    parser.add_argument(
        "--headless",
        default=1,
        type=int,
        choices=[0, 1],
        help="1=headless (default), 0=show Chrome browser",
    )
    return parser.parse_args()


def create_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
    )
    adapter = HTTPAdapter(max_retries=retry)
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


def build_driver(headless: bool) -> webdriver.Chrome:
    options = Options()

    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--window-size=1600,1200")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    try:
        driver = webdriver.Chrome(options=options)
    except Exception:
        driver = webdriver.Chrome(service=Service(), options=options)

    driver.set_page_load_timeout(90)
    return driver


def ensure_csv() -> None:
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()


def clean_text(value: str) -> str:
    if not value:
        return ""
    value = value.replace("\xa0", " ").replace("\u200b", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def safe_filename(value: str) -> str:
    value = clean_text(value)
    value = value.replace("/", "-")
    value = re.sub(r'[<>:"\\|?*]', "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value[:180]


def load_existing_rows(logger: logging.Logger) -> Tuple[Set[str], Set[Tuple[str, str]]]:
    seen_pdf_urls: Set[str] = set()
    seen_keys: Set[Tuple[str, str]] = set()

    if not CSV_PATH.exists():
        logger.info("CSV does not exist yet. Starting fresh.")
        return seen_pdf_urls, seen_keys

    with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pdf_url = clean_text(row.get("pdf_url", ""))
            opinion_number = clean_text(row.get("opinion_number", ""))
            case_name = clean_text(row.get("case_name", ""))

            if pdf_url:
                seen_pdf_urls.add(pdf_url)
            if opinion_number or case_name:
                seen_keys.add((opinion_number, case_name))

    logger.info(
        "Loaded existing CSV rows: unique pdf_url=%s | unique keys=%s",
        len(seen_pdf_urls),
        len(seen_keys),
    )
    return seen_pdf_urls, seen_keys


def append_row(row: Dict[str, str]) -> None:
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerow(row)


def download_pdf(
    session: requests.Session,
    pdf_url: str,
    out_path: Path,
    logger: logging.Logger,
) -> bool:
    if out_path.exists() and out_path.stat().st_size > 0:
        logger.info("PDF already exists, skipping download: %s", out_path.name)
        return True

    try:
        with session.get(pdf_url, stream=True, timeout=(20, 180)) as resp:
            resp.raise_for_status()

            tmp_path = out_path.with_suffix(out_path.suffix + ".part")
            with tmp_path.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        f.write(chunk)

            if tmp_path.stat().st_size == 0:
                tmp_path.unlink(missing_ok=True)
                raise ValueError("Downloaded PDF is empty")

            tmp_path.replace(out_path)
            logger.info("Downloaded PDF: %s", out_path.name)
            return True

    except Exception as e:
        logger.error("Failed PDF download: %s | error=%s", pdf_url, e)
        try:
            out_path.with_suffix(out_path.suffix + ".part").unlink(missing_ok=True)
        except Exception:
            pass
        return False


def get_target_urls() -> List[str]:
    current_year = datetime.now().year
    urls = []

    for year in range(2017, current_year):
        urls.append(YEARLY_URL_TEMPLATE.format(year=year))

    urls.append(CURRENT_YEAR_URL)
    return urls


def make_pdf_path(opinion_number: str, case_name: str, pdf_url: str) -> Path:
    ext = Path(pdf_url).suffix or ".pdf"
    filename = safe_filename(f"{opinion_number} - {case_name}") + ext
    return PDF_DIR / filename


def scrape_rows_from_page(driver: webdriver.Chrome, page_url: str, logger: logging.Logger) -> List[Dict[str, str]]:
    logger.info("Opening URL: %s", page_url)
    driver.get(page_url)
    time.sleep(3)

    html = driver.page_source
    if not html:
        raise RuntimeError("Empty page source")

    if "Published Opinions" not in html and "Opinion #" not in html:
        logger.warning("Page source does not look like opinion page")

    rows_out: List[Dict[str, str]] = []

    # old/new visible tables
    tables = driver.find_elements(By.CSS_SELECTOR, "table")
    logger.info("Total tables found on page: %s", len(tables))

    for table_idx, table in enumerate(tables, start=1):
        rows = table.find_elements(By.TAG_NAME, "tr")
        if len(rows) <= 1:
            continue

        for tr in rows[1:]:
            tds = tr.find_elements(By.TAG_NAME, "td")
            if len(tds) < 3:
                continue

            opinion_number = clean_text(tds[0].text)
            date_filed = clean_text(tds[2].text)

            if not opinion_number or "ME" not in opinion_number:
                continue

            anchors = tds[1].find_elements(By.TAG_NAME, "a")
            if not anchors:
                continue

            first_anchor = anchors[0]
            case_name = clean_text(first_anchor.text)
            pdf_href = clean_text(first_anchor.get_attribute("href") or "")

            if not case_name or not pdf_href:
                continue

            if not pdf_href.lower().endswith(".pdf"):
                continue

            rows_out.append(
                {
                    "opinion_number": opinion_number,
                    "case_name": case_name,
                    "date_filed": date_filed,
                    "pdf_url": urljoin(page_url, pdf_href),
                }
            )

    # fallback for current page if table parsing fails
    if not rows_out:
        logger.info("No rows from table parsing, trying generic block parsing")

        anchors = driver.find_elements(By.CSS_SELECTOR, "#maincontent2 a[href$='.pdf']")
        logger.info("PDF anchors found: %s", len(anchors))

        for a in anchors:
            case_name = clean_text(a.text)
            pdf_href = clean_text(a.get_attribute("href") or "")
            if not case_name or not pdf_href:
                continue

            try:
                row_el = a.find_element(By.XPATH, "./ancestor::tr[1]")
                tds = row_el.find_elements(By.TAG_NAME, "td")
                if len(tds) >= 3:
                    opinion_number = clean_text(tds[0].text)
                    date_filed = clean_text(tds[2].text)

                    if opinion_number and date_filed:
                        rows_out.append(
                            {
                                "opinion_number": opinion_number,
                                "case_name": case_name,
                                "date_filed": date_filed,
                                "pdf_url": urljoin(page_url, pdf_href),
                            }
                        )
            except Exception:
                continue

    # final dedupe
    cleaned: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str, str]] = set()

    for row in rows_out:
        opinion_number = clean_text(row.get("opinion_number", ""))
        case_name = clean_text(row.get("case_name", ""))
        date_filed = clean_text(row.get("date_filed", ""))
        pdf_url = clean_text(row.get("pdf_url", ""))

        if not all([opinion_number, case_name, date_filed, pdf_url]):
            continue

        key = (opinion_number, case_name, pdf_url)
        if key in seen:
            continue
        seen.add(key)

        cleaned.append(
            {
                "opinion_number": opinion_number,
                "case_name": case_name,
                "date_filed": date_filed,
                "pdf_url": pdf_url,
            }
        )

    logger.info("Final extracted rows from page: %s", len(cleaned))
    return cleaned


def main() -> None:
    args = parse_args()
    setup_dirs()
    logger = setup_logging()
    ensure_csv()

    logger.info("Run started")
    logger.info("Headless mode: %s", bool(args.headless))

    target_urls = get_target_urls()

    # debug example: only 2022 to current page
    # target_urls = [u for u in target_urls if any(x in u for x in ["2022", "2023", "2024", "2025", "opinions.html"])]

    logger.info("Target pages count: %s", len(target_urls))
    for u in target_urls:
        logger.info("Target page: %s", u)

    seen_pdf_urls, seen_keys = load_existing_rows(logger)
    session = create_session()
    driver = None

    try:
        driver = build_driver(headless=bool(args.headless))

        total_found = 0
        total_new = 0
        total_skip = 0
        total_fail = 0

        for page_no, page_url in enumerate(target_urls, start=1):
            logger.info("Processing page %s/%s", page_no, len(target_urls))

            try:
                scraped_rows = scrape_rows_from_page(driver, page_url, logger)
            except Exception as e:
                logger.error("Failed to scrape page: %s | error=%s", page_url, e)
                continue

            total_found += len(scraped_rows)

            for idx, item in enumerate(scraped_rows, start=1):
                opinion_number = clean_text(item["opinion_number"])
                case_name = clean_text(item["case_name"])
                date_filed = clean_text(item["date_filed"])
                pdf_url = clean_text(item["pdf_url"])

                key = (opinion_number, case_name)

                if pdf_url in seen_pdf_urls or key in seen_keys:
                    logger.info(
                        "[page %s row %s] Already in CSV, skipping: %s | %s",
                        page_no, idx, opinion_number, case_name
                    )
                    total_skip += 1
                    continue

                pdf_path = make_pdf_path(opinion_number, case_name, pdf_url)

                logger.info(
                    "[page %s row %s] Processing: %s | %s",
                    page_no, idx, opinion_number, case_name
                )

                ok = download_pdf(session, pdf_url, pdf_path, logger)
                if not ok:
                    total_fail += 1
                    continue

                row = {
                    "opinion_number": opinion_number,
                    "case_name": case_name,
                    "date_filed": date_filed,
                    "pdf_url": pdf_url,
                    "pdf_local_path": str(pdf_path.as_posix()),
                }

                append_row(row)
                seen_pdf_urls.add(pdf_url)
                seen_keys.add(key)
                total_new += 1

                logger.info("Saved CSV row: %s | %s", opinion_number, case_name)
                time.sleep(0.15)

        logger.info(
            "Run complete | found=%s | new=%s | skipped=%s | failed=%s",
            total_found,
            total_new,
            total_skip,
            total_fail,
        )

    except TimeoutException as e:
        logger.exception("Timeout error: %s", e)
        sys.exit(1)
    except WebDriverException as e:
        logger.exception("WebDriver error: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        sys.exit(1)
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    main()