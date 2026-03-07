#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Maine Supreme Judicial Court - 2017 Published Opinions scraper

Run:
    python scripngfile.py
    python scripngfile.py --headless 0

Outputs:
    downloads/CSV/maine_published_opinions_2017.csv
    downloads/PDF/*.pdf
    Log/maine_published_opinions_YYYY-MM-DD.log

Notes:
- Uses Selenium + Chrome to load the page.
- Uses requests for PDF downloads.
- No database.
- Deduplicates against existing CSV and existing PDFs.
- Appends each successful/new record immediately to CSV.
- Only stores case rows, not citation/help/footer content.
"""

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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


BASE_URL = "https://www.courts.maine.gov/courts/sjc/lawcourt/2017/index.html"
CSV_PATH = Path("downloads") / "CSV" / "maine_published_opinions_2017.csv"
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
    parser = argparse.ArgumentParser(description="Scrape Maine 2017 published opinions")
    parser.add_argument(
        "--headless",
        default=1,
        type=int,
        choices=[0, 1],
        help="1=headless (default), 0=show browser",
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
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    try:
        driver = webdriver.Chrome(options=options)
    except Exception:
        # fallback if system requires explicit Service()
        driver = webdriver.Chrome(service=Service(), options=options)

    driver.set_page_load_timeout(60)
    return driver


def ensure_csv() -> None:
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()


def clean_text(value: str) -> str:
    value = value.replace("\xa0", " ")
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
        "Loaded existing CSV rows: %s | unique pdf_url: %s | unique keys: %s",
        len(seen_keys),
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
        with session.get(pdf_url, stream=True, timeout=(20, 120)) as resp:
            resp.raise_for_status()
            content_type = (resp.headers.get("Content-Type") or "").lower()
            if "pdf" not in content_type and not pdf_url.lower().endswith(".pdf"):
                logger.warning(
                    "Response may not be a PDF for %s | content-type=%s",
                    pdf_url,
                    content_type,
                )

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


def extract_main_case_link(td_case) -> Tuple[str, str]:
    """
    Return (case_name, pdf_url_relative) from the FIRST anchor in case cell.
    This avoids errata/order links being treated as case rows.
    """
    anchors = td_case.find_elements(By.TAG_NAME, "a")
    if not anchors:
        return "", ""

    first_anchor = anchors[0]
    case_name = clean_text(first_anchor.text or "")
    href = clean_text(first_anchor.get_attribute("href") or "")

    return case_name, href


def scrape_rows(driver: webdriver.Chrome, logger: logging.Logger) -> List[Dict[str, str]]:
    logger.info("Opening URL: %s", BASE_URL)
    driver.get(BASE_URL)

    wait = WebDriverWait(driver, 30)
    wait.until(EC.presence_of_element_located((By.ID, "maincontent2")))
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#maincontent2 table.tbstriped")))

    main = driver.find_element(By.ID, "maincontent2")
    tables = main.find_elements(By.CSS_SELECTOR, "table.tbstriped")

    logger.info("Found opinion tables: %s", len(tables))

    all_rows: List[Dict[str, str]] = []

    for table_idx, table in enumerate(tables, start=1):
        rows = table.find_elements(By.CSS_SELECTOR, "tr")
        logger.info("Table %s row count including header: %s", table_idx, len(rows))

        for tr in rows[1:]:  # skip header
            tds = tr.find_elements(By.TAG_NAME, "td")
            if len(tds) < 3:
                continue

            opinion_number = clean_text(tds[0].text)
            date_filed = clean_text(tds[2].text)
            case_name, pdf_href = extract_main_case_link(tds[1])

            if not opinion_number or not case_name or not pdf_href:
                continue

            pdf_url = urljoin(BASE_URL, pdf_href)

            all_rows.append(
                {
                    "opinion_number": opinion_number,
                    "case_name": case_name,
                    "date_filed": date_filed,
                    "pdf_url": pdf_url,
                }
            )

    logger.info("Total extracted case rows: %s", len(all_rows))
    return all_rows


def make_pdf_path(opinion_number: str, case_name: str, pdf_url: str) -> Path:
    ext = Path(pdf_url).suffix or ".pdf"
    filename = safe_filename(f"{opinion_number} - {case_name}") + ext
    return PDF_DIR / filename


def main() -> None:
    args = parse_args()
    setup_dirs()
    logger = setup_logging()
    ensure_csv()

    logger.info("Run started")
    logger.info("Headless mode: %s", bool(args.headless))

    seen_pdf_urls, seen_keys = load_existing_rows(logger)
    session = create_session()
    driver = None

    try:
        driver = build_driver(headless=bool(args.headless))
        scraped_rows = scrape_rows(driver, logger)

        new_count = 0
        skip_count = 0
        fail_count = 0

        for idx, item in enumerate(scraped_rows, start=1):
            opinion_number = clean_text(item["opinion_number"])
            case_name = clean_text(item["case_name"])
            date_filed = clean_text(item["date_filed"])
            pdf_url = clean_text(item["pdf_url"])

            key = (opinion_number, case_name)

            if pdf_url in seen_pdf_urls or key in seen_keys:
                logger.info(
                    "[%s/%s] Already in CSV, skipping: %s | %s",
                    idx, len(scraped_rows), opinion_number, case_name
                )
                skip_count += 1
                continue

            pdf_path = make_pdf_path(opinion_number, case_name, pdf_url)

            logger.info(
                "[%s/%s] Processing: %s | %s",
                idx, len(scraped_rows), opinion_number, case_name
            )

            ok = download_pdf(session, pdf_url, pdf_path, logger)
            if not ok:
                fail_count += 1
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
            new_count += 1

            logger.info("Saved CSV row: %s | %s", opinion_number, case_name)
            time.sleep(0.2)

        logger.info(
            "Run complete | new=%s | skipped=%s | failed=%s | total_seen_now=%s",
            new_count,
            skip_count,
            fail_count,
            len(seen_keys),
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