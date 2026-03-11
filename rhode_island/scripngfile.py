#!/usr/bin/env python3
"""
Rhode Island Judiciary - Opinions, Decisions, and Orders scraper

Run:
    python scripngfile.py
    python scripngfile.py --headless 0
    python scripngfile.py --headless 1

Outputs:
    downloads/CSV/rhode_island_cases.csv
    downloads/PDF/*.pdf
    Log/rhode_island_cases-YYYY-MM-DD.log

Notes:
- Uses Selenium + Chrome because the page is JS-driven.
- Deduplicates by pdf_url primarily.
- If run again, already-scraped rows/PDFs are skipped.
- No database used.
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
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    JavascriptException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


START_URL = "https://www.courts.ri.gov/Pages/ood.aspx"

BASE_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
CSV_DIR = DOWNLOADS_DIR / "CSV"
PDF_DIR = DOWNLOADS_DIR / "PDF"
LOG_DIR = BASE_DIR / "Log"

CSV_PATH = CSV_DIR / "rhode_island_cases.csv"

CSV_COLUMNS = [
    "case_title",
    "case_number",
    "case_date",
    "pdf_url",
    "pdf_local_path",
]


def ensure_directories() -> None:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logger() -> logging.Logger:
    ensure_directories()
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = LOG_DIR / f"rhode_island_cases-{today}.log"

    logger = logging.getLogger("ri_cases_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False

    return logger


def init_csv(csv_path: Path, logger: logging.Logger) -> None:
    if not csv_path.exists():
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
        logger.info("Created CSV: %s", csv_path)


def load_existing_keys(
    csv_path: Path, logger: logging.Logger
) -> Tuple[Set[str], Set[Tuple[str, str, str]]]:
    existing_pdf_urls: Set[str] = set()
    existing_fallback_keys: Set[Tuple[str, str, str]] = set()

    if not csv_path.exists():
        return existing_pdf_urls, existing_fallback_keys

    try:
        with csv_path.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pdf_url = (row.get("pdf_url") or "").strip()
                case_title = (row.get("case_title") or "").strip()
                case_number = (row.get("case_number") or "").strip()
                case_date = (row.get("case_date") or "").strip()

                if pdf_url:
                    existing_pdf_urls.add(pdf_url)

                existing_fallback_keys.add((case_title, case_number, case_date))

        logger.info(
            "Existing rows in CSV (dedupe by pdf_url): %s",
            len(existing_pdf_urls),
        )
    except Exception as exc:
        logger.exception("Failed loading existing CSV keys: %s", exc)

    return existing_pdf_urls, existing_fallback_keys


def create_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET"],
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


def sanitize_filename(name: str, max_length: int = 180) -> str:
    name = (name or "").strip()
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = name.strip(" ._-")
    if not name:
        name = "document"
    if len(name) > max_length:
        name = name[:max_length].rstrip(" ._-")
    return name


def make_pdf_filename(
    case_number: str,
    case_title: str,
    pdf_url: str,
    pdf_dir: Path,
) -> Path:
    base = sanitize_filename(case_number) if case_number.strip() else sanitize_filename(case_title)
    if not base:
        parsed = urlparse(pdf_url)
        base = sanitize_filename(Path(parsed.path).stem) or "document"

    candidate = pdf_dir / f"{base}.pdf"
    if not candidate.exists():
        return candidate

    counter = 2
    while True:
        candidate = pdf_dir / f"{base}_{counter}.pdf"
        if not candidate.exists():
            return candidate
        counter += 1


def find_existing_pdf_path_for_same_base(
    case_number: str,
    case_title: str,
    pdf_dir: Path,
) -> Optional[Path]:
    base = sanitize_filename(case_number) if case_number.strip() else sanitize_filename(case_title)
    if not base:
        return None

    direct = pdf_dir / f"{base}.pdf"
    if direct.exists():
        return direct

    matches = sorted(pdf_dir.glob(f"{base}*.pdf"))
    if matches:
        return matches[0]
    return None


def download_pdf(
    session: requests.Session,
    pdf_url: str,
    target_path: Path,
    logger: logging.Logger,
    timeout: int = 60,
) -> bool:
    try:
        with session.get(pdf_url, stream=True, timeout=timeout) as resp:
            resp.raise_for_status()
            content_type = (resp.headers.get("Content-Type") or "").lower()

            # Some servers return octet-stream; allow that too.
            if "pdf" not in content_type and "octet-stream" not in content_type:
                logger.warning(
                    "Unexpected content type for PDF URL %s: %s",
                    pdf_url,
                    content_type,
                )

            temp_path = target_path.with_suffix(target_path.suffix + ".part")
            with temp_path.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        f.write(chunk)

            temp_path.replace(target_path)
            logger.info("Downloaded PDF: %s", target_path)
            return True

    except Exception as exc:
        logger.exception("Failed downloading PDF %s -> %s", pdf_url, exc)
        try:
            temp_path = target_path.with_suffix(target_path.suffix + ".part")
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return False


def get_chrome_driver(headless: bool) -> webdriver.Chrome:
    options = ChromeOptions()
    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--window-size=1600,2200")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--lang=en-US")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(120)
    return driver


def wait_for_results_page(driver: webdriver.Chrome, timeout: int = 60) -> None:
    wait = WebDriverWait(driver, timeout)
    wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.ms-srch-result"))
    )
    wait.until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.ms-srch-item"))
    )


def safe_text(el: Optional[WebElement]) -> str:
    if el is None:
        return ""
    try:
        return el.text.strip()
    except Exception:
        return ""


def clean_value_after_label(text: str, label: str) -> str:
    text = (text or "").strip()
    text = re.sub(rf"^{re.escape(label)}\s*", "", text, flags=re.I).strip()
    return text


def extract_result_card(card: WebElement) -> Optional[Dict[str, str]]:
    try:
        title_link = card.find_element(By.CSS_SELECTOR, "div.title a")
        case_title = safe_text(title_link)
        pdf_url = (title_link.get_attribute("href") or "").strip()

        number_el = None
        date_el = None

        try:
            number_el = card.find_element(By.CSS_SELECTOR, "div.info div.number")
        except NoSuchElementException:
            pass

        try:
            date_el = card.find_element(By.CSS_SELECTOR, "div.info div.date")
        except NoSuchElementException:
            pass

        case_number = clean_value_after_label(safe_text(number_el), "Number:")
        case_date = clean_value_after_label(safe_text(date_el), "Date:")

        if not case_title or not pdf_url:
            return None

        record = {
            "case_title": case_title,
            "case_number": case_number,
            "case_date": case_date,
            "pdf_url": pdf_url,
            "pdf_local_path": "",
        }
        return record

    except Exception:
        return None


def get_current_page_cards(driver: webdriver.Chrome) -> List[WebElement]:
    cards = driver.find_elements(By.CSS_SELECTOR, "div.ms-srch-item")
    usable_cards: List[WebElement] = []

    for card in cards:
        try:
            if card.find_elements(By.CSS_SELECTOR, "div.title a"):
                usable_cards.append(card)
        except StaleElementReferenceException:
            continue

    return usable_cards


def get_current_page_signature(driver: webdriver.Chrome) -> str:
    """
    Build a lightweight page signature from first few result hrefs/titles
    so we can detect when page changes after clicking pagination.
    """
    parts: List[str] = []
    cards = get_current_page_cards(driver)[:5]
    for card in cards:
        try:
            a = card.find_element(By.CSS_SELECTOR, "div.title a")
            parts.append((a.get_attribute("href") or "").strip())
            parts.append((a.text or "").strip())
        except Exception:
            continue
    return "||".join(parts)


def get_active_page_number(driver: webdriver.Chrome) -> Optional[int]:
    try:
        el = driver.find_element(By.CSS_SELECTOR, "li#PagingSelf a")
        text = (el.text or "").strip()
        if text.isdigit():
            return int(text)
    except Exception:
        pass
    return None


def close_any_overlay(driver: webdriver.Chrome) -> None:
    try:
        body = driver.find_element(By.TAG_NAME, "body")
        body.send_keys(Keys.ESCAPE)
    except Exception:
        pass


def click_next_page(driver: webdriver.Chrome, logger: logging.Logger, timeout: int = 45) -> bool:
    old_sig = get_current_page_signature(driver)
    old_page = get_active_page_number(driver)

    close_any_overlay(driver)

    next_el = None
    selectors = [
        "a#PageLinkNext",
        "li#PagingImageLink a",
    ]

    for selector in selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, selector)
            if el.is_displayed():
                next_el = el
                break
        except NoSuchElementException:
            continue

    if next_el is None:
        logger.info("No next-page button found. Pagination finished.")
        return False

    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_el)
        time.sleep(0.8)
    except JavascriptException:
        pass

    clicked = False
    click_errors = []

    for mode in ("normal", "js"):
        try:
            if mode == "normal":
                next_el.click()
            else:
                driver.execute_script("arguments[0].click();", next_el)
            clicked = True
            break
        except (ElementClickInterceptedException, WebDriverException, JavascriptException) as exc:
            click_errors.append(f"{mode}: {exc}")
            time.sleep(1.0)

    if not clicked:
        logger.error("Failed clicking next page: %s", " | ".join(click_errors))
        return False

    wait = WebDriverWait(driver, timeout)

    def page_changed(drv: webdriver.Chrome) -> bool:
        try:
            new_sig = get_current_page_signature(drv)
            new_page = get_active_page_number(drv)

            if old_page is not None and new_page is not None and new_page != old_page:
                return True
            return bool(new_sig and new_sig != old_sig)
        except Exception:
            return False

    try:
        wait.until(page_changed)
        wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.ms-srch-item"))
        )
        time.sleep(1.0)
        return True
    except TimeoutException:
        logger.warning("Timed out waiting for next page to load.")
        return False


def append_row(csv_path: Path, row: Dict[str, str]) -> None:
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerow({col: row.get(col, "") for col in CSV_COLUMNS})


def process_record(
    record: Dict[str, str],
    session: requests.Session,
    existing_pdf_urls: Set[str],
    existing_fallback_keys: Set[Tuple[str, str, str]],
    logger: logging.Logger,
) -> bool:
    case_title = record["case_title"].strip()
    case_number = record["case_number"].strip()
    case_date = record["case_date"].strip()
    pdf_url = record["pdf_url"].strip()

    fallback_key = (case_title, case_number, case_date)

    if pdf_url and pdf_url in existing_pdf_urls:
        logger.info("Skipped duplicate by pdf_url: %s", pdf_url)
        return False

    if fallback_key in existing_fallback_keys:
        logger.info(
            "Skipped duplicate by fallback key: %s | %s | %s",
            case_title,
            case_number,
            case_date,
        )
        return False

    existing_pdf_path = find_existing_pdf_path_for_same_base(case_number, case_title, PDF_DIR)

    if existing_pdf_path and existing_pdf_path.exists():
        record["pdf_local_path"] = str(existing_pdf_path.resolve())
        logger.info("PDF already exists, skipping download: %s", existing_pdf_path)
    else:
        target_pdf_path = make_pdf_filename(case_number, case_title, pdf_url, PDF_DIR)
        ok = download_pdf(session, pdf_url, target_pdf_path, logger)
        if not ok:
            logger.warning("Skipping CSV write because PDF download failed: %s", pdf_url)
            return False
        record["pdf_local_path"] = str(target_pdf_path.resolve())

    append_row(CSV_PATH, record)

    if pdf_url:
        existing_pdf_urls.add(pdf_url)
    existing_fallback_keys.add(fallback_key)

    logger.info(
        "Saved record: title=%s | number=%s | date=%s",
        case_title,
        case_number,
        case_date,
    )
    return True


def scrape_all(headless: bool) -> None:
    ensure_directories()
    logger = setup_logger()
    init_csv(CSV_PATH, logger)
    existing_pdf_urls, existing_fallback_keys = load_existing_keys(CSV_PATH, logger)

    logger.info("Run started")
    logger.info("Start URL: %s", START_URL)
    logger.info("Headless: %s", headless)
    logger.info("CSV path: %s", CSV_PATH)
    logger.info("PDF folder path: %s", PDF_DIR)

    session = create_session()
    driver = None

    total_seen = 0
    total_saved = 0
    page_index = 1
    visited_signatures: Set[str] = set()

    try:
        driver = get_chrome_driver(headless=headless)
        driver.get(START_URL)

        wait_for_results_page(driver, timeout=60)
        time.sleep(2)

        while True:
            try:
                wait_for_results_page(driver, timeout=45)
            except TimeoutException:
                logger.warning("Results did not load on page %s. Stopping.", page_index)
                break

            page_sig = get_current_page_signature(driver)
            if not page_sig:
                logger.warning("Could not build page signature on page %s. Stopping.", page_index)
                break

            if page_sig in visited_signatures:
                logger.warning("Detected repeated page signature on page %s. Stopping to avoid loop.", page_index)
                break
            visited_signatures.add(page_sig)

            active_page = get_active_page_number(driver) or page_index
            logger.info("========== START PAGE %s ==========", active_page)

            cards = get_current_page_cards(driver)
            logger.info("Results found on page %s: %s", active_page, len(cards))

            for idx in range(len(cards)):
                try:
                    fresh_cards = get_current_page_cards(driver)
                    if idx >= len(fresh_cards):
                        break

                    card = fresh_cards[idx]
                    record = extract_result_card(card)
                    if not record:
                        logger.warning("Skipped malformed card at page %s item %s", active_page, idx + 1)
                        continue

                    total_seen += 1
                    saved = process_record(
                        record=record,
                        session=session,
                        existing_pdf_urls=existing_pdf_urls,
                        existing_fallback_keys=existing_fallback_keys,
                        logger=logger,
                    )
                    if saved:
                        total_saved += 1

                except StaleElementReferenceException:
                    logger.warning("Stale card on page %s item %s; skipped.", active_page, idx + 1)
                    continue
                except Exception as exc:
                    logger.exception(
                        "Failed processing page %s item %s: %s",
                        active_page,
                        idx + 1,
                        exc,
                    )
                    continue

            moved = click_next_page(driver, logger=logger, timeout=45)
            if not moved:
                break

            page_index += 1
            time.sleep(1.5)

    except Exception as exc:
        logger.exception("Fatal scraper error: %s", exc)
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

        logger.info("Run finished")
        logger.info("Total records seen: %s", total_seen)
        logger.info("Total new records saved: %s", total_saved)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Rhode Island Judiciary opinions, decisions, and orders."
    )
    parser.add_argument(
        "--headless",
        type=int,
        choices=[0, 1],
        default=1,
        help="Run Chrome in headless mode (0=visible, 1=headless). Default: 1",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    scrape_all(headless=bool(args.headless))