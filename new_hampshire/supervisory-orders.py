#!/usr/bin/env python3
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
from urllib.parse import urljoin

from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


BASE_URL = "https://www.courts.nh.gov"
START_YEAR = 2011
CURRENT_YEAR = datetime.now().year

CSV_COLUMNS = [
    "case_number",
    "case_title",
    "case_date",
    "pdf_url",
    "pdf_local_path",
]

ROOT_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = ROOT_DIR / "downloads/supervisory_orders"
CSV_DIR = DOWNLOADS_DIR / "CSV"
PDF_DIR = DOWNLOADS_DIR / "PDF"
LOG_DIR = ROOT_DIR / "Log"
CSV_PATH = CSV_DIR / "nh_supervisory_orders.csv"

DESKTOP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)


def build_year_url(year: int) -> str:
    return f"{BASE_URL}/our-courts/supreme-court/orders-and-opinions/supervisory-orders/{year}"


def ensure_dirs() -> None:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logger() -> logging.Logger:
    ensure_dirs()

    logger = logging.getLogger("nh_opinions_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    log_name = f"nh_supervisory_orders_opinions-{datetime.now().strftime('%Y-%m-%d')}.log"
    log_path = LOG_DIR / log_name

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def init_csv(logger: logging.Logger) -> None:
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
        logger.info("Created CSV: %s", CSV_PATH)


def load_existing_pdf_urls(logger: logging.Logger) -> Set[str]:
    existing: Set[str] = set()

    if not CSV_PATH.exists():
        return existing

    with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pdf_url = (row.get("pdf_url") or "").strip()
            if pdf_url:
                existing.add(pdf_url)

    logger.info("Existing rows in CSV (dedupe by pdf_url): %s", len(existing))
    return existing


def append_row(row: Dict[str, str], logger: logging.Logger) -> None:
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerow(row)
    logger.info("Saved row to CSV: %s | %s", row["case_number"], row["case_title"])


def make_safe_filename(text: str, max_len: int = 180) -> str:
    text = re.sub(r'[\\/:*?"<>|]+', "_", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace(",", "")
    text = text.replace("&", "and")
    if not text:
        text = "document"
    return text[:max_len].strip(" ._")


def split_case_text(full_text: str) -> Tuple[str, str]:
    full_text = re.sub(r"\s+", " ", full_text).strip()
    if "," in full_text:
        first, rest = full_text.split(",", 1)
        return first.strip(), rest.strip()
    return "", full_text


def parse_case_date(text: str) -> str:
    text = text.strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text


def cleanup_temp_downloads(logger: logging.Logger) -> None:
    for p in PDF_DIR.glob("*.crdownload"):
        try:
            p.unlink(missing_ok=True)
            logger.info("Removed stale temp download: %s", p.name)
        except Exception:
            pass


def build_chrome_options(headless: bool) -> Options:
    options = Options()

    if headless:
        # More compatible than --headless=new for some JS-heavy sites
        options.add_argument("--headless")

    options.add_argument("--window-size=1600,2200")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--lang=en-US")
    options.add_argument(f"--user-agent={DESKTOP_USER_AGENT}")

    prefs = {
        "download.default_directory": str(PDF_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "download_restrictions": 0,
        "plugins.always_open_pdf_externally": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
    }
    options.add_experimental_option("prefs", prefs)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    return options


def get_driver(headless: bool) -> webdriver.Chrome:
    options = build_chrome_options(headless=headless)
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(90)

    try:
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {
                "behavior": "allow",
                "downloadPath": str(PDF_DIR),
            },
        )
    except Exception:
        pass

    try:
        driver.execute_cdp_cmd(
            "Network.setUserAgentOverride",
            {"userAgent": DESKTOP_USER_AGENT, "platform": "MacIntel"},
        )
    except Exception:
        pass

    # Hide webdriver flag a bit more
    try:
        driver.execute_script(
            """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            """
        )
    except Exception:
        pass

    return driver


def recreate_driver(old_driver: Optional[webdriver.Chrome], headless: bool, logger: logging.Logger) -> webdriver.Chrome:
    if old_driver:
        try:
            old_driver.quit()
        except Exception:
            pass

    logger.info("Recreating Chrome driver | headless=%s", headless)
    return get_driver(headless=headless)


def wait_for_document_ready(driver: webdriver.Chrome, timeout: int = 30) -> bool:
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        return True
    except TimeoutException:
        return False


def wait_for_rows(driver: webdriver.Chrome, timeout: int = 30) -> bool:
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".tabulator-table .tabulator-row"))
        )
        return True
    except TimeoutException:
        return False


def page_has_rows(driver: webdriver.Chrome) -> bool:
    try:
        return len(driver.find_elements(By.CSS_SELECTOR, ".tabulator-table .tabulator-row")) > 0
    except Exception:
        return False


def page_has_document_list(driver: webdriver.Chrome) -> bool:
    try:
        return len(driver.find_elements(By.CSS_SELECTOR, ".document__list")) > 0
    except Exception:
        return False


def page_has_no_results(driver: webdriver.Chrome) -> bool:
    try:
        rows = driver.find_elements(By.CSS_SELECTOR, ".tabulator-table .tabulator-row")
        return len(rows) == 0
    except Exception:
        return True


def load_year_page(driver: webdriver.Chrome, year: int, logger: logging.Logger) -> bool:
    url = build_year_url(year)
    logger.info("Opening year page: %s", url)

    try:
        driver.get(url)
    except Exception as e:
        logger.error("Year %s | Failed to open page: %s", year, e)
        return False

    wait_for_document_ready(driver, timeout=30)
    time.sleep(2)

    # Scroll to trigger lazy JS render
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.35);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)
    except Exception:
        pass

    if wait_for_rows(driver, timeout=12):
        return True

    # Retry once with refresh
    logger.warning("Year %s | First load had no rows, refreshing page once.", year)
    try:
        driver.refresh()
    except Exception:
        pass

    wait_for_document_ready(driver, timeout=30)
    time.sleep(3)

    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.35);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)
    except Exception:
        pass

    if wait_for_rows(driver, timeout=12):
        return True

    logger.warning(
        "Year %s | Still no rows after refresh | document_list=%s | rows=%s | current_url=%s",
        year,
        page_has_document_list(driver),
        page_has_rows(driver),
        driver.current_url,
    )
    return False


def get_current_page_number(driver: webdriver.Chrome) -> Optional[str]:
    for _ in range(5):
        try:
            active = driver.find_element(By.CSS_SELECTOR, ".tabulator-pages .tabulator-page.active")
            page_no = active.get_attribute("data-page")
            if page_no:
                return str(page_no).strip()

            text = active.text.strip()
            if text:
                return text

            return None
        except (NoSuchElementException, StaleElementReferenceException):
            time.sleep(0.5)
            continue
        except Exception:
            time.sleep(0.5)
            continue
    return None


def extract_date_from_row(row: WebElement) -> str:
    infos = row.find_elements(By.CSS_SELECTOR, ".document__detail__information")
    for info in infos:
        try:
            txt = info.text.strip()
        except StaleElementReferenceException:
            continue
        if txt.lower().startswith("date:"):
            raw = txt.split(":", 1)[1].strip()
            return parse_case_date(raw)
    return ""


def extract_row_data(row: WebElement) -> Optional[Dict[str, str]]:
    try:
        link = row.find_element(By.CSS_SELECTOR, ".document__detail__title a")
        title_text = re.sub(r"\s+", " ", link.text).strip()
        pdf_url = urljoin(BASE_URL, link.get_attribute("href") or "")
        case_date = extract_date_from_row(row)

        case_number, case_title = split_case_text(title_text)

        return {
            "case_number": case_number,
            "case_title": case_title,
            "case_date": case_date,
            "pdf_url": pdf_url,
            "pdf_local_path": "",
        }
    except (NoSuchElementException, StaleElementReferenceException):
        return None


def scrape_current_page(driver: webdriver.Chrome, logger: logging.Logger, year: int) -> List[Dict[str, str]]:
    rows = driver.find_elements(By.CSS_SELECTOR, ".tabulator-table .tabulator-row")
    logger.info("Year %s | Rows found on current page: %s", year, len(rows))

    page_items: List[Dict[str, str]] = []
    for row in rows:
        try:
            data = extract_row_data(row)
            if data and data["pdf_url"]:
                page_items.append(data)
        except StaleElementReferenceException:
            continue

    logger.info("Year %s | Parsed valid case rows on current page: %s", year, len(page_items))
    return page_items


def click_next_page(driver: webdriver.Chrome, logger: logging.Logger, year: int) -> bool:
    retries = 3

    for attempt in range(1, retries + 1):
        try:
            current_page = get_current_page_number(driver)

            next_btn = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, ".document__list__pagination .tabulator-page[data-page='next']")
                )
            )

            disabled = (next_btn.get_attribute("disabled") or "").strip()
            classes = (next_btn.get_attribute("class") or "").lower()

            if disabled or "disabled" in classes:
                logger.info("Year %s | Next button disabled. Pagination finished.", year)
                return False

            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
            time.sleep(0.5)

            try:
                driver.execute_script("arguments[0].click();", next_btn)
            except Exception:
                next_btn.click()

            def page_changed(d: webdriver.Chrome) -> bool:
                try:
                    new_page = get_current_page_number(d)
                    return new_page is not None and new_page != current_page
                except Exception:
                    return False

            WebDriverWait(driver, 20).until(page_changed)
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".tabulator-table .tabulator-row"))
            )
            time.sleep(1)

            logger.info(
                "Year %s | Moved pagination from page %s to %s",
                year,
                current_page,
                get_current_page_number(driver),
            )
            return True

        except StaleElementReferenceException:
            logger.warning(
                "Year %s | Stale element during pagination click, retry %s/%s",
                year,
                attempt,
                retries,
            )
            time.sleep(1)
            continue

        except TimeoutException:
            logger.warning(
                "Year %s | Pagination timeout on retry %s/%s",
                year,
                attempt,
                retries,
            )
            time.sleep(1)
            continue

        except Exception as e:
            logger.warning(
                "Year %s | Pagination click failed on retry %s/%s | %s",
                year,
                attempt,
                retries,
                e,
            )
            time.sleep(1)
            continue

    logger.error("Year %s | Failed to move to next page after retries.", year)
    return False


def scrape_all_pages_for_year(driver: webdriver.Chrome, year: int, logger: logging.Logger) -> List[Dict[str, str]]:
    if not load_year_page(driver, year, logger):
        logger.warning("Year %s | No rows found.", year)
        return []

    all_items: List[Dict[str, str]] = []
    seen_pdf_urls: Set[str] = set()
    seen_pages: Set[str] = set()

    time.sleep(1)

    while True:
        try:
            page_no = get_current_page_number(driver) or "1"

            if page_no in seen_pages:
                logger.info("Year %s | Already visited page %s. Stopping loop.", year, page_no)
                break

            seen_pages.add(page_no)
            logger.info("Year %s | Scraping page: %s", year, page_no)

            page_items = scrape_current_page(driver, logger, year)

            added_this_page = 0
            for item in page_items:
                pdf_url = item["pdf_url"]
                if pdf_url in seen_pdf_urls:
                    continue
                seen_pdf_urls.add(pdf_url)
                all_items.append(item)
                added_this_page += 1

            logger.info("Year %s | Added unique rows from page %s: %s", year, page_no, added_this_page)

            if not click_next_page(driver, logger, year):
                break

        except StaleElementReferenceException:
            logger.warning("Year %s | Stale element in page loop, retrying current page.", year)
            time.sleep(1)
            continue

        except Exception as e:
            logger.error("Year %s | Error during pagination loop: %s", year, e)
            break

    logger.info("Year %s | Total unique rows scraped: %s", year, len(all_items))
    return all_items


def snapshot_files() -> Set[str]:
    return {p.name for p in PDF_DIR.iterdir() if p.is_file()}


def newest_completed_pdf(before_names: Set[str]) -> Optional[Path]:
    current_files = [p for p in PDF_DIR.iterdir() if p.is_file()]
    candidates = [
        p for p in current_files
        if p.name not in before_names and not p.name.endswith(".crdownload")
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def wait_for_download(before_files: Set[str], timeout: int = 120, stable_checks: int = 3) -> Optional[Path]:
    start = time.time()
    last_seen_name = None
    stable_count = 0

    while time.time() - start < timeout:
        crdownloads = list(PDF_DIR.glob("*.crdownload"))
        candidate = newest_completed_pdf(before_files)

        if crdownloads:
            stable_count = 0
            time.sleep(1)
            continue

        if candidate is not None:
            if candidate.name == last_seen_name:
                stable_count += 1
            else:
                last_seen_name = candidate.name
                stable_count = 1

            if stable_count >= stable_checks:
                return candidate

        time.sleep(1)

    return None


def download_pdf_via_chrome(
    driver: webdriver.Chrome,
    pdf_url: str,
    case_number: str,
    case_title: str,
    logger: logging.Logger,
) -> str:
    base_name = " ".join(x for x in [case_number.strip(), case_title.strip()] if x).strip()
    if not base_name:
        base_name = Path(pdf_url).stem or "document"

    final_name = make_safe_filename(base_name) + ".pdf"
    final_path = PDF_DIR / final_name

    if final_path.exists() and final_path.stat().st_size > 0:
        logger.info("PDF already exists, skipping download: %s", final_path.name)
        return str(final_path)

    before_files = snapshot_files()

    try:
        driver.get(pdf_url)
    except Exception as e:
        logger.error("Could not open PDF URL in Chrome: %s | %s", pdf_url, e)
        return ""

    downloaded_file = wait_for_download(before_files=before_files, timeout=120)

    if downloaded_file is None:
        logger.error("Download timeout or no file downloaded: %s", pdf_url)
        return ""

    try:
        if downloaded_file.resolve() == final_path.resolve():
            logger.info("Downloaded PDF: %s", final_path.name)
            return str(final_path)

        if final_path.exists():
            suffix = datetime.now().strftime("%Y%m%d%H%M%S")
            final_path = PDF_DIR / f"{final_path.stem}_{suffix}.pdf"

        downloaded_file.rename(final_path)
        logger.info("Downloaded PDF: %s", final_path.name)
        return str(final_path)

    except Exception as e:
        logger.error(
            "Downloaded file found but rename failed: %s -> %s | %s",
            downloaded_file,
            final_path,
            e,
        )
        return str(downloaded_file)


def main() -> None:
    parser = argparse.ArgumentParser(description="NH Supreme Court Opinions scraper")
    parser.add_argument("--headless", type=int, default=1, help="1=headless, 0=visible Chrome")
    args = parser.parse_args()

    logger = setup_logger()
    ensure_dirs()
    cleanup_temp_downloads(logger)
    init_csv(logger)
    existing_pdf_urls = load_existing_pdf_urls(logger)

    driver: Optional[webdriver.Chrome] = None
    effective_headless = bool(args.headless)

    try:
        logger.info("Run started")
        logger.info("Year range: %s to %s", START_YEAR, CURRENT_YEAR)
        logger.info("Headless: %s", effective_headless)

        driver = get_driver(headless=effective_headless)

        # Auto fallback if first year has no rows in headless
        first_year_test = scrape_all_pages_for_year(driver, START_YEAR, logger)
        if effective_headless and not first_year_test:
            logger.warning(
                "Headless mode did not return rows for first year. Falling back to visible Chrome."
            )
            effective_headless = False
            driver = recreate_driver(driver, headless=False, logger=logger)
            first_year_test = scrape_all_pages_for_year(driver, START_YEAR, logger)

        total_saved = 0
        total_skipped = 0

        years_data: Dict[int, List[Dict[str, str]]] = {START_YEAR: first_year_test}

        for year in range(START_YEAR + 1, CURRENT_YEAR + 1):
            logger.info("========== START YEAR %s ==========", year)

            try:
                site_rows = scrape_all_pages_for_year(driver, year, logger)
            except Exception as e:
                logger.error("Year %s | Fatal year-level error: %s", year, e)
                logger.info("========== END YEAR %s ==========", year)
                continue

            years_data[year] = site_rows

            if not site_rows:
                logger.info("Year %s | No records found.", year)

            logger.info("========== END YEAR %s ==========", year)

        # Add explicit start/end logs for first year too
        if START_YEAR not in years_data:
            years_data[START_YEAR] = []

        logger.info("========== START YEAR %s ==========", START_YEAR)
        if not years_data[START_YEAR]:
            logger.info("Year %s | No records found.", START_YEAR)
        logger.info("========== END YEAR %s ==========", START_YEAR)

        # Process rows year by year in order
        for year in range(START_YEAR, CURRENT_YEAR + 1):
            site_rows = years_data.get(year, [])

            for idx, item in enumerate(site_rows, start=1):
                pdf_url = item["pdf_url"]

                if pdf_url in existing_pdf_urls:
                    total_skipped += 1
                    logger.info(
                        "Year %s | Skipping existing CSV row (%s/%s): %s",
                        year,
                        idx,
                        len(site_rows),
                        pdf_url,
                    )
                    continue

                logger.info(
                    "Year %s | Processing record %s/%s | %s",
                    year,
                    idx,
                    len(site_rows),
                    item["case_title"] or pdf_url,
                )

                local_pdf_path = download_pdf_via_chrome(
                    driver=driver,
                    pdf_url=pdf_url,
                    case_number=item["case_number"],
                    case_title=item["case_title"],
                    logger=logger,
                )

                row = {
                    "case_number": item["case_number"],
                    "case_title": item["case_title"],
                    "case_date": item["case_date"],
                    "pdf_url": item["pdf_url"],
                    "pdf_local_path": local_pdf_path,
                }

                append_row(row, logger)
                existing_pdf_urls.add(pdf_url)
                total_saved += 1

        logger.info("Total skipped already-existing rows: %s", total_skipped)
        logger.info("Total new rows added: %s", total_saved)
        logger.info("Run completed successfully")

    except KeyboardInterrupt:
        logger.warning("Run stopped by user.")
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    main()