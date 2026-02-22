#!/usr/bin/env python3
import argparse
import base64
import csv
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

DEFAULT_START_TERM = "1997-01"
DEFAULT_TERM = datetime.now().strftime("%Y-%m")
BASE_URL = (
    "https://www.sccourts.org/opinions-orders/opinions/published-opinions/"
    "supreme-court/?term={term}"
)
HEADLESS = True
PAGE_LOAD_TIMEOUT = 45
WAIT_TIMEOUT = 25
PDF_TIMEOUT = 60
DESCRIPTION_RETRY_COUNT = 4
DESCRIPTION_RETRY_SLEEP = 0.8
CSV_COLUMNS = [
    "Date",
    "case_no",
    "case",
    "descpiction",
    "PDF url",
    "Donwload PDF path",
]


def ensure_folders(base_dir: Path) -> Dict[str, Path]:
    downloads_dir = base_dir / "downloads/published_supreme_court"
    csv_dir = downloads_dir / "CSV"
    pdf_dir = downloads_dir / "PDF"
    log_dir = base_dir / "Log/published_supreme_court"

    csv_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    return {
        "downloads_dir": downloads_dir,
        "csv_dir": csv_dir,
        "pdf_dir": pdf_dir,
        "log_dir": log_dir,
    }


def setup_logger(log_dir: Path) -> logging.Logger:
    logger = logging.getLogger("sccourts_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    log_file = log_dir / f"scrape-{time.strftime('%Y-%m-%d')}.log"
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def load_existing_case_nos(csv_path: Path, pdf_dir: Path) -> Tuple[Set[str], Set[str]]:
    existing_case_nos: Set[str] = set()
    existing_pdf_case_nos = load_existing_pdf_case_nos(pdf_dir)

    if csv_path.exists():
        with csv_path.open("r", encoding="utf-8-sig", newline="") as infile:
            reader = csv.DictReader(infile)
            if reader.fieldnames:
                for row in reader:
                    case_no = (row.get("case_no") or "").strip()
                    if case_no:
                        existing_case_nos.add(case_no)

    return existing_case_nos, existing_pdf_case_nos


def load_all_existing_case_nos(csv_dir: Path) -> Set[str]:
    existing_case_nos: Set[str] = set()
    for csv_file in csv_dir.glob("*.csv"):
        if not csv_file.is_file():
            continue
        try:
            with csv_file.open("r", encoding="utf-8-sig", newline="") as infile:
                reader = csv.DictReader(infile)
                if reader.fieldnames:
                    for row in reader:
                        case_no = (row.get("case_no") or "").strip()
                        if case_no:
                            existing_case_nos.add(case_no)
        except Exception:
            continue
    return existing_case_nos


def load_existing_pdf_case_nos(pdf_dir: Path) -> Set[str]:
    existing_pdf_case_nos: Set[str] = set()
    for pdf_file in pdf_dir.glob("*.pdf"):
        if pdf_file.is_file():
            case_no = pdf_file.stem.strip()
            if case_no:
                existing_pdf_case_nos.add(case_no)
    return existing_pdf_case_nos


def _build_driver() -> webdriver.Chrome:
    options = Options()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    try:
        return webdriver.Chrome(options=options)
    except WebDriverException:
        try:
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager

            service = Service(ChromeDriverManager().install())
            return webdriver.Chrome(service=service, options=options)
        except Exception as exc:
            raise RuntimeError(
                "Unable to initialize Chrome WebDriver. Install Chrome and selenium/webdriver-manager."
            ) from exc


def _normalize_case_no(raw_case_no: str) -> str:
    text = (raw_case_no or "").strip()
    if not text:
        return ""
    if text.isdigit():
        return text
    matches = re.findall(r"\d+", text)
    if not matches:
        return text
    return max(matches, key=len)


def _is_html_opinion_url(url: str) -> bool:
    try:
        path = urlparse((url or "").strip()).path.lower()
    except Exception:
        return False
    return path.endswith(".htm") or path.endswith(".html")


def _convert_html_to_pdf(url: str, output_path: Path, logger: logging.Logger) -> bool:
    driver = _build_driver()
    try:
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        driver.get(url)
        WebDriverWait(driver, WAIT_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(0.8)

        pdf_data = driver.execute_cdp_cmd(
            "Page.printToPDF",
            {
                "printBackground": True,
                "landscape": False,
            },
        )
        data = pdf_data.get("data")
        if not data:
            logger.error("pdf download fail: empty PDF data from html url %s", url)
            return False

        output_path.write_bytes(base64.b64decode(data))
        if output_path.exists() and output_path.stat().st_size > 0:
            logger.info("pdf download success: %s", output_path.as_posix())
            return True

        logger.error("pdf download fail: empty file after html conversion for %s", url)
        return False
    except Exception:
        logger.exception("pdf download fail: html-to-pdf conversion failed for %s", url)
        if output_path.exists():
            try:
                output_path.unlink()
            except Exception:
                logger.exception("Failed to remove partial file: %s", output_path.as_posix())
        return False
    finally:
        driver.quit()


def _get_nonempty_description(case_item, driver: webdriver.Chrome) -> str:
    description = ""
    for _ in range(DESCRIPTION_RETRY_COUNT):
        try:
            title = case_item.find_element(By.CSS_SELECTOR, ".result-title")
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", title)
            driver.execute_script("arguments[0].click();", title)
            time.sleep(0.2)
        except NoSuchElementException:
            pass
        except Exception:
            pass

        try:
            paragraphs = case_item.find_elements(By.CSS_SELECTOR, "div.result-info p")
            description = " ".join(p.text.strip() for p in paragraphs if p.text.strip()).strip()
            if description:
                return description
            raw_info = case_item.find_element(By.CSS_SELECTOR, "div.result-info").get_attribute(
                "textContent"
            )
            description = (raw_info or "").strip()
            if description:
                return description
        except NoSuchElementException:
            pass

        time.sleep(DESCRIPTION_RETRY_SLEEP)

    return description


def _page_has_no_records_message(driver: webdriver.Chrome) -> bool:
    source = (driver.page_source or "").lower()
    return (
        "no published opinions for this month" in source
        or "there are no published opinions" in source
    )


def scrape_cases_with_selenium(url: str, logger: logging.Logger) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    driver = _build_driver()

    try:
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        logger.info("open page")
        driver.get(url)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.accordion-block")))
        wait.until(
            lambda d: (
                len(d.find_elements(By.CSS_SELECTOR, "div.accordion-item.case-result")) > 0
                or _page_has_no_records_message(d)
            )
        )

        blocks = driver.find_elements(By.CSS_SELECTOR, "div.accordion-block")
        total_items = len(driver.find_elements(By.CSS_SELECTOR, "div.accordion-item.case-result"))
        logger.info("found total items: %d", total_items)
        if total_items == 0 and _page_has_no_records_message(driver):
            logger.info("no records found for this term")
            return rows
        logger.info("expand items")

        for block in blocks:
            current_date = ""
            children = block.find_elements(By.XPATH, "./*")
            for child in children:
                class_name = (child.get_attribute("class") or "").strip()
                if "result-heading" in class_name:
                    try:
                        current_date = child.find_element(By.CSS_SELECTOR, "h3").text.strip()
                    except NoSuchElementException:
                        current_date = ""
                    continue

                if "accordion-item" in class_name and "case-result" in class_name:
                    try:
                        raw_case_no = child.find_element(By.CSS_SELECTOR, "p.case-number").text.strip()
                        case_no = _normalize_case_no(raw_case_no)
                        case_name = child.find_element(By.CSS_SELECTOR, "p.case-name").text.strip()
                        description = _get_nonempty_description(child, driver)
                        try:
                            pdf_url = child.find_element(By.CSS_SELECTOR, "a.download-link").get_attribute(
                                "href"
                            )
                        except NoSuchElementException:
                            pdf_url = ""

                        rows.append(
                            {
                                "Date": current_date,
                                "case_no": case_no,
                                "case": case_name,
                                "descpiction": description,
                                "PDF url": (pdf_url or "").strip(),
                                "Donwload PDF path": "",
                            }
                        )
                    except Exception:
                        logger.exception("Error while parsing a case item")
    except TimeoutException:
        logger.exception("Timed out while loading or reading the page")
    except Exception:
        logger.exception("Unexpected error during Selenium scrape")
    finally:
        driver.quit()

    return rows


def download_pdf(pdf_url: str, output_path: Path, logger: logging.Logger) -> bool:
    if not pdf_url:
        logger.error("pdf download fail: missing PDF URL for %s", output_path.name)
        return False

    if _is_html_opinion_url(pdf_url):
        logger.info("html opinion detected, converting to PDF: %s", pdf_url)
        return _convert_html_to_pdf(pdf_url, output_path, logger)

    session = requests.Session()
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
    }

    try:
        with session.get(pdf_url, stream=True, headers=headers, timeout=PDF_TIMEOUT) as response:
            response.raise_for_status()
            content_type = (response.headers.get("Content-Type") or "").lower()
            if "pdf" not in content_type and not pdf_url.lower().endswith(".pdf"):
                logger.error("pdf download fail: non-PDF response for %s", pdf_url)
                return False

            with output_path.open("wb") as outfile:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        outfile.write(chunk)

        if output_path.exists() and output_path.stat().st_size > 0:
            logger.info("pdf download success: %s", output_path.as_posix())
            return True

        logger.error("pdf download fail: empty file for %s", output_path.as_posix())
        return False
    except Exception:
        logger.exception("pdf download fail: %s", pdf_url)
        if output_path.exists():
            try:
                output_path.unlink()
            except Exception:
                logger.exception("Failed to remove partial file: %s", output_path.as_posix())
        return False
    finally:
        session.close()


def append_rows_to_csv(csv_path: Path, rows: List[Dict[str, str]], logger: logging.Logger) -> None:
    if not rows:
        logger.info("csv write success: no new rows to append")
        return

    file_exists = csv_path.exists()
    with csv_path.open("a", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=CSV_COLUMNS)
        if not file_exists or csv_path.stat().st_size == 0:
            writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in CSV_COLUMNS})
    logger.info("csv write success: appended %d rows", len(rows))


def _parse_term(term: str) -> datetime:
    return datetime.strptime(term, "%Y-%m")


def _generate_terms(start_term: str, end_term: str) -> List[str]:
    start_dt = _parse_term(start_term)
    end_dt = _parse_term(end_term)
    if start_dt > end_dt:
        raise ValueError(f"start-term {start_term} cannot be after end-term {end_term}")

    terms: List[str] = []
    year = start_dt.year
    month = start_dt.month
    while (year, month) <= (end_dt.year, end_dt.month):
        terms.append(f"{year:04d}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return terms


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape South Carolina Supreme Court published opinions.")
    parser.add_argument("--term", default=None, help="Single term in YYYY-MM format")
    parser.add_argument(
        "--start-term",
        default=DEFAULT_START_TERM,
        help="Start term in YYYY-MM format (default: 1997-01)",
    )
    parser.add_argument(
        "--end-term",
        default=DEFAULT_TERM,
        help="End term in YYYY-MM format (default: current year-month)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    base_dir = Path(__file__).resolve().parent
    paths = ensure_folders(base_dir)
    pdf_dir = paths["pdf_dir"]
    logger = setup_logger(paths["log_dir"])

    logger.info("start")

    try:
        if args.term:
            term = args.term.strip()
            _parse_term(term)
            terms = [term]
        else:
            start_term = (args.start_term or DEFAULT_START_TERM).strip()
            end_term = (args.end_term or DEFAULT_TERM).strip()
            terms = _generate_terms(start_term, end_term)

        logger.info("terms to process: %d (%s -> %s)", len(terms), terms[0], terms[-1])

        global_existing_case_nos = load_all_existing_case_nos(paths["csv_dir"])
        global_existing_pdf_case_nos = load_existing_pdf_case_nos(pdf_dir)
        logger.info(
            "existing records loaded: csv_case_nos=%d, existing_pdfs=%d",
            len(global_existing_case_nos),
            len(global_existing_pdf_case_nos),
        )

        for term in terms:
            url = BASE_URL.format(term=term)
            csv_path = paths["csv_dir"] / f"case.csv"
            logger.info("target url: %s", url)

            term_csv_case_nos, _ = load_existing_case_nos(csv_path, pdf_dir)
            existing_case_nos = global_existing_case_nos.union(term_csv_case_nos)
            existing_pdf_case_nos = global_existing_pdf_case_nos

            scraped_rows = scrape_cases_with_selenium(url, logger)
            if not scraped_rows:
                logger.info("new items count: 0")
                logger.info("skipped duplicates count: 0")
                append_rows_to_csv(csv_path, [], logger)
                continue

            rows_to_write: List[Dict[str, str]] = []
            skipped_duplicates = 0
            seen_in_run: Set[str] = set()

            for row in scraped_rows:
                case_no = (row.get("case_no") or "").strip()
                pdf_url = (row.get("PDF url") or "").strip()
                if not case_no or not pdf_url:
                    logger.error("Skipping record with missing case_no or PDF url: %s", row)
                    continue

                pdf_path = pdf_dir / f"{case_no}.pdf"
                if (
                    case_no in existing_case_nos
                    or case_no in existing_pdf_case_nos
                    or case_no in seen_in_run
                    or pdf_path.exists()
                ):
                    skipped_duplicates += 1
                    continue

                if download_pdf(pdf_url, pdf_path, logger):
                    row["Donwload PDF path"] = f"downloads/PDF/{case_no}.pdf"
                    rows_to_write.append(row)
                    seen_in_run.add(case_no)
                    global_existing_case_nos.add(case_no)
                    global_existing_pdf_case_nos.add(case_no)
                else:
                    logger.error("Skipping CSV write due to PDF download failure for case_no=%s", case_no)

            logger.info("new items count: %d", len(rows_to_write))
            logger.info("skipped duplicates count: %d", skipped_duplicates)
            append_rows_to_csv(csv_path, rows_to_write, logger)

        logger.info("end")
    except Exception:
        logger.exception("Fatal error in main")
        logger.info("end")
        raise


if __name__ == "__main__":
    main()
