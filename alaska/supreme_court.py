#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
from selenium.webdriver.support.ui import WebDriverWait


BASE_URL = "https://appellate-records.courts.alaska.gov"
SEARCH_URL_TEMPLATE = (
    "https://appellate-records.courts.alaska.gov/"
    "CMSPublic/Search/CaseNumber?CaseNumber={letter}&TrialCourtNumber="
)
CASE_LETTERS = [chr(c) for c in range(ord("A"), ord("B") + 1)]

SCRIPT_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = SCRIPT_DIR / "downloads"
CSV_DIR = DOWNLOADS_DIR / "CSV"
HTML_ROOT_DIR = DOWNLOADS_DIR / "HTML"
LOG_DIR = SCRIPT_DIR / "Log"
CSV_PATH = CSV_DIR / "case_data.csv"

CSV_COLUMNS = [
    "Partial Match",
    "Case Name",
    "Case Type",
    "Status",
    "Trial Court Number",
    "Date Opened",
    "page url",
    "case summary html path",
    "participants attorneys html path",
    "record html path",
    "docket html path",
    "motions and orders html path",
    "briefs html path",
    "Case Summary",
    "Full Case Caption",
    "Case Type Detail",
    "Contact Case Manager",
    "Date Filed",
    "Oral Argument Status",
    "Oral Argument Date/Time",
    "Oral Argument Location",
    "Oral Argument Video",
    "Opinions Number",
    "Opinions Type",
    "Opinions Decision",
    "Opinions Date",
    "Opinions Citation",
    "Opinions Document",
    "Lower Court Case Number",
    "Lower Court Judgment Date",
    "Lower Court Distribution Date",
    "Lower Court or Agency",
    "Lower Court Judge",
]

SUBPAGE_SPECS = [
    ("case_summary", "Case Summary"),
    ("participants_attorneys", "Participants & Attorneys"),
    ("record", "Record"),
    ("docket", "Docket"),
    ("motions_and_orders", "Motions and Orders"),
    ("briefs", "Briefs"),
]

CSV_SUBPAGE_COLUMN_MAP = {
    "case_summary": "case summary html path",
    "participants_attorneys": "participants attorneys html path",
    "record": "record html path",
    "docket": "docket html path",
    "motions_and_orders": "motions and orders html path",
    "briefs": "briefs html path",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Alaska appellate partial-match scraper")
    parser.add_argument("--headless", type=int, choices=[0, 1], default=1)
    parser.add_argument("--timeout", type=int, default=25)
    return parser.parse_args()


def ensure_dirs():
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    HTML_ROOT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logger() -> logging.Logger:
    ensure_dirs()
    logger = logging.getLogger("supreme_court")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    log_file = LOG_DIR / f"supreme_court-{datetime.now().strftime('%Y-%m-%d')}.log"
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.propagate = False
    return logger


def build_requests_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        }
    )
    return session


def build_driver(headless: int) -> webdriver.Chrome:
    options = Options()
    if headless == 1:
        options.add_argument("--headless=new")

    options.add_argument("--window-size=1800,1200")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--log-level=3")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver


def wait_ready(driver: webdriver.Chrome, timeout: int = 25):
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
    )


def normalize_text(value) -> str:
    if value is None:
        return "N/A"
    value = str(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value if value else "N/A"


def sanitize_filename(name: str, fallback: str = "file") -> str:
    name = normalize_text(name)
    if name == "N/A":
        name = fallback
    name = re.sub(r'[\\/*?:"<>|]+', "_", name)
    name = re.sub(r"\s+", "_", name).strip("._ ")
    return name or fallback


def safe_find(parent, by, value):
    try:
        return parent.find_element(by, value)
    except NoSuchElementException:
        return None


def safe_finds(parent, by, value):
    try:
        return parent.find_elements(by, value)
    except Exception:
        return []


def create_csv_if_missing(logger: logging.Logger):
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
        logger.info("Created CSV: %s", CSV_PATH)


def append_csv_row(row: Dict[str, str]):
    with CSV_PATH.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerow(row)


def load_existing_page_urls(logger: logging.Logger) -> Set[str]:
    seen = set()
    if not CSV_PATH.exists():
        return seen

    with CSV_PATH.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            page_url = normalize_text(row.get("page url", ""))
            if page_url != "N/A":
                seen.add(page_url)

    logger.info("Loaded existing page urls: %s", len(seen))
    return seen


def click_element(driver: webdriver.Chrome, el) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.4)
        el.click()
        return True
    except (ElementClickInterceptedException, StaleElementReferenceException, WebDriverException):
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False


def element_disabled(el) -> bool:
    if el is None:
        return True
    cls = (el.get_attribute("class") or "").lower()
    aria = (el.get_attribute("aria-disabled") or "").lower()
    style = (el.get_attribute("style") or "").lower()
    return ("disabled" in cls) or ("cssdisabled" in cls) or (aria == "true") or ("display: none" in style)


def save_html_content(html: str, out_path: Path) -> str:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        f.write(html)
    return str(out_path)


def save_case_html(driver: webdriver.Chrome, partial_match: str, logger: logging.Logger) -> str:
    case_folder = sanitize_filename(partial_match, "unknown_case")
    case_dir = HTML_ROOT_DIR / case_folder
    case_dir.mkdir(parents=True, exist_ok=True)

    html_path = case_dir / "case_summary.html"
    if not html_path.exists() or html_path.stat().st_size == 0:
        save_html_content(driver.page_source, html_path)
        logger.info("Saved HTML: %s", html_path)
    else:
        logger.info("HTML already exists: %s", html_path)

    return str(html_path)


def click_list_view(driver: webdriver.Chrome, timeout: int, logger: logging.Logger) -> None:
    try:
        wait_ready(driver, timeout)
        time.sleep(1.2)

        button = None
        for el in driver.find_elements(By.TAG_NAME, "button"):
            txt = normalize_text(el.text).lower()
            if txt == "list view":
                button = el
                break

        if button is None:
            logger.info("List View button not found, maybe already in list view.")
            return

        logger.info("Clicking List View button")
        before_headers = [normalize_text(x.text) for x in driver.find_elements(By.CSS_SELECTOR, "#tblPartialSearch thead th")]

        if not click_element(driver, button):
            logger.info("Could not click List View button")
            return

        time.sleep(2)

        try:
            WebDriverWait(driver, timeout).until(
                lambda d: any(
                    "Date Opened" in normalize_text(th.text)
                    for th in d.find_elements(By.CSS_SELECTOR, "#tblPartialSearch thead th")
                )
                or any(
                    normalize_text(btn.text).lower() == "page view"
                    for btn in d.find_elements(By.TAG_NAME, "button")
                )
            )
        except TimeoutException:
            logger.info("List View click timeout, continuing anyway")

        after_headers = [normalize_text(x.text) for x in driver.find_elements(By.CSS_SELECTOR, "#tblPartialSearch thead th")]
        logger.info("List View active | headers before=%s | headers after=%s", before_headers, after_headers)

    except Exception as e:
        logger.error("List View click failed: %s", e)


def get_visible_partial_rows(driver: webdriver.Chrome, logger: logging.Logger) -> List[Dict[str, str]]:
    out = []
    table = driver.find_element(By.CSS_SELECTOR, "#tblPartialSearch")
    rows = safe_finds(table, By.CSS_SELECTOR, "tbody tr")

    headers = [
        normalize_text(th.text)
        for th in safe_finds(table, By.CSS_SELECTOR, "thead th")
    ]

    for row in rows:
        try:
            style = (row.get_attribute("style") or "").lower()
            if "display: none" in style:
                continue

            link_el = safe_find(row, By.CSS_SELECTOR, "a.search-link")
            if link_el is None:
                continue

            cells = safe_finds(row, By.TAG_NAME, "td")
            cell_texts = [normalize_text(td.text) for td in cells]

            partial_match = normalize_text(link_el.text)
            page_url = urljoin(BASE_URL, link_el.get_attribute("href") or "")

            case_name = "N/A"
            case_type = "N/A"
            status = "N/A"
            trial_court_number = "N/A"
            date_opened = "N/A"

            for idx, header in enumerate(headers):
                if idx >= len(cell_texts):
                    continue

                value = cell_texts[idx]
                if header == "Case Name":
                    case_name = value
                elif header == "Case Type":
                    case_type = value
                elif header == "Status":
                    status = value
                elif header == "Trial Court Number":
                    trial_court_number = value
                elif header == "Date Opened":
                    date_opened = value

            out.append(
                {
                    "Partial Match": partial_match,
                    "Case Name": case_name,
                    "Case Type": case_type,
                    "Status": status,
                    "Trial Court Number": trial_court_number,
                    "Date Opened": date_opened,
                    "page url": page_url,
                }
            )
        except Exception as e:
            logger.error("Failed parsing one partial row: %s", e)

    return out


def collect_all_partial_rows(driver: webdriver.Chrome, timeout: int, logger: logging.Logger) -> List[Dict[str, str]]:
    all_rows = []
    seen_urls = set()

    for letter in CASE_LETTERS:
        start_url = SEARCH_URL_TEMPLATE.format(letter=letter)
        logger.info("Opening search for letter: %s | %s", letter, start_url)

        driver.get(start_url)
        wait_ready(driver, timeout)
        time.sleep(2)

        try:
            WebDriverWait(driver, timeout).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, "#tblPartialSearch tbody tr")
            )
        except TimeoutException:
            logger.info("No partial rows found for letter: %s", letter)
            continue

        click_list_view(driver, timeout, logger)
        time.sleep(1.5)

        page_no = 1

        while True:
            wait_ready(driver, timeout)
            time.sleep(1)

            rows = get_visible_partial_rows(driver, logger)
            new_count = 0

            for row in rows:
                page_url = row["page url"]
                if page_url not in seen_urls:
                    seen_urls.add(page_url)
                    all_rows.append(row)
                    new_count += 1

            logger.info(
                "Letter %s | list page %s | new=%s | total=%s",
                letter,
                page_no,
                new_count,
                len(all_rows),
            )

            pager = safe_find(driver, By.CSS_SELECTOR, "#partialMatchPager")
            next_btn = None
            if pager:
                btns = safe_finds(pager, By.CSS_SELECTOR, ".next")
                if btns:
                    next_btn = btns[0]

            if next_btn is None or element_disabled(next_btn):
                logger.info("No more pages for letter: %s", letter)
                break

            before = set(seen_urls)

            if not click_element(driver, next_btn):
                logger.info("Could not click next button for letter: %s", letter)
                break

            time.sleep(1.5)

            try:
                WebDriverWait(driver, timeout).until(
                    lambda d: len(
                        {
                            urljoin(BASE_URL, a.get_attribute("href") or "")
                            for a in d.find_elements(By.CSS_SELECTOR, "#tblPartialSearch tbody tr a.search-link")
                            if (a.get_attribute("href") or "").strip()
                        } - before
                    ) > 0
                )
            except TimeoutException:
                logger.info("Next page did not load new rows for letter: %s", letter)
                break

            page_no += 1

    return all_rows


def parse_dl_mapping(container) -> Dict[str, str]:
    result = {}
    if container is None:
        return result

    dts = safe_finds(container, By.TAG_NAME, "dt")
    dds = safe_finds(container, By.TAG_NAME, "dd")

    for dt, dd in zip(dts, dds):
        key = normalize_text(dt.text).rstrip(":")
        value = normalize_text(dd.text)
        result[key] = value

    return result


def parse_case_summary(driver: webdriver.Chrome) -> Dict[str, str]:
    result = {
        "Case Summary": "Case Summary",
        "Full Case Caption": "N/A",
        "Case Type Detail": "N/A",
        "Contact Case Manager": "N/A",
        "Date Filed": "N/A",
    }

    try:
        blocks = driver.find_elements(By.CSS_SELECTOR, "dl.cms-case-dl")
        if len(blocks) >= 1:
            left = parse_dl_mapping(blocks[0])
            result["Full Case Caption"] = normalize_text(left.get("Full Case Caption", "N/A"))
            result["Contact Case Manager"] = normalize_text(left.get("Contact Case Manager", "N/A"))

        if len(blocks) >= 2:
            right = parse_dl_mapping(blocks[1])
            result["Case Type Detail"] = normalize_text(right.get("Case Type", "N/A"))
            result["Date Filed"] = normalize_text(right.get("Date Filed", "N/A"))
    except Exception:
        pass

    return result


def parse_oral_argument(driver: webdriver.Chrome) -> Dict[str, str]:
    result = {
        "Oral Argument Status": "N/A",
        "Oral Argument Date/Time": "N/A",
        "Oral Argument Location": "N/A",
        "Oral Argument Video": "N/A",
    }

    try:
        oral_h4 = driver.find_element(By.XPATH, "//h4[contains(normalize-space(.), 'Oral Argument')]")
        oral_box = oral_h4.find_element(By.XPATH, "./following-sibling::div[contains(@class, 'cms-div-oral')][1]")
        mapping = parse_dl_mapping(oral_box)

        result["Oral Argument Status"] = normalize_text(mapping.get("Status", "N/A"))
        result["Oral Argument Date/Time"] = normalize_text(mapping.get("Date/Time", "N/A"))
        result["Oral Argument Location"] = normalize_text(mapping.get("Location", "N/A"))
        result["Oral Argument Video"] = normalize_text(mapping.get("Video", "N/A"))
    except Exception:
        pass

    return result


def sync_selenium_cookies_to_requests(driver: webdriver.Chrome, session: requests.Session) -> None:
    session.cookies.clear()
    for cookie in driver.get_cookies():
        try:
            session.cookies.set(
                cookie["name"],
                cookie["value"],
                domain=cookie.get("domain"),
                path=cookie.get("path", "/"),
            )
        except Exception:
            pass


def is_real_pdf_bytes(content: bytes, content_type: str) -> bool:
    if not content:
        return False
    if content[:5] == b"%PDF-":
        return True
    if "application/pdf" in (content_type or "").lower():
        return True
    return False


def try_download_pdf_with_requests(
    driver: webdriver.Chrome,
    session: requests.Session,
    url: str,
    out_path: Path,
    logger: logging.Logger,
) -> str:
    try:
        sync_selenium_cookies_to_requests(driver, session)

        headers = {
            "Referer": driver.current_url,
            "User-Agent": driver.execute_script("return navigator.userAgent;") or session.headers.get("User-Agent", ""),
        }

        resp = session.get(url, headers=headers, timeout=90, allow_redirects=True)
        resp.raise_for_status()

        content = resp.content
        content_type = resp.headers.get("Content-Type", "")

        if not is_real_pdf_bytes(content, content_type):
            logger.info(
                "Requests response is NOT PDF | url=%s | content_type=%s",
                url,
                content_type,
            )
            return "N/A"

        with out_path.open("wb") as f:
            f.write(content)

        logger.info("Downloaded PDF by requests: %s", out_path)
        return str(out_path)

    except Exception as e:
        logger.info("Requests PDF download failed: %s", e)
        return "N/A"


def try_download_pdf_with_browser_tab(
    driver: webdriver.Chrome,
    session: requests.Session,
    url: str,
    out_path: Path,
    logger: logging.Logger,
) -> str:
    original_handle = driver.current_window_handle
    before_handles = set(driver.window_handles)

    try:
        driver.execute_script("window.open(arguments[0], '_blank');", url)

        WebDriverWait(driver, 15).until(lambda d: len(d.window_handles) > len(before_handles))
        new_handles = [h for h in driver.window_handles if h not in before_handles]
        if not new_handles:
            logger.info("Browser tab not created for PDF")
            return "N/A"

        pdf_handle = new_handles[0]
        driver.switch_to.window(pdf_handle)
        time.sleep(3)
        wait_ready(driver, 10)

        final_url = driver.current_url
        logger.info("Browser PDF tab final URL: %s", final_url)

        sync_selenium_cookies_to_requests(driver, session)

        headers = {
            "Referer": url,
            "User-Agent": driver.execute_script("return navigator.userAgent;") or session.headers.get("User-Agent", ""),
        }

        resp = session.get(final_url, headers=headers, timeout=90, allow_redirects=True)
        resp.raise_for_status()

        content = resp.content
        content_type = resp.headers.get("Content-Type", "")

        if not is_real_pdf_bytes(content, content_type):
            logger.info(
                "Browser-tab response is NOT PDF | final_url=%s | content_type=%s",
                final_url,
                content_type,
            )
            return "N/A"

        with out_path.open("wb") as f:
            f.write(content)

        logger.info("Downloaded PDF by browser-tab method: %s", out_path)
        return str(out_path)

    except Exception as e:
        logger.info("Browser-tab PDF download failed: %s", e)
        return "N/A"

    finally:
        try:
            current = driver.current_window_handle
            if current != original_handle:
                driver.close()
        except Exception:
            pass

        try:
            driver.switch_to.window(original_handle)
        except Exception:
            pass


def download_case_pdf(
    driver: webdriver.Chrome,
    session: requests.Session,
    pdf_url: str,
    out_path: Path,
    logger: logging.Logger,
) -> str:
    if out_path.exists() and out_path.stat().st_size > 0:
        try:
            with out_path.open("rb") as f:
                if f.read(5) == b"%PDF-":
                    logger.info("PDF already exists: %s", out_path)
                    return str(out_path)
        except Exception:
            pass

        try:
            out_path.unlink()
            logger.info("Removed invalid existing PDF: %s", out_path)
        except Exception:
            pass

    result = try_download_pdf_with_requests(driver, session, pdf_url, out_path, logger)
    if result != "N/A":
        return result

    result = try_download_pdf_with_browser_tab(driver, session, pdf_url, out_path, logger)
    if result != "N/A":
        return result

    if out_path.exists():
        try:
            out_path.unlink()
        except Exception:
            pass

    logger.info("Final result: response was not a valid PDF, so file not stored | url=%s", pdf_url)
    return "N/A"


def parse_opinions(
    driver: webdriver.Chrome,
    session: requests.Session,
    partial_match: str,
    logger: logging.Logger,
) -> Dict[str, str]:
    result = {
        "Opinions Number": "N/A",
        "Opinions Type": "N/A",
        "Opinions Decision": "N/A",
        "Opinions Date": "N/A",
        "Opinions Citation": "N/A",
        "Opinions Document": "N/A",
    }

    try:
        opinion_heading = safe_find(driver, By.XPATH, "//h5[contains(normalize-space(.), 'Opinions')]")
        if opinion_heading is None:
            return result

        opinion_tables = driver.find_elements(
            By.XPATH,
            "//h5[contains(normalize-space(.), 'Opinions')]/following-sibling::table[1]"
        )
        if not opinion_tables:
            return result

        table = opinion_tables[0]
        tbody_rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        if not tbody_rows:
            return result

        row = tbody_rows[0]
        cells = row.find_elements(By.TAG_NAME, "td")
        if len(cells) < 6:
            return result

        result["Opinions Number"] = normalize_text(cells[0].text)
        result["Opinions Type"] = normalize_text(cells[1].text)
        result["Opinions Decision"] = normalize_text(cells[2].text)
        result["Opinions Date"] = normalize_text(cells[3].text)
        result["Opinions Citation"] = normalize_text(cells[4].text)

        doc_link = safe_find(cells[5], By.CSS_SELECTOR, "a.documentLink, a[href*='OpenOpinionDocument']")
        if doc_link is not None:
            href = doc_link.get_attribute("href") or ""
            if href.strip():
                pdf_url = urljoin(BASE_URL, href)
                case_folder = HTML_ROOT_DIR / sanitize_filename(partial_match, "unknown_case")
                case_folder.mkdir(parents=True, exist_ok=True)

                pdf_name = sanitize_filename(partial_match, "case")
                if result["Opinions Number"] != "N/A":
                    pdf_name += f"_opinion_{sanitize_filename(result['Opinions Number'], '1')}"
                pdf_path = case_folder / f"{pdf_name}.pdf"

                logger.info("Opinions PDF URL found: %s", pdf_url)

                result["Opinions Document"] = download_case_pdf(
                    driver,
                    session,
                    pdf_url,
                    pdf_path,
                    logger,
                )

    except Exception as e:
        logger.error("Opinions parse failed for %s: %s", partial_match, e)

    return result


def parse_lower_court(driver: webdriver.Chrome) -> Dict[str, str]:
    result = {
        "Lower Court Case Number": "N/A",
        "Lower Court Judgment Date": "N/A",
        "Lower Court Distribution Date": "N/A",
        "Lower Court or Agency": "N/A",
        "Lower Court Judge": "N/A",
    }

    try:
        table = driver.find_element(
            By.XPATH,
            "//h5[contains(normalize-space(.), 'Lower Court or Agency Information')]/following-sibling::table[1]"
        )
        row = table.find_element(By.CSS_SELECTOR, "tbody tr")
        cells = row.find_elements(By.TAG_NAME, "td")

        if len(cells) >= 5:
            result["Lower Court Case Number"] = normalize_text(cells[0].text)
            result["Lower Court Judgment Date"] = normalize_text(cells[1].text)
            result["Lower Court Distribution Date"] = normalize_text(cells[2].text)
            result["Lower Court or Agency"] = normalize_text(cells[3].text)
            result["Lower Court Judge"] = normalize_text(cells[4].text)
    except Exception:
        pass

    return result


def get_case_dir(partial_match: str) -> Path:
    return HTML_ROOT_DIR / sanitize_filename(partial_match, "unknown_case")


def save_current_page_to_named_file(
    driver: webdriver.Chrome,
    partial_match: str,
    filename: str,
    logger: logging.Logger,
) -> str:
    case_dir = get_case_dir(partial_match)
    case_dir.mkdir(parents=True, exist_ok=True)
    out_path = case_dir / filename
    save_html_content(driver.page_source, out_path)
    logger.info("Saved subpage HTML: %s", out_path)
    return str(out_path)


def click_case_left_nav(driver: webdriver.Chrome, label: str, timeout: int, logger: logging.Logger) -> bool:
    xpath_candidates = [
        f"//a[normalize-space(.)='{label}']",
        f"//a[contains(normalize-space(.), '{label}')]",
    ]

    nav_link = None
    for xp in xpath_candidates:
        nav_link = safe_find(driver, By.XPATH, xp)
        if nav_link is not None:
            break

    if nav_link is None:
        logger.error("Left nav link not found: %s", label)
        return False

    before_html = driver.page_source[:5000]

    if not click_element(driver, nav_link):
        logger.error("Could not click left nav link: %s", label)
        return False

    time.sleep(1.5)
    wait_ready(driver, timeout)

    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.page_source[:5000] != before_html
        )
    except TimeoutException:
        logger.info("Page source did not visibly change after click: %s", label)

    return True


def save_all_case_subpages(
    driver: webdriver.Chrome,
    partial_match: str,
    timeout: int,
    logger: logging.Logger,
) -> Dict[str, str]:
    saved_paths = {
        "case_summary": "N/A",
        "participants_attorneys": "N/A",
        "record": "N/A",
        "docket": "N/A",
        "motions_and_orders": "N/A",
        "briefs": "N/A",
    }

    # Current page is case summary
    try:
        saved_paths["case_summary"] = save_current_page_to_named_file(
            driver, partial_match, "case_summary.html", logger
        )
    except Exception as e:
        logger.error("Failed saving case summary page for %s: %s", partial_match, e)

    for key, label in SUBPAGE_SPECS[1:]:
        try:
            ok = click_case_left_nav(driver, label, timeout, logger)
            if not ok:
                logger.error("Subpage not opened for %s: %s", partial_match, label)
                continue

            saved_paths[key] = save_current_page_to_named_file(
                driver, partial_match, f"{key}.html", logger
            )
        except Exception as e:
            logger.error("Failed saving subpage for %s | %s | %s", partial_match, label, e)
            logger.error(traceback.format_exc())

    return saved_paths


def process_case(
    driver: webdriver.Chrome,
    session: requests.Session,
    row: Dict[str, str],
    timeout: int,
    logger: logging.Logger,
) -> Dict[str, str]:
    page_url = row["page url"]
    partial_match = row["Partial Match"]

    logger.info("Opening case page: %s", page_url)
    driver.get(page_url)
    wait_ready(driver, timeout)
    time.sleep(2)

    subpage_paths = save_all_case_subpages(driver, partial_match, timeout, logger)

    # Go back to case summary page for structured parsing
    driver.get(page_url)
    wait_ready(driver, timeout)
    time.sleep(1.5)

    case_summary = parse_case_summary(driver)
    oral_argument = parse_oral_argument(driver)
    opinions = parse_opinions(driver, session, partial_match, logger)
    lower_court = parse_lower_court(driver)

    return {
        "Partial Match": normalize_text(row["Partial Match"]),
        "Case Name": normalize_text(row["Case Name"]),
        "Case Type": normalize_text(row["Case Type"]),
        "Status": normalize_text(row["Status"]),
        "Trial Court Number": normalize_text(row.get("Trial Court Number", "N/A")),
        "Date Opened": normalize_text(row.get("Date Opened", "N/A")),
        "page url": normalize_text(page_url),
        "case summary html path": normalize_text(subpage_paths["case_summary"]),
        "participants attorneys html path": normalize_text(subpage_paths["participants_attorneys"]),
        "record html path": normalize_text(subpage_paths["record"]),
        "docket html path": normalize_text(subpage_paths["docket"]),
        "motions and orders html path": normalize_text(subpage_paths["motions_and_orders"]),
        "briefs html path": normalize_text(subpage_paths["briefs"]),
        "Case Summary": normalize_text(case_summary["Case Summary"]),
        "Full Case Caption": normalize_text(case_summary["Full Case Caption"]),
        "Case Type Detail": normalize_text(case_summary["Case Type Detail"]),
        "Contact Case Manager": normalize_text(case_summary["Contact Case Manager"]),
        "Date Filed": normalize_text(case_summary["Date Filed"]),
        "Oral Argument Status": normalize_text(oral_argument["Oral Argument Status"]),
        "Oral Argument Date/Time": normalize_text(oral_argument["Oral Argument Date/Time"]),
        "Oral Argument Location": normalize_text(oral_argument["Oral Argument Location"]),
        "Oral Argument Video": normalize_text(oral_argument["Oral Argument Video"]),
        "Opinions Number": normalize_text(opinions["Opinions Number"]),
        "Opinions Type": normalize_text(opinions["Opinions Type"]),
        "Opinions Decision": normalize_text(opinions["Opinions Decision"]),
        "Opinions Date": normalize_text(opinions["Opinions Date"]),
        "Opinions Citation": normalize_text(opinions["Opinions Citation"]),
        "Opinions Document": normalize_text(opinions["Opinions Document"]),
        "Lower Court Case Number": normalize_text(lower_court["Lower Court Case Number"]),
        "Lower Court Judgment Date": normalize_text(lower_court["Lower Court Judgment Date"]),
        "Lower Court Distribution Date": normalize_text(lower_court["Lower Court Distribution Date"]),
        "Lower Court or Agency": normalize_text(lower_court["Lower Court or Agency"]),
        "Lower Court Judge": normalize_text(lower_court["Lower Court Judge"]),
    }


def main():
    args = parse_args()
    logger = setup_logger()
    ensure_dirs()
    create_csv_if_missing(logger)

    logger.info("Run started")
    logger.info("Headless: %s", bool(args.headless))

    existing_urls = load_existing_page_urls(logger)
    session = build_requests_session()

    driver = None
    total_found = 0
    total_written = 0
    total_skipped = 0
    total_failed = 0

    try:
        driver = build_driver(args.headless)
        all_rows = collect_all_partial_rows(driver, args.timeout, logger)
        total_found = len(all_rows)

        for index, row in enumerate(all_rows, start=1):
            page_url = row["page url"]
            partial_match = row["Partial Match"]

            try:
                if page_url in existing_urls:
                    total_skipped += 1
                    logger.info("[%s/%s] Skip already scraped: %s", index, total_found, partial_match)
                    continue

                logger.info("[%s/%s] Processing: %s", index, total_found, partial_match)
                out_row = process_case(driver, session, row, args.timeout, logger)
                append_csv_row(out_row)
                existing_urls.add(page_url)
                total_written += 1
                logger.info("CSV row appended: %s", partial_match)

            except Exception as e:
                total_failed += 1
                logger.error("Failed case: %s | %s", partial_match, e)
                logger.error(traceback.format_exc())

    except Exception as e:
        logger.error("Fatal error: %s", e)
        logger.error(traceback.format_exc())
        raise

    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    logger.info(
        "Run finished | found=%s | written=%s | skipped=%s | failed=%s",
        total_found,
        total_written,
        total_skipped,
        total_failed,
    )


if __name__ == "__main__":
    main()