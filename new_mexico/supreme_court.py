#!/usr/bin/env python3
"""
NMOneSource scraper (Decisia) - STREAMING SAVE (row-by-row):

Runs:
  python scrapingfile.py                # headless=1 default
  python scrapingfile.py --headless 0   # visible chrome

Changes from previous version:
1) Single CSV only: downloads/CSV/cases.csv
2) Years auto-run from 1852 to current year (dynamic)
3) --headless default = 1
4) Command remains simple (no need year/court args)
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


BASE = "https://nmonesource.com"

# Single CSV for everything
CSV_COLUMNS = [
    "item_id",
    "case_title",
    "publication_date",
    "court",
    "category",
    "item_url",
    "pdf_url",
    "pdf_local_path",
]


@dataclass
class CaseRow:
    item_id: str
    case_title: str
    publication_date: str
    court: str
    category: str
    item_url: str
    pdf_url: str
    pdf_local_path: str


def setup_logger(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"nmone-{today}.log"

    logger = logging.getLogger("nmone_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.INFO)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.setLevel(logging.INFO)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def safe_filename(name: str, max_len: int = 160) -> str:
    name = name.strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r'[\\/*?:"<>|]', "_", name)
    name = re.sub(r"[^A-Za-z0-9._ -]+", "", name)
    name = name.strip(" ._-")
    return (name or "file")[:max_len]


def extract_item_id(item_url: str) -> str:
    m = re.search(r"/item/(\d+)/", item_url)
    return m.group(1) if m else ""


def ensure_csv_header(csv_path: Path) -> None:
    if csv_path.exists():
        return
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()


def read_existing_ids(csv_path: Path) -> Set[str]:
    ids: Set[str] = set()
    if not csv_path.exists():
        return ids
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            v = (row.get("item_id") or "").strip()
            if v:
                ids.add(v)
    return ids


def append_one_row(csv_path: Path, row: CaseRow) -> None:
    # streaming append
    with csv_path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writerow({
            "item_id": row.item_id,
            "case_title": row.case_title,
            "publication_date": row.publication_date,
            "court": row.court,
            "category": row.category,
            "item_url": row.item_url,
            "pdf_url": row.pdf_url,
            "pdf_local_path": row.pdf_local_path,
        })
        f.flush()


def parse_listing(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    ul = soup.select_one("ul.collectionItemList")
    if not ul:
        return []

    out: List[Dict[str, str]] = []
    for li in ul.select("li.list-item-expanded"):
        title_a = li.select_one("span.title a")
        pub = li.select_one("span.publicationDate")
        sub_spans = li.select("div.subMetadata span")
        pdf_a = li.select_one("div.documents a.matomo_download")

        title = title_a.get_text(strip=True) if title_a else ""
        item_href = title_a.get("href", "") if title_a else ""
        publication_date = pub.get_text(strip=True) if pub else ""

        court = sub_spans[0].get_text(strip=True) if len(sub_spans) >= 1 else ""
        category = sub_spans[1].get_text(strip=True) if len(sub_spans) >= 2 else ""

        pdf_href = pdf_a.get("href", "") if pdf_a else ""

        item_url = urljoin(BASE, item_href) if item_href else ""
        pdf_url = urljoin(BASE, pdf_href) if pdf_href else ""

        if not item_url:
            continue

        item_id = extract_item_id(item_url)
        if not item_id:
            continue

        out.append({
            "item_id": item_id,
            "case_title": title,
            "publication_date": publication_date,
            "court": court,
            "category": category,
            "item_url": item_url,
            "pdf_url": pdf_url,
        })

    return out


def requests_get_html(session: requests.Session, url: str, logger: logging.Logger) -> str:
    logger.info(f"GET (requests): {url}")
    r = session.get(url, timeout=45)
    r.raise_for_status()
    return r.text


def build_chrome(headless: bool, timeout: int) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1400,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    chromedriver = os.environ.get("CHROMEDRIVER", "").strip()
    service = ChromeService(executable_path=chromedriver) if chromedriver else ChromeService()

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(timeout)

    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        })
    except Exception:
        pass

    return driver


def _wait_for_list_in_current_context(driver: webdriver.Chrome, timeout: int) -> bool:
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "ul.collectionItemList li.list-item-expanded"))
        )
        return True
    except Exception:
        return False


def _scan_iframes_recursive(driver: webdriver.Chrome, timeout: int, depth: int, logger: logging.Logger) -> Optional[str]:
    if depth <= 0:
        return None

    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    for idx, fr in enumerate(iframes):
        driver.switch_to.default_content()
        driver.switch_to.frame(fr)

        if _wait_for_list_in_current_context(driver, timeout=5):
            logger.info(f"Found listing inside iframe depth={depth} index={idx}")
            return driver.page_source

        nested = _scan_iframes_recursive(driver, timeout, depth - 1, logger)
        if nested:
            return nested

    driver.switch_to.default_content()
    return None


def selenium_get_listing_html(driver: webdriver.Chrome, url: str, timeout: int, logger: logging.Logger) -> str:
    logger.info(f"OPEN (Chrome): {url}")
    driver.get(url)

    time.sleep(3)

    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.5)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1.0)
    except Exception:
        pass

    if _wait_for_list_in_current_context(driver, timeout=timeout):
        return driver.page_source

    found = _scan_iframes_recursive(driver, timeout=timeout, depth=3, logger=logger)
    if found:
        return found

    driver.switch_to.default_content()
    return driver.page_source


def download_pdf(
    session: requests.Session,
    pdf_url: str,
    pdf_path: Path,
    logger: logging.Logger,
    referer: str = "",
    max_retries: int = 3,
) -> bool:
    if not pdf_url:
        return False

    if pdf_path.exists() and pdf_path.stat().st_size > 0:
        logger.info(f"PDF exists, skip: {pdf_path.name}")
        return True

    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Download PDF: {pdf_url}")

    extra_headers: Dict[str, str] = {}
    if referer:
        extra_headers["Referer"] = referer
    extra_headers["Accept"] = "application/pdf,*/*;q=0.8"

    for attempt in range(1, max_retries + 1):
        try:
            with session.get(
                pdf_url,
                stream=True,
                timeout=90,
                headers=extra_headers,
            ) as r:
                if r.status_code in (403, 429, 500, 502, 503, 504) and attempt < max_retries:
                    wait_s = attempt * 2
                    logger.warning(
                        f"PDF retry {attempt}/{max_retries} (status={r.status_code}) for {pdf_url}; "
                        f"sleep {wait_s}s"
                    )
                    time.sleep(wait_s)
                    continue

                r.raise_for_status()
                with pdf_path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 128):
                        if chunk:
                            f.write(chunk)

            ok = pdf_path.exists() and pdf_path.stat().st_size > 0
            if ok:
                logger.info(f"Saved PDF: {pdf_path}")
                return True

            logger.warning(f"Downloaded file is empty: {pdf_url}")
            return False

        except requests.RequestException as e:
            if attempt < max_retries:
                wait_s = attempt * 2
                logger.warning(
                    f"PDF attempt {attempt}/{max_retries} failed for {pdf_url}: {e}; retry in {wait_s}s"
                )
                time.sleep(wait_s)
                continue

            logger.error(f"PDF download failed after {max_retries} attempts: {pdf_url} | error={e}")
            break

    if pdf_path.exists() and pdf_path.stat().st_size == 0:
        try:
            pdf_path.unlink()
        except Exception:
            pass
    return False


def main():
    ap = argparse.ArgumentParser()
    # default headless = 1 (requested)
    ap.add_argument("--headless", type=int, default=1, help="1=headless (default), 0=visible Chrome")
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--sleep", type=float, default=0.35)
    args = ap.parse_args()

    # ✅ years dynamic 1852 to current year
    start_year = 1852
    current_year = datetime.now().year
    years = list(range(start_year, current_year + 1))

    root_downloads = Path("downloads/supreme_court")
    csv_path = root_downloads / "CSV" / "cases.csv"   # ✅ single CSV
    pdf_dir = root_downloads / "PDF"
    log_dir = Path("Log/supreme_court")

    logger = setup_logger(log_dir)
    logger.info("Run started")
    logger.info(f"Years: {start_year}..{current_year} (total {len(years)})")
    logger.info(f"headless={args.headless}")

    ensure_csv_header(csv_path)
    existing_ids = read_existing_ids(csv_path)
    logger.info(f"Existing rows in CSV (dedupe by item_id): {len(existing_ids)}")

    # we'll scrape both courts (nmca + nmsc) across all years
    courts = ["nmsc"]

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })

    driver = build_chrome(headless=bool(args.headless), timeout=args.timeout)

    total_new = 0

    try:
        for court_code in courts:
            for year in years:
                logger.info(f"==== START court={court_code} year={year} ====")
                base_list_url = f"{BASE}/nmos/{court_code}/en/{year}/nav_date.do"
                page = 1

                while True:
                    page_url = base_list_url if page == 1 else f"{base_list_url}?page={page}"

                    items: List[Dict[str, str]] = []
                    html = ""

                    # requests first
                    try:
                        html = requests_get_html(session, page_url, logger)
                        items = parse_listing(html)
                    except Exception as e:
                        logger.warning(f"Requests failed: {e}")

                    # selenium fallback
                    if not items:
                        html = selenium_get_listing_html(driver, page_url, args.timeout, logger)
                        items = parse_listing(html)

                    logger.info(f"court={court_code} year={year} page={page}: found {len(items)} items")

                    if page == 1 and len(items) == 0:
                        # not an error for very old years sometimes; just continue next year
                        break

                    if len(items) == 0:
                        break

                    for it in items:
                        item_id = (it.get("item_id") or "").strip()
                        if not item_id:
                            continue

                        if item_id in existing_ids:
                            continue

                        title = (it.get("case_title") or "").strip()
                        pub_date = (it.get("publication_date") or "").strip()
                        court = (it.get("court") or "").strip()
                        category = (it.get("category") or "").strip()
                        item_url = (it.get("item_url") or "").strip()
                        pdf_url = (it.get("pdf_url") or "").strip()

                        date_part = pub_date.replace("/", "-") if pub_date else f"{year}"
                        file_name = safe_filename(f"{date_part}_{item_id}_{title}") + ".pdf"
                        pdf_path = pdf_dir / file_name

                        pdf_local_path = ""
                        if pdf_url:
                            if download_pdf(session, pdf_url, pdf_path, logger, referer=item_url):
                                pdf_local_path = str(pdf_path.as_posix())

                        row = CaseRow(
                            item_id=item_id,
                            case_title=title,
                            publication_date=pub_date,
                            court=court,
                            category=category,
                            item_url=item_url,
                            pdf_url=pdf_url,
                            pdf_local_path=pdf_local_path,
                        )

                        # ✅ streaming save
                        append_one_row(csv_path, row)

                        existing_ids.add(item_id)
                        total_new += 1
                        logger.info(f"SAVED row: {item_id} | total_new={total_new}")

                        time.sleep(args.sleep)

                    page += 1
                    time.sleep(args.sleep)

        logger.info(f"Done. Total new rows added: {total_new}")
        logger.info(f"CSV saved: {csv_path}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        session.close()


if __name__ == "__main__":
    main()
