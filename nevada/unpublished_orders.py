#!/usr/bin/env python3
import argparse
import base64
import csv
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


TARGET_URL = "https://nvcourts.gov/supreme/decisions/unpublished_orders"
CASEINFO_BASE = "https://caseinfo.nvsupremecourt.us"

# Required output structure
BASE_DOWNLOAD = Path("downloads") / "unpublished_orders"
CSV_DIR = BASE_DOWNLOAD / "CSV"
PDF_DIR = BASE_DOWNLOAD / "PDF"
LOG_DIR = Path("Log") / "unpublished_orders"
CSV_PATH = CSV_DIR / "case.csv"

CSV_COLUMNS = [
    "case_number",
    "case_title",
    "order_date",
    "docket_url",
    "pdf_url",
    "pdf_local_path",
]


def setup_logger() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"unpublished_orders-{datetime.now().strftime('%Y-%m-%d')}.log"

    logger = logging.getLogger("unpublished_orders")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)

    logger.info(f"Log file: {log_file.resolve()}")
    return logger


def b64decode_str(s: str) -> str:
    s = s.strip()
    pad = (-len(s)) % 4
    s = s + ("=" * pad)
    return base64.b64decode(s).decode("utf-8", errors="replace")


def build_urls_from_tokens(case_token_b64: str, doc_token_b64: str) -> Tuple[str, str]:
    """
    case token decodes to csIID (e.g. "74059")
    doc token decodes to "csNameID_csIID_deLinkID_onBaseDocumentNumber"
      e.g. "74059_74059_1051542_26-08319"
    """
    csIID = b64decode_str(case_token_b64).strip()
    decoded_doc = b64decode_str(doc_token_b64).strip()
    parts = decoded_doc.split("_")
    if len(parts) < 4:
        raise ValueError(f"Unexpected doc token: {decoded_doc}")

    csNameID, csIID2, deLinkID, onBaseDocumentNumber = parts[0], parts[1], parts[2], parts[3]

    docket_url = f"{CASEINFO_BASE}/public/caseView.do?csIID={csIID}"
    pdf_url = (
        f"{CASEINFO_BASE}/document/view.do"
        f"?csNameID={csNameID}&csIID={csIID2}&deLinkID={deLinkID}&onBaseDocumentNumber={onBaseDocumentNumber}"
    )
    return docket_url, pdf_url


def make_driver(headless: bool) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--lang=en-US")

    # less automation noise
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(90)
    return driver


def ensure_outputs(logger: logging.Logger):
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            w.writeheader()
        logger.info(f"Created CSV: {CSV_PATH.resolve()}")
    else:
        logger.info(f"Using existing CSV: {CSV_PATH.resolve()}")


def load_existing_keys() -> set:
    """
    Dedupe key = (case_number, order_date)
    """
    keys = set()
    if not CSV_PATH.exists():
        return keys
    with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            cn = (row.get("case_number") or "").strip()
            od = (row.get("order_date") or "").strip()
            if cn and od:
                keys.add((cn, od))
    return keys


def selenium_cookies_to_requests_session(driver: webdriver.Chrome) -> requests.Session:
    """
    Important: caseinfo sometimes returns HTML unless you send cookies from Selenium session.
    """
    s = requests.Session()
    for c in driver.get_cookies():
        # requests expects domain without leading dot sometimes; keep as-is
        s.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
    return s


def is_pdf_bytes(first_bytes: bytes) -> bool:
    return first_bytes.startswith(b"%PDF-")


def safe_filename(case_number: str, order_date: str) -> str:
    # convert "Feb 24, 2026" -> "2026-02-24" if possible; else safe fallback
    date_part = order_date.strip()
    iso = None
    try:
        dt = datetime.strptime(date_part, "%b %d, %Y")
        iso = dt.strftime("%Y-%m-%d")
    except Exception:
        # fallback: sanitize raw
        iso = re.sub(r"[^0-9A-Za-z._-]+", "_", date_part)[:50] or "date"

    cn = re.sub(r"[^0-9A-Za-z._-]+", "_", case_number)[:50] or "case"
    return f"{cn}__{iso}.pdf"


def download_pdf(
    logger: logging.Logger,
    session: requests.Session,
    pdf_url: str,
    out_path: Path,
    referer: str = TARGET_URL,
    timeout: int = 60
) -> Optional[str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        "Referer": referer,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, 4):
        try:
            with session.get(pdf_url, headers=headers, stream=True, timeout=timeout, allow_redirects=True) as r:
                r.raise_for_status()

                it = r.iter_content(chunk_size=8192)
                first = b""
                try:
                    first = next(it) or b""
                except StopIteration:
                    first = b""

                # Many servers mislabel content-type; trust PDF header bytes
                if not is_pdf_bytes(first):
                    ct = r.headers.get("content-type", "")
                    logger.warning(f"Not PDF attempt {attempt}: ct={ct} url={pdf_url}")
                    time.sleep(2 * attempt)
                    continue

                with out_path.open("wb") as f:
                    f.write(first)
                    for chunk in it:
                        if chunk:
                            f.write(chunk)

                return str(out_path.resolve())

        except Exception as e:
            logger.warning(f"Download failed attempt {attempt}: {pdf_url} | {e}")
            time.sleep(2 * attempt)

    return None


def append_row_one_by_one(row: Dict[str, str]):
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writerow(row)
        f.flush()
        os.fsync(f.fileno())


def scrape(headless: bool, download: bool, limit: int):
    logger = setup_logger()
    logger.info(f"Run started: {datetime.now().isoformat(timespec='seconds')}")
    logger.info(f"Target URL: {TARGET_URL}")
    logger.info(f"Headless: {headless}")
    logger.info(f"Download PDFs: {download}")

    ensure_outputs(logger)
    existing_keys = load_existing_keys()

    driver = make_driver(headless=headless)
    added = 0
    processed = 0

    try:
        driver.get(TARGET_URL)

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#dynamic-table-wrapper table"))
        )

        # Build requests session from Selenium cookies (IMPORTANT)
        session = selenium_cookies_to_requests_session(driver)

        rows = driver.find_elements(By.CSS_SELECTOR, "#dynamic-table-wrapper table tr")
        data_rows = rows[1:] if len(rows) > 1 else []

        if limit > 0:
            data_rows = data_rows[:limit]

        logger.info(f"Listing rows found: {len(data_rows)}")

        for tr in data_rows:
            processed += 1
            tds = tr.find_elements(By.TAG_NAME, "td")
            if len(tds) < 3:
                continue

            case_a = tds[0].find_element(By.TAG_NAME, "a")
            case_number = (case_a.text or "").strip()
            case_onclick = case_a.get_attribute("onclick") or ""

            case_title = (tds[1].text or "").strip()

            date_a = tds[2].find_element(By.TAG_NAME, "a")
            order_date = (date_a.text or "").strip()
            doc_onclick = date_a.get_attribute("onclick") or ""

            m_case = re.search(r"requestUrl\(\s*['\"]case['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", case_onclick)
            m_doc = re.search(r"requestUrl\(\s*['\"]doc['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", doc_onclick)
            if not m_case or not m_doc:
                logger.warning(f"Row missing onclick tokens: case={case_number}")
                continue

            key = (case_number, order_date)
            if key in existing_keys:
                continue

            try:
                docket_url, pdf_url = build_urls_from_tokens(m_case.group(1), m_doc.group(1))
            except Exception as e:
                logger.warning(f"Token decode failed: case={case_number} err={e}")
                continue

            pdf_local_path = ""
            if download:
                fname = safe_filename(case_number, order_date)
                out_path = PDF_DIR / fname
                saved = download_pdf(logger, session, pdf_url, out_path)
                if saved:
                    pdf_local_path = saved
                    logger.info(f"PDF saved: {pdf_local_path}")
                else:
                    logger.warning(f"PDF NOT saved: case={case_number} url={pdf_url}")

            row = {
                "case_number": case_number,
                "case_title": case_title,
                "order_date": order_date,
                "docket_url": docket_url,
                "pdf_url": pdf_url,
                "pdf_local_path": pdf_local_path,
            }

            # Save one-by-one immediately
            append_row_one_by_one(row)
            existing_keys.add(key)
            added += 1

            if added % 25 == 0:
                logger.info(f"Progress: added={added} processed={processed}")

        logger.info(f"Done. New rows appended: {added}")
        logger.info(f"CSV: {CSV_PATH.resolve()}")
        logger.info(f"PDF folder: {PDF_DIR.resolve()}")
        logger.info("Scraping finished")

    finally:
        try:
            driver.quit()
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--headless", type=int, default=1, help="1=headless on, 0=headless off")
    ap.add_argument("--download", type=int, default=1, help="1=download PDFs, 0=no")
    ap.add_argument("--limit", type=int, default=0, help="0=no limit, else number of rows")
    args = ap.parse_args()

    scrape(
        headless=bool(args.headless),
        download=bool(args.download),
        limit=int(args.limit),
    )


if __name__ == "__main__":
    main()