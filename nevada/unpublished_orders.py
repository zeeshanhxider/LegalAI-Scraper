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
from typing import Dict, Optional, Set, Tuple

import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


TARGET_URL = "https://nvcourts.gov/supreme/decisions/unpublished_orders"
CASEINFO_BASE = "https://caseinfo.nvsupremecourt.us"

# Required output structure
BASE_DIR = Path(__file__).resolve().parent
DOWNLOADS_ROOT = BASE_DIR / "downloads"
COURT_PATH = Path("supreme_court") / "unpublished_orders"
BASE_DOWNLOAD = DOWNLOADS_ROOT / COURT_PATH
CSV_DIR = BASE_DOWNLOAD / "CSV"
PDF_DIR = BASE_DOWNLOAD
LOG_DIR = BASE_DIR / "Log" / "unpublished_orders"
CSV_PATH = CSV_DIR / "case.csv"

CSV_COLUMNS = [
    "case_number",
    "case_title",
    "order_date",
    "docket_url",
    "pdf_url",
    "pdf_local_path",
]
COMBINED_COLUMNS = [
    "source_type",
    "advance_no",
    "case_number",
    "case_title",
    "opinion_filed_on",
    "opinion_date",
    "order_date",
    "docket_url",
    "pdf_url",
    "pdf_local_path",
]
SOURCE_TYPE = "unpublished_orders"
COMBINED_CSV_PATH = DOWNLOADS_ROOT / "CSV" / "case.csv"


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


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def sanitize_for_filename(value: str, fallback: str = "unknown", max_len: int = 100) -> str:
    cleaned = normalize_text(value)
    cleaned = re.sub(r"[\\/:*?\"<>|]+", "_", cleaned)
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", cleaned)
    cleaned = cleaned.strip("._-")
    if not cleaned:
        cleaned = fallback
    return cleaned[:max_len]


def parse_order_date(order_date: str) -> Tuple[str, str]:
    date_part = normalize_text(order_date)
    try:
        dt = datetime.strptime(date_part, "%b %d, %Y")
        iso = dt.strftime("%Y-%m-%d")
        return dt.strftime("%Y"), iso
    except Exception:
        pass

    m = re.search(r"(19|20)\d{2}", date_part)
    year = m.group(0) if m else "unknownyear"
    token = sanitize_for_filename(date_part, fallback="date", max_len=50)
    return year, token


def build_case_folder(case_number: str, case_title: str) -> str:
    safe_title = sanitize_for_filename(case_title, fallback="untitled_case", max_len=100)
    safe_case_number = (
        sanitize_for_filename(case_number, fallback="", max_len=60)
        if normalize_text(case_number)
        else ""
    )
    if safe_case_number:
        return f"{safe_case_number}"
    return safe_title


def build_pdf_filename(case_number: str, case_title: str, order_date: str) -> str:
    _, date_token = parse_order_date(order_date)
    case_id = sanitize_for_filename(case_number or case_title, fallback="case", max_len=80)
    return f"{case_id}__{date_token}.pdf"


def build_pdf_destination(case_number: str, case_title: str, order_date: str) -> Path:
    year_folder, _ = parse_order_date(order_date)
    case_folder = build_case_folder(case_number, case_title)
    filename = build_pdf_filename(case_number, case_title, order_date)
    return PDF_DIR / year_folder / case_folder / filename


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
    COMBINED_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    BASE_DOWNLOAD.mkdir(parents=True, exist_ok=True)
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            w.writeheader()
        logger.info(f"Created CSV: {CSV_PATH.resolve()}")
    else:
        logger.info(f"Using existing CSV: {CSV_PATH.resolve()}")

    if not COMBINED_CSV_PATH.exists():
        with COMBINED_CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=COMBINED_COLUMNS)
            w.writeheader()
        logger.info(f"Created combined CSV: {COMBINED_CSV_PATH.resolve()}")


def load_existing_keys() -> set:
    """
    Dedupe key = (case_number|case_title, order_date)
    """
    keys = set()
    if not CSV_PATH.exists():
        return keys
    with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            cn = (row.get("case_number") or "").strip()
            ct = (row.get("case_title") or "").strip()
            od = (row.get("order_date") or "").strip()
            key_id = cn or ct
            if key_id and od:
                keys.add((key_id, od))
    return keys


def build_combined_key(case_number: str, case_title: str, order_date: str) -> Tuple[str, str]:
    key_id = normalize_text(case_number) or normalize_text(case_title)
    return key_id, normalize_text(order_date)


def load_existing_combined_keys() -> Set[Tuple[str, str]]:
    keys: Set[Tuple[str, str]] = set()
    if not COMBINED_CSV_PATH.exists() or COMBINED_CSV_PATH.stat().st_size == 0:
        return keys

    with COMBINED_CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            source = normalize_text(row.get("source_type", ""))
            if source and source != SOURCE_TYPE:
                continue

            key = build_combined_key(
                row.get("case_number", ""),
                row.get("case_title", ""),
                row.get("order_date", ""),
            )
            if key[0] and key[1]:
                keys.add(key)
    return keys


def build_combined_row(row: Dict[str, str]) -> Dict[str, str]:
    return {
        "source_type": SOURCE_TYPE,
        "advance_no": "",
        "case_number": row.get("case_number", ""),
        "case_title": row.get("case_title", ""),
        "opinion_filed_on": "",
        "opinion_date": "",
        "order_date": row.get("order_date", ""),
        "docket_url": row.get("docket_url", ""),
        "pdf_url": row.get("pdf_url", ""),
        "pdf_local_path": row.get("pdf_local_path", ""),
    }


def append_combined_row(row: Dict[str, str]):
    with COMBINED_CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COMBINED_COLUMNS)
        w.writerow(build_combined_row(row))
        f.flush()
        os.fsync(f.fileno())


def sync_existing_csv_to_combined(logger: logging.Logger, combined_keys: Set[Tuple[str, str]]) -> int:
    if not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0:
        return 0

    backfilled = 0
    with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            row = {k: normalize_text(v or "") for k, v in row.items()}
            key = build_combined_key(
                row.get("case_number", ""),
                row.get("case_title", ""),
                row.get("order_date", ""),
            )
            if not key[0] or not key[1] or key in combined_keys:
                continue

            append_combined_row(row)
            combined_keys.add(key)
            backfilled += 1

    if backfilled:
        logger.info(f"Backfilled {backfilled} unpublished order rows into combined CSV")
    return backfilled


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

    if out_path.exists() and out_path.stat().st_size > 0:
        return str(out_path.resolve())

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
    combined_keys = load_existing_combined_keys()
    sync_existing_csv_to_combined(logger, combined_keys)

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

            key = (case_number or case_title, order_date)
            if key in existing_keys:
                continue

            try:
                docket_url, pdf_url = build_urls_from_tokens(m_case.group(1), m_doc.group(1))
            except Exception as e:
                logger.warning(f"Token decode failed: case={case_number} err={e}")
                continue

            pdf_local_path = ""
            if download:
                out_path = build_pdf_destination(case_number, case_title, order_date)
                saved = download_pdf(logger, session, pdf_url, out_path)
                if saved:
                    pdf_local_path = str(out_path.relative_to(BASE_DIR).as_posix())
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
            combined_key = build_combined_key(case_number, case_title, order_date)
            if combined_key not in combined_keys:
                append_combined_row(row)
                combined_keys.add(combined_key)
            existing_keys.add(key)
            added += 1

            if added % 25 == 0:
                logger.info(f"Progress: added={added} processed={processed}")

        logger.info(f"Done. New rows appended: {added}")
        logger.info(f"CSV: {CSV_PATH.resolve()}")
        logger.info(f"PDF root: {PDF_DIR.resolve()}")
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
