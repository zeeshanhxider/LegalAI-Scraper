#!/usr/bin/env python3
"""
Nevada Supreme Court - Advance Opinions scraper (FAST + STABLE)

Run:
  python advance_opinions.py
  python advance_opinions.py --headless
  python advance_opinions.py --limit 25

Outputs (relative to this file):
  downloads/advance_opinions/CSV/case.csv
  downloads/advance_opinions/PDF/*.pdf
  Log/advance_opinions/advance_opinions-YYYY-MM-DD.log

Notes:
- The site uses onclick="requestUrl('doc'|'case', token)" and requestUrl is NOT always window.requestUrl.
- We capture the real URL by intercepting window.open + location.assign/replace and calling requestUrl directly.
"""

from __future__ import annotations

import argparse
import base64
import csv
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from urllib3.util.retry import Retry


TARGET_URL = "https://nvcourts.gov/supreme/decisions/advance_opinions"
DATE_COLUMN = "opinion_date"

# Keep same columns as your latest runs (includes opinion_date)
EXPECTED_COLUMNS = [
    "advance_no",
    "case_number",
    "case_title",
    "opinion_filed_on",
    DATE_COLUMN,
    "docket_url",
    "pdf_url",
    "pdf_local_path",
]

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "downloads" / "advance_opinions" / "CSV" / "case.csv"
PDF_DIR = BASE_DIR / "downloads" / "advance_opinions" / "PDF"
LOG_DIR = BASE_DIR / "Log" / "advance_opinions"


# ---------------------------
# Helpers
# ---------------------------
def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def sanitize_for_filename(value: str, fallback: str = "unknown", max_len: int = 80) -> str:
    cleaned = normalize_text(value)
    cleaned = re.sub(r"[\\/:*?\"<>|]+", "_", cleaned)
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", cleaned)
    cleaned = cleaned.strip("._-")
    if not cleaned:
        cleaned = fallback
    return cleaned[:max_len]


def extract_opinion_date(opinion_filed_on: str) -> str:
    text = normalize_text(opinion_filed_on)
    if not text:
        return ""

    month_date_pattern = (
        r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|"
        r"Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}"
    )
    m = re.search(month_date_pattern, text, flags=re.IGNORECASE)
    if m:
        return normalize_text(m.group(0))

    m2 = re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", text)
    if m2:
        return normalize_text(m2.group(0))

    fallback = re.sub(r"^\s*Opinion\s+filed\s+on\s*", "", text, flags=re.IGNORECASE)
    return normalize_text(fallback) if fallback else text


def parse_year_from_text(text: str) -> str:
    m = re.search(r"(19|20)\d{2}", text or "")
    return m.group(0) if m else "unknownyear"


def build_pdf_filename(advance_no: str, case_number: str, opinion_filed_on: str) -> str:
    year = parse_year_from_text(opinion_filed_on)
    safe_advance = sanitize_for_filename(advance_no, fallback="unknownadvance")
    safe_case = sanitize_for_filename(case_number, fallback="unknowncase")
    return f"{year}_{safe_advance}_{safe_case}.pdf"


def ensure_output_dirs() -> None:
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logger() -> logging.Logger:
    run_date = datetime.now().strftime("%Y-%m-%d")
    log_path = LOG_DIR / f"advance_opinions-{run_date}.log"

    logger = logging.getLogger("advance_opinions")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def create_requests_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
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


def sync_cookies_from_selenium(driver: webdriver.Chrome, session: requests.Session) -> None:
    for c in driver.get_cookies():
        try:
            session.cookies.set(
                c.get("name", ""),
                c.get("value", ""),
                domain=c.get("domain"),
                path=c.get("path", "/"),
            )
        except Exception:
            pass


def ensure_csv_file(logger: logging.Logger) -> List[str]:
    if CSV_PATH.exists() and CSV_PATH.stat().st_size > 0:
        with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = [normalize_text(x) for x in next(reader, [])]
        return header if header else EXPECTED_COLUMNS[:]

    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(EXPECTED_COLUMNS)
    logger.info("Created new CSV with expected columns: %s", ", ".join(EXPECTED_COLUMNS))
    return EXPECTED_COLUMNS[:]


def load_existing_keys() -> Set[Tuple[str, str]]:
    keys: Set[Tuple[str, str]] = set()
    if not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0:
        return keys
    with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            adv = normalize_text(row.get("advance_no", ""))
            cas = normalize_text(row.get("case_number", ""))
            if adv and cas:
                keys.add((adv, cas))
    return keys


def append_csv_row(record: Dict[str, str], csv_header: Sequence[str]) -> None:
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([record.get(col, "") for col in csv_header])


def parse_requesturl_onclick(onclick: str) -> Tuple[str, str]:
    """
    Extract ('doc'/'case', token) from onclick like:
      requestUrl("doc", " NzA2ODRf....==")
    """
    if not onclick:
        return "", ""
    m = re.search(
        r"requestUrl\(\s*[\"'](doc|case)[\"']\s*,\s*[\"']\s*([^\"']+?)\s*[\"']\s*\)",
        onclick,
        re.I,
    )
    if not m:
        return "", ""
    return m.group(1).lower().strip(), m.group(2).strip()


def decode_token_debug(token: str) -> str:
    token = token.strip()
    try:
        return base64.b64decode(token).decode("utf-8", errors="replace")
    except Exception:
        return ""


# ---------------------------
# Selenium
# ---------------------------
def make_driver(headless: bool) -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(120)
    return driver


def wait_page_ready(driver: webdriver.Chrome, timeout: int = 30) -> None:
    try:
        WebDriverWait(driver, timeout).until(lambda d: d.execute_script("return document.readyState") == "complete")
    except Exception:
        pass


def click_search_if_present(driver: webdriver.Chrome) -> None:
    # Some environments need a Search click to populate tables
    try:
        btns = driver.find_elements(By.XPATH, "//button[normalize-space()='Search']")
        if btns:
            try:
                btns[0].click()
            except Exception:
                driver.execute_script("arguments[0].click();", btns[0])
    except Exception:
        pass


def wait_for_requesturl(driver: webdriver.Chrome, timeout: int = 40) -> None:
    """
    IMPORTANT FIX:
    requestUrl may exist as global function but NOT as window.requestUrl.
    So we check `typeof requestUrl`.
    """
    end = time.time() + timeout
    while time.time() < end:
        try:
            ok = driver.execute_script("return (typeof requestUrl === 'function');")
            if ok:
                return
        except Exception:
            pass
        time.sleep(0.25)
    raise TimeoutException("requestUrl() not available on page.")


def wait_for_rows(driver: webdriver.Chrome, timeout: int = 60) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            tables = driver.find_elements(By.CSS_SELECTOR, "#dynamic-table-wrapper table")
            if tables:
                trs = driver.find_elements(By.CSS_SELECTOR, "#dynamic-table-wrapper table tr")
                if len(trs) > 2:
                    return
        except Exception:
            pass
        time.sleep(0.3)
    raise TimeoutException("Could not find listing table rows.")


def read_all_rows(driver: webdriver.Chrome) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    tables = driver.find_elements(By.CSS_SELECTOR, "#dynamic-table-wrapper table")

    for table in tables:
        trs = table.find_elements(By.CSS_SELECTOR, "tr")
        for tr in trs:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 4:
                continue

            case_a = None
            doc_a = None

            try:
                for a in tds[1].find_elements(By.TAG_NAME, "a"):
                    if "requestUrl(" in (a.get_attribute("onclick") or ""):
                        case_a = a
                        break
            except Exception:
                pass

            try:
                for a in tds[3].find_elements(By.TAG_NAME, "a"):
                    if "requestUrl(" in (a.get_attribute("onclick") or ""):
                        doc_a = a
                        break
            except Exception:
                pass

            out.append(
                {
                    "advance_no": normalize_text(tds[0].text),
                    "case_number": normalize_text(tds[1].text),
                    "case_title": normalize_text(tds[2].text),
                    "opinion_filed_on": normalize_text(tds[3].text),
                    "case_onclick": (case_a.get_attribute("onclick") if case_a else "") or "",
                    "doc_onclick": (doc_a.get_attribute("onclick") if doc_a else "") or "",
                }
            )
    return out


def capture_url_from_requesturl(driver: webdriver.Chrome, kind: str, token: str, timeout: int = 25) -> str:
    """
    Calls requestUrl(kind, token) and captures the URL it tries to open/navigate to.

    IMPORTANT FIX:
      Uses `requestUrl(...)` directly, NOT `window.requestUrl(...)`.
    """
    kind = (kind or "").strip()
    token = (token or "").strip()
    if not kind or not token:
        return ""

    js = r"""
        const kind = arguments[0];
        const token = arguments[1];
        const timeoutMs = arguments[2];
        const done = arguments[arguments.length - 1];

        // holders
        window.__ao_capture = window.__ao_capture || {};
        window.__ao_capture.lastUrl = "";

        // hook window.open
        const oldOpen = window.open;
        window.open = function(u) {
            try { window.__ao_capture.lastUrl = u || ""; } catch(e) {}
            return null;
        };

        // hook location assign/replace (sometimes used)
        const oldAssign = window.location.assign.bind(window.location);
        const oldReplace = window.location.replace.bind(window.location);

        window.location.assign = function(u) {
            try { window.__ao_capture.lastUrl = u || ""; } catch(e) {}
            return oldAssign(u);
        };
        window.location.replace = function(u) {
            try { window.__ao_capture.lastUrl = u || ""; } catch(e) {}
            return oldReplace(u);
        };

        function cleanup() {
            try { window.open = oldOpen; } catch(e) {}
            try { window.location.assign = oldAssign; } catch(e) {}
            try { window.location.replace = oldReplace; } catch(e) {}
        }

        try {
            if (typeof requestUrl !== "function") {
                cleanup();
                done("");
                return;
            }
            requestUrl(kind, token);
        } catch (e) {
            cleanup();
            done("");
            return;
        }

        const start = Date.now();
        const timer = setInterval(() => {
            const u = (window.__ao_capture && window.__ao_capture.lastUrl) ? window.__ao_capture.lastUrl : "";
            if (u) {
                clearInterval(timer);
                cleanup();
                done(u);
                return;
            }
            if (Date.now() - start > timeoutMs) {
                clearInterval(timer);
                cleanup();
                done("");
                return;
            }
        }, 200);
    """

    try:
        url = driver.execute_async_script(js, kind, token, int(timeout * 1000))
    except WebDriverException:
        return ""

    url = (url or "").strip()
    if url and not url.startswith("http"):
        url = urljoin(driver.current_url, url)
    return url


# ---------------------------
# Download
# ---------------------------
def download_pdf_file(
    session: requests.Session,
    pdf_url: str,
    destination: Path,
    logger: logging.Logger,
    max_attempts: int = 3,
) -> Tuple[bool, str]:
    """
    Download and validate bytes start with %PDF-
    """
    if destination.exists() and destination.stat().st_size > 0:
        return True, pdf_url

    last_err = ""
    for attempt in range(1, max_attempts + 1):
        tmp = destination.with_suffix(destination.suffix + ".part")
        try:
            with session.get(pdf_url, stream=True, timeout=(20, 180), allow_redirects=True) as r:
                if r.status_code >= 400:
                    raise requests.HTTPError(f"HTTP {r.status_code}")

                final_url = r.url or pdf_url

                with tmp.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 128):
                        if chunk:
                            f.write(chunk)

            if not tmp.exists() or tmp.stat().st_size == 0:
                raise ValueError("Downloaded file is empty")

            with tmp.open("rb") as f:
                head = f.read(5)
            if head != b"%PDF-":
                raise ValueError("Downloaded content is not a valid PDF")

            tmp.replace(destination)
            return True, final_url

        except Exception as e:
            last_err = str(e)
            logger.warning("PDF download failed (attempt %s/%s): %s", attempt, max_attempts, last_err)
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            time.sleep(min(2 * attempt, 6))

    return False, last_err


# ---------------------------
# Main
# ---------------------------
def scrape(args: argparse.Namespace) -> int:
    ensure_output_dirs()
    logger = setup_logger()
    session = create_requests_session()

    logger.info("Run started: %s", datetime.now().isoformat(timespec="seconds"))
    logger.info("Target URL: %s", TARGET_URL)
    logger.info("Headless: %s", bool(args.headless))
    if args.limit:
        logger.info("Limit: %s", args.limit)

    csv_header = ensure_csv_file(logger)
    existing_keys = load_existing_keys()
    existing_pdfs: Set[str] = {p.name for p in PDF_DIR.glob("*.pdf")}

    driver: Optional[webdriver.Chrome] = None

    total = 0
    added = 0
    skipped = 0
    pdf_ok = 0
    errors = 0

    try:
        driver = make_driver(headless=True)
        driver.get(TARGET_URL)
        wait_page_ready(driver, 30)

        click_search_if_present(driver)

        # FIXED: checks typeof requestUrl
        wait_for_requesturl(driver, 45)

        wait_for_rows(driver, 70)

        rows = read_all_rows(driver)
        total = len(rows)
        logger.info("Listing rows found: %s", total)

        for idx, row in enumerate(rows, start=1):
            if args.limit and idx > args.limit:
                break

            advance_no = row["advance_no"]
            case_number = row["case_number"]
            case_title = row["case_title"]
            opinion_filed_on = row["opinion_filed_on"]
            opinion_date = extract_opinion_date(opinion_filed_on)

            key = (advance_no, case_number)
            pdf_filename = build_pdf_filename(advance_no, case_number, opinion_filed_on or opinion_date)
            pdf_path = PDF_DIR / pdf_filename

            if key in existing_keys or pdf_filename in existing_pdfs or pdf_path.exists():
                skipped += 1
                continue

            try:
                docket_url = ""
                pdf_url = ""

                ck, ctoken = parse_requesturl_onclick(row["case_onclick"])
                if ck == "case" and ctoken:
                    docket_url = capture_url_from_requesturl(driver, "case", ctoken, timeout=20)

                dk, dtoken = parse_requesturl_onclick(row["doc_onclick"])
                if dk == "doc" and dtoken:
                    pdf_url = capture_url_from_requesturl(driver, "doc", dtoken, timeout=25)

                if not pdf_url:
                    errors += 1
                    # debug token decode (optional)
                    dbg = decode_token_debug(dtoken)[:200] if dtoken else ""
                    logger.error("NO PDF [%s/%s]: advance_no=%s case_number=%s token_dbg=%s", idx, total, advance_no, case_number, dbg)
                    continue

                # Download PDF
                sync_cookies_from_selenium(driver, session)
                ok, final_or_err = download_pdf_file(session, pdf_url, pdf_path, logger, max_attempts=3)
                if not ok:
                    errors += 1
                    logger.error(
                        "PDF DOWNLOAD FAIL [%s/%s]: advance_no=%s case_number=%s url=%s err=%s",
                        idx, total, advance_no, case_number, pdf_url, final_or_err
                    )
                    continue

                pdf_ok += 1
                existing_pdfs.add(pdf_filename)

                record = {
                    "advance_no": advance_no,
                    "case_number": case_number,
                    "case_title": case_title,
                    "opinion_filed_on": opinion_filed_on,
                    DATE_COLUMN: opinion_date,
                    "docket_url": docket_url,
                    "pdf_url": final_or_err,
                    "pdf_local_path": pdf_filename,
                }
                append_csv_row(record, csv_header)
                existing_keys.add(key)
                added += 1

                logger.info("Saved [%s/%s]: advance_no=%s case_number=%s pdf=%s", idx, total, advance_no, case_number, pdf_filename)

            except Exception as e:
                errors += 1
                logger.exception(
                    "RECORD ERROR [%s/%s]: advance_no=%s case_number=%s err=%s",
                    idx, total, advance_no, case_number, e
                )

        logger.info("Scraping finished")
        logger.info("Summary: total=%s added=%s pdf_ok=%s skipped=%s errors=%s", total, added, pdf_ok, skipped, errors)
        return 0

    except Exception as e:
        logger.exception("Fatal error: %s", e)
        return 1
    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scrape Nevada Advance Opinions and download PDFs (no DB).")
    p.add_argument("--headless", action="store_false", help="Run Chrome visible (default headless)")
    p.add_argument("--limit", type=int, default=None, help="Process only first N rows (testing)")
    return p.parse_args()


if __name__ == "__main__":
    raise SystemExit(scrape(parse_args()))