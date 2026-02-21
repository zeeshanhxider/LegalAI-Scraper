# scrapingfile.py
# Maryland opinions index scraper (Open in Chrome) + parse ALL records (~9862)
# RESUME-SAFE + NO DUPLICATES + CSV ONE-BY-ONE (NOT IN BUNCH)
#
# Key rules implemented:
# 1) Dedup uses case_pdf_url from CSV (NO DB)
# 2) Resume-safe: CSV row is written ONLY after PDF exists (download ok OR already on disk)
# 3) If run stops/crashes, next run will skip already-saved CSV rows and already-downloaded PDFs
# 4) Logs written date-wise + also printed in terminal
#
# Requirements:
#   pip install selenium requests beautifulsoup4
# ChromeDriver must be installed and in PATH.

import os
import re
import csv
import time
import hashlib
import logging
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


URL = "https://www.courts.state.md.us/cgi-bin/indexlist.pl?court=both&year=all&order=bydate&submit=Submit"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CSV_DIR = os.path.join(BASE_DIR, "downloads", "CSV")
PDF_DIR = os.path.join(BASE_DIR, "downloads", "PDF")
LOG_DIR = os.path.join(BASE_DIR, "Log")
CSV_PATH = os.path.join(CSV_DIR, "cases.csv")

CSV_COLUMNS = [
    "line_no",
    "docket_term",
    "citation",
    "filed_date",
    "judge",
    "parties",
    "case_pdf_url",
    "pdf_local_path",
]

# keep Chrome visible
HEADLESS = True

# PDF download settings
PDF_RETRIES = 3
PDF_DELAY_SECONDS = 0.10


def ensure_dirs():
    os.makedirs(CSV_DIR, exist_ok=True)
    os.makedirs(PDF_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)


def setup_logger():
    run_date = datetime.now().strftime("%Y-%m-%d")
    log_path = os.path.join(LOG_DIR, f"scraper-{run_date}.log")

    logger = logging.getLogger("md_case_scraper")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

        # file
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        # terminal
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    return logger


def ensure_csv_header():
    if os.path.exists(CSV_PATH):
        return
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()


def load_existing_keys():
    """
    Dedup across runs using case_pdf_url stored in CSV.
    """
    keys = set()
    if not os.path.exists(CSV_PATH):
        return keys

    with open(CSV_PATH, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            u = (r.get("case_pdf_url") or "").strip()
            if u:
                keys.add(u)
    return keys


def norm(s: str) -> str:
    return " ".join((s or "").split())


def slugify(text: str, max_len: int = 140) -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^a-z0-9\s\-_./]", "", text)
    text = text.replace("/", "-").replace(".", "-")
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return (text or "file")[:max_len]


def safe_pdf_filename(docket_term: str, filed_date: str, parties: str, case_pdf_url: str) -> str:
    base = slugify(f"{docket_term}_{filed_date}_{parties}")
    url_hash = hashlib.md5((case_pdf_url or "").encode("utf-8")).hexdigest()[:8]
    return f"{base}-{url_hash}.pdf"


def build_driver():
    chrome_options = Options()
    if HEADLESS:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--window-size=1400,900")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=chrome_options)


def download_pdf(session: requests.Session, url: str, dest_path: str, logger: logging.Logger) -> bool:
    """
    Lazy download:
    - If already exists (non-empty), return True
    - Download to .part then rename
    - Return True ONLY if success
    """
    if not url:
        return False

    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
        return True

    tmp = dest_path + ".part"
    headers = {"User-Agent": "Mozilla/5.0 (LegalAI-Scraper/1.0)"}

    for attempt in range(1, PDF_RETRIES + 1):
        try:
            with session.get(url, headers=headers, timeout=60, stream=True) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)
            os.replace(tmp, dest_path)
            logger.info(f"PDF downloaded: {url} -> {dest_path}")
            return True
        except Exception as e:
            logger.error(f"PDF download failed {attempt}/{PDF_RETRIES}: {url} | {e}")
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass
            time.sleep(2 * attempt)

    return False


def extract_grouped_record_lines_and_pdf_urls(html: str, base_url: str, logger: logging.Logger):
    """
    The page text sometimes splits a single record across multiple lines.
    We group lines starting with docket_term (e.g., 36ag/25) until we hit line_no (digits).
    """
    soup = BeautifulSoup(html, "html.parser")

    # PDF URLs in DOM order
    pdf_urls = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        abs_url = urljoin(base_url, href)
        if "/data/opinions/" in abs_url and abs_url.lower().endswith(".pdf"):
            pdf_urls.append(abs_url)

    # Extract text lines
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

    # Find "CASE PDF" section start
    start_idx = None
    for i, ln in enumerate(lines):
        if ln.strip() == "CASE PDF":
            start_idx = i + 1
            break

    if start_idx is None:
        logger.error("Could not find 'CASE PDF' header.")
        return [], []

    docket_re = re.compile(r"^[0-9]{1,6}[A-Za-z]{0,4}/[0-9]{2}$")

    grouped = []
    current = []

    for ln in lines[start_idx:]:
        # Start of record
        if docket_re.match(ln):
            if current:
                grouped.append(" ".join(current))
                current = []
            current.append(ln)
            continue

        # Continue record
        if current:
            current.append(ln)
            # End of record at line number
            if ln.isdigit():
                grouped.append(" ".join(current))
                current = []

    if current:
        grouped.append(" ".join(current))

    logger.info(f"Found record text lines (grouped): {len(grouped)}")
    logger.info(f"Found PDF urls: {len(pdf_urls)}")
    return grouped, pdf_urls


def parse_record_line(grouped_line: str):
    grouped_line = norm(grouped_line)
    if not grouped_line:
        return None

    tokens = grouped_line.split()
    if len(tokens) < 5:
        return None

    docket_term = tokens[0]
    line_no = tokens[-1] if tokens[-1].isdigit() else ""

    # Remove docket and line_no
    core = grouped_line[len(docket_term):].strip()
    if line_no and core.endswith(line_no):
        core = core[: -len(line_no)].strip()

    # Extract filed_date (with optional corrected)
    m = re.search(r"\b(\d{4}-\d{2}-\d{2})(?:\s+corrected\s+(\d{4}-\d{2}-\d{2}))?\b", core)
    if not m:
        return None

    d1 = m.group(1)
    d2 = m.group(2)
    filed_date = d1 if not d2 else f"{d1} corrected {d2}"

    # Everything before date = citation
    citation = core[: m.start()].strip()

    # Everything after date = judge + parties
    after_date = core[m.end():].strip()

    # Split judge and parties
    parts = after_date.split(" ", 1)

    judge = parts[0].strip()
    parties = parts[1].strip() if len(parts) > 1 else ""

    return {
        "line_no": line_no,
        "docket_term": docket_term,
        "citation": citation,
        "filed_date": filed_date,
        "judge": judge,
        "parties": parties,
    }

def main():
    ensure_dirs()
    logger = setup_logger()
    logger.info("Run started")

    ensure_csv_header()
    existing_keys = load_existing_keys()
    logger.info(f"Existing PDF URLs in CSV: {len(existing_keys)}")

    driver = None
    session = requests.Session()

    total_records = 0
    new_rows_added = 0
    pdf_downloaded = 0
    parse_failed = 0
    skipped_pdf_failed = 0

    try:
        # Open in Chrome
        driver = build_driver()
        driver.get(URL)
        time.sleep(2)

        html = driver.page_source

        grouped_lines, pdf_urls = extract_grouped_record_lines_and_pdf_urls(html, URL, logger)

        # safe alignment
        n = min(len(grouped_lines), len(pdf_urls))
        total_records = n
        logger.info(f"Processing records: {n}")

        with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)

            for i in range(n):
                pdf_url = (pdf_urls[i] or "").strip()
                if not pdf_url:
                    continue

                # 1) If already in CSV => skip (no duplicates)
                if pdf_url in existing_keys:
                    continue

                parsed = parse_record_line(grouped_lines[i])
                if not parsed:
                    logger.error(f"Failed to parse record: {grouped_lines[i]}")
                    parse_failed += 1
                    continue

                row = {
                    "line_no": parsed["line_no"],
                    "docket_term": parsed["docket_term"],
                    "citation": parsed["citation"],
                    "filed_date": parsed["filed_date"],
                    "judge": parsed["judge"],
                    "parties": parsed["parties"],
                    "case_pdf_url": pdf_url,
                    "pdf_local_path": "",
                }

                pdf_name = safe_pdf_filename(row["docket_term"], row["filed_date"], row["parties"], pdf_url)
                pdf_path = os.path.join(PDF_DIR, pdf_name)
                row["pdf_local_path"] = pdf_path

                # 2) If PDF already exists => write CSV NOW (resume-safe)
                if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0:
                    writer.writerow(row)               # ONE BY ONE
                    existing_keys.add(pdf_url)         # mark only after CSV write
                    continue

                # 3) Download PDF
                ok = download_pdf(session, pdf_url, pdf_path, logger)
                if ok:
                    pdf_downloaded += 1

                # 4) Write CSV only if PDF exists (download ok OR already exists)
                if ok and os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0:
                    writer.writerow(row)               # ONE BY ONE
                    existing_keys.add(pdf_url)         # mark only after CSV write
                    new_rows_added += 1
                elif os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0:
                    # if it exists for any reason, still write row
                    writer.writerow(row)
                    existing_keys.add(pdf_url)
                    new_rows_added += 1
                else:
                    logger.error(f"Skip CSV because PDF failed: {pdf_url}")
                    skipped_pdf_failed += 1

                time.sleep(PDF_DELAY_SECONDS)

        logger.info(f"Parse failed: {parse_failed}")
        logger.info(f"Skipped (PDF failed): {skipped_pdf_failed}")
        logger.info("Run finished")

        print(
            f"Total records processed: {total_records} | New rows added: {new_rows_added} | "
            f"PDFs downloaded: {pdf_downloaded} | Parse failed: {parse_failed} | "
            f"Skipped(PDF failed): {skipped_pdf_failed} | CSV: {CSV_PATH}"
        )

    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    main()