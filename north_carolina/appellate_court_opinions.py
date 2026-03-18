#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NC Courts - Appellate Court Opinions scraper
URL: https://www.nccourts.gov/documents/appellate-court-opinions

Outputs:
- CSV: downloads/<court_name>/nc_appellate_opinions.csv
- Merged CSV: downloads/CSV/north_carolina_opinions_merged.csv
- Files (pdf/zip): downloads/<court_name>/<year>/<case_name>/

Features:
- Follows pagination using "Next" until it disappears (fixes the "only 9 pages" bug)
- Correct parsing of court/status (court = first span, status = last span)
- Streams CSV rows as it scrapes (writes one-by-one)
- Downloads PDFs and ZIPs
"""

import csv
import os
import re
import sys
import time
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.nccourts.gov/documents/appellate-court-opinions"

DOWNLOADS_DIR = "downloads"
CSV_FILENAME = "nc_appellate_opinions.csv"
MERGED_CSV_DIR = os.path.join(DOWNLOADS_DIR, "CSV")
MERGED_CSV_PATH = os.path.join(MERGED_CSV_DIR, "north_carolina_opinions_merged.csv")
CSV_FIELDS = [
    "date",
    "court",
    "status",
    "docket",
    "case_name",
    "description",
    "page_url",
    "pdf_url",
    "zip_url",
    "downloaded_pdf",
    "downloaded_zip",
    "download_status",
]

# polite delay between page fetches
PAGE_DELAY_SEC = 0.5

# Optional safety limits (set None for no limit)
MAX_PAGES = None        # e.g. 50 for testing, None for all
MAX_ITEMS = None        # e.g. 200 for testing, None for all

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari/537.36"


@dataclass
class Row:
    date: str
    court: str
    status: str
    docket: str
    case_name: str
    description: str
    page_url: str
    pdf_url: str
    zip_url: str
    downloaded_pdf: str
    downloaded_zip: str
    download_status: str


def ensure_dirs():
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)
    os.makedirs(MERGED_CSV_DIR, exist_ok=True)


def clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def safe_filename(name: str, default: str = "file") -> str:
    name = clean_ws(name)
    name = re.sub(r"[^\w\-. ()\[\]]+", "_", name)
    name = name.strip("._ ")
    return name if name else default


def pick_filename_from_url(url: str) -> str:
    try:
        path = urlparse(url).path
        base = os.path.basename(path)
        if base:
            return base
    except Exception:
        pass
    return "download.bin"


def extract_year(date_text: str) -> str:
    m = re.search(r"\b(19|20)\d{2}\b", date_text or "")
    return m.group(0) if m else "unknown_year"


def get_court_dir(court: str) -> str:
    court_folder = safe_filename(court, default="unknown_court")
    court_dir = os.path.join(DOWNLOADS_DIR, court_folder)
    os.makedirs(court_dir, exist_ok=True)
    return court_dir


def get_case_dir(court: str, date_text: str, case_name: str) -> str:
    court_dir = get_court_dir(court)
    year_dir = os.path.join(court_dir, extract_year(date_text))
    case_dir = os.path.join(year_dir, safe_filename(case_name, default="unknown_case"))
    os.makedirs(case_dir, exist_ok=True)
    return case_dir


def get_csv_writer(court: str, csv_writers: dict, csv_files: dict):
    court_dir = get_court_dir(court)
    csv_path = os.path.join(court_dir, CSV_FILENAME)
    if csv_path in csv_writers:
        return csv_writers[csv_path], csv_files[csv_path], csv_path

    file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0
    csv_f = open(csv_path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_f, fieldnames=CSV_FIELDS)
    if not file_exists:
        writer.writeheader()
        csv_f.flush()

    csv_writers[csv_path] = writer
    csv_files[csv_path] = csv_f
    return writer, csv_f, csv_path


def download_file(session: requests.Session, url: str, out_path: str, timeout=60) -> bool:
    if not url:
        return False
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return True

    try:
        with session.get(url, stream=True, timeout=timeout, allow_redirects=True) as r:
            r.raise_for_status()
            tmp_path = out_path + ".part"
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 128):
                    if chunk:
                        f.write(chunk)
            os.replace(tmp_path, out_path)
        return True
    except Exception as e:
        print(f"   ⚠️ download failed: {url} -> {e}")
        return False


def fetch_html(session: requests.Session, url: str, timeout=60) -> str:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


def parse_article(article) -> Row:
    # page_url
    title_a = article.select_one("h5.list__title a")
    page_url = title_a["href"].strip() if title_a and title_a.has_attr("href") else ""
    page_url = urljoin("https://www.nccourts.gov", page_url)

    # date
    time_el = article.select_one(".meta time")
    date = clean_ws(time_el.get_text()) if time_el else ""

    # meta spans: variable length
    spans = [clean_ws(s.get_text()) for s in article.select(".meta span") if clean_ws(s.get_text())]
    court = spans[1] if len(spans) >= 3 else ""
    status = spans[-1] if len(spans) >= 2 else ""
    docket = spans[0] if len(spans) >= 1 else ""  # only present for opinion rows

    # case name
    case_name = clean_ws(title_a.get_text()) if title_a else ""

    # description text inside itemprop=description (but avoid "download/view" words)
    desc_wrap = article.select_one('[itemprop="description"]')
    description = ""
    if desc_wrap:
        # Prefer explicit <p> content
        p = desc_wrap.find("p")
        description = clean_ws(p.get_text()) if p else ""
    description = description or ""

    pdf_url = ""
    zip_url = ""

    pdf_link = article.select_one("a.pdf_link")
    if pdf_link and pdf_link.has_attr("href"):
        pdf_url = pdf_link["href"].strip()

    zip_link = article.select_one("a.zip_link")
    if zip_link and zip_link.has_attr("href"):
        zip_url = zip_link["href"].strip()

    pdf_url = urljoin("https://www.nccourts.gov", pdf_url) if pdf_url else ""
    zip_url = urljoin("https://www.nccourts.gov", zip_url) if zip_url else ""

    return Row(
        date=date,
        court=court,
        status=status,
        docket=docket,
        case_name=case_name,
        description=description,
        page_url=page_url,
        pdf_url=pdf_url,
        zip_url=zip_url,
        downloaded_pdf="",
        downloaded_zip="",
        download_status="",
    )


def get_next_page_url(soup: BeautifulSoup, current_url: str) -> str:
    # Use rel="next" when available
    next_a = soup.select_one('nav.pagination a[rel="next"]')
    if next_a and next_a.has_attr("href"):
        href = next_a["href"].strip()
        return urljoin(current_url, href)

    # fallback: find "Next" link
    next_a = soup.select_one('nav.pagination li.pagination__next a')
    if next_a and next_a.has_attr("href"):
        return urljoin(current_url, next_a["href"].strip())

    return ""


def main():
    ensure_dirs()

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    # One CSV per court folder: downloads/<court_name>/nc_appellate_opinions.csv
    csv_writers = {}
    csv_files = {}

    # Shared merged CSV: downloads/CSV/north_carolina_opinions_merged.csv
    merged_exists = os.path.exists(MERGED_CSV_PATH) and os.path.getsize(MERGED_CSV_PATH) > 0
    merged_csv_f = open(MERGED_CSV_PATH, "a", newline="", encoding="utf-8")
    merged_writer = csv.DictWriter(merged_csv_f, fieldnames=CSV_FIELDS)
    if not merged_exists:
        merged_writer.writeheader()
        merged_csv_f.flush()

    count_items = 0
    count_pages = 0

    url = BASE_URL
    visited = set()

    while url:
        if url in visited:
            print(f"⚠️ Detected loop, stopping. url={url}")
            break
        visited.add(url)

        count_pages += 1
        if MAX_PAGES is not None and count_pages > MAX_PAGES:
            print("Reached MAX_PAGES. Stop.")
            break

        print(f"\nPage {count_pages} | {url}")
        html = fetch_html(session, url)
        soup = BeautifulSoup(html, "html.parser")

        articles = soup.select("section.list--documents article.list__item")
        print(f" items found: {len(articles)}")

        if not articles:
            print("No items on this page. Stop.")
            break

        for art in articles:
            row = parse_article(art)
            case_dir = get_case_dir(row.court, row.date, row.case_name)

            # Download file(s)
            status_parts = []

            if row.zip_url:
                # build filename
                # Example zip url: getopzip.php?c=2&d=...
                # We'll name it with date + status for clarity
                zip_name = safe_filename(f"{row.case_name} {row.date}") + ".zip"
                zip_path = os.path.join(case_dir, zip_name)
                ok = download_file(session, row.zip_url, zip_path)
                row.downloaded_zip = "zip:ok" if ok else "zip:fail"
                status_parts.append(row.downloaded_zip)

            if row.pdf_url:
                # pdf may have query param pdf=xxxxx; keep a nice filename
                # try to extract pdf id for uniqueness
                pdf_id = ""
                try:
                    qs = parse_qs(urlparse(row.pdf_url).query)
                    if "pdf" in qs and qs["pdf"]:
                        pdf_id = qs["pdf"][0]
                except Exception:
                    pass

                base = safe_filename(row.case_name)
                if pdf_id:
                    pdf_name = f"{base}_{pdf_id}.pdf"
                else:
                    pdf_name = pick_filename_from_url(row.pdf_url)
                    if not pdf_name.lower().endswith(".pdf"):
                        pdf_name = f"{base}.pdf"

                pdf_path = os.path.join(case_dir, pdf_name)
                ok = download_file(session, row.pdf_url, pdf_path)
                row.downloaded_pdf = "pdf:ok" if ok else "pdf:fail"
                status_parts.append(row.downloaded_pdf)

            if not status_parts:
                row.download_status = "no-file"
            else:
                row.download_status = "|".join(status_parts)

            # Write streaming CSV row in the corresponding court folder
            writer, csv_f, _ = get_csv_writer(row.court, csv_writers, csv_files)
            row_data = {
                "date": row.date,
                "court": row.court,
                "status": row.status,
                "docket": row.docket,
                "case_name": row.case_name,
                "description": row.description,
                "page_url": row.page_url,
                "pdf_url": row.pdf_url,
                "zip_url": row.zip_url,
                "downloaded_pdf": row.downloaded_pdf,
                "downloaded_zip": row.downloaded_zip,
                "download_status": row.download_status,
            }
            writer.writerow(row_data)
            csv_f.flush()
            merged_writer.writerow(row_data)
            merged_csv_f.flush()

            count_items += 1
            if MAX_ITEMS is not None and count_items >= MAX_ITEMS:
                print("Reached MAX_ITEMS. Stop.")
                url = ""
                break

        # Follow Next until it disappears (THIS fixes your 9-page issue)
        next_url = get_next_page_url(soup, url)
        if not next_url:
            print("No Next page link found. Done.")
            break

        url = next_url
        time.sleep(PAGE_DELAY_SEC)

    for csv_f in csv_files.values():
        csv_f.close()
    merged_csv_f.close()

    print("\n✅ Done")
    print(f"Download root: {DOWNLOADS_DIR}")
    print(f"Merged CSV: {MERGED_CSV_PATH}")
    print(f"CSV files written: {len(csv_files)}")
    for csv_path in sorted(csv_files):
        print(f" - {csv_path}")
    print(f"Pages scraped: {count_pages}")
    print(f"Rows written  : {count_items}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)
