#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NC Courts - Business Court Opinions scraper
URL: https://www.nccourts.gov/documents/business-court-opinions

Outputs:
- CSV : download/business_court_opinions/business_court_opinions.csv
- Files: download/business_court_opinions/file/

Notes:
- Business court pages have an EMPTY court span in meta; we set court="Business Court"
- Status is the LAST meta <span> (Published/Unpublished)
- Docket/judge/location is the middle meta span like "23-CVS-17361 (Mecklenburg - Julianna Theall Earp)"
- Mostly PDF only; ZIP usually not present on this page (zip_url will stay empty)
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


BASE_URL = "https://www.nccourts.gov/documents/business-court-opinions"

OUT_ROOT = os.path.join("download", "business_court_opinions")
FILES_DIR = os.path.join(OUT_ROOT, "file")
CSV_PATH = os.path.join(OUT_ROOT, "business_court_opinions.csv")

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari/537.36"

PAGE_DELAY_SEC = 0.5
MAX_PAGES = None   # set e.g. 5 for test
MAX_ITEMS = None   # set e.g. 50 for test


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
    os.makedirs(OUT_ROOT, exist_ok=True)
    os.makedirs(FILES_DIR, exist_ok=True)


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


def fetch_html(session: requests.Session, url: str, timeout=60) -> str:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


def download_file(session: requests.Session, url: str, out_path: str, timeout=120) -> bool:
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


def get_next_page_url(soup: BeautifulSoup, current_url: str) -> str:
    # Drupal pager uses rel="next"
    a = soup.select_one('nav.pagination a[rel="next"]')
    if a and a.has_attr("href"):
        return urljoin(current_url, a["href"].strip())
    # fallback
    a = soup.select_one("li.pagination__next a")
    if a and a.has_attr("href"):
        return urljoin(current_url, a["href"].strip())
    return ""


def parse_article(article) -> Row:
    # Title / page
    title_a = article.select_one("h5.list__title a")
    page_url = ""
    case_name = ""
    if title_a and title_a.has_attr("href"):
        page_url = urljoin("https://www.nccourts.gov", title_a["href"].strip())
        case_name = clean_ws(title_a.get_text())

    # Date
    time_el = article.select_one(".meta time")
    date = clean_ws(time_el.get_text()) if time_el else ""

    # Meta spans (some are empty)
    spans = []
    for s in article.select(".meta span"):
        t = clean_ws(s.get_text())
        if t:
            spans.append(t)

    # Business Court: meta spans typically are:
    # [ "23-CVS-17361 (Mecklenburg - Julianna Theall Earp)", "Published" ]
    docket = spans[0] if len(spans) >= 1 else ""
    status = spans[-1] if len(spans) >= 1 else ""

    court = "Business Court"  # FIXED (because site leaves court span empty)

    # Description text
    desc_wrap = article.select_one('[itemprop="description"]')
    description = ""
    if desc_wrap:
        p = desc_wrap.find("p")
        description = clean_ws(p.get_text()) if p else ""

    # File links
    pdf_url = ""
    zip_url = ""

    # PDF is in file--teaser a[itemprop=url]
    file_a = article.select_one(".file--teaser a[itemprop='url']")
    if file_a and file_a.has_attr("href"):
        href = file_a["href"].strip()
        # relative -> absolute
        pdf_url = urljoin("https://www.nccourts.gov", href)

    # ZIP not shown in your HTML; keep support anyway (if appears later)
    zip_a = article.select_one("a.zip_link")
    if zip_a and zip_a.has_attr("href"):
        zip_url = urljoin("https://www.nccourts.gov", zip_a["href"].strip())

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


def main():
    ensure_dirs()

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    # Streaming CSV writer (append-safe)
    file_exists = os.path.exists(CSV_PATH) and os.path.getsize(CSV_PATH) > 0
    csv_f = open(CSV_PATH, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(
        csv_f,
        fieldnames=[
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
        ],
    )
    if not file_exists:
        writer.writeheader()
        csv_f.flush()

    url = BASE_URL
    visited = set()

    pages = 0
    rows = 0

    while url:
        if url in visited:
            print(f"⚠️ Loop detected, stop: {url}")
            break
        visited.add(url)

        pages += 1
        if MAX_PAGES is not None and pages > MAX_PAGES:
            print("Reached MAX_PAGES. Stop.")
            break

        print(f"\nPage {pages} | {url}")
        html = fetch_html(session, url)
        soup = BeautifulSoup(html, "html.parser")

        articles = soup.select("section.list--documents article.list__item")
        print(f" items found: {len(articles)}")
        if not articles:
            print("No items. Stop.")
            break

        for art in articles:
            row = parse_article(art)

            status_parts = []

            # PDF download
            if row.pdf_url:
                # build safe filename: case name + date, keep original pdf name too
                base_name = safe_filename(f"{row.case_name}_{row.date}")
                orig_name = pick_filename_from_url(row.pdf_url)
                if not orig_name.lower().endswith(".pdf"):
                    orig_name = base_name + ".pdf"

                # make filename unique using VersionId if present
                version_id = ""
                try:
                    qs = parse_qs(urlparse(row.pdf_url).query)
                    if "VersionId" in qs and qs["VersionId"]:
                        version_id = qs["VersionId"][0]
                except Exception:
                    pass

                if version_id:
                    out_name = f"{base_name}_{version_id}.pdf"
                else:
                    out_name = f"{base_name}_{orig_name}"

                out_name = safe_filename(out_name, default="opinion.pdf")
                if not out_name.lower().endswith(".pdf"):
                    out_name += ".pdf"

                pdf_path = os.path.join(FILES_DIR, out_name)
                ok = download_file(session, row.pdf_url, pdf_path)
                row.downloaded_pdf = "pdf:ok" if ok else "pdf:fail"
                status_parts.append(row.downloaded_pdf)

            # ZIP download (if ever present)
            if row.zip_url:
                zip_name = safe_filename(f"{row.case_name}_{row.date}") + ".zip"
                zip_path = os.path.join(FILES_DIR, zip_name)
                ok = download_file(session, row.zip_url, zip_path)
                row.downloaded_zip = "zip:ok" if ok else "zip:fail"
                status_parts.append(row.downloaded_zip)

            row.download_status = "|".join(status_parts) if status_parts else "no-file"

            # Write one-by-one
            writer.writerow(
                {
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
            )
            csv_f.flush()

            rows += 1
            if MAX_ITEMS is not None and rows >= MAX_ITEMS:
                print("Reached MAX_ITEMS. Stop.")
                url = ""
                break

        next_url = get_next_page_url(soup, url)
        if not next_url:
            print("No Next link. Done.")
            break

        url = next_url
        time.sleep(PAGE_DELAY_SEC)

    csv_f.close()
    print("\n✅ Done")
    print(f"CSV  : {CSV_PATH}")
    print(f"Files: {FILES_DIR}")
    print(f"Pages: {pages}")
    print(f"Rows : {rows}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)
