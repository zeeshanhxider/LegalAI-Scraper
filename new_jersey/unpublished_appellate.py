#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NJ Courts - Unpublished Appellate Court Opinions scraper
URL: https://www.njcourts.gov/attorneys/opinions/unpublished-appellate

CSV columns (as requested):
- date
- no
- court
- case_name

Also includes:
- page_url
- pdf_url
- pdf_file
- download_status

Outputs:
- download/unpublished_appellate/unpublished_appellate.csv
- download/unpublished_appellate/file/*.pdf
"""

import csv
import os
import re
import sys
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


START_URL = "https://www.njcourts.gov/attorneys/opinions/unpublished-appellate"
BASE = "https://www.njcourts.gov"

OUT_DIR = os.path.join("download", "unpublished_appellate")
FILES_DIR = os.path.join(OUT_DIR, "file")
CSV_PATH = os.path.join(OUT_DIR, "unpublished_appellate.csv")

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari/537.36"
PAGE_DELAY_SEC = 0.35

MAX_PAGES = None   # set to 2 for testing
MAX_ITEMS = None   # set to 50 for testing


def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(FILES_DIR, exist_ok=True)


def clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def safe_filename(name: str, default="file") -> str:
    name = clean_ws(name)
    name = re.sub(r"[^\w\-. ()\[\]/]+", "_", name)
    name = name.replace("/", "_")
    name = name.strip("._ ")
    return name if name else default


def get_html(session: requests.Session, url: str, timeout=60) -> str:
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
            tmp = out_path + ".part"
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 128):
                    if chunk:
                        f.write(chunk)
            os.replace(tmp, out_path)
        return True
    except Exception as e:
        print(f"   ⚠️ PDF download failed: {url} -> {e}")
        return False


def find_next_page_url(soup: BeautifulSoup, current_url: str) -> str:
    a = soup.select_one("nav.njcourts-pager a[rel='next']")
    if a and a.get("href"):
        return urljoin(current_url, a["href"].strip())
    return ""


def parse_cards(soup: BeautifulSoup, page_url: str):
    """
    Each item:
      article.w-100 -> div.card.mb-3

    PDF link:
      .card-title a.text-underline-hover[href]   (href points to /system/files/...)

    Docket + Court:
      span.badge (first is docket, second is court type)

    Date:
      .small.text-muted
    """
    cards = soup.select("article.w-100 div.card.mb-3")
    rows = []

    for card in cards:
        title_a = card.select_one(".card-title a.text-underline-hover[href]")
        if not title_a:
            continue

        case_name = clean_ws(title_a.get_text(" ", strip=True))

        pdf_href = (title_a.get("href") or "").strip()
        pdf_url = urljoin(BASE, pdf_href) if pdf_href else ""

        badges = [clean_ws(b.get_text(" ", strip=True)) for b in card.select("span.badge") if clean_ws(b.get_text())]
        no = badges[0] if len(badges) >= 1 else ""
        court = badges[1] if len(badges) >= 2 else ""

        date_el = card.select_one(".small.text-muted")
        date = clean_ws(date_el.get_text(" ", strip=True)) if date_el else ""

        pdf_file = ""
        if pdf_url:
            base = os.path.basename(urlparse(pdf_url).path) or "opinion.pdf"
            pdf_file = safe_filename(f"{no}_{base}" if no else base, default="opinion.pdf")
            if not pdf_file.lower().endswith(".pdf"):
                pdf_file += ".pdf"

        rows.append({
            "date": date,
            "no": no,
            "court": court,
            "case_name": case_name,
            "page_url": page_url,
            "pdf_url": pdf_url,
            "pdf_file": pdf_file,
        })

    return rows


def main():
    ensure_dirs()

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    file_exists = os.path.exists(CSV_PATH) and os.path.getsize(CSV_PATH) > 0
    csv_f = open(CSV_PATH, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(
        csv_f,
        fieldnames=[
            "date",
            "no",
            "court",
            "case_name",
            "page_url",
            "pdf_url",
            "pdf_file",
            "download_status",
        ],
    )
    if not file_exists:
        writer.writeheader()
        csv_f.flush()

    url = START_URL
    visited = set()
    page_count = 0
    item_count = 0

    while url:
        if url in visited:
            print(f"⚠️ loop detected, stopping: {url}")
            break
        visited.add(url)

        page_count += 1
        if MAX_PAGES is not None and page_count > MAX_PAGES:
            print("Reached MAX_PAGES. Stop.")
            break

        print(f"\nPage {page_count}: {url}")
        html = get_html(session, url)
        soup = BeautifulSoup(html, "html.parser")

        rows = parse_cards(soup, url)
        print(f" items found: {len(rows)}")
        if not rows:
            print("No items found. Stop.")
            break

        for r in rows:
            status = "no-pdf"
            if r["pdf_url"] and r["pdf_file"]:
                out_path = os.path.join(FILES_DIR, r["pdf_file"])
                ok = download_file(session, r["pdf_url"], out_path)
                status = "pdf:ok" if ok else "pdf:fail"

            writer.writerow({**r, "download_status": status})
            csv_f.flush()

            item_count += 1
            if MAX_ITEMS is not None and item_count >= MAX_ITEMS:
                print("Reached MAX_ITEMS. Stop.")
                url = ""
                break

        next_url = find_next_page_url(soup, url)
        if not next_url:
            print("No next page. Done.")
            break

        url = next_url
        time.sleep(PAGE_DELAY_SEC)

    csv_f.close()

    print("\n✅ DONE")
    print(f"CSV : {CSV_PATH}")
    print(f"PDFs: {FILES_DIR}")
    print(f"Pages scraped: {page_count}")
    print(f"Items scraped: {item_count}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)
