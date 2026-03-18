#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NJ Courts - Published Appellate Court Opinions scraper
URL: https://www.njcourts.gov/attorneys/opinions/published-appellate

CSV columns:
- date
- no
- court
- case_name
- description

Also includes:
- page_url
- pdf_url
- pdf_file
- pdf_full_path
- download_status

Folder structure:
- downloads/published_appellate/CSV/published_appellate.csv
- downloads/published_appellate/<year>/<no>/<pdf file>.pdf
"""

import csv
import hashlib
import os
import re
import sys
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


START_URL = "https://www.njcourts.gov/attorneys/opinions/published-appellate"
BASE = "https://www.njcourts.gov"

COURT_FOLDER = "published_appellate"

BASE_DIR = os.path.join("downloads", COURT_FOLDER)
CSV_DIR = os.path.join(BASE_DIR, "CSV")
CSV_PATH = os.path.join(CSV_DIR, "published_appellate.csv")

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari/537.36"
PAGE_DELAY_SEC = 0.35

MAX_PAGES = None   # set to 2 for testing
MAX_ITEMS = None   # set to 50 for testing
MAX_FILENAME_LEN = 150


def ensure_dirs():
    os.makedirs(BASE_DIR, exist_ok=True)
    os.makedirs(CSV_DIR, exist_ok=True)


def clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def safe_filename(name: str, default="file", max_len: int = MAX_FILENAME_LEN) -> str:
    name = clean_ws(name)
    name = re.sub(r'[<>:"\\|?*]+', "_", name)
    name = name.replace("/", "_").replace("\\", "_")
    name = name.strip("._ ")
    if not name:
        return default

    if len(name) <= max_len:
        return name

    base, ext = os.path.splitext(name)
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:10]
    suffix = f"_{digest}{ext}" if ext else f"_{digest}"
    keep = max_len - len(suffix)
    if keep <= 0:
        return (digest + ext)[:max_len]
    return f"{base[:keep]}{suffix}"


def safe_folder_name(name: str, default="unknown") -> str:
    name = clean_ws(name)
    name = re.sub(r'[<>:"\\|?*]+', "_", name)
    name = name.replace("/", "_").replace("\\", "_")
    name = name.strip("._ ")
    return name if name else default


def extract_year(date_text: str) -> str:
    if not date_text:
        return "unknown_year"

    m = re.search(r"\b(19|20)\d{2}\b", date_text)
    if m:
        return m.group(0)

    return "unknown_year"


def get_html(session: requests.Session, url: str, timeout=60) -> str:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


def download_file(session: requests.Session, url: str, out_path: str, timeout=120) -> bool:
    if not url:
        return False

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return True

    tmp = out_path + ".part"
    try:
        with session.get(url, stream=True, timeout=timeout, allow_redirects=True) as r:
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 128):
                    if chunk:
                        f.write(chunk)
        os.replace(tmp, out_path)
        return True
    except Exception as e:
        print(f"   ⚠️ PDF download failed: {url} -> {e}")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        return False


def parse_summary_from_card(card: BeautifulSoup) -> str:
    btn = card.select_one("button[data-bs-target*='summary-arg-modal']")
    if not btn:
        return ""

    target = (btn.get("data-bs-target") or "").strip()
    if not target.startswith("#"):
        return ""

    modal = card.select_one(target)
    if not modal:
        return ""

    parts = []
    for p in modal.select(".modal-body p"):
        txt = clean_ws(p.get_text(" ", strip=True))
        if txt:
            parts.append(txt)

    return " ".join(parts)


def find_next_page_url(soup: BeautifulSoup, current_url: str) -> str:
    a = soup.select_one("nav.njcourts-pager a[rel='next']")
    if a and a.get("href"):
        return urljoin(current_url, a["href"].strip())
    return ""


def parse_cards(soup: BeautifulSoup, page_url: str):
    cards = soup.select("article.w-100 div.card.mb-3")
    rows = []

    for card in cards:
        title_a = card.select_one(".card-title a.text-underline-hover[href]")
        if not title_a:
            continue

        case_name = clean_ws(title_a.get_text(" ", strip=True))

        pdf_href = (title_a.get("href") or "").strip()
        pdf_url = urljoin(BASE, pdf_href) if pdf_href else ""

        badges = [
            clean_ws(b.get_text(" ", strip=True))
            for b in card.select("span.badge")
            if clean_ws(b.get_text())
        ]
        no = badges[0] if len(badges) >= 1 else ""
        court = badges[1] if len(badges) >= 2 else ""

        date_el = card.select_one(".small.text-muted")
        date = clean_ws(date_el.get_text(" ", strip=True)) if date_el else ""

        description = parse_summary_from_card(card)
        year = extract_year(date)

        pdf_file = ""
        pdf_full_path = ""

        if pdf_url:
            base = os.path.basename(urlparse(pdf_url).path) or "opinion.pdf"
            pdf_file = safe_filename(base, default="opinion.pdf", max_len=MAX_FILENAME_LEN)
            if not pdf_file.lower().endswith(".pdf"):
                pdf_file += ".pdf"

            docket_folder = safe_folder_name(no if no else "unknown_no")
            pdf_dir = os.path.join(BASE_DIR, year, docket_folder)
            pdf_full_path = os.path.join(pdf_dir, pdf_file)

        rows.append({
            "date": date,
            "no": no,
            "court": court,
            "case_name": case_name,
            "description": description,
            "page_url": page_url,
            "pdf_url": pdf_url,
            "pdf_file": pdf_file,
            "pdf_full_path": pdf_full_path,
        })

    return rows


def load_existing_pdf_urls(csv_path: str):
    existing = set()
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return existing

    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pdf_url = (row.get("pdf_url") or "").strip()
                if pdf_url:
                    existing.add(pdf_url)
    except Exception as e:
        print(f"⚠️ Failed to read existing CSV for dedupe: {e}")

    return existing


def main():
    ensure_dirs()

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    existing_pdf_urls = load_existing_pdf_urls(CSV_PATH)

    file_exists = os.path.exists(CSV_PATH) and os.path.getsize(CSV_PATH) > 0
    csv_f = open(CSV_PATH, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(
        csv_f,
        fieldnames=[
            "date",
            "no",
            "court",
            "case_name",
            "description",
            "page_url",
            "pdf_url",
            "pdf_file",
            "pdf_full_path",
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
    saved_count = 0

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
            item_count += 1

            pdf_url = (r.get("pdf_url") or "").strip()
            if pdf_url and pdf_url in existing_pdf_urls:
                print(f"   Skipping duplicate: {pdf_url}")
                continue

            status = "no-pdf"
            if r["pdf_url"] and r["pdf_full_path"]:
                ok = download_file(session, r["pdf_url"], r["pdf_full_path"])
                status = "pdf:ok" if ok else "pdf:fail"

            writer.writerow({**r, "download_status": status})
            csv_f.flush()

            if pdf_url:
                existing_pdf_urls.add(pdf_url)

            saved_count += 1

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
    print(f"Base folder: {BASE_DIR}")
    print(f"Pages scraped: {page_count}")
    print(f"Items found: {item_count}")
    print(f"Rows saved: {saved_count}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)