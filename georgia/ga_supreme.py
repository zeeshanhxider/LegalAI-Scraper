# ga_supreme_all_years.py
# Scrape all "Opinion Year" pages (2017-2026 etc), write ONE CSV row-by-row (streaming),
# and download PDFs to: download/{year}/{month}/{pdf_file}

import os
import re
import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

BASE = "https://www.gasupreme.us"
START_URL = "https://www.gasupreme.us/2026-opinions/"

OUT_BASE = "download"  # base folder for CSV + PDFs
CSV_PATH = os.path.join(OUT_BASE, "ga_supreme_all_years.csv")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    )
}

# Matches S25A0124, S25C1409, S26Y0121, etc.
CASE_ID_RE = re.compile(r"\bS\d{2}[A-Z]\d{4}\b")


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def safe_filename(name: str, max_len: int = 180) -> str:
    name = (name or "").strip()
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)
    name = re.sub(r"\s+", " ", name)
    return name[:max_len].rstrip() if len(name) > max_len else name


def clean_date_text(s: str) -> str:
    """
    Example: 'February 3, 2026—SUMMARIES of NOTEWORTHY OPINIONS'
    Return:  'February 3, 2026'
    """
    s = (s or "").strip()
    for sep in ["—", "–", "-"]:
        if sep in s:
            left = s.split(sep, 1)[0].strip()
            if left:
                return left
    return s


def parse_case_text(text: str):
    """
    Example: 'S25A0124. HUITRON v. TOBY, WARDEN'
    Or:      'S25A1023, S25A1024. FLAKES v. THE STATE (two cases)'
    Return: (list_of_case_ids, case_title)
    """
    text = " ".join((text or "").split())
    ids = CASE_ID_RE.findall(text)

    if ". " in text:
        title = text.split(". ", 1)[1].strip()
    else:
        title = text
        for cid in ids:
            title = title.replace(cid, "").strip()
        title = title.lstrip(".").strip()

    return ids, title


def filename_from_url(pdf_url: str) -> str:
    name = os.path.basename(urlparse(pdf_url).path) or "document.pdf"
    return safe_filename(name)


def download_pdf(pdf_url: str, out_path: str) -> str:
    """
    Download and save PDF. Return status:
    - downloaded
    - already_exists
    - failed_html_instead_of_pdf (tiny HTML saved)
    """
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return "already_exists"

    try:
        with requests.get(pdf_url, headers=HEADERS, stream=True, timeout=90) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        f.write(chunk)
    except Exception as e:
        # remove partial file if present
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except:
            pass
        return f"failed_download: {e}"

    # sanity check: tiny downloads are often HTML error pages
    try:
        if os.path.getsize(out_path) < 1200:
            with open(out_path, "rb") as f:
                head = f.read(200).lower()
            if b"<html" in head or b"doctype html" in head:
                return "failed_html_instead_of_pdf"
    except:
        pass

    return "downloaded"


def get_year_links(start_html: str):
    """
    Extract dropdown year links from:
    <nav class="gcnavbar"> ... Opinion Year ... <a href=".../2025-opinions/">2025</a>
    Returns list[(year_int, url)] sorted newest->oldest.
    """
    soup = BeautifulSoup(start_html, "html.parser")
    nav = soup.select_one("nav.gcnavbar")
    if not nav:
        raise RuntimeError("Could not find nav.gcnavbar (Opinion Year menu).")

    year_links = []
    for a in nav.select("li ul li a"):
        txt = (a.get_text(strip=True) or "")
        href = (a.get("href") or "").strip()
        if txt.isdigit() and len(txt) == 4 and href:
            year_links.append((int(txt), urljoin(BASE, href)))

    year_links.sort(key=lambda x: x[0], reverse=True)
    return year_links


def scrape_year_page_streaming(year: int, url: str, writer, csv_file_handle):
    """
    Scrape ONE year page and write rows to CSV immediately (streaming).
    PDFs saved to download/{year}/{month}/{pdf_file}
    """
    print(f"\n=== YEAR {year} === {url}")

    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    months = soup.find_all("h3")
    if not months:
        raise RuntimeError(f"No <h3> headings found on {url}")

    # Deduplicate downloads per year page by URL (same pdf may map to multiple case_ids)
    pdf_urls_seen = set()
    rows_written = 0

    for h3 in months:
        month = h3.get_text(strip=True)
        if not month:
            continue

        current_date = None
        node = h3.next_sibling

        while node:
            if getattr(node, "name", None) == "h3":
                break

            # Date paragraph
            if getattr(node, "name", None) == "p":
                strong = node.find("strong")
                if strong:
                    date_text = strong.get_text(" ", strip=True)
                    if date_text:
                        current_date = clean_date_text(date_text)

            # Case list
            if getattr(node, "name", None) == "ul":
                if not current_date:
                    node = node.next_sibling
                    continue

                for li in node.find_all("li", recursive=False):
                    a = li.find("a", href=True)
                    if not a:
                        continue

                    text = a.get_text(" ", strip=True)
                    pdf_url = (a["href"] or "").strip()

                    # Case PDFs are .pdf
                    if not pdf_url.lower().endswith(".pdf"):
                        continue

                    case_ids, case_title = parse_case_text(text)
                    if not case_ids:
                        continue

                    pdf_file = filename_from_url(pdf_url)

                    # Folder: download/{year}/{month}/
                    pdf_dir = os.path.join(OUT_BASE, str(year), month)
                    ensure_dir(pdf_dir)
                    pdf_path = os.path.join(pdf_dir, pdf_file)

                    # Download once per pdf_url
                    if pdf_url not in pdf_urls_seen:
                        dl_status = download_pdf(pdf_url, pdf_path)
                        pdf_urls_seen.add(pdf_url)
                    else:
                        dl_status = "already_exists" if os.path.exists(pdf_path) else "skipped_duplicate_url"

                    # Write 1 row per case_id
                    for cid in case_ids:
                        row = {
                            "year": year,
                            "month": month,
                            "date": current_date,
                            "case_id": cid,
                            "case_title": case_title,
                            "pdf_url": pdf_url,
                            "pdf_file": pdf_file,
                            "pdf_path": pdf_path,
                            "download_status": dl_status,
                        }
                        writer.writerow(row)
                        csv_file_handle.flush()  # ✅ write immediately
                        rows_written += 1

                        print(f"✅ {year} | {month} | {current_date} | {cid} | {dl_status}")

            node = node.next_sibling

    return rows_written


def main():
    ensure_dir(OUT_BASE)

    # Fetch one page to get the dropdown year links
    start = requests.get(START_URL, headers=HEADERS, timeout=60)
    start.raise_for_status()

    year_links = get_year_links(start.text)
    print("Found years:", [y for y, _ in year_links])

    fieldnames = [
        "year", "month", "date",
        "case_id", "case_title",
        "pdf_url", "pdf_file",
        "pdf_path", "download_status"
    ]

    # ✅ Streaming CSV: open once, write row-by-row while scraping
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        f.flush()

        total_rows = 0
        for year, url in year_links:
            try:
                rows_written = scrape_year_page_streaming(year, url, writer, f)
                total_rows += rows_written
                print(f"Rows written for {year}: {rows_written}")
            except Exception as e:
                print(f"❌ Failed year {year}: {e}")

    print("\nDONE ✅")
    print("Total rows written:", total_rows)
    print("CSV:", CSV_PATH)
    print("PDF base folder:", OUT_BASE + "/{year}/{month}/...")


if __name__ == "__main__":
    main()
