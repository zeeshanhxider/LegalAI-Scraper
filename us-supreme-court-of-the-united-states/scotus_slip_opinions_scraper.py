import os
import re
import csv
import time
import random
import pathlib
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE = "https://www.supremecourt.gov"

# MAIN PAGE (tabs are here)
START_URL = "https://www.supremecourt.gov/opinions/slipopinion"

OUTPUT_ROOT = "downloads"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)


def ensure_dir(path: str):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)


def safe_filename(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"[^\w\-. ()]+", "_", name)
    name = re.sub(r"\s+", " ", name)
    return name[:180].strip() or "file"


def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def fetch_html(session: requests.Session, url: str) -> str:
    r = session.get(url, timeout=60)
    r.raise_for_status()
    return r.text


def discover_term_urls(session: requests.Session, start_url: str):
    """
    FIXED:
    Tabs sometimes use href like 'slipopinion/25' (relative),
    which must resolve under '/opinions/' not under site root.
    """
    html = fetch_html(session, start_url)
    soup = BeautifulSoup(html, "lxml")

    term_urls = set()

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()

        # Match tab links (relative or absolute) for 2-digit term pages
        if re.search(r"(?:^|/)slipopinion/\d{2}/?$", href):

            # If href is like "slipopinion/25" (relative),
            # resolve it to https://www.supremecourt.gov/opinions/slipopinion/25
            if href.startswith("slipopinion/"):
                abs_url = urljoin(BASE + "/opinions/", href)
            else:
                abs_url = urljoin(BASE, href)

            term_urls.add(abs_url)

    # Keep only actual term pages /..../NN (skip the base page)
    term_urls = {u for u in term_urls if re.search(r"/slipopinion/\d{2}/?$", u)}

    # Sort descending by NN (25,24,...)
    def term_key(u):
        m = re.search(r"/(\d{2})/?$", u)
        return int(m.group(1)) if m else -1

    return sorted(term_urls, key=term_key, reverse=True)


def infer_term_year(term_url: str, soup: BeautifulSoup):
    """
    Prefer reading 'Term Year: 2025' from page text.
    Fallback: URL /25 -> 2025
    """
    text = soup.get_text(" ", strip=True)
    m = re.search(r"Term Year:\s*(\d{4})", text)
    if m:
        return int(m.group(1))

    m2 = re.search(r"/(\d{2})/?$", term_url)
    if m2:
        return 2000 + int(m2.group(1))

    return None


def extract_rows_from_term_page(session: requests.Session, term_url: str):
    html = fetch_html(session, term_url)
    soup = BeautifulSoup(html, "lxml")

    term_year = infer_term_year(term_url, soup)

    rows = []
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all(["td", "th"])
            if not tds:
                continue

            # Skip header rows
            header_join = " ".join(td.get_text(" ", strip=True) for td in tds).lower()
            if "docket" in header_join and "citation" in header_join and "date" in header_join:
                continue

            cell_texts = [td.get_text(" ", strip=True) for td in tds]

            # Skip revision-only rows like "Revisions: 1/14/26"
            if "Revisions:" in " ".join(cell_texts):
                continue

            # Must look like a real data row
            if len(cell_texts) < 5:
                continue

            # Typical: R- | Date | Docket | Name | J. | Citation
            r_number = cell_texts[0]
            date = cell_texts[1]
            docket = cell_texts[2]

            if not re.match(r"^\d{1,2}/\d{1,2}/\d{2}$", date):
                continue

            # Find case title link (PDF)
            pdf_a = tr.find("a", href=True)
            if not pdf_a:
                continue

            pdf_url = urljoin(BASE, pdf_a["href"].strip())
            name = pdf_a.get_text(" ", strip=True)

            justice = cell_texts[-2]
            citation = cell_texts[-1]

            rows.append({
                "term_year": term_year,
                "term_page_url": term_url,
                "r_number": r_number,
                "date": date,
                "docket": docket,
                "name": name,
                "justice": justice,
                "citation": citation,
                "pdf_url": pdf_url,
            })

    # De-duplicate
    unique = {}
    for r in rows:
        key = (r["term_year"], r["date"], r["docket"], r["pdf_url"])
        unique[key] = r

    return list(unique.values())


def download_pdf(session: requests.Session, pdf_url: str, out_path: str):
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return "already_exists"

    r = session.get(pdf_url, stream=True, timeout=180)
    r.raise_for_status()

    tmp_path = out_path + ".part"
    with open(tmp_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 64):
            if chunk:
                f.write(chunk)

    os.replace(tmp_path, out_path)
    return "downloaded"


def write_term_csv(term_dir: str, term_year: int, rows: list):
    csv_path = os.path.join(term_dir, f"{term_year}.csv")

    fieldnames = [
        "r_number",
        "date",
        "docket",
        "name",
        "justice",
        "citation",
        "term_year",
        "term_page_url",
        "pdf_url",
        "pdf_filename",
        "download_status",
    ]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in sorted(rows, key=lambda x: (x["date"], x["docket"])):
            w.writerow({k: r.get(k, "") for k in fieldnames})


def main():
    ensure_dir(OUTPUT_ROOT)
    session = get_session()

    term_urls = discover_term_urls(session, START_URL)

    print(f"\nFound {len(term_urls)} term pages (tabs):")
    for u in term_urls:
        print("  -", u)

    all_rows = []
    for term_url in term_urls:
        print(f"\nScraping: {term_url}")
        rows = extract_rows_from_term_page(session, term_url)
        print(f"  Rows found: {len(rows)}")
        all_rows.extend(rows)

        # polite delay
        time.sleep(random.uniform(0.6, 1.2))

    # Group by TERM YEAR
    by_term = {}
    for r in all_rows:
        if not r.get("term_year"):
            continue
        by_term.setdefault(r["term_year"], []).append(r)

    # Download PDFs + write CSV for each term year
    for term_year, rows in sorted(by_term.items(), reverse=True):
        term_dir = os.path.join(OUTPUT_ROOT, str(term_year))
        ensure_dir(term_dir)

        print(f"\nSaving TERM {term_year} -> {term_dir} ({len(rows)} rows)")

        for r in rows:
            docket = safe_filename(r["docket"])
            name = safe_filename(r["name"])

            pdf_filename = f"{docket} - {name}.pdf"
            pdf_path = os.path.join(term_dir, pdf_filename)

            try:
                status = download_pdf(session, r["pdf_url"], pdf_path)
            except Exception as e:
                status = f"failed: {e.__class__.__name__}"

            r["pdf_filename"] = pdf_filename
            r["download_status"] = status

            # polite delay
            time.sleep(random.uniform(0.25, 0.8))

        write_term_csv(term_dir, term_year, rows)
        print(f"  CSV written: {os.path.join(term_dir, str(term_year) + '.csv')}")

    print("\nDone.")


if __name__ == "__main__":
    main()
