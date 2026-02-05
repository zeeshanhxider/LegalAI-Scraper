import asyncio
import csv
import os
import re
from urllib.parse import urlparse

import requests
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

URL = (
    "https://supremecourt.flcourts.gov/case-information/opinions/"
    "Opinion-Search-For-All-Appellate-Courts?searchtype=opinions&"
    "scopes%5B%5D=supreme_court&scopes%5B%5D=first_district_court_of_appeal&"
    "scopes%5B%5D=second_district_court_of_appeal&scopes%5B%5D=third_district_court_of_appeal&"
    "scopes%5B%5D=fourth_district_court_of_appeal&scopes%5B%5D=fifth_district_court_of_appeal&"
    "scopes%5B%5D=sixth_district_court_of_appeal"
)

OUT_DIR = "download"
PDF_DIR = os.path.join(OUT_DIR, "pdf")
CSV_PATH = os.path.join(OUT_DIR, "fl_opinions.csv")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    )
}

# Table selectors
ROW_SEL = "div[role='row'][id^='row-']"
CELL_SEL = "div[role='cell'][data-column-id]"

# Pagination selectors (from your HTML)
PAGINATION_TEXT_SEL = "nav.rdt_Pagination span"  # shows like "1-50 of 91216"
NEXT_BTN_SEL = "#pagination-next-page"


def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(PDF_DIR, exist_ok=True)


def safe_filename(name: str, max_len: int = 180) -> str:
    name = (name or "").strip()
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)
    name = re.sub(r"\s+", " ", name)
    return name[:max_len].rstrip() if len(name) > max_len else name


def filename_from_url(pdf_url: str) -> str:
    path = urlparse(pdf_url).path
    base = os.path.basename(path)
    return safe_filename(base or "document.pdf")


def download_pdf(pdf_url: str, out_path: str) -> str:
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return "already_exists"

    try:
        with requests.get(pdf_url, headers=HEADERS, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        f.write(chunk)

        # tiny file check (sometimes HTML error)
        if os.path.getsize(out_path) < 1200:
            with open(out_path, "rb") as f:
                head = f.read(200).lower()
            if b"<html" in head or b"doctype html" in head:
                return "failed_html_instead_of_pdf"

        return "downloaded"

    except Exception as e:
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except:
            pass
        return f"failed_download: {e}"


async def extract_row(row):
    """
    Based on your HTML:
    column-id=1 -> Release Date
    column-id=2 -> Court
    column-id=3 -> Case No.
    column-id=4 -> Case Name
    column-id=5 -> Disposition
    column-id=6 -> PDF link
    """
    cells = row.locator(CELL_SEL)
    n = await cells.count()

    by_col = {}
    pdf_url = ""

    for i in range(n):
        cell = cells.nth(i)
        col_id = (await cell.get_attribute("data-column-id")) or ""
        col_id = col_id.strip()

        text = (await cell.inner_text()) or ""
        text = " ".join(text.split())
        by_col[col_id] = text

        a = cell.locator("a[href$='.pdf']").first
        if await a.count():
            href = (await a.get_attribute("href")) or ""
            href = href.strip()
            if href.lower().endswith(".pdf"):
                pdf_url = href

    if not pdf_url:
        a2 = row.locator("a[href$='.pdf']").first
        if await a2.count():
            pdf_url = ((await a2.get_attribute("href")) or "").strip()

    return {
        "release_date": by_col.get("1", ""),
        "court": by_col.get("2", ""),
        "case_no": by_col.get("3", ""),
        "case_name": by_col.get("4", ""),
        "disposition": by_col.get("5", ""),
        "pdf_url": pdf_url,
    }


async def get_pagination_text(page) -> str:
    try:
        el = page.locator(PAGINATION_TEXT_SEL).first
        if await el.count():
            txt = (await el.inner_text()) or ""
            return " ".join(txt.split())
    except:
        pass
    return ""


async def is_next_disabled(page) -> bool:
    btn = page.locator(NEXT_BTN_SEL)
    if not await btn.count():
        return True
    aria_disabled = (await btn.get_attribute("aria-disabled")) or ""
    if aria_disabled.lower() == "true":
        return True
    disabled = await btn.is_disabled()
    return bool(disabled)


async def click_next(page):
    """
    Click next and wait table refresh.
    We'll wait until pagination text changes OR first row changes.
    """
    before_pag = await get_pagination_text(page)

    rows = page.locator(ROW_SEL)
    before_first_row = ""
    if await rows.count():
        before_first_row = " ".join(((await rows.nth(0).inner_text()) or "").split())[:120]

    await page.click(NEXT_BTN_SEL)
    await page.wait_for_timeout(600)

    # Wait for pagination text to change or first row to change
    async def changed():
        now_pag = await get_pagination_text(page)
        if now_pag and now_pag != before_pag:
            return True
        now_rows = page.locator(ROW_SEL)
        if await now_rows.count():
            now_first = " ".join(((await now_rows.nth(0).inner_text()) or "").split())[:120]
            if now_first and now_first != before_first_row:
                return True
        return False

    for _ in range(60):  # up to ~60 * 300ms = 18s
        if await changed():
            return
        await page.wait_for_timeout(300)


async def main():
    ensure_dirs()

    fieldnames = [
        "release_date",
        "court",
        "case_no",
        "case_name",
        "disposition",
        "pdf_url",
        "pdf_file",
        "pdf_path",
        "download_status",
        "page_range",
    ]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1400, "height": 900})
        page = await context.new_page()

        print("Opening:", URL)
        await page.goto(URL, wait_until="domcontentloaded", timeout=180000)

        # Wait for first page rows
        await page.wait_for_selector(ROW_SEL, timeout=180000)
        await page.wait_for_selector("nav.rdt_Pagination", timeout=180000)

        seen = set()
        total_written = 0
        page_num = 1

        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            f.flush()

            while True:
                page_range = await get_pagination_text(page)  # e.g. "1-50 of 91216"
                print(f"\n=== Page {page_num} | {page_range} ===")

                rows = page.locator(ROW_SEL)
                count = await rows.count()

                for i in range(count):
                    row_loc = rows.nth(i)
                    data = await extract_row(row_loc)

                    # Deduplicate key
                    key = (data["release_date"], data["court"], data["case_no"], data["pdf_url"])
                    if key in seen:
                        continue
                    seen.add(key)

                    pdf_url = data["pdf_url"]
                    if pdf_url:
                        pdf_file = filename_from_url(pdf_url)
                        pdf_path = os.path.join(PDF_DIR, pdf_file)
                        status = download_pdf(pdf_url, pdf_path)
                    else:
                        pdf_file = ""
                        pdf_path = ""
                        status = "no_pdf_url"

                    out_row = {
                        **data,
                        "pdf_file": pdf_file,
                        "pdf_path": pdf_path,
                        "download_status": status,
                        "page_range": page_range,
                    }

                    writer.writerow(out_row)
                    f.flush()
                    total_written += 1

                    print(
                        f"✅ {total_written} | {data['release_date']} | {data['court']} | "
                        f"{data['case_no']} | {status}"
                    )

                # Stop if next is disabled
                if await is_next_disabled(page):
                    print("\nNext page disabled. Finished pagination ✅")
                    break

                # Go next page
                try:
                    await click_next(page)
                except PWTimeoutError:
                    print("⚠️ Timeout waiting next page. Stopping.")
                    break

                page_num += 1

        await browser.close()

        print("\nDONE ✅")
        print("CSV:", CSV_PATH)
        print("PDF folder:", PDF_DIR)
        print("Total rows:", total_written)


if __name__ == "__main__":
    asyncio.run(main())
