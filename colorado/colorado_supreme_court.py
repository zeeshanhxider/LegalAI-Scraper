import asyncio
import csv
import os
import re
from urllib.parse import urljoin
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

BASE = "https://research.coloradojudicial.gov"
SEARCH_URL = "https://research.coloradojudicial.gov/search/jurisdiction:US+content_type:2+court:14024_01/*"

# Your folders
CSV_DIR = "download/colorado_supreme_court"
PDF_DIR = "download/colorado_supreme_court/pdf"
CSV_PATH = os.path.join(CSV_DIR, "colorado_supreme_court.csv")

# Search page selectors
ROW_SEL = "div.results-list li.documento__result-item.document"
MORE_BTN = "li.section-pager.epag-next-page span.more"

def ensure_dirs():
    os.makedirs(CSV_DIR, exist_ok=True)
    os.makedirs(PDF_DIR, exist_ok=True)

def safe_filename(name: str, max_len: int = 140) -> str:
    name = name.strip()
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)
    name = re.sub(r"\s+", " ", name)
    return name[:max_len].rstrip() if len(name) > max_len else name

async def extract_row(li):
    a = li.locator("a.result").first
    title = (await a.inner_text()).strip() if await a.count() else ""
    href = (await a.get_attribute("href")) if await a.count() else ""
    href = (href or "").strip()
    detail_url = urljoin(BASE, href)

    case_id = ""
    inner = li.locator("div.inner-content[id]").first
    if await inner.count():
        case_id = ((await inner.get_attribute("id")) or "").strip()

    values = li.locator("div.result-metadata-properties span.value")
    n = await values.count()
    meta = []
    for i in range(n):
        meta.append((await values.nth(i).inner_text()).strip())

    court = meta[0] if len(meta) >= 1 else ""
    date = meta[1] if len(meta) >= 2 else ""
    citation = " | ".join(meta[2:]) if len(meta) >= 3 else ""

    return {
        "case_id": case_id,
        "title": title,
        "court": court,
        "date": date,
        "citation": citation,
        "detail_url": detail_url,
    }

async def wait_and_click_download(detail_page):
    # Your HTML: <div role="button" aria-label="Download" id="formats" ...>
    candidates = [
        "div#formats",
        "div#formats[aria-label='Download']",
        "div[role='button'][aria-label='Download']",
        "[aria-label='Download'][data-toggle='dropdown']",
        "svg[title='Download']",
    ]

    await detail_page.evaluate("window.scrollTo(0, 0)")
    await detail_page.wait_for_timeout(500)

    for sel in candidates:
        try:
            await detail_page.wait_for_selector(sel, timeout=15000)
            loc = detail_page.locator(sel).first
            if await loc.count():
                await loc.click(timeout=3000, force=True)
                return True
        except:
            continue
    return False

async def get_pdf_download_url(detail_page):
    ok = await wait_and_click_download(detail_page)
    if not ok:
        raise RuntimeError("Download button not found on detail page")

    # Menu + PDF link (from your HTML)
    await detail_page.wait_for_timeout(500)

    pdf_link = detail_page.locator(
        "#downloadable-formats a.download-action[data-type='application/pdf']"
    ).first
    if await pdf_link.count() == 0:
        pdf_link = detail_page.locator("#downloadable-formats a[data-type='application/pdf']").first
    if await pdf_link.count() == 0:
        pdf_link = detail_page.locator("a[data-type='application/pdf']").first

    if await pdf_link.count() == 0:
        raise RuntimeError("PDF link not found in download menu")

    href = ((await pdf_link.get_attribute("href")) or "").strip()
    if not href:
        raise RuntimeError("PDF href empty")

    return urljoin(BASE, href)

async def download_pdf_via_request(context, pdf_url, save_path):
    resp = await context.request.get(pdf_url, timeout=120000)
    if not resp.ok:
        raise RuntimeError(f"PDF request failed: {resp.status} {resp.status_text}")
    data = await resp.body()
    if len(data) < 1000:
        # often HTML error page
        text = (await resp.text())[:300]
        raise RuntimeError(f"Not PDF / too small. First chars: {text}")
    with open(save_path, "wb") as f:
        f.write(data)

async def main():
    ensure_dirs()

    # ✅ No limit
    LIMIT_CASES = None  # keep None for all

    async with async_playwright() as p:
        # ✅ background
        browser = await p.chromium.launch(headless=True)

        context = await browser.new_context(
            accept_downloads=True,
            viewport={"width": 1400, "height": 900}
        )

        # Keep search page always open
        search_page = await context.new_page()
        await search_page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=120000)
        await search_page.wait_for_selector(ROW_SEL, timeout=120000)

        fieldnames = [
            "case_id", "title", "court", "date", "citation", "detail_url",
            "pdf_url", "pdf_filename", "download_status", "error"
        ]

        seen = set()
        written = 0
        last_count = 0

        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            while True:
                rows_loc = search_page.locator(ROW_SEL)
                count = await rows_loc.count()

                # process only new rows currently loaded in the search page
                for i in range(last_count, count):
                    li = rows_loc.nth(i)
                    row = await extract_row(li)

                    key = row["case_id"] or row["detail_url"] or row["title"]
                    if key in seen:
                        continue
                    seen.add(key)

                    case_id = row["case_id"] or f"row_{written+1}"
                    title = row["title"] or ""
                    base_name = safe_filename(f"{case_id}_{title}" if case_id else title)
                    pdf_filename = f"{base_name}.pdf"
                    pdf_path = os.path.join(PDF_DIR, pdf_filename)

                    try:
                        # Open detail in a NEW page (do not disturb search page)
                        detail_page = await context.new_page()
                        await detail_page.goto(row["detail_url"], wait_until="domcontentloaded", timeout=120000)
                        await detail_page.wait_for_timeout(800)

                        pdf_url = await get_pdf_download_url(detail_page)
                        row["pdf_url"] = pdf_url
                        row["pdf_filename"] = pdf_filename

                        # skip if already exists
                        if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0:
                            row["download_status"] = "already_exists"
                            row["error"] = ""
                        else:
                            await download_pdf_via_request(context, pdf_url, pdf_path)
                            row["download_status"] = "downloaded"
                            row["error"] = ""

                        await detail_page.close()
                        print(f"✅ {written+1} | {row['download_status']} | {title}")

                    except Exception as e:
                        try:
                            await detail_page.close()
                        except:
                            pass
                        row["pdf_url"] = row.get("pdf_url", "")
                        row["pdf_filename"] = pdf_filename
                        row["download_status"] = "failed"
                        row["error"] = str(e)
                        print(f"❌ {written+1} | failed | {title} | {e}")

                    writer.writerow(row)
                    f.flush()
                    written += 1

                    if LIMIT_CASES is not None and written >= LIMIT_CASES:
                        print("Reached LIMIT_CASES. Stop.")
                        await browser.close()
                        return

                # update how many rows we have processed on the search page
                last_count = count

                # Now load more results on the SAME search page
                more = search_page.locator(MORE_BTN)
                if await more.count() == 0:
                    print("✅ No more 'More results'. Done.")
                    break

                await search_page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await search_page.wait_for_timeout(800)
                await more.first.click()
                await search_page.wait_for_timeout(1500)

                new_count = await search_page.locator(ROW_SEL).count()
                if new_count <= count:
                    print("⚠️ More results not loading. Stop.")
                    break

        await browser.close()
        print("DONE ✅")
        print("CSV:", CSV_PATH)
        print("PDF folder:", PDF_DIR)

asyncio.run(main())
