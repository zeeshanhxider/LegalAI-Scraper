import asyncio
import csv
import os
import re
from contextlib import suppress
from urllib.parse import urlparse

import requests
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

START_URL = "https://www.pacourts.us/site-search?c=Opinions&q=#sort=relevancy&f:@filetype=[pdf]"
DOWNLOAD_DIR = "downloads"
OUT_CSV = "pacourts_pdfs.csv"

CSV_FIELDS = [
    "title",
    "date",
    "excerpt",
    "publication",
    "source",
    "size",
    "pages",
    "pdf_url",
    "pdf_file",
]


def clean_text(s: str) -> str:
    return " ".join((s or "").split()).strip()


def safe_filename(name: str, max_len: int = 150) -> str:
    name = clean_text(name)
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:max_len] if len(name) > max_len else name


async def locator_text_first(locator, default=""):
    """
    Fix strict mode violation by always reading the FIRST matched element.
    """
    new_tab_task = None
    try:
        if await locator.count() == 0:
            return default
        return clean_text(await locator.first.inner_text())
    except Exception:
        return default


def download_pdf(url: str, out_path: str, timeout: int = 120) -> None:
    headers = {"User-Agent": "Mozilla/5.0"}
    with requests.get(url, headers=headers, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)


def build_pdf_filename(title: str, pdf_url: str) -> str:
    base = safe_filename(title) or "document"
    url_name = os.path.basename(urlparse(pdf_url).path)
    if url_name.lower().endswith(".pdf") and len(url_name) > 4:
        return f"{base}__{url_name}"
    return f"{base}.pdf"


async def click_and_capture_pdf(page, title_locator) -> str:
    """
    Clicks the Coveo title and captures the first network response that ends with .pdf
    (No Coveo API call; this is just reading what the browser loads.)
    """
    pdf_url = None

    def handle_response(response):
        nonlocal pdf_url
        try:
            u = response.url
            if u and u.lower().endswith(".pdf"):
                pdf_url = u
        except Exception:
            pass

    # Add listener
    page.on("response", handle_response)
    new_tab_task = None

    try:
        # Clicking sometimes needs a small wait to let coveo attach handlers
        await page.wait_for_timeout(200)

        new_tab_task = asyncio.create_task(page.context.wait_for_event("page"))

        # Click title
        try:
            await title_locator.click()
        except Exception:
            pass

        # If clicking opened a new tab, capture the PDF URL from it
        new_page = None
        try:
            new_page = await asyncio.wait_for(new_tab_task, timeout=5)
        except asyncio.TimeoutError:
            new_page = None
        except Exception:
            new_page = None
        if new_page:
            try:
                await new_page.wait_for_load_state("domcontentloaded", timeout=5000)
            except PlaywrightTimeoutError:
                pass
            try:
                candidate = new_page.url
                if candidate and candidate.lower().endswith(".pdf"):
                    pdf_url = candidate
            except Exception:
                pass
            finally:
                with suppress(Exception):
                    await new_page.close()

        # Wait up to ~6 seconds for PDF request to appear (same tab or from the listener)
        for _ in range(24):
            if pdf_url:
                break
            await page.wait_for_timeout(250)

    except Exception:
        pass
    finally:
        # IMPORTANT: Playwright Python uses remove_listener, not off()
        if new_tab_task:
            if not new_tab_task.done():
                new_tab_task.cancel()
                with suppress(asyncio.CancelledError):
                    await new_tab_task
        try:
            page.remove_listener("response", handle_response)
        except Exception:
            pass

    return pdf_url or ""


async def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)  # set False to debug
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(START_URL, wait_until="domcontentloaded")
        await page.wait_for_selector("div.coveo-list-layout.CoveoResult", timeout=60000)

        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()

            page_no = 1
            total_written = 0

            while True:
                await page.wait_for_selector("div.coveo-list-layout.CoveoResult", timeout=60000)

                results = page.locator("div.coveo-list-layout.CoveoResult")
                count = await results.count()
                print(f"\nPage {page_no} - Results found: {count}")

                for i in range(count):
                    r = results.nth(i)

                    title_el = r.locator(".coveo-result-cell.c-title span.CoveoResultLink")
                    title = await locator_text_first(title_el, f"row_{page_no}_{i+1}")

                    date = await locator_text_first(
                        r.locator(".coveo-result-cell.c-date span.CoveoFieldValue span")
                    )

                    excerpt = await locator_text_first(r.locator("span.CoveoExcerpt"))

                    publication = await locator_text_first(
                        r.locator('span.CoveoFieldValue[data-field="@publicationtype"] span:last-child')
                    )

                    source = await locator_text_first(
                        r.locator('span.CoveoFieldValue[data-field="@source"] span:last-child')
                    )

                    size = await locator_text_first(
                        r.locator('span.CoveoFieldValue[data-field="@size"] span:last-child')
                    )

                    pages = await locator_text_first(
                        r.locator('span.CoveoFieldValue[data-field="@syspages"] span:last-child')
                    )

                    # Capture pdf url by listening to network responses
                    pdf_url = await click_and_capture_pdf(page, title_el)

                    if not pdf_url:
                        print(f"  - Skip (no pdf url): {title[:90]}")
                        continue

                    pdf_file_name = build_pdf_filename(title, pdf_url)
                    pdf_path = os.path.join(DOWNLOAD_DIR, pdf_file_name)

                    if not os.path.exists(pdf_path):
                        try:
                            print(f"  - Downloading: {pdf_file_name}")
                            download_pdf(pdf_url, pdf_path)
                        except Exception as e:
                            print(f"  ! Download failed: {pdf_url} -> {e}")
                            continue
                    else:
                        print(f"  - Already downloaded: {pdf_file_name}")

                    # Write CSV row-by-row (one by one)
                    writer.writerow({
                        "title": title,
                        "date": date,
                        "excerpt": excerpt,
                        "publication": publication,
                        "source": source,
                        "size": size,
                        "pages": pages,
                        "pdf_url": pdf_url,
                        "pdf_file": pdf_path,
                    })
                    total_written += 1

                print(f"Written total: {total_written}")

                # Pagination: Next
                next_btn = page.locator("li.coveo-pager-next[role='button']")
                if await next_btn.count() == 0:
                    print("No Next button. Done.")
                    break

                cls = (await next_btn.get_attribute("class")) or ""
                aria_disabled = await next_btn.get_attribute("aria-disabled")
                if "coveo-disabled" in cls or aria_disabled == "true":
                    print("Next disabled. Done.")
                    break

                active_before = clean_text(
                    await page.locator("li.coveo-pager-list-item.coveo-active").inner_text()
                )

                await next_btn.click()

                await page.wait_for_function(
                    """(prev) => {
                        const el = document.querySelector("li.coveo-pager-list-item.coveo-active");
                        return el && el.innerText.trim() !== prev.trim();
                    }""",
                    arg=active_before,
                    timeout=60000
                )

                page_no += 1

        await browser.close()

    print("\n✅ Done!")
    print(f"CSV saved: {OUT_CSV}")
    print(f"PDF folder: {DOWNLOAD_DIR}/")


if __name__ == "__main__":
    asyncio.run(main())
