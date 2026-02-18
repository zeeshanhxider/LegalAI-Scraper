import asyncio
import csv
import html
import json
import os
import re
import time
from contextlib import suppress
from urllib.parse import unquote, urljoin, urlparse

import requests
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

START_URL = "https://www.pacourts.us/site-search?c=Opinions&q=#sort=relevancy&f:@filetype=[pdf]"
DOWNLOAD_DIR = "downloads"
DEBUG_DIR = "debug"
OUT_CSV = "pacourts_pdfs.csv"
HEADLESS = os.getenv("HEADLESS", "1") != "0"
WAIT_TIMEOUT_MS = int(os.getenv("WAIT_TIMEOUT_MS", "60000"))
DUMP_SKIPS = int(os.getenv("DUMP_SKIPS", "1"))
BROWSER_CHANNEL = os.getenv("BROWSER_CHANNEL", "")
MAX_DOWNLOADS = int(os.getenv("MAX_DOWNLOADS", "4"))
MAX_DOWNLOAD_QUEUE = int(os.getenv("MAX_DOWNLOAD_QUEUE", str(MAX_DOWNLOADS * 3)))

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


def is_http_url(url: str) -> bool:
    try:
        return urlparse(url).scheme in {"http", "https"}
    except Exception:
        return False


def is_pdf_url(url: str) -> bool:
    if not url:
        return False
    try:
        parts = urlparse(url)
        haystack = unquote(f"{parts.path}?{parts.query}").lower()
        return ".pdf" in haystack
    except Exception:
        return False


def normalize_url(url: str, base_url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if url.startswith("//"):
        url = "https:" + url
    if base_url and not is_http_url(url):
        url = urljoin(base_url, url)
    return url if is_http_url(url) else ""


def download_pdf(url: str, out_path: str, timeout: int = 120, referer: str = "") -> None:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/pdf,*/*",
    }
    if referer:
        headers["Referer"] = referer
    with requests.get(url, headers=headers, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)


async def download_pdf_async(url: str, out_path: str, referer: str, sem: asyncio.Semaphore) -> str:
    async with sem:
        await asyncio.to_thread(download_pdf, url, out_path, 120, referer)
    return out_path


async def drain_downloads(tasks: list, keep: int = 0) -> None:
    if len(tasks) <= keep:
        return
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in done:
        try:
            task.result()
        except Exception as e:
            print(f"  ! Download failed (async): {e}")
    tasks[:] = list(pending)


async def finalize_downloads(tasks: list) -> None:
    if not tasks:
        return
    done, _ = await asyncio.wait(tasks)
    for task in done:
        try:
            task.result()
        except Exception as e:
            print(f"  ! Download failed (async): {e}")


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
            headers = response.headers or {}
            content_type = headers.get("content-type", "").lower()
            content_disp = headers.get("content-disposition", "").lower()
            if (
                "application/pdf" in content_type
                or ".pdf" in content_disp
                or "filename=" in content_disp and ".pdf" in content_disp
                or is_pdf_url(u)
            ):
                pdf_url = u
        except Exception:
            pass

    # Add listener
    page.on("response", handle_response)
    def handle_download(download):
        nonlocal pdf_url
        try:
            pdf_url = download.url or pdf_url
        except Exception:
            pass
    page.on("download", handle_download)
    new_tab_task = None
    new_page = None

    try:
        # Clicking sometimes needs a small wait to let coveo attach handlers
        await page.wait_for_timeout(200)

        new_tab_task = asyncio.create_task(page.wait_for_event("popup"))

        # Click title
        try:
            await title_locator.scroll_into_view_if_needed()
            await title_locator.click()
        except Exception:
            pass

        # If clicking opened a new tab, capture the PDF URL from it
        try:
            new_page = await asyncio.wait_for(new_tab_task, timeout=5)
        except asyncio.TimeoutError:
            new_page = None
        except Exception:
            new_page = None
        if new_page:
            try:
                new_page.on("response", handle_response)
                await new_page.wait_for_load_state("domcontentloaded", timeout=5000)
            except PlaywrightTimeoutError:
                pass
            try:
                candidate = await extract_pdf_url_from_page(new_page)
                if candidate:
                    pdf_url = candidate
            except Exception:
                pass

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
        with suppress(Exception):
            page.remove_listener("download", handle_download)
        if new_page:
            with suppress(Exception):
                new_page.remove_listener("response", handle_response)
            with suppress(Exception):
                await new_page.wait_for_timeout(1500)
            with suppress(Exception):
                await new_page.close()

    return pdf_url or ""


def find_pdf_url_in_text(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    match = re.search(r"https?://[^\s\"'<>]+\.pdf[^\s\"'<>]*", text, re.IGNORECASE)
    if match:
        return match.group(0)
    return ""


def looks_like_pdf_url(url: str) -> bool:
    if not url or not is_http_url(url):
        return False
    u = unquote(url).lower()
    return ".pdf" in u or "filetype=pdf" in u or "format=pdf" in u


async def extract_url_from_element(el, base_url: str) -> str:
    try:
        info = await el.evaluate(
            """el => {
                const attrs = {};
                for (const n of el.getAttributeNames()) attrs[n] = el.getAttribute(n);
                const dataset = {};
                if (el.dataset) {
                    for (const k of Object.keys(el.dataset)) dataset[k] = el.dataset[k];
                }
                return { attrs, href: el.href || "", dataset };
            }"""
        )
    except Exception:
        return ""

    candidates = []
    href = (info or {}).get("href") or ""
    if href:
        candidates.append(href)
    for v in ((info or {}).get("attrs") or {}).values():
        if v:
            candidates.append(v)
    for v in ((info or {}).get("dataset") or {}).values():
        if v:
            candidates.append(v)

    for cand in candidates:
        url = normalize_url(cand, base_url)
        if url and looks_like_pdf_url(url):
            return url
        url = find_pdf_url_in_text(cand)
        if url:
            return url

    return ""


async def extract_pdf_url_from_page(page) -> str:
    if is_pdf_url(page.url):
        return page.url

    selectors = [
        ("embed[type='application/pdf']", "src"),
        ("embed[src*='.pdf']", "src"),
        ("iframe[src*='.pdf']", "src"),
        ("object[data*='.pdf']", "data"),
        ("a[href*='.pdf']", "href"),
    ]
    for sel, attr in selectors:
        loc = page.locator(sel)
        if await loc.count():
            raw = await loc.first.get_attribute(attr)
            url = normalize_url(raw, page.url)
            if url and looks_like_pdf_url(url):
                return url

    try:
        html_blob = await page.content()
    except Exception:
        html_blob = ""
    url = find_pdf_url_in_text(html_blob)
    if url:
        return url

    return ""


async def extract_pdf_url_from_result(result, base_url: str) -> str:
    # Prefer direct hrefs in the result, then fall back to data-* attributes/metadata.
    selectors = [
        "a.CoveoResultLink",
        ".coveo-result-cell.c-title a[href]",
        "a[href$='.pdf']",
        "a[href*='.pdf?']",
        "a[href*='.PDF']",
    ]
    for sel in selectors:
        loc = result.locator(sel)
        if await loc.count():
            url = await extract_url_from_element(loc.first, base_url)
            if url:
                return url

    link_loc = result.locator(".coveo-result-cell.c-title .CoveoResultLink, .CoveoResultLink")
    if await link_loc.count():
        url = await extract_url_from_element(link_loc.first, base_url)
        if url:
            return url

    uri_field = result.locator("span.CoveoFieldValue[data-field='@uri'] span")
    if await uri_field.count():
        text = await locator_text_first(uri_field)
        url = normalize_url(text, base_url)
        if url:
            return url

    data_nodes = result.locator("[data-result]")
    data_count = await data_nodes.count()
    for idx in range(min(data_count, 5)):
        data_result = await data_nodes.nth(idx).get_attribute("data-result")
        if not data_result:
            continue
        for candidate in (data_result, html.unescape(data_result)):
            try:
                payload = json.loads(candidate)
            except Exception:
                payload = None
            if not isinstance(payload, dict):
                continue
            for key in ("clickUri", "uri", "rawUri", "printableUri"):
                url = normalize_url(payload.get(key, ""), base_url)
                if url:
                    return url
            raw = payload.get("raw", {}) or {}
            for key in ("uri", "clickuri", "fileuri", "fileurl"):
                url = normalize_url(raw.get(key, ""), base_url)
                if url:
                    return url

    try:
        html_blob = await result.evaluate("el => el.innerHTML")
    except Exception:
        html_blob = ""
    url = find_pdf_url_in_text(html_blob)
    if url:
        return url

    return ""


async def try_accept_cookies(page) -> None:
    selectors = [
        "button#onetrust-accept-btn-handler",
        "button[aria-label='Accept cookies']",
        "button:has-text('Accept')",
        "button:has-text('I Accept')",
    ]
    for sel in selectors:
        btn = page.locator(sel)
        if await btn.count():
            with suppress(Exception):
                await btn.first.click()
            break


async def wait_for_results(page, timeout_ms: int, label: str = "") -> bool:
    selectors = "div.coveo-list-layout.CoveoResult, div.CoveoResult"
    try:
        await page.wait_for_selector(selectors, timeout=timeout_ms)
        return True
    except PlaywrightTimeoutError:
        no_results = page.locator("div.coveo-no-results, div.CoveoNoResults")
        if await no_results.count():
            print(f"  - No results found{f' ({label})' if label else ''}.")
            return False

        os.makedirs(DEBUG_DIR, exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        suffix = f"_{label}" if label else ""
        screenshot_path = os.path.join(DEBUG_DIR, f"timeout{suffix}_{stamp}.png")
        html_path = os.path.join(DEBUG_DIR, f"timeout{suffix}_{stamp}.html")
        with suppress(Exception):
            await page.screenshot(path=screenshot_path, full_page=True)
        with suppress(Exception):
            html = await page.content()
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html)
        print(
            "  ! Timed out waiting for Coveo results. "
            f"Saved debug files: {screenshot_path}, {html_path}"
        )
        return False


async def dump_result_debug(result, page, title: str, page_no: int, idx: int) -> None:
    os.makedirs(DEBUG_DIR, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    base = f"skip_p{page_no}_{idx+1}_{stamp}"
    html_path = os.path.join(DEBUG_DIR, f"{base}.html")
    json_path = os.path.join(DEBUG_DIR, f"{base}.json")

    try:
        outer_html = await result.evaluate("el => el.outerHTML")
    except Exception:
        outer_html = ""
    with suppress(Exception):
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(outer_html or "")

    try:
        payload = await result.evaluate(
            """el => {
                const attrs = {};
                for (const n of el.getAttributeNames()) attrs[n] = el.getAttribute(n);
                const titleEl = el.querySelector('.CoveoResultLink') || el.querySelector('.coveo-result-cell.c-title');
                const titleAttrs = {};
                const titleDataset = {};
                if (titleEl) {
                    for (const n of titleEl.getAttributeNames()) titleAttrs[n] = titleEl.getAttribute(n);
                    if (titleEl.dataset) {
                        for (const k of Object.keys(titleEl.dataset)) titleDataset[k] = titleEl.dataset[k];
                    }
                }
                const anchors = [];
                for (const a of el.querySelectorAll('a')) {
                    const aAttrs = {};
                    for (const n of a.getAttributeNames()) aAttrs[n] = a.getAttribute(n);
                    anchors.push({ href: a.href || a.getAttribute('href') || '', attrs: aAttrs });
                }
                const dataResults = [];
                for (const node of el.querySelectorAll('[data-result]')) {
                    dataResults.push(node.getAttribute('data-result') || '');
                }
                return {
                    pageUrl: location.href,
                    resultAttrs: attrs,
                    titleAttrs,
                    titleDataset,
                    anchors,
                    dataResults,
                };
            }"""
        )
    except Exception:
        payload = {"pageUrl": page.url}

    payload["title"] = title
    payload["pageNo"] = page_no
    payload["index"] = idx + 1

    with suppress(Exception):
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"  ! Debug saved: {html_path}, {json_path}")


async def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    async with async_playwright() as p:
        launch_args = {"headless": HEADLESS}
        if BROWSER_CHANNEL:
            launch_args["channel"] = BROWSER_CHANNEL
        browser = await p.chromium.launch(**launch_args)  # set HEADLESS=0 to debug
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        download_sem = asyncio.Semaphore(MAX_DOWNLOADS)
        download_tasks = []

        await page.goto(START_URL, wait_until="domcontentloaded")
        await try_accept_cookies(page)
        if not await wait_for_results(page, WAIT_TIMEOUT_MS, "initial"):
            await browser.close()
            return

        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()

            page_no = 1
            total_written = 0

            while True:
                if not await wait_for_results(page, WAIT_TIMEOUT_MS, f"page{page_no}"):
                    break

                results = page.locator("div.coveo-list-layout.CoveoResult")
                count = await results.count()
                print(f"\nPage {page_no} - Results found: {count}")

                dumped = 0
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

                    pdf_url = await extract_pdf_url_from_result(r, page.url)
                    if not pdf_url:
                        # Fallback: capture by listening to network responses on click
                        pdf_url = await click_and_capture_pdf(page, title_el)

                    if not pdf_url:
                        print(f"  - Skip (no pdf url): {title[:90]}")
                        if DUMP_SKIPS > 0 and dumped < DUMP_SKIPS:
                            await dump_result_debug(r, page, title, page_no, i)
                            dumped += 1
                        continue

                    pdf_file_name = build_pdf_filename(title, pdf_url)
                    pdf_path = os.path.join(DOWNLOAD_DIR, pdf_file_name)

                    if not os.path.exists(pdf_path):
                        print(f"  - Queue download: {pdf_file_name}")
                        task = asyncio.create_task(
                            download_pdf_async(pdf_url, pdf_path, page.url, download_sem)
                        )
                        download_tasks.append(task)
                        if len(download_tasks) > MAX_DOWNLOAD_QUEUE:
                            await drain_downloads(download_tasks, keep=MAX_DOWNLOADS)
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

        await finalize_downloads(download_tasks)
        await browser.close()

    print("\n✅ Done!")
    print(f"CSV saved: {OUT_CSV}")
    print(f"PDF folder: {DOWNLOAD_DIR}/")


if __name__ == "__main__":
    asyncio.run(main())
