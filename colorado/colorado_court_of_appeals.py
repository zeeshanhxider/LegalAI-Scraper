import asyncio
import csv
import os
import re
from urllib.parse import urljoin
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

BASE = "https://research.coloradojudicial.gov"
SEARCH_URL = "https://research.coloradojudicial.gov/search/jurisdiction:US+content_type:2+court:14024_02/*"

# Folder structure:
# downloads/<court_name>/CSV/<court_name>.csv
# downloads/<court_name>/<year>/<docket_number>/<pdf_file>.pdf
COURT_NAME = "colorado_court_of_appeals"
ROOT_DIR = os.path.join("downloads", COURT_NAME)
CSV_DIR = os.path.join(ROOT_DIR, "CSV")
CSV_PATH = os.path.join(CSV_DIR, f"{COURT_NAME}.csv")

# Search page selectors
ROW_SEL = "div.results-list li.documento__result-item.document"
MORE_BTN = "li.section-pager.epag-next-page span.more"

def ensure_dirs():
    os.makedirs(CSV_DIR, exist_ok=True)

def safe_filename(name: str, max_len: int = 140) -> str:
    name = name.strip()
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)
    name = re.sub(r"\s+", " ", name)
    return name[:max_len].rstrip() if len(name) > max_len else name

def normalize_docket_number(raw: str) -> str:
    text = re.sub(r"\s+", " ", (raw or "")).strip()
    text = re.sub(r"(?i)^docket\s*number\s*[:\-]?\s*", "", text).strip()
    if not text:
        return ""

    for token in re.split(r"[,\s;|]+", text):
        token = token.strip()
        if re.match(r"^(?=.*[A-Za-z])(?=.*\d)[A-Za-z0-9./-]{3,}$", token):
            return token
    return text

def extract_year_value(date_text: str, docket_number: str = "") -> str:
    m = re.search(r"\b(19|20)\d{2}\b", (date_text or ""))
    if m:
        return m.group(0)

    # Fallback: some dockets include year at the end (e.g., 25CA2026).
    m = re.search(r"(19|20)\d{2}$", (docket_number or ""))
    if m:
        return m.group(0)

    return "unknown_year"

async def extract_row(li):
    a = li.locator("a.result").first
    title = (await a.inner_text()).strip() if await a.count() else ""
    href = (await a.get_attribute("href")) if await a.count() else ""
    href = (href or "").strip()
    detail_url = urljoin(BASE, href)

    values = li.locator("div.result-metadata-properties span.value")
    n = await values.count()
    meta = []
    for i in range(n):
        meta.append((await values.nth(i).inner_text()).strip())

    court = meta[0] if len(meta) >= 1 else ""
    date = meta[1] if len(meta) >= 2 else ""
    citation = " | ".join(meta[2:]) if len(meta) >= 3 else ""

    return {
        "docket_number": "",
        "title": title,
        "court": court,
        "date": date,
        "citation": citation,
        "detail_url": detail_url,
    }

async def extract_docket_number(detail_page):
    # Preferred: read the metadata block where the label is exactly "Docket Number".
    docket = await detail_page.evaluate(
        """
        () => {
          const norm = (s) => (s || "").replace(/\\s+/g, " ").trim();
          const isLabel = (s) => /^docket\\s*number$/i.test(norm(s));

          const nodes = Array.from(document.querySelectorAll("div, span, dt, th, p, strong, b, label"));
          for (const node of nodes) {
            if (!isLabel(node.textContent)) continue;

            const siblings = [];
            if (node.nextElementSibling) siblings.push(node.nextElementSibling);
            const parent = node.parentElement;
            if (parent) {
              const children = Array.from(parent.children);
              const idx = children.indexOf(node);
              if (idx >= 0) siblings.push(...children.slice(idx + 1));
            }

            for (const sib of siblings) {
              const v = norm(sib.textContent);
              if (v && !isLabel(v)) return v;
            }
          }
          return "";
        }
        """
    )
    docket = normalize_docket_number(docket)
    if docket:
        return docket

    # Fallback: parse visible text lines around "Docket Number".
    try:
        body_text = await detail_page.inner_text("body")
    except Exception:
        return ""

    lines = [ln.strip() for ln in (body_text or "").splitlines() if ln.strip()]
    for i, line in enumerate(lines):
        if re.fullmatch(r"docket\s*number", line, flags=re.IGNORECASE):
            if i + 1 < len(lines):
                return normalize_docket_number(lines[i + 1])
        m = re.search(r"docket\s*number\s*[:\-]?\s*([A-Za-z0-9\-/.]+)", line, flags=re.IGNORECASE)
        if m:
            return normalize_docket_number(m.group(1))

    return ""

async def wait_for_detail_page_ready(detail_page):
    try:
        await detail_page.wait_for_function(
            "() => document.body && document.body.innerText.includes('Docket Number')",
            timeout=12000,
        )
        return
    except Exception:
        pass

    for sel in ("div#formats", "[aria-label='Download']"):
        try:
            await detail_page.wait_for_selector(sel, timeout=4000)
            return
        except Exception:
            continue

    await detail_page.wait_for_timeout(1500)

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
            "docket_number", "title", "court", "date", "citation", "detail_url",
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

                    key = row["detail_url"] or row["title"]
                    if key in seen:
                        continue
                    seen.add(key)

                    title = row["title"] or ""
                    pdf_filename = ""
                    pdf_path = ""

                    try:
                        # Open detail in a NEW page (do not disturb search page)
                        detail_page = await context.new_page()
                        await detail_page.goto(row["detail_url"], wait_until="domcontentloaded", timeout=120000)
                        await wait_for_detail_page_ready(detail_page)

                        docket_number = await extract_docket_number(detail_page)
                        if not docket_number:
                            await detail_page.wait_for_timeout(2000)
                            docket_number = await extract_docket_number(detail_page)
                        row["docket_number"] = docket_number

                        year_value = extract_year_value(row.get("date", ""), docket_number)
                        base_id = docket_number or f"row_{written+1}"
                        base_name = safe_filename(f"{base_id}_{title}" if title else base_id)
                        pdf_filename = f"{base_name}.pdf"
                        pdf_dir = os.path.join(ROOT_DIR, year_value, base_id)
                        os.makedirs(pdf_dir, exist_ok=True)
                        pdf_path = os.path.join(pdf_dir, pdf_filename)

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
                        if not pdf_filename:
                            base_name = safe_filename(f"row_{written+1}_{title}" if title else f"row_{written+1}")
                            pdf_filename = f"{base_name}.pdf"
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
        print("PDF root folder:", ROOT_DIR)

asyncio.run(main())
