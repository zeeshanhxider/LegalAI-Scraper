#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import datetime
import logging
import re
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError  # type: ignore

SEARCH_RESULTS_URL = "https://appellatecases.courtinfo.ca.gov/search/searchResults.cfm"
SEARCH_FORM_URL = "https://appellatecases.courtinfo.ca.gov/search.cfm"
NAV_TIMEOUT_MS = 60000
RESULTS_TIMEOUT_MS = 45000
NETWORK_IDLE_TIMEOUT_MS = 15000
FETCH_RETRIES = 3

ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / "logs"
OUTPUT_DIR = ROOT / "downloads" / "court_opinions" / "case_numbers"
META_DIR = OUTPUT_DIR / "metadata"

LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "party-search-case-numbers.log"
META_FILE = META_DIR / f"party-search-metadata-{datetime.date.today():%Y%m%d}.csv"

def configure_logger() -> logging.Logger:
    logger = logging.getLogger("case-numbers")
    if getattr(logger, "_configured", False):
        return logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("[%(levelname)s] %(asctime)s %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    logger.propagate = False
    logger._configured = True  # type: ignore[attr-defined]
    return logger


log = configure_logger()

ALPHABET = string.ascii_uppercase

COURTS: list[tuple[str, str]] = [
    ("0", "Supreme"),
    ("1", "District 1"),
    ("2", "District 2"),
    ("3", "District 3"),
    ("41", "District 4 Div 1"),
    ("42", "District 4 Div 2"),
    ("43", "District 4 Div 3"),
    ("5", "District 5"),
    ("6", "District 6"),
]

META_FIELDS = [
    "prefix",
    "dist",
    "court",
    "status",
    "case_count",
    "raw_count",
    "dedupe",
    "first_url",
    "error",
    "scraped_at",
]


def generate_prefixes() -> list[str]:
    return [f"{a}{b}" for a in ALPHABET for b in ALPHABET]


def parse_prefixes(value: str) -> list[str]:
    candidates = [part.strip().upper() for part in value.split(",") if part.strip()]
    valid: list[str] = []
    for part in candidates:
        if len(part) == 2 and all(ch in ALPHABET for ch in part):
            valid.append(part)
        else:
            log.warning("Ignoring invalid prefix %r", part)
    return valid


def parse_dists(value: str) -> list[str]:
    parts = [part.strip() for part in value.split(",") if part.strip()]
    valid = []
    for part in parts:
        if part.isdigit():
            valid.append(part)
        else:
            log.warning("Ignoring invalid dist %r", part)
    return valid


def build_search_url(prefix: str, dist: str, start_value: int = 0) -> str:
    query = {
        "dist": dist,
        "search": "party",
        "query_partyLastNameOrOrg": prefix,
        "start": str(start_value),
    }
    return f"{SEARCH_RESULTS_URL}?{urlencode(query)}"


def build_form_url(dist: str) -> str:
    return f"{SEARCH_FORM_URL}?dist={dist}"


def update_start(url: str, start_value: int) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query))
    query["start"] = str(start_value)
    return urlunparse(parsed._replace(query=urlencode(query)))


def looks_like_bot_challenge(html: str) -> bool:
    h = (html or "").lower()
    return "istl-infinite-loop" in h or "from-page-runscript" in h or "initcustomevent" in h


def looks_like_case_summary(html: str, url: str) -> bool:
    if "maincasescreen.cfm" in (url or "").lower():
        return True
    h = (html or "").lower()
    return "case summary" in h and ("casedetails" in h or "maincasescreen.cfm" in h)


def extract_case_number_from_case_page(html: str, url: str) -> str:
    if url:
        qs = parse_qs(urlparse(url).query)
        for key in ("doc_no", "case_no", "caseNumber", "case_number", "caseNo"):
            value = (qs.get(key) or [""])[0].strip()
            if value:
                return value
    soup = BeautifulSoup(html or "", "lxml")
    for row in soup.select("div.row"):
        label = row.select_one("div.col-xs-5")
        if label and "case number" in label.get_text(" ", strip=True).lower():
            value = row.select_one("div.col-xs-7")
            if value:
                text = value.get_text(" ", strip=True)
                if text:
                    return text
    match = re.search(r"\b[A-Z]\d{4,}\b", html or "")
    return match.group(0) if match else ""


def extract_pagination(html: str) -> tuple[int, int, int] | None:
    match = re.search(r"(\d+)\s*-\s*(\d+)\s*of\s*([0-9,]+)\s*Records Found", html, re.IGNORECASE)
    if not match:
        return None
    start_display = int(match.group(1))
    end_display = int(match.group(2))
    total = int(match.group(3).replace(",", ""))
    return start_display, end_display, total


def parse_case_numbers(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one("table")
    if table is None:
        return []
    numbers: list[str] = []
    for tr in table.select("tr")[1:]:
        tds = tr.select("td")
        if not tds:
            continue
        anchor = tds[0].select_one("a")
        case_number = anchor.get_text(strip=True) if anchor else tds[0].get_text(strip=True).splitlines()[0]
        if case_number:
            numbers.append(case_number)
    return numbers


def append_metadata(row: dict[str, str]) -> None:
    write_header = not META_FILE.exists() or META_FILE.stat().st_size == 0
    with META_FILE.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=META_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in META_FIELDS})


class Browser:
    def __init__(self, headless: bool = True) -> None:
        self.headless = headless
        self._playwright = None
        self._browser = None
        self._context = None

    def start(self) -> None:
        if self._context is not None:
            return
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=self.headless,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        self._context = self._browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
            ),
        )
        self._context.set_default_timeout(60000)
        self._context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

    @property
    def context(self):
        self.start()
        return self._context

    def close(self) -> None:
        if self._browser is not None:
            self._browser.close()
        if self._playwright is not None:
            self._playwright.stop()
        self._context = None
        self._browser = None
        self._playwright = None

    def fetch_html(self, url: str) -> str:
        page = self.context.new_page()
        try:
            return fetch_html_with_page(page, url)
        finally:
            page.close()


def fetch_html_with_page(page, url: str) -> str:
    html = ""
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
            try:
                page.wait_for_selector("table", timeout=RESULTS_TIMEOUT_MS)
            except PlaywrightTimeoutError:
                pass
            try:
                page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_TIMEOUT_MS)
            except PlaywrightTimeoutError:
                pass
            html = page.content()
            if looks_like_bot_challenge(html):
                log.warning("Bot/session page detected (attempt %d/%d): %s", attempt, FETCH_RETRIES, url)
                html = ""
            if html:
                break
        except PlaywrightTimeoutError:
            log.warning(
                "Timeout loading search page (attempt %d/%d): %s",
                attempt,
                FETCH_RETRIES,
                url,
            )
        if attempt < FETCH_RETRIES:
            page.wait_for_timeout(1000)
    return html


def submit_party_search(page, prefix: str, dist: str) -> str:
    form_url = build_form_url(dist)
    html = ""
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            page.goto(form_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
            page.wait_for_selector('input[name="query_partyLastNameOrOrg"], #query_partyLastNameOrOrg', timeout=RESULTS_TIMEOUT_MS)
            try:
                if page.locator('input[name="search"][value="party"]').count() > 0:
                    page.locator('input[name="search"][value="party"]').first.check()
            except PlaywrightTimeoutError:
                pass
            if page.locator("#query_partyLastNameOrOrg").count() > 0:
                page.locator("#query_partyLastNameOrOrg").first.fill(prefix)
            else:
                page.locator('input[name="query_partyLastNameOrOrg"]').first.fill(prefix)
            if page.locator("#partySubmit").count() > 0:
                page.locator("#partySubmit").first.click()
            elif page.locator('button[type="submit"]').count() > 0:
                page.locator('button[type="submit"]').first.click()
            elif page.locator('input[type="submit"]').count() > 0:
                page.locator('input[type="submit"]').first.click()
            else:
                page.keyboard.press("Enter")
            page.wait_for_load_state("domcontentloaded")
            try:
                page.wait_for_selector("table", timeout=RESULTS_TIMEOUT_MS)
            except PlaywrightTimeoutError:
                pass
            try:
                page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_TIMEOUT_MS)
            except PlaywrightTimeoutError:
                pass
            html = page.content()
            if looks_like_bot_challenge(html):
                log.warning("Bot/session page detected (attempt %d/%d): %s", attempt, FETCH_RETRIES, form_url)
                html = ""
            if html:
                break
        except PlaywrightTimeoutError:
            log.warning("Timeout submitting search (attempt %d/%d): %s", attempt, FETCH_RETRIES, form_url)
        if attempt < FETCH_RETRIES:
            page.wait_for_timeout(1000)
    return html


def dump_debug_html(dump_dir: Path, dist: str, prefix: str, start_value: int, html: str) -> None:
    dump_dir.mkdir(parents=True, exist_ok=True)
    safe_prefix = re.sub(r"[^A-Z0-9_-]+", "_", prefix.upper())
    filename = f"{dist}_{safe_prefix}_start{start_value}.html"
    try:
        (dump_dir / filename).write_text(html or "", encoding="utf-8")
    except OSError as exc:
        log.warning("Failed to write debug HTML %s: %s", dump_dir / filename, exc)


def iterate_prefix(
    browser: Browser,
    prefix: str,
    dist: str,
    dump_dir: Path | None = None,
    debug: bool = False,
    use_form: bool = False,
) -> tuple[list[str], bool]:
    start_value = 0
    total_found = 0
    all_numbers: list[str] = []
    failures = 0
    had_error = False
    page = browser.context.new_page()
    base_url: str | None = None
    try:
        while True:
            if use_form:
                if base_url is None:
                    html = submit_party_search(page, prefix, dist)
                    base_url = page.url
                    if not html:
                        log.warning("Form search returned empty HTML for %s (dist=%s)", prefix, dist)
                else:
                    page_url = update_start(base_url, start_value)
                    html = fetch_html_with_page(page, page_url)
            else:
                page_url = update_start(build_search_url(prefix, dist, 0), start_value)
                html = fetch_html_with_page(page, page_url)

            if debug:
                log.debug("Fetched %s bytes for %s", len(html or ""), page.url)
            if not html:
                failures += 1
                log.warning("Empty response for %s (dist=%s, start=%s)", prefix, dist, start_value)
                if failures >= 2:
                    had_error = True
                    break
                continue
            failures = 0
            numbers = parse_case_numbers(html)
            if numbers:
                all_numbers.extend(numbers)
            else:
                if looks_like_case_summary(html, page.url):
                    single = extract_case_number_from_case_page(html, page.url)
                    if single:
                        log.info("Single-case redirect detected for %s (dist=%s): %s", prefix, dist, single)
                        all_numbers.append(single)
                        total_found = 1
                        break
                if total_found == 0:
                    if dump_dir is not None:
                        dump_debug_html(dump_dir, dist, prefix, start_value, html)
                    break
            pagination = extract_pagination(html)
            if pagination:
                start_display, end_display, total_found = pagination
                if debug:
                    log.debug(
                        "Pagination %s: %s-%s of %s",
                        prefix,
                        start_display,
                        end_display,
                        total_found,
                    )
                base_url = page.url
                next_start = end_display + 1
                if next_start <= start_value:
                    break
                start_value = next_start
                if total_found and start_value > total_found:
                    break
            else:
                start_value += len(numbers)
                if not numbers:
                    if dump_dir is not None:
                        dump_debug_html(dump_dir, dist, prefix, start_value, html)
                    break
        return all_numbers, had_error
    finally:
        page.close()


def write_csv(prefix: str, numbers: list[str], dist: str, court_label: str, dedupe: bool = False) -> Path:
    target_dir = OUTPUT_DIR / court_label
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{dist}_{prefix}.csv"
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for num in numbers:
        status = "duplicate" if num in seen else "found"
        if dedupe and num in seen:
            continue
        seen.add(num)
        rows.append({"case_number": num, "status": status})
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["case_number", "status"])
        writer.writeheader()
        writer.writerows(rows)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Save case numbers by party prefix")
    parser.add_argument("--prefixes", help="Comma-separated prefixes (default AA..ZZ)")
    parser.add_argument("--dist", help="Comma-separated dist codes (e.g., 0,1,41)")
    parser.add_argument("--headed", action="store_true", help="Open browser window")
    parser.add_argument(
        "--dedupe",
        action="store_true",
        help="Remove duplicate case numbers before writing",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--debug-file",
        action="store_true",
        help="Log debug output to file only (console stays INFO)",
    )
    parser.add_argument(
        "--dump-html-dir",
        help="Directory to write debug HTML when no results are parsed",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip prefixes that already have an output CSV",
    )
    parser.add_argument(
        "--start-prefix",
        help="Resume from this two-letter prefix (e.g., MC)",
    )
    parser.add_argument(
        "--use-form",
        action="store_true",
        help="Submit the search form instead of direct results URL",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel workers (default: 1)",
    )
    args = parser.parse_args()
    if args.debug or args.debug_file:
        log.setLevel(logging.DEBUG)
        for handler in log.handlers:
            if args.debug:
                handler.setLevel(logging.DEBUG)
            elif isinstance(handler, logging.FileHandler):
                handler.setLevel(logging.DEBUG)
            else:
                handler.setLevel(logging.INFO)

    prefixes = parse_prefixes(args.prefixes) if args.prefixes else generate_prefixes()
    if args.start_prefix:
        start = args.start_prefix.strip().upper()
        if len(start) == 2 and all(ch in ALPHABET for ch in start):
            if start in prefixes:
                prefixes = prefixes[prefixes.index(start):]
            else:
                prefixes = [p for p in prefixes if p >= start]
                if not prefixes:
                    log.warning("Start prefix %s not found in prefix list; using full list.", start)
                    prefixes = parse_prefixes(args.prefixes) if args.prefixes else generate_prefixes()
        else:
            log.warning("Ignoring invalid --start-prefix value: %r", args.start_prefix)
    if args.headed and args.workers > 1:
        log.warning("Headed mode does not support multiple workers; forcing workers=1.")
        args.workers = 1

    selected_courts = COURTS
    if args.dist:
        wanted = set(parse_dists(args.dist))
        selected_courts = [item for item in COURTS if item[0] in wanted]
        if not selected_courts:
            log.warning("No matching courts for dist=%s; defaulting to all courts.", args.dist)
            selected_courts = COURTS

    dump_dir: Path | None = None
    if args.dump_html_dir:
        dump_dir = Path(args.dump_html_dir)
    elif args.debug:
        dump_dir = OUTPUT_DIR / "debug_html"

    def should_skip(prefix: str, dist: str, court_label: str) -> bool:
        if not args.skip_existing:
            return False
        path = OUTPUT_DIR / court_label / f"{dist}_{prefix}.csv"
        if not path.exists():
            return False
        try:
            with path.open(newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    case_number = (row.get("case_number") or "").strip()
                    if case_number:
                        return True
        except OSError as exc:
            log.warning("Failed to read %s for skip-existing check: %s", path, exc)
        return False

    def run_prefix(prefix: str, dist: str, court_label: str) -> tuple[str, str, int, int, Path]:
        browser = Browser(headless=not args.headed)
        try:
            log.info("Searching %s prefix %s", court_label, prefix)
            numbers, had_error = iterate_prefix(
                browser,
                prefix,
                dist,
                dump_dir=dump_dir,
                debug=args.debug,
                use_form=args.use_form,
            )
            out_path = write_csv(prefix, numbers, dist, court_label, dedupe=args.dedupe)
            status = "error" if had_error else ("empty" if not numbers else "success")
            append_metadata(
                {
                    "prefix": prefix,
                    "dist": dist,
                    "court": court_label,
                    "status": status,
                    "case_count": str(len(set(numbers)) if args.dedupe else len(numbers)),
                    "raw_count": str(len(numbers)),
                    "dedupe": "yes" if args.dedupe else "no",
                    "first_url": build_search_url(prefix, dist, 0),
                    "error": "fetch_error" if had_error else "",
                    "scraped_at": datetime.datetime.utcnow().isoformat(),
                }
            )
            return prefix, court_label, (len(set(numbers)) if args.dedupe else len(numbers)), len(numbers), out_path
        except Exception as exc:
            log.exception("Failed to process %s %s: %s", court_label, prefix, exc)
            out_path = write_csv(prefix, [], dist, court_label, dedupe=args.dedupe)
            append_metadata(
                {
                    "prefix": prefix,
                    "dist": dist,
                    "court": court_label,
                    "status": "error",
                    "case_count": "0",
                    "raw_count": "0",
                    "dedupe": "yes" if args.dedupe else "no",
                    "first_url": build_search_url(prefix, dist, 0),
                    "error": f"exception:{exc.__class__.__name__}",
                    "scraped_at": datetime.datetime.utcnow().isoformat(),
                }
            )
            return prefix, court_label, 0, 0, out_path
        finally:
            browser.close()

    if args.workers <= 1:
        for dist, court_label in selected_courts:
            for prefix in prefixes:
                if should_skip(prefix, dist, court_label):
                    log.info("Skipping %s prefix %s (existing CSV)", court_label, prefix)
                    continue
                prefix, court_label, count, raw_count, out_path = run_prefix(prefix, dist, court_label)
                log.info("Saved %d case numbers (%d raw) -> %s", count, raw_count, out_path)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            skipped = []
            for dist, court_label in selected_courts:
                for prefix in prefixes:
                    if should_skip(prefix, dist, court_label):
                        skipped.append((prefix, court_label))
            for prefix, court_label in skipped:
                log.info("Skipping %s prefix %s (existing CSV)", court_label, prefix)
            futures = {
                pool.submit(run_prefix, prefix, dist, court_label): (prefix, court_label)
                for dist, court_label in selected_courts
                for prefix in prefixes
                if not should_skip(prefix, dist, court_label)
            }
            for fut in as_completed(futures):
                prefix, court_label, count, raw_count, out_path = fut.result()
                log.info("Saved %d case numbers (%d raw) -> %s", count, raw_count, out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
