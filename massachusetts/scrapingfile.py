#!/usr/bin/env python3
"""CSV-based incremental scraper for 128archive Massachusetts listings."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import logging
import random
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# https://www.mass.gov/appellate-opinion-portal
DEFAULT_START_URL = "https://128archive.com/"

ROOT_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = ROOT_DIR / "downloads"
CSV_DIR = DOWNLOADS_DIR / "CSV"
PDF_DIR = DOWNLOADS_DIR / "PDF"
LOG_DIR = ROOT_DIR / "Log"
CSV_PATH = CSV_DIR / "cases.csv"

CSV_COLUMNS = [
    "docket_number",
    "case_name",
    "release_date",
    "pdf_url",
    "read_full_url",
    "pdf_local_path",
]

REQUEST_TIMEOUT = (10, 40)
PDF_TIMEOUT = (10, 120)
MAX_RETRIES = 3
REQUEST_DELAY_MIN = 0.5
REQUEST_DELAY_MAX = 1.5

DOCKET_REGEX = re.compile(r"\b\d{1,4}-[A-Za-z]-\d{2,6}\b")
DATE_REGEX = re.compile(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b")

LOGGER = logging.getLogger("massachusetts_scraper")


def safe_text(value: str) -> str:
    return " ".join((value or "").split()).strip()


def ensure_dirs() -> None:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logging() -> None:
    LOGGER.setLevel(logging.INFO)
    LOGGER.handlers.clear()

    fmt = logging.Formatter("[%(levelname)s] %(asctime)s %(message)s")

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    LOGGER.addHandler(console)

    log_path = LOG_DIR / f"scraper-{dt.date.today():%Y-%m-%d}.log"
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(fmt)
    LOGGER.addHandler(file_handler)


def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD", "OPTIONS"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return session


def load_existing_docket_numbers() -> set[str]:
    existing: set[str] = set()
    if not CSV_PATH.exists():
        return existing

    with CSV_PATH.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            docket = safe_text(row.get("docket_number", ""))
            if docket:
                existing.add(docket)

    return existing


def ensure_csv_schema() -> None:
    if not CSV_PATH.exists():
        return

    with CSV_PATH.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        existing_columns = reader.fieldnames or []
        rows = list(reader)

    if existing_columns == CSV_COLUMNS:
        return

    # Normalize old/new formats to the expected column order.
    normalized_rows: list[dict[str, str]] = []
    for row in rows:
        normalized_rows.append({col: safe_text(row.get(col, "")) for col in CSV_COLUMNS})

    with CSV_PATH.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(normalized_rows)

    LOGGER.info("Normalized CSV columns to: %s", ", ".join(CSV_COLUMNS))


def append_cases_to_csv(cases: list[dict[str, str]], dry_run: bool = False) -> None:
    if not cases or dry_run:
        return

    file_exists = CSV_PATH.exists() and CSV_PATH.stat().st_size > 0
    with CSV_PATH.open("a", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(cases)


def normalize_date(date_text: str) -> str:
    if not date_text:
        return ""

    match = DATE_REGEX.search(date_text)
    candidate = match.group(0) if match else safe_text(date_text)

    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(candidate, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def sanitize_filename_component(text: str, max_len: int = 140) -> str:
    component = safe_text(text)
    component = component.replace(" ", "_")
    component = re.sub(r"[\\/:*?\"<>|]+", "_", component)
    component = re.sub(r"[^A-Za-z0-9._-]+", "_", component)
    component = re.sub(r"_+", "_", component).strip("._")
    if not component:
        component = "case"
    return component[:max_len]


def build_pdf_filename(case: dict[str, str]) -> str:
    release = sanitize_filename_component(case["release_date"], max_len=20)
    docket = sanitize_filename_component(case["docket_number"], max_len=80)
    return f"{release}_{docket}.pdf"


def is_valid_pdf(path: Path) -> bool:
    try:
        with path.open("rb") as fp:
            return fp.read(5).startswith(b"%PDF")
    except OSError:
        return False


def request_with_retries(
    session: requests.Session,
    url: str,
    timeout: tuple[int, int],
    stream: bool = False,
) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=timeout, stream=stream)
            response.raise_for_status()
            return response
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            LOGGER.error(
                "Request failed (attempt %s/%s): %s | %s",
                attempt,
                MAX_RETRIES,
                url,
                exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep((2 ** (attempt - 1)) + random.uniform(0.2, 0.6))

    raise RuntimeError(f"Failed request after retries: {url}") from last_error


def fetch_page(session: requests.Session, url: str) -> str:
    response = request_with_retries(session, url, timeout=REQUEST_TIMEOUT, stream=False)
    return response.text


def extract_urls(container: Tag, base_url: str) -> tuple[str, str]:
    pdf_url = ""
    read_full_url = ""

    for anchor in container.find_all("a", href=True):
        href = urljoin(base_url, anchor.get("href", "").strip())
        text = safe_text(anchor.get_text(" ", strip=True)).lower()
        href_lower = href.lower()

        if (
            "download pdf" in text
            or "downloaddisposition" in href_lower
            or href_lower.endswith(".pdf")
        ) and not pdf_url:
            pdf_url = href

        if (
            "read in full" in text
            or "viewdispositiondocinhtml" in href_lower
            or ("read" in text and "full" in text)
        ) and not read_full_url:
            read_full_url = href

    return pdf_url, read_full_url


def get_label_value(lines: list[str], label: str) -> str:
    target = label.lower()
    for idx, line in enumerate(lines):
        low = line.lower().strip()
        if low.startswith(target):
            if ":" in line:
                value = safe_text(line.split(":", 1)[1])
                if value:
                    return value
            if idx + 1 < len(lines):
                return safe_text(lines[idx + 1])
    return ""


def extract_case_from_container(container: Tag, base_url: str) -> dict[str, str] | None:
    lines = [safe_text(x) for x in container.stripped_strings]
    lines = [x for x in lines if x]
    text_block = "\n".join(lines)

    docket = get_label_value(lines, "Docket Number")
    case_name = get_label_value(lines, "Case Name")
    release_raw = get_label_value(lines, "Release Date")

    if not docket:
        dm = DOCKET_REGEX.search(text_block)
        docket = dm.group(0) if dm else ""

    if not release_raw:
        rm = DATE_REGEX.search(text_block)
        release_raw = rm.group(0) if rm else ""

    if not case_name:
        for line in lines:
            low = line.lower()
            if low in {"docket number", "case name", "release date"}:
                continue
            if "download pdf" in low or "read in full" in low:
                continue
            if DOCKET_REGEX.search(line) or DATE_REGEX.search(line):
                continue
            if len(line) < 5:
                continue
            case_name = line
            break

    release_date = normalize_date(release_raw)
    pdf_url, read_full_url = extract_urls(container, base_url)

    if not docket or not case_name or not release_date:
        return None

    return {
        "docket_number": safe_text(docket),
        "case_name": safe_text(case_name),
        "release_date": release_date,
        "pdf_url": pdf_url,
        "read_full_url": read_full_url,
    }


def parse_table_cases(soup: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
    cases: list[dict[str, str]] = []

    for table in soup.find_all("table"):
        headers = [
            safe_text(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")
        ]
        if not headers:
            continue

        docket_idx = next((i for i, h in enumerate(headers) if "docket" in h), None)
        case_idx = next((i for i, h in enumerate(headers) if "case" in h and "name" in h), None)
        date_idx = next((i for i, h in enumerate(headers) if "release" in h and "date" in h), None)

        if docket_idx is None and case_idx is None and date_idx is None:
            continue

        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if not cells:
                continue

            docket = (
                safe_text(cells[docket_idx].get_text(" ", strip=True))
                if docket_idx is not None and docket_idx < len(cells)
                else ""
            )
            case_name = (
                safe_text(cells[case_idx].get_text(" ", strip=True))
                if case_idx is not None and case_idx < len(cells)
                else ""
            )
            release_raw = (
                safe_text(cells[date_idx].get_text(" ", strip=True))
                if date_idx is not None and date_idx < len(cells)
                else ""
            )

            row_text = safe_text(row.get_text(" ", strip=True))
            if not docket:
                dm = DOCKET_REGEX.search(row_text)
                docket = dm.group(0) if dm else ""
            if not release_raw:
                rm = DATE_REGEX.search(row_text)
                release_raw = rm.group(0) if rm else ""

            release_date = normalize_date(release_raw)
            pdf_url, read_full_url = extract_urls(row, base_url)

            if docket and case_name and release_date:
                cases.append(
                    {
                        "docket_number": docket,
                        "case_name": case_name,
                        "release_date": release_date,
                        "pdf_url": pdf_url,
                        "read_full_url": read_full_url,
                    }
                )

    return cases


def parse_cases(html: str, base_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    cases_by_docket: dict[str, dict[str, str]] = {}

    for case in parse_table_cases(soup, base_url):
        docket = case["docket_number"]
        cases_by_docket[docket] = case

    for anchor in soup.find_all("a", href=True):
        href = urljoin(base_url, anchor.get("href", "").strip())
        text = safe_text(anchor.get_text(" ", strip=True)).lower()

        is_pdf_link = (
            "download pdf" in text
            or "downloaddisposition" in href.lower()
            or href.lower().endswith(".pdf")
        )
        if not is_pdf_link:
            continue

        container: Tag | None = None
        for parent in anchor.parents:
            if not isinstance(parent, Tag):
                continue
            if parent.name not in {"tr", "li", "article", "section", "div"}:
                continue
            parent_text = safe_text(parent.get_text(" ", strip=True))
            parent_low = parent_text.lower()
            if (
                ("docket number" in parent_low and "release date" in parent_low)
                or (DOCKET_REGEX.search(parent_text) and DATE_REGEX.search(parent_text))
            ):
                container = parent
                break

        if container is None and isinstance(anchor.parent, Tag):
            container = anchor.parent
        if container is None:
            continue

        case = extract_case_from_container(container, base_url)
        if not case:
            continue

        docket = case["docket_number"]
        cases_by_docket[docket] = case

    return list(cases_by_docket.values())


def set_query_param(url: str, key: str, value: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query[key] = [value]
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def parse_int(value: str | None) -> int | None:
    try:
        return int(safe_text(value or ""))
    except (TypeError, ValueError):
        return None


def get_form_params(form: Tag) -> dict[str, str]:
    params: dict[str, str] = {}

    for field in form.find_all(["input", "select", "textarea"]):
        name = safe_text(field.get("name", ""))
        if not name:
            continue

        tag_name = field.name.lower()
        value = ""

        if tag_name == "input":
            field_type = (field.get("type") or "text").lower()
            if field_type in {"checkbox", "radio"} and not field.has_attr("checked"):
                continue
            value = field.get("value", "")
        elif tag_name == "select":
            selected = field.find("option", selected=True)
            if selected is None:
                selected = field.find("option")
            value = selected.get("value", "") if selected else ""
        else:
            value = field.get_text(" ", strip=True)

        params[name] = safe_text(value)

    return params


def parse_page_info(soup: BeautifulSoup) -> tuple[int | None, int | None]:
    current = None
    total = None

    current_input = soup.select_one(
        "input[name='CurrentPageNo'], input#CurrentPageNodn, input[name='CurrentPage']"
    )
    pages_input = soup.select_one("input[name='Pages'], input[name='TotalPages']")

    if current_input:
        current = parse_int(current_input.get("value"))
    if pages_input:
        total = parse_int(pages_input.get("value"))

    if current is not None and total is not None:
        return current, total

    all_text = safe_text(soup.get_text(" ", strip=True))
    page_of_match = re.search(
        r"Current\s*Page.*?(\d+)\s*of\s*(\d+)", all_text, re.IGNORECASE
    )
    if page_of_match:
        return parse_int(page_of_match.group(1)), parse_int(page_of_match.group(2))

    current_match = re.search(r"Current\s*Page\s*:?\s*(\d+)", all_text, re.IGNORECASE)
    last_match = re.search(r"Last\s*Page\s*:?\s*(\d+)", all_text, re.IGNORECASE)
    current = int(current_match.group(1)) if current_match else None
    total = int(last_match.group(1)) if last_match else None
    return current, total


def build_next_page_url(current_url: str, soup: BeautifulSoup) -> str | None:
    current_norm = urlunparse(urlparse(current_url)._replace(fragment=""))

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        if not href or href.lower().startswith("javascript:"):
            continue

        text = safe_text(anchor.get_text(" ", strip=True)).lower()
        aria = (anchor.get("aria-label") or "").lower()
        title = (anchor.get("title") or "").lower()

        if (
            "next" in text
            or text in {">", "\u00bb", "\u203a", "next >"}
            or "next" in aria
            or "next" in title
        ):
            candidate = urljoin(current_url, href)
            candidate = urlunparse(urlparse(candidate)._replace(fragment=""))
            if candidate != current_norm:
                return candidate

    next_button = soup.select_one(
        "button#btnmovenext, button[name='btnmovenext'], "
        "input#btnmovenext, input[name='btnmovenext']"
    )
    form = next_button.find_parent("form") if next_button else None

    if form:
        form_action = safe_text(form.get("action", "")) or current_url
        action_url = urljoin(current_url, form_action)
        action_url = urlunparse(urlparse(action_url)._replace(fragment=""))

        params = {k: v[0] for k, v in parse_qs(urlparse(current_url).query, keep_blank_values=True).items()}
        params.update(get_form_params(form))

        current_key = None
        current_page = None
        for key in ("CurrentPageNo", "CurrentPage", "page", "Page", "p"):
            parsed_value = parse_int(params.get(key))
            if parsed_value is not None:
                current_key = key
                current_page = parsed_value
                break

        total_pages = None
        for key in ("Pages", "TotalPages", "LastPage"):
            parsed_value = parse_int(params.get(key))
            if parsed_value is not None:
                total_pages = parsed_value
                break

        if current_page is None:
            page_info_current, _ = parse_page_info(soup)
            current_page = page_info_current
            if current_page is not None:
                current_key = "CurrentPageNo"

        if total_pages is None:
            _, page_info_total = parse_page_info(soup)
            total_pages = page_info_total

        if current_page is not None and total_pages is not None and current_page >= total_pages:
            return None

        if current_key is None:
            current_key = "CurrentPageNo"
            next_page = 2
        else:
            next_page = (current_page or 1) + 1

        params[current_key] = str(next_page)

        if next_button:
            button_name = safe_text(next_button.get("name", ""))
            button_value = safe_text(next_button.get("value", ""))
            if button_name:
                params[button_name] = button_value or "movenext"

        next_url = urlunparse(urlparse(action_url)._replace(query=urlencode(params, doseq=True)))
        next_norm = urlunparse(urlparse(next_url)._replace(fragment=""))
        if next_norm != current_norm:
            return next_url

    current_page, last_page = parse_page_info(soup)
    if current_page is not None and last_page is not None and current_page < last_page:
        return set_query_param(current_url, "CurrentPageNo", str(current_page + 1))

    parsed = urlparse(current_url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    for key in ("CurrentPageNo", "CurrentPage", "page", "Page", "p"):
        val = query.get(key, [None])[0]
        if val is None:
            continue
        try:
            current = int(val)
        except (TypeError, ValueError):
            continue
        return set_query_param(current_url, key, str(current + 1))

    return None


def download_pdf(
    session: requests.Session,
    case: dict[str, str],
    dry_run: bool = False,
) -> tuple[str, str]:
    pdf_url = safe_text(case.get("pdf_url", ""))
    if not pdf_url:
        return "", "missing_pdf_url"

    filename = build_pdf_filename(case)
    output_path = PDF_DIR / filename
    relative_path = output_path.relative_to(ROOT_DIR).as_posix()

    if output_path.exists() and is_valid_pdf(output_path):
        return relative_path, "exists"

    if dry_run:
        LOGGER.info("[dry-run] Would download PDF: %s", pdf_url)
        return relative_path, "dry_run"

    temp_path = output_path.with_suffix(".part")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with request_with_retries(
                session,
                pdf_url,
                timeout=PDF_TIMEOUT,
                stream=True,
            ) as response:
                content_type = response.headers.get("Content-Type", "").lower()
                with temp_path.open("wb") as fp:
                    for chunk in response.iter_content(chunk_size=1024 * 64):
                        if chunk:
                            fp.write(chunk)

            if "application/pdf" not in content_type and not is_valid_pdf(temp_path):
                raise ValueError(f"Downloaded file is not a PDF (content-type={content_type})")

            temp_path.replace(output_path)
            return relative_path, "downloaded"

        except Exception as exc:  # noqa: BLE001
            LOGGER.error(
                "PDF download failed (attempt %s/%s): %s | %s",
                attempt,
                MAX_RETRIES,
                pdf_url,
                exc,
            )
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
            if attempt < MAX_RETRIES:
                time.sleep((2 ** (attempt - 1)) + random.uniform(0.2, 0.6))

    return "", "failed"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape 128archive listing pages into CSV with incremental updates."
    )
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum pages to scrape.")
    parser.add_argument("--dry-run", action="store_true", help="Parse only; no CSV/PDF writes.")
    parser.add_argument(
        "--download-pdf",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable/disable PDF downloads (default: enabled).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    ensure_dirs()
    setup_logging()
    ensure_csv_schema()

    session = create_session()
    existing_dockets = load_existing_docket_numbers()

    LOGGER.info("Run started at UTC: %s", dt.datetime.now(dt.timezone.utc).isoformat())
    LOGGER.info("Loaded %s existing docket numbers from CSV", len(existing_dockets))

    current_url = DEFAULT_START_URL
    visited_urls: set[str] = set()

    pages_visited = 0
    new_cases_added = 0
    skipped_cases = 0
    pdf_downloaded = 0
    pdf_skipped_existing = 0

    while current_url:
        if args.max_pages is not None and pages_visited >= args.max_pages:
            LOGGER.info("Reached max pages limit: %s", args.max_pages)
            break

        normalized_current = urlunparse(urlparse(current_url)._replace(fragment=""))
        if normalized_current in visited_urls:
            LOGGER.info("Pagination loop detected. Stopping at: %s", normalized_current)
            break
        visited_urls.add(normalized_current)

        pages_visited += 1
        LOGGER.info("Fetching page %s: %s", pages_visited, current_url)

        try:
            html = fetch_page(session, current_url)
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Stopping due to page fetch failure: %s", exc)
            break

        page_cases = parse_cases(html, current_url)
        LOGGER.info("Page %s cases found: %s", pages_visited, len(page_cases))

        rows_to_append: list[dict[str, str]] = []

        for case in page_cases:
            docket = case["docket_number"]
            if docket in existing_dockets:
                skipped_cases += 1
                continue

            row = {
                "docket_number": docket,
                "case_name": case["case_name"],
                "release_date": case["release_date"],
                "pdf_url": case.get("pdf_url", ""),
                "read_full_url": case.get("read_full_url", ""),
                "pdf_local_path": "",
            }

            if args.download_pdf:
                local_path, pdf_status = download_pdf(session, row, dry_run=args.dry_run)
                if local_path:
                    row["pdf_local_path"] = local_path
                if pdf_status == "downloaded":
                    pdf_downloaded += 1
                    LOGGER.info("PDF downloaded: %s", local_path)
                elif pdf_status == "exists":
                    pdf_skipped_existing += 1
                    LOGGER.info("PDF already exists, skipped: %s", local_path)
                elif pdf_status == "failed":
                    LOGGER.error("PDF download failed for docket %s", docket)

            rows_to_append.append(row)
            existing_dockets.add(docket)

        append_cases_to_csv(rows_to_append, dry_run=args.dry_run)

        if rows_to_append:
            new_cases_added += len(rows_to_append)
            LOGGER.info("Page %s new cases added: %s", pages_visited, len(rows_to_append))
        else:
            LOGGER.info("Page %s no new cases added", pages_visited)

        soup = BeautifulSoup(html, "html.parser")
        next_url = build_next_page_url(current_url, soup)
        if not next_url:
            LOGGER.info("No next page detected. Scraping complete.")
            break

        current_url = next_url
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

    LOGGER.info(
        "Run summary: pages_visited=%s, new_cases_added=%s, skipped_cases=%s, "
        "pdf_downloaded=%s, pdf_skipped_existing=%s",
        pages_visited,
        new_cases_added,
        skipped_cases,
        pdf_downloaded,
        pdf_skipped_existing,
    )


if __name__ == "__main__":
    main()
