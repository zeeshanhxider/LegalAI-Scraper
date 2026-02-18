"""Standalone tester that loads the party search results page for AA and dumps the table."""

from __future__ import annotations

import argparse
import csv
import datetime
import logging
import re
import string
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

SEARCH_BASE_URL = "https://appellatecases.courtinfo.ca.gov/search/searchResults.cfm"
DEFAULT_LOG_DIR = Path(__file__).resolve().parent / "logs"
DEFAULT_LOG_FILE = DEFAULT_LOG_DIR / f"test-scraper-{datetime.date.today():%Y%m%d}.log"

logger = logging.getLogger("test-scraper")

PDF_TEMPLATES = [
    "https://www.courts.ca.gov/opinions/documents/{case}.PDF",
    "https://www.courts.ca.gov/opinions/archive/{case}.PDF",
    "https://www.courts.ca.gov/opinions/revnppub/{case}.PDF",
]

BASE_SEARCH_URL = "https://appellatecases.courtinfo.ca.gov/search/"

METADATA_FIELDS = [
    "opinion_type",
    "publication_status",
    "year",
    "month",
    "file_date",
    "case_number",
    "division",
    "case_title",
    "file_contains",
    "case_info_url",
    "pdf_url",
    "pdf_filename",
    "download_status",
    "scraped_at",
    "court_of_appeal_case_number",
    "trial_court_case_number",
    "notes",
]


ALPHABET = string.ascii_uppercase


def generate_prefixes() -> list[str]:
    return [f"{a}{b}" for a in ALPHABET for b in ALPHABET]


def parse_prefixes(value: str) -> list[str]:
    candidates = [part.strip().upper() for part in value.split(",") if part.strip()]
    valid: list[str] = []
    for part in candidates:
        if len(part) == 2 and all(ch in ALPHABET for ch in part):
            valid.append(part)
        else:
            logger.warning("Ignoring invalid prefix %r", part)
    return valid


def build_search_url(prefix: str) -> str:
    query = {
        "dist": "0",
        "search": "party",
        "query_partyLastNameOrOrg": prefix,
        "start": "0",
    }
    return f"{SEARCH_BASE_URL}?{urlencode(query)}"

METADATA_FIELDS = [
    "opinion_type",
    "publication_status",
    "year",
    "month",
    "file_date",
    "case_number",
    "division",
    "case_title",
    "file_contains",
    "case_info_url",
    "pdf_url",
    "pdf_filename",
    "download_status",
    "scraped_at",
    "court_of_appeal_case_number",
    "trial_court_case_number",
    "notes",
]


@dataclass(frozen=True)
class Pagination:
    start: int
    end: int
    total: int


def normalize_header(name: str) -> str:
    """Create safe dict key from the header cell text."""
    normalized = "_".join(name.lower().split())
    return normalized.replace("#", "no").strip("_")


def derive_year_month(file_date: str, fallback: datetime.datetime) -> tuple[str, str]:
    cleaned = (file_date or "").strip()
    if cleaned:
        cleaned = cleaned.replace(".", "")
        cleaned = cleaned.replace("Sept", "Sep")
        for fmt in ("%b %d, %Y", "%B %d, %Y", "%m/%d/%Y"):
            try:
                parsed = datetime.datetime.strptime(cleaned, fmt)
                return parsed.strftime("%Y"), parsed.strftime("%B")
            except ValueError:
                continue
    return str(fallback.year), fallback.strftime("%B")


def pdf_output_dir(year: str, month: str) -> Path:
    root = Path(__file__).resolve().parent / "downloads" / "supreme_court_opinions"
    target = root / year / month
    target.mkdir(parents=True, exist_ok=True)
    return target


def download_pdf(session: requests.Session, url: str, row: dict[str, str]) -> tuple[str, str]:
    if not url:
        return "", "missing_url"
    year, month = derive_year_month(row.get("file_date", ""), datetime.datetime.utcnow())
    target_dir = pdf_output_dir(year, month)
    title_part = sanitize_filename(row.get("case_title", "") or row.get("case_number", "case"))
    filename = f"{sanitize_filename(row.get('case_number', 'case'))}_{title_part}.pdf"
    target = target_dir / filename
    if target.exists():
        return filename, "cached"
    response = session.get(url, stream=True, timeout=60)
    try:
        response.raise_for_status()
        with target.open("wb") as out_file:
            for chunk in response.iter_content(chunk_size=32768):
                if chunk:
                    out_file.write(chunk)
    except requests.RequestException as exc:
        logger.warning("Failed to download PDF %s: %s", url, exc)
        return "", "download_error"
    return filename, "downloaded"
def sanitize_filename(text: str) -> str:
    cleaned = re.sub(r"[^\w\s-]", "", text or "")
    cleaned = re.sub(r"[\s]+", "_", cleaned).strip("_")
    return cleaned[:200]


def derive_year_month(file_date: str, fallback: datetime.datetime) -> tuple[str, str]:
    cleaned = (file_date or "").strip()
    if cleaned:
        cleaned = cleaned.replace(".", "")
        cleaned = cleaned.replace("Sept", "Sep")
        for fmt in ("%b %d, %Y", "%B %d, %Y", "%m/%d/%Y"):
            try:
                parsed = datetime.datetime.strptime(cleaned, fmt)
                return parsed.strftime("%Y"), parsed.strftime("%B")
            except ValueError:
                continue
    return str(fallback.year), fallback.strftime("%B")


def build_metadata_row(row: dict[str, str], scraped_at: datetime.datetime) -> dict[str, str]:
    file_date = row.get("file_date", "")
    year, month = derive_year_month(file_date, scraped_at)
    pdf_url = row.get("pdf_url") or ""
    pdf_filename = row.get("pdf_filename") or (Path(pdf_url).name if pdf_url else "")
    base_note = row.get("notes", "").strip()
    detail_flag = "detail_fetched" if row.get("_case_detail_fetched") else "detail_missing"
    notes = ";".join(filter(None, [base_note, detail_flag]))
    return {
        "opinion_type": "Supreme Court",
        "publication_status": "Published",
        "year": year,
        "month": month,
        "file_date": file_date,
        "case_number": row.get("case_number", ""),
        "division": row.get("division", ""),
        "case_title": row.get("case_title", ""),
        "file_contains": row.get("file_contains", ""),
        "case_info_url": row.get("case_url", ""),
        "pdf_url": pdf_url,
        "pdf_filename": pdf_filename,
        "download_status": row.get("download_status", "pending"),
        "scraped_at": scraped_at.isoformat(),
        "court_of_appeal_case_number": row.get("court_of_appeal_case_number", ""),
        "trial_court_case_number": row.get("trial_court_case_number", ""),
        "notes": notes,
    }


def metadata_csv_path() -> Path:
    root = Path(__file__).resolve().parent / "downloads" / "supreme_court_opinions"
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{datetime.date.today():%Y%m%d}-metadata.csv"


def load_existing_case_numbers(path: Path) -> set[str]:
    if not path.exists():
        return set()
    seen: set[str] = set()
    with path.open("r", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            case_number = row.get("case_number", "").strip()
            if case_number:
                seen.add(case_number)
    return seen


def configure_logging(log_file: Path | str) -> None:
    """Set up console + file logging so steps are tracked."""
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("[%(levelname)s] %(asctime)s %(message)s", "%Y-%m-%d %H:%M:%S")

    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.debug("Logging initialized; file=%s", log_path)


def create_session() -> requests.Session:
    """Return session pre-configured with retries and headers."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.7, status_forcelist=(500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return session


def fetch_page(session: requests.Session, url: str) -> str:
    """Request the given URL and log basic stats."""
    logger.info("Requesting %s", url)
    response = session.get(url, timeout=30)
    response.raise_for_status()
    logger.info("Received %s (%d bytes)", response.url, len(response.text))
    logger.debug("Page snippet: %s", " ".join(response.text.split())[:1000])
    return response.text


def fetch_case_detail(session: requests.Session, url: str) -> str | None:
    """Navigate to a Supreme Court case detail page via the given URL."""
    if not url:
        return None
    logger.info("Fetching case detail %s", url)
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        logger.info("Case detail page %s (%d bytes)", url, len(response.text))
        logger.debug("Case detail snippet: %s", " ".join(response.text.split())[:600])
        return response.text
    except requests.RequestException as exc:
        logger.warning("Failed to fetch case detail %s: %s", url, exc)
        return None


def check_pdf_url(session: requests.Session, url: str) -> bool:
    """Test the PDF URL via HEAD / GET."""
    logger.debug("Testing PDF %s", url)
    response = session.head(url, allow_redirects=True, timeout=20)
    if response.status_code == 405:
        response = session.get(url, stream=True, timeout=20)
        response.raise_for_status()
    if response.status_code != 200:
        logger.debug("%s returned %s", url, response.status_code)
        return False
    return "pdf" in response.headers.get("Content-Type", "").lower()


def find_pdf_url(
    session: requests.Session,
    case_number: str,
    detail_html: str | None = None,
) -> str | None:
    """Return the first template-resolved PDF URL that exists."""
    if not case_number:
        return None
    sanitized = case_number.strip().upper()
    for template in PDF_TEMPLATES:
        url = template.format(case=sanitized)
        try:
            if check_pdf_url(session, url):
                logger.info("Valid PDF found for %s -> %s", case_number, url)
                return url
        except requests.RequestException as exc:
            logger.warning("PDF check failed for %s: %s", url, exc)
    if detail_html:
        soup = BeautifulSoup(detail_html, "lxml")
        for anchor in soup.select('a[href$=".PDF"]'):
            href = anchor["href"]
            candidate = urljoin(BASE_SEARCH_URL, href)
            if sanitized and sanitized in candidate.upper():
                return candidate
            if ".PDF" in href.upper():
                return candidate
    return None


def find_table(soup: BeautifulSoup) -> BeautifulSoup | None:
    """Locate the results table by matching expected headers."""
    for table in soup.select("table"):
        headers = [th.get_text(strip=True).lower() for th in table.select("tr > th")]
        header_line = " ".join(headers)
        if "supreme court" in header_line and "court of appeal" in header_line:
            return table
    return soup.select_one("table")


def parse_table(html: str, base_url: str) -> list[dict[str, str]]:
    """Return rows from the results table, focusing on the first three columns."""
    soup = BeautifulSoup(html, "lxml")
    table = find_table(soup)
    if table is None:
        logger.warning("No results table found")
        return []

    header_cells = [th.get_text(strip=True) for th in table.select("tr > th")]
    rows = []
    for tr in table.select("tr")[1:]:
        cells = [td.get_text(strip=True) for td in tr.select("td")]
        if not cells:
            continue
        entry: dict[str, str] = {}
        tds = tr.select("td")
        if not tds:
            continue
        case_td = tds[0]
        anchor = case_td.select_one("a")
        case_number = anchor.get_text(strip=True) if anchor else case_td.get_text(strip=True).splitlines()[0]
        href = anchor["href"] if anchor and anchor.has_attr("href") else ""
        case_url = urljoin(BASE_SEARCH_URL, href) if href else ""
        lines = [line.strip() for line in case_td.get_text(separator="\n").splitlines() if line.strip()]
        case_title = lines[1] if len(lines) > 1 else ""
        entry["case_number"] = case_number
        entry["case_title"] = case_title
        entry["case_url"] = case_url
        entry["court_of_appeal_case_number"] = cells[1] if len(cells) > 1 else ""
        entry["trial_court_case_number"] = cells[2] if len(cells) > 2 else ""
        for idx, value in enumerate(cells[1:3], start=1):
            header = header_cells[idx] if idx < len(header_cells) else f"column_{idx}"
            entry[normalize_header(header)] = value
        rows.append(entry)
    return rows


def extract_pagination(html: str) -> Pagination | None:
    """Extract the displayed row range and total count from the page."""
    match = re.search(r"(\d+)\s*-\s*(\d+)\s*of\s*([0-9,]+)\s*Records Found", html, re.IGNORECASE)
    if not match:
        return None
    start_display = int(match.group(1))
    end_display = int(match.group(2))
    total = int(match.group(3).replace(",", ""))
    return Pagination(start=start_display, end=end_display, total=total)


def update_start(url: str, start_value: int) -> str:
    """Return a copy of the URL with the updated start query parameter."""
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query))
    query["start"] = str(start_value)
    return urlunparse(parsed._replace(query=urlencode(query)))


def iterate_pages(session: requests.Session, base_url: str) -> tuple[list[dict[str, str]], int, str | None]:
    """Yield every row across all paginated pages and return the total count."""
    start_value = 0
    step = 25
    total_found = 0
    rows: list[dict[str, str]] = []
    first_page_html: str | None = None

    while True:
        page_url = update_start(base_url, start_value)
        html = fetch_page(session, page_url)
        if first_page_html is None:
            first_page_html = html
        page_rows = parse_table(html, base_url)
        if not page_rows and total_found == 0:
            logger.warning("No rows returned; stopping pagination loop")
            break
        rows.extend(page_rows)
        pagination = extract_pagination(html)
        if pagination:
            total_found = pagination.total
            logger.debug("Page shows rows %d-%d of %d", pagination.start, pagination.end, pagination.total)
            next_start = pagination.end + 1
            if next_start <= start_value:
                break
            start_value = next_start
            if total_found and start_value > total_found:
                break
        else:
            step = len(page_rows)
            if step <= 0:
                break
            start_value += step
    return rows, total_found or len(rows), first_page_html


def main() -> int:
    parser = argparse.ArgumentParser(description="Test scraper for CA Supreme Court party results")
    parser.add_argument(
        "--url",
        help="Optional custom search URL; overrides the prefix list when provided",
    )
    parser.add_argument(
        "--prefixes",
        help="Comma-separated party prefix list (default: AA, AB, ..., ZZ)",
    )
    parser.add_argument(
        "--log-file",
        default=str(DEFAULT_LOG_FILE),
        help="Path to write the dated log file (default inside california/logs)",
    )
    parser.add_argument("--output-html", help="Optional path to save the raw HTML response")
    args = parser.parse_args()

    configure_logging(args.log_file)
    session = create_session()
    prefixes: list[str] = []
    tasks: list[tuple[str, str]] = []
    if args.url:
        tasks = [(args.url, "custom")]
    else:
        prefixes = (
            parse_prefixes(args.prefixes)
            if args.prefixes
            else generate_prefixes()
        )
        tasks = [(build_search_url(prefix), prefix) for prefix in prefixes]

    all_rows: list[dict[str, str]] = []
    total_rows = 0
    scraped_at = datetime.datetime.utcnow()
    sample_html: str | None = None

    metadata_file = metadata_csv_path()
    seen_cases = load_existing_case_numbers(metadata_file)
    file_exists = metadata_file.exists()

    try:
        with metadata_file.open("a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=METADATA_FIELDS)
            if not file_exists:
                writer.writeheader()
            for url, prefix in tasks:
                logger.info("Running search for prefix %s (%s)", prefix, url)
                try:
                    rows, count, prefix_html = iterate_pages(session, url)
                except requests.RequestException as exc:
                    logger.warning("Skipping %s due to search failure: %s", prefix, exc)
                    continue
                if sample_html is None and prefix_html:
                    sample_html = prefix_html
                total_rows += count
                for row in rows:
                    case_number = row.get("case_number", "").strip()
                    if not case_number or case_number in seen_cases:
                        logger.debug("Skipping duplicate or empty case %s", case_number)
                        continue
                    detail_html = fetch_case_detail(session, row.get("case_url", ""))
                    row["_case_detail_fetched"] = bool(detail_html)
                    pdf_url = find_pdf_url(session, row.get("court_of_appeal_case_number", ""), detail_html) or ""
                    row["pdf_url"] = pdf_url
                    if row["pdf_url"]:
                        filename, status = download_pdf(session, row["pdf_url"], row)
                    else:
                        filename, status = "", "missing_pdf"
                    row["pdf_filename"] = filename
                    row["download_status"] = status
                    row["notes"] = f"prefix={prefix}"
                    metadata_row = build_metadata_row(row, scraped_at)
                    writer.writerow(metadata_row)
                    csvfile.flush()
                    seen_cases.add(case_number)
                    all_rows.append(row)
        if args.output_html and sample_html:
            Path(args.output_html).write_text(sample_html, encoding="utf-8")
            logger.info("Saved HTML to %s", args.output_html)
        logger.info("Saved metadata CSV to %s", metadata_file)
        logger.info("Aggregated %d row(s) after pagination", total_rows)
        for idx, row in enumerate(all_rows, start=1):
            logger.info("Row %d: %s", idx, row)
    except requests.RequestException as exc:
        logger.exception("Request failed: %s", exc)
        return 1
    except Exception as exc:
        logger.exception("Unhandled error: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
