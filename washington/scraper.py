"""
Washington State Courts Opinion Scraper

This scraper downloads Supreme Court opinions from the Washington Courts website
and saves them with associated metadata.
"""

import os
import re
import csv
import json
import time
import random
import logging
import signal
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime
from typing import Optional, List, Dict, Any, Set, Tuple
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from config import (
    BASE_URL, OPINIONS_URL, HEADERS,
    REQUEST_DELAY_MIN, REQUEST_DELAY_MAX, REQUEST_TIMEOUT,
    MAX_RETRIES, RETRY_DELAY, MAX_RETRY_DELAY,
    OUTPUT_DIR, METADATA_FILENAME, CHECKPOINT_FILE,
    COURT_LEVELS, PUB_STATUS, OPINION_TYPES
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class OpinionMetadata:
    """Data class to store opinion metadata"""
    opinion_type: str
    publication_status: str
    year: str
    month: str
    file_date: str
    case_number: str
    division: str
    case_title: str
    file_contains: str
    case_info_url: str
    pdf_url: str
    pdf_filename: str
    download_status: str
    scraped_at: str


class WashingtonCourtsScraper:
    """Scraper for Washington State Courts opinions"""
    
    def __init__(self, output_dir: str = OUTPUT_DIR, opinion_type: str = "supreme_court", resume: bool = True, workers: int = 5):
        self.base_output_dir = output_dir
        self.opinion_type = opinion_type
        self.workers = workers
        self.lock = Lock()  # Thread-safe operations
        
        # Get settings from OPINION_TYPES config
        if opinion_type not in OPINION_TYPES:
            raise ValueError(f"Unknown opinion type: {opinion_type}. Valid types: {list(OPINION_TYPES.keys())}")
        
        type_config = OPINION_TYPES[opinion_type]
        self.court_level = type_config["court_level"]
        self.pub_status = type_config["pub_status"]
        self.opinion_folder = type_config["folder"]
        self.opinion_type_name = type_config["opinion_type"]
        self.publication_status = type_config["publication_status"]
        
        # Set output directory to include opinion type folder
        self.output_dir = os.path.join(output_dir, self.opinion_folder)
        
        self.resume = resume
        # Don't create shared session - each thread will create its own
        self.metadata_records: List[OpinionMetadata] = []
        
        # Track downloaded files for resume capability
        self.downloaded_cases: Set[str] = set()
        self.failed_cases: Set[str] = set()  # Track failed downloads for retry
        self.checkpoint_file = os.path.join(self.output_dir, CHECKPOINT_FILE)
        
        # Graceful shutdown flag
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Load checkpoint if resuming
        if resume:
            self._load_checkpoint()
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.warning("Shutdown signal received. Finishing current download and saving progress...")
        self.shutdown_requested = True
    
    def _load_checkpoint(self):
        """Load checkpoint data from previous run"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    self.downloaded_cases = set(data.get('downloaded_cases', []))
                    self.failed_cases = set(data.get('failed_cases', []))
                    logger.info(f"Loaded checkpoint: {len(self.downloaded_cases)} successful, {len(self.failed_cases)} failed (will retry)")
            except Exception as e:
                logger.warning(f"Could not load checkpoint: {e}")
    
    def _save_checkpoint(self):
        """Save checkpoint data for resume capability"""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump({
                    'downloaded_cases': list(self.downloaded_cases),
                    'failed_cases': list(self.failed_cases),
                    'last_saved': datetime.now().isoformat()
                }, f)
            logger.debug(f"Checkpoint saved: {len(self.downloaded_cases)} successful, {len(self.failed_cases)} failed")
        except Exception as e:
            logger.error(f"Could not save checkpoint: {e}")
        
    def _delay(self):
        """Add random delay between requests"""
        delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
        time.sleep(delay)
        
    def _make_request(self, url: str, retries: int = MAX_RETRIES, is_pdf: bool = False) -> Optional[requests.Response]:
        """Make HTTP request with exponential backoff retry logic - creates session per call for thread safety"""
        # Create a new session for this request (thread-safe)
        session = requests.Session()
        session.headers.update(HEADERS)
        
        for attempt in range(retries):
            try:
                self._delay()
                
                # For PDF downloads, use stream mode
                if is_pdf:
                    response = session.get(url, timeout=REQUEST_TIMEOUT * 2, stream=True)
                else:
                    response = session.get(url, timeout=REQUEST_TIMEOUT)
                
                response.raise_for_status()
                
                # Ensure proper encoding for HTML responses
                if not is_pdf:
                    response.encoding = 'utf-8'
                
                session.close()
                return response
                
            except requests.exceptions.ConnectionError as e:
                # Connection issues - use longer backoff
                retry_delay = min(RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                logger.warning(f"Connection error (attempt {attempt + 1}/{retries}): {url}")
                logger.warning(f"Waiting {retry_delay}s before retry...")
                if attempt < retries - 1:
                    time.sleep(retry_delay)
                    
            except requests.exceptions.Timeout as e:
                retry_delay = min(RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                logger.warning(f"Timeout (attempt {attempt + 1}/{retries}): {url}")
                if attempt < retries - 1:
                    time.sleep(retry_delay)
                    
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Rate limited
                    retry_delay = min(RETRY_DELAY * (3 ** attempt), MAX_RETRY_DELAY)
                    logger.warning(f"Rate limited! Waiting {retry_delay}s...")
                    time.sleep(retry_delay)
                elif e.response.status_code >= 500:  # Server error
                    retry_delay = min(RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                    logger.warning(f"Server error {e.response.status_code} (attempt {attempt + 1}/{retries}): {url}")
                    if attempt < retries - 1:
                        time.sleep(retry_delay)
                else:
                    logger.error(f"HTTP error {e.response.status_code}: {url}")
                    session.close()
                    return None
                    
            except requests.RequestException as e:
                retry_delay = min(RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                logger.warning(f"Request failed (attempt {attempt + 1}/{retries}): {url} - {e}")
                if attempt < retries - 1:
                    time.sleep(retry_delay)
        
        session.close()
        logger.error(f"All {retries} retries failed for: {url}")
        return None
    
    def get_available_years(self) -> List[str]:
        """Get all available years for Supreme Court Opinions from the main page"""
        logger.info("Fetching available years...")
        
        url = f"{OPINIONS_URL}?fa=opinions.displayAll"
        response = self._make_request(url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        years = []
        
        # Find year links - they use byYear with crtLevel parameter
        # Example: ?fa=opinions.byYear&fileYear=2013&crtLevel=S&pubStatus=PUB
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Clean up text - remove pipe character and whitespace
            text = text.replace('|', '').strip()
            
            # Check if it's a year link for the specified court level
            if 'byYear' in href and f'crtLevel={self.court_level}' in href and re.match(r'^\d{4}$', text):
                years.append(text)
        
        # Remove duplicates and sort
        years = sorted(list(set(years)), reverse=True)
        logger.info(f"Found {len(years)} years: {years}")
        return years
    
    def get_year_page_url(self, year: str) -> str:
        """Construct URL for a specific year's opinions page"""
        # Correct URL pattern: ?fa=opinions.byYear&fileYear=2013&crtLevel=S&pubStatus=PUB
        return f"{OPINIONS_URL}?fa=opinions.byYear&fileYear={year}&crtLevel={self.court_level}&pubStatus={self.pub_status}"
    
    def get_cases_for_year(self, year: str) -> List[Dict[str, Any]]:
        """Get all cases for a given year"""
        url = self.get_year_page_url(year)
        logger.info(f"Fetching cases for year {year}...")
        logger.info(f"URL: {url}")
        
        response = self._make_request(url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        all_cases = []
        
        # Find all tables with case data
        tables = soup.find_all('table')
        
        for table in tables:
            # Parse the table rows
            rows = table.find_all('tr')
            
            for row in rows[1:]:  # Skip header row
                cells = row.find_all('td')
                if len(cells) >= 4:
                    try:
                        file_date = cells[0].get_text(strip=True)
                        
                        # Extract month from file_date for accuracy
                        month = self._extract_month_from_date(file_date) or "Unknown"
                        
                        # Get case number and link
                        case_cell = cells[1]
                        case_link = case_cell.find('a')
                        case_number = case_link.get_text(strip=True) if case_link else case_cell.get_text(strip=True)
                        case_info_url = urljoin(BASE_URL, case_link.get('href')) if case_link else ""
                        
                        # Court of Appeals has 5 columns (with Division), Supreme Court has 4
                        # Check if cell[2] is a Division (I, II, III) or a case title
                        cell2_text = cells[2].get_text(strip=True)
                        
                        if len(cells) >= 5 and cell2_text in ['I', 'II', 'III']:
                            # Court of Appeals format: Date, Case#, Div, Title, Contains
                            division = cell2_text
                            case_title = cells[3].get_text(strip=True)
                            file_contains = cells[4].get_text(strip=True) if len(cells) > 4 else ""
                        else:
                            # Supreme Court format: Date, Case#, Title, Contains
                            division = ""
                            case_title = cell2_text
                            file_contains = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                        
                        # Remove leading asterisk from case title
                        case_title = re.sub(r'^\*\s*', '', case_title)
                        
                        if case_number and case_info_url:
                            all_cases.append({
                                'year': year,
                                'month': month,
                                'file_date': file_date,
                                'case_number': case_number,
                                'division': division,
                                'case_title': case_title,
                                'file_contains': file_contains,
                                'case_info_url': case_info_url
                            })
                    except Exception as e:
                        logger.warning(f"Error parsing row: {e}")
                        continue
        
        logger.info(f"Found {len(all_cases)} cases for year {year}")
        return all_cases

    def get_months_and_cases(self, year: str) -> Dict[str, List[Dict[str, Any]]]:
        """Get all months and their cases for a given year"""
        url = self.get_year_page_url(year)
        logger.info(f"Fetching cases for year {year}...")
        
        response = self._make_request(url)
        if not response:
            return {}
        
        soup = BeautifulSoup(response.text, 'html.parser')
        months_data = {}
        
        current_month = None
        
        # Find all tables with case data
        # The structure shows month headers followed by tables
        for element in soup.find_all(['h2', 'h3', 'p', 'table']):
            # Check if this is a month header
            text = element.get_text(strip=True)
            month_match = re.match(r'^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}$', text)
            
            if month_match:
                current_month = month_match.group(1)
                if current_month not in months_data:
                    months_data[current_month] = []
                continue
            
            # If this is a table and we have a current month, parse it
            if element.name == 'table' and current_month:
                cases = self._parse_cases_table(element, year, current_month)
                months_data[current_month].extend(cases)
        
        # If no months found with headers, try parsing all tables
        if not months_data:
            tables = soup.find_all('table')
            for table in tables:
                cases = self._parse_cases_table(table, year, "Unknown")
                if cases:
                    # Try to extract month from file dates
                    for case in cases:
                        month = self._extract_month_from_date(case.get('file_date', ''))
                        if month:
                            if month not in months_data:
                                months_data[month] = []
                            months_data[month].append(case)
        
        total_cases = sum(len(cases) for cases in months_data.values())
        logger.info(f"Found {total_cases} cases across {len(months_data)} months for year {year}")
        
        return months_data
    
    def _parse_cases_table(self, table, year: str, month: str) -> List[Dict[str, Any]]:
        """Parse a table containing case information"""
        cases = []
        rows = table.find_all('tr')
        
        # Skip header row
        for row in rows[1:] if len(rows) > 1 else rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 4:
                try:
                    file_date = cells[0].get_text(strip=True)
                    
                    # Get case number and link
                    case_cell = cells[1]
                    case_link = case_cell.find('a')
                    case_number = case_link.get_text(strip=True) if case_link else case_cell.get_text(strip=True)
                    case_info_url = urljoin(BASE_URL, case_link.get('href')) if case_link else ""
                    
                    case_title = cells[2].get_text(strip=True)
                    # Remove asterisk if present
                    case_title = re.sub(r'^\*\s*', '', case_title)
                    
                    file_contains = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                    
                    if case_number and case_info_url:
                        cases.append({
                            'year': year,
                            'month': month,
                            'file_date': file_date,
                            'case_number': case_number,
                            'case_title': case_title,
                            'file_contains': file_contains,
                            'case_info_url': case_info_url
                        })
                except Exception as e:
                    logger.warning(f"Error parsing table row: {e}")
                    continue
        
        return cases
    
    def _extract_month_from_date(self, date_str: str) -> Optional[str]:
        """Extract month name from a date string like 'Feb. 28, 2013'"""
        month_map = {
            'jan': 'January', 'feb': 'February', 'mar': 'March',
            'apr': 'April', 'may': 'May', 'jun': 'June',
            'jul': 'July', 'aug': 'August', 'sep': 'September',
            'oct': 'October', 'nov': 'November', 'dec': 'December'
        }
        
        for abbrev, full in month_map.items():
            if abbrev in date_str.lower():
                return full
        return None
    
    def get_pdf_url(self, case_info_url: str) -> Optional[str]:
        """Get the PDF URL from the case info page"""
        response = self._make_request(case_info_url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for "View the Slip Opinion(s) Filed for this Case" link
        # This link leads directly to the PDF
        for link in soup.find_all('a', href=True):
            text = link.get_text(strip=True)
            href = link.get('href', '')
            
            # Check for the slip opinion link
            if 'slip opinion' in text.lower() or 'View the Slip Opinion' in text:
                pdf_url = urljoin(BASE_URL, href)
                return pdf_url
        
        # Alternative: look for direct PDF links
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if '/opinions/pdf/' in href or href.endswith('.pdf'):
                return urljoin(BASE_URL, href)
        
        # Try to construct PDF URL from case number in the URL
        # Pattern: filename=871051MAJ -> PDF is /opinions/pdf/871051.pdf
        parsed = urlparse(case_info_url)
        query_params = parse_qs(parsed.query)
        if 'filename' in query_params:
            filename = query_params['filename'][0]
            # Remove suffix like MAJ, MIN, etc.
            base_number = re.sub(r'[A-Z]+$', '', filename)
            pdf_url = f"{BASE_URL}/opinions/pdf/{base_number}.pdf"
            return pdf_url
        
        logger.warning(f"Could not find PDF link on page: {case_info_url}")
        return None
    
    def download_pdf(self, pdf_url: str, save_path: str) -> bool:
        """Download a PDF file with retry logic"""
        try:
            # Check if file already exists and has content (for resume)
            if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                logger.info(f"Already exists, skipping: {save_path}")
                return True
            
            response = self._make_request(pdf_url, is_pdf=True)
            if not response:
                return False
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            # Download with progress tracking
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
            
            # Verify download completed
            if total_size > 0 and downloaded < total_size:
                logger.warning(f"Incomplete download: {downloaded}/{total_size} bytes for {save_path}")
                os.remove(save_path)  # Remove incomplete file
                return False
            
            logger.info(f"Downloaded: {save_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to download {pdf_url}: {e}")
            # Clean up partial file if exists
            if os.path.exists(save_path):
                try:
                    os.remove(save_path)
                except:
                    pass
            return False
    
    def sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for Windows compatibility"""
        # Remove or replace invalid characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        # Limit length
        if len(filename) > 200:
            filename = filename[:200]
        return filename
    
    def _process_single_case(self, case: Dict[str, Any], year: str, case_index: int, total_cases: int) -> Tuple[Optional[OpinionMetadata], bool]:
        """Process a single case - used by parallel executor. Returns (metadata, success)"""
        case_id = f"{year}_{case['case_number']}"
        
        # Skip if already successfully processed (resume capability)
        if self.resume and case_id in self.downloaded_cases:
            logger.info(f"Skipping already processed case {case_index+1}/{total_cases}: {case['case_number']}")
            return None, True
        
        # If it was a failed case before, we're retrying it
        is_retry = case_id in self.failed_cases
        if is_retry:
            logger.info(f"Retrying previously failed case {case_index+1}/{total_cases}: {case['case_number']}")
            with self.lock:
                self.failed_cases.discard(case_id)
        else:
            logger.info(f"Processing case {case_index+1}/{total_cases}: {case['case_number']}")
        
        try:
            # Get PDF URL from case info page
            pdf_url = self.get_pdf_url(case['case_info_url'])
            
            if not pdf_url:
                metadata = OpinionMetadata(
                    opinion_type=self.opinion_type_name,
                    publication_status=self.publication_status,
                    year=year,
                    month=case['month'],
                    file_date=case['file_date'],
                    case_number=case['case_number'],
                    division=case.get('division', ''),
                    case_title=case['case_title'],
                    file_contains=case['file_contains'],
                    case_info_url=case['case_info_url'],
                    pdf_url="",
                    pdf_filename="",
                    download_status="PDF URL not found",
                    scraped_at=datetime.now().isoformat()
                )
                with self.lock:
                    self.downloaded_cases.add(case_id)
                return metadata, True
            
            # Create filename and path
            safe_title = self.sanitize_filename(case['case_title'])[:50]
            pdf_filename = f"{case['case_number']}_{safe_title}.pdf"
            month = case['month'] or "Unknown"
            save_dir = os.path.join(self.output_dir, year, month)
            save_path = os.path.join(save_dir, pdf_filename)
            
            # Download PDF
            success = self.download_pdf(pdf_url, save_path)
            
            metadata = OpinionMetadata(
                opinion_type=self.opinion_type_name,
                publication_status=self.publication_status,
                year=year,
                month=month,
                file_date=case['file_date'],
                case_number=case['case_number'],
                division=case.get('division', ''),
                case_title=case['case_title'],
                file_contains=case['file_contains'],
                case_info_url=case['case_info_url'],
                pdf_url=pdf_url,
                pdf_filename=pdf_filename if success else "",
                download_status="Success" if success else "Download failed - will retry",
                scraped_at=datetime.now().isoformat()
            )
            
            with self.lock:
                if success:
                    self.downloaded_cases.add(case_id)
                    # Save checkpoint periodically (every 50 successful downloads)
                    if len(self.downloaded_cases) % 50 == 0:
                        self._save_checkpoint()
                else:
                    self.failed_cases.add(case_id)
                    logger.warning(f"Failed to download {case['case_number']} - will retry on next run")
            
            return metadata, success
            
        except Exception as e:
            logger.error(f"Error processing case {case.get('case_number', 'unknown')}: {e}")
            metadata = OpinionMetadata(
                opinion_type=self.opinion_type_name,
                publication_status=self.publication_status,
                year=year,
                month=case.get('month', ''),
                file_date=case.get('file_date', ''),
                case_number=case.get('case_number', ''),
                division=case.get('division', ''),
                case_title=case.get('case_title', ''),
                file_contains=case.get('file_contains', ''),
                case_info_url=case.get('case_info_url', ''),
                pdf_url="",
                pdf_filename="",
                download_status=f"Error: {str(e)}",
                scraped_at=datetime.now().isoformat()
            )
            return metadata, False

    def scrape_year(self, year: str) -> List[OpinionMetadata]:
        """Scrape all opinions for a given year using parallel processing"""
        logger.info(f"Starting scrape for year {year} with {self.workers} workers")
        year_metadata = []
        
        cases = self.get_cases_for_year(year)
        total_cases = len(cases)
        
        # Filter out already processed cases for better progress tracking
        cases_to_process = []
        for i, case in enumerate(cases):
            case_id = f"{year}_{case['case_number']}"
            if not (self.resume and case_id in self.downloaded_cases):
                cases_to_process.append((case, i))
        
        if not cases_to_process:
            logger.info(f"All {total_cases} cases already processed for year {year}")
            return year_metadata
        
        logger.info(f"Processing {len(cases_to_process)} cases (skipping {total_cases - len(cases_to_process)} already done)")
        
        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            # Submit all tasks
            future_to_case = {
                executor.submit(self._process_single_case, case, year, idx, total_cases): case
                for case, idx in cases_to_process
            }
            
            # Process results as they complete
            for future in as_completed(future_to_case):
                if self.shutdown_requested:
                    logger.warning("Shutdown requested, cancelling remaining tasks...")
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                
                try:
                    metadata, success = future.result()
                    if metadata:
                        year_metadata.append(metadata)
                except Exception as e:
                    logger.error(f"Task failed with exception: {e}")
        
        # Save checkpoint after each year
        self._save_checkpoint()
        
        return year_metadata
    
    def save_metadata(self, metadata_list: List[OpinionMetadata], filename: str = None):
        """Save metadata to CSV file"""
        if not metadata_list:
            logger.warning("No metadata to save")
            return
        
        filename = filename or os.path.join(self.output_dir, METADATA_FILENAME)
        
        # Check if file exists to determine if we need headers
        file_exists = os.path.exists(filename)
        
        with open(filename, 'a', newline='', encoding='utf-8') as f:
            fieldnames = list(asdict(metadata_list[0]).keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            for metadata in metadata_list:
                writer.writerow(asdict(metadata))
        
        logger.info(f"Saved {len(metadata_list)} records to {filename}")
    
    def run(self, years: List[str] = None):
        """Run the scraper for specified years or all available years"""
        logger.info("Starting Washington Courts Opinion Scraper")
        logger.info(f"Resume mode: {'enabled' if self.resume else 'disabled'}")
        
        # Get years to scrape
        if years is None:
            years = self.get_available_years()
        
        if not years:
            logger.error("No years found to scrape")
            return
        
        logger.info(f"Will scrape {len(years)} years: {years}")
        
        all_metadata = []
        
        for year in years:
            # Check for shutdown
            if self.shutdown_requested:
                logger.warning("Shutdown requested, stopping...")
                break
                
            try:
                year_metadata = self.scrape_year(year)
                all_metadata.extend(year_metadata)
                
                # Save after each year (incremental saves)
                if year_metadata:
                    self.save_metadata(year_metadata)
                
                logger.info(f"Completed year {year}: {len(year_metadata)} opinions")
                
            except Exception as e:
                logger.error(f"Error scraping year {year}: {e}")
                # Save checkpoint on error
                self._save_checkpoint()
                continue
        
        # Final checkpoint save
        self._save_checkpoint()
        
        logger.info(f"Scraping complete. Total opinions processed: {len(all_metadata)}")
        logger.info(f"Total cases in checkpoint: {len(self.downloaded_cases)}")
        return all_metadata


def main():
    """Main entry point"""
    scraper = WashingtonCourtsScraper()
    
    # Run for all years (or specify specific years like ['2023', '2024'])
    scraper.run()


if __name__ == "__main__":
    main()
