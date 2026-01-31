"""
Opinion Info Sheet Scraper

This scraper downloads the printer-friendly "Opinion Information Sheet" HTML pages
for all cases already in the metadata CSVs.

The info sheets contain:
- Docket Number, Title, File Date, Oral Argument Date
- Source of Appeal (Court, Docket No, Judge)
- Justices and their votes
- Counsel for all parties (Petitioner, Respondent, Amicus Curiae)

Usage:
    python scrape_info_sheets.py                    # All opinion types
    python scrape_info_sheets.py --type supreme_court
    python scrape_info_sheets.py --type appeals_published
    python scrape_info_sheets.py --no-resume        # Fresh start
"""

import os
import csv
import json
import time
import random
import logging
import signal
import argparse
import requests
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from typing import Set, List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from config import (
    BASE_URL, HEADERS,
    REQUEST_DELAY_MIN, REQUEST_DELAY_MAX, REQUEST_TIMEOUT,
    MAX_RETRIES, RETRY_DELAY, MAX_RETRY_DELAY,
    OUTPUT_DIR, OPINION_TYPES
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('info_sheet_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Checkpoint filename for info sheets
INFO_SHEET_CHECKPOINT = "info_sheet_checkpoint.json"


# Global shutdown flag for signal handling
shutdown_requested = False

def global_signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    logger.warning("Shutdown signal received, finishing current downloads...")
    shutdown_requested = True

# Register signal handlers in main thread only
try:
    signal.signal(signal.SIGINT, global_signal_handler)
    signal.signal(signal.SIGTERM, global_signal_handler)
except ValueError:
    pass  # Not in main thread, skip signal registration


class InfoSheetScraper:
    """Scraper for Opinion Information Sheet HTML pages"""
    
    def __init__(self, opinion_type: str, resume: bool = True, workers: int = 3, years: Optional[List[str]] = None):
        self.opinion_type = opinion_type
        self.workers = workers
        self.years = years  # Filter by specific years if provided
        
        if opinion_type not in OPINION_TYPES:
            raise ValueError(f"Unknown opinion type: {opinion_type}")
        
        type_config = OPINION_TYPES[opinion_type]
        self.opinion_folder = type_config["folder"]
        self.output_dir = os.path.join(OUTPUT_DIR, self.opinion_folder)
        
        self.resume = resume
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
        # Track downloaded info sheets (thread-safe)
        self.downloaded_sheets: Set[str] = set()
        self.checkpoint_file = os.path.join(self.output_dir, INFO_SHEET_CHECKPOINT)
        self.lock = Lock()  # Thread-safe operations
        
        # Load checkpoint if resuming
        if resume:
            self._load_checkpoint()
    
    @property
    def shutdown_requested(self):
        """Check global shutdown flag"""
        global shutdown_requested
        return shutdown_requested
    
    def _load_checkpoint(self):
        """Load checkpoint from file"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    self.downloaded_sheets = set(data.get('downloaded_sheets', []))
                logger.info(f"Loaded checkpoint: {len(self.downloaded_sheets)} info sheets already downloaded")
            except Exception as e:
                logger.warning(f"Could not load checkpoint: {e}")
    
    def _save_checkpoint(self):
        """Save checkpoint to file"""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump({
                    'downloaded_sheets': list(self.downloaded_sheets),
                    'last_updated': datetime.now().isoformat()
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save checkpoint: {e}")
    
    def _make_request(self, url: str, retry_count: int = 0) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.encoding = 'utf-8'
            response.raise_for_status()
            
            # Random delay between requests
            delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
            time.sleep(delay)
            
            return response
            
        except requests.exceptions.RequestException as e:
            if retry_count < MAX_RETRIES:
                wait_time = min(RETRY_DELAY * (2 ** retry_count), MAX_RETRY_DELAY)
                logger.warning(f"Request failed, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
                return self._make_request(url, retry_count + 1)
            else:
                logger.error(f"Max retries exceeded for {url}: {e}")
                return None
    
    def get_filename_from_url(self, case_info_url: str) -> Optional[str]:
        """Extract filename parameter from case info URL"""
        try:
            parsed = urlparse(case_info_url)
            params = parse_qs(parsed.query)
            if 'filename' in params:
                return params['filename'][0]
        except Exception as e:
            logger.warning(f"Could not parse URL {case_info_url}: {e}")
        return None
    
    def get_info_sheet_url(self, filename: str) -> str:
        """Build the printer-friendly info sheet URL"""
        return f"{BASE_URL}/opinions/index.cfm?fa=opinions.showOpinionTextOnly&filename={filename}&printOnly=y"
    
    def load_metadata(self) -> List[Dict]:
        """Load cases from metadata CSV"""
        metadata_path = os.path.join(self.output_dir, "metadata.csv")
        
        if not os.path.exists(metadata_path):
            logger.error(f"Metadata file not found: {metadata_path}")
            return []
        
        cases = []
        with open(metadata_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Filter by year if specified
                if self.years is None or row.get('year', '') in self.years:
                    cases.append(row)
        
        if self.years:
            logger.info(f"Loaded {len(cases)} cases from metadata (filtered to years: {', '.join(self.years)})")
        else:
            logger.info(f"Loaded {len(cases)} cases from metadata")
        return cases
    
    def download_info_sheet(self, case: Dict) -> bool:
        """Download info sheet HTML for a case"""
        case_info_url = case.get('case_info_url', '')
        case_number = case.get('case_number', 'unknown')
        year = case.get('year', '')
        month = case.get('month', '')
        
        # Extract filename from URL
        filename = self.get_filename_from_url(case_info_url)
        if not filename:
            logger.warning(f"Could not extract filename for case {case_number}")
            return False
        
        # Build info sheet URL
        info_url = self.get_info_sheet_url(filename)
        
        # Download HTML
        response = self._make_request(info_url)
        if not response:
            return False
        
        # Create save directory (same as PDF location)
        save_dir = os.path.join(self.output_dir, year, month)
        os.makedirs(save_dir, exist_ok=True)
        
        # Save HTML file
        html_filename = f"{case_number}_info.html"
        save_path = os.path.join(save_dir, html_filename)
        
        try:
            with open(save_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
            return True
        except Exception as e:
            logger.error(f"Could not save HTML for {case_number}: {e}")
            return False
    
    def _process_case(self, case: Dict, index: int, total: int) -> Dict[str, any]:
        """Process a single case (for parallel execution)"""
        case_number = case.get('case_number', 'unknown')
        case_info_url = case.get('case_info_url', '')
        
        # Create unique ID for checkpoint
        filename = self.get_filename_from_url(case_info_url)
        if not filename:
            return {'status': 'failed', 'case_id': None}
        
        case_id = f"{case_number}_{filename}"
        
        # Skip if already downloaded (thread-safe check)
        with self.lock:
            if case_id in self.downloaded_sheets:
                return {'status': 'skipped', 'case_id': case_id}
        
        logger.info(f"Processing {index+1}/{total}: {case_number}")
        
        # Download info sheet
        success = self.download_info_sheet(case)
        
        if success:
            logger.info(f"Downloaded: {case_number}_info.html")
            return {'status': 'downloaded', 'case_id': case_id}
        else:
            return {'status': 'failed', 'case_id': case_id}
    
    def scrape(self) -> Dict[str, int]:
        """Main scraping method with parallel processing"""
        logger.info(f"Starting info sheet scraper for {self.opinion_type} with {self.workers} workers")
        
        # Load cases from metadata
        cases = self.load_metadata()
        if not cases:
            return {'total': 0, 'downloaded': 0, 'skipped': 0, 'failed': 0}
        
        stats = {
            'total': len(cases),
            'downloaded': 0,
            'skipped': 0,
            'failed': 0
        }
        
        # Filter out already downloaded cases
        cases_to_process = []
        for case in cases:
            case_info_url = case.get('case_info_url', '')
            filename = self.get_filename_from_url(case_info_url)
            if filename:
                case_number = case.get('case_number', 'unknown')
                case_id = f"{case_number}_{filename}"
                if case_id not in self.downloaded_sheets:
                    cases_to_process.append(case)
                else:
                    stats['skipped'] += 1
        
        logger.info(f"Processing {len(cases_to_process)} cases ({stats['skipped']} already downloaded)")
        
        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(self._process_case, case, i, len(cases_to_process)): case 
                for i, case in enumerate(cases_to_process)
            }
            
            # Process completed tasks
            for future in as_completed(futures):
                if self.shutdown_requested:
                    logger.warning("Shutdown requested, cancelling remaining tasks...")
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                
                try:
                    result = future.result()
                    
                    if result['status'] == 'downloaded':
                        with self.lock:
                            self.downloaded_sheets.add(result['case_id'])
                            stats['downloaded'] += 1
                            
                            # Save checkpoint every 10 downloads
                            if stats['downloaded'] % 10 == 0:
                                self._save_checkpoint()
                    
                    elif result['status'] == 'failed':
                        stats['failed'] += 1
                    
                    # 'skipped' shouldn't happen here since we pre-filtered
                    
                except Exception as e:
                    logger.error(f"Task failed with exception: {e}")
                    stats['failed'] += 1
        
        # Final checkpoint save
        self._save_checkpoint()
        
        return stats


def process_opinion_type(opinion_type: str, resume: bool, workers: int, years: Optional[List[str]] = None) -> Dict[str, int]:
    """Process a single opinion type (for parallel execution)"""
    print(f"\n[{opinion_type}] Starting...")
    
    scraper = InfoSheetScraper(
        opinion_type=opinion_type,
        resume=resume,
        workers=workers,
        years=years
    )
    
    stats = scraper.scrape()
    
    print(f"\n[{opinion_type}] Completed: {stats['downloaded']} downloaded, {stats['skipped']} skipped, {stats['failed']} failed")
    return stats


def main():
    parser = argparse.ArgumentParser(description="Download Opinion Information Sheet HTML pages")
    parser.add_argument('--type', choices=['supreme_court', 'appeals_published', 'appeals_partial', 'appeals_unpublished', 'all'],
                        default='all', help='Opinion type to process')
    parser.add_argument('--years', nargs='+', help='Specific years to process (e.g., 2023 2024 2025)')
    parser.add_argument('--no-resume', action='store_true', help='Start fresh, ignore checkpoint')
    parser.add_argument('--workers', type=int, default=3, help='Number of parallel workers per opinion type (default: 3)')
    parser.add_argument('--sequential', action='store_true', help='Process opinion types sequentially instead of in parallel')
    
    args = parser.parse_args()
    
    # Determine which types to process
    if args.type == 'all':
        types_to_process = list(OPINION_TYPES.keys())
    else:
        types_to_process = [args.type]
    
    total_stats = {'total': 0, 'downloaded': 0, 'skipped': 0, 'failed': 0}
    
    if args.sequential or len(types_to_process) == 1:
        # Sequential processing
        for opinion_type in types_to_process:
            print("\n" + "=" * 60)
            print(f"Processing: {OPINION_TYPES[opinion_type]['folder']}")
            print("=" * 60)
            
            stats = process_opinion_type(opinion_type, not args.no_resume, args.workers, args.years)
            
            for key in total_stats:
                total_stats[key] += stats[key]
    else:
        # Parallel processing of all opinion types
        print("\n" + "=" * 60)
        print(f"Processing {len(types_to_process)} opinion types in PARALLEL")
        print(f"Each type uses {args.workers} workers = {len(types_to_process) * args.workers} total concurrent downloads")
        print("=" * 60)
        
        with ThreadPoolExecutor(max_workers=len(types_to_process)) as executor:
            futures = {
                executor.submit(process_opinion_type, opinion_type, not args.no_resume, args.workers, args.years): opinion_type
                for opinion_type in types_to_process
            }
            
            for future in as_completed(futures):
                opinion_type = futures[future]
                try:
                    stats = future.result()
                    for key in total_stats:
                        total_stats[key] += stats[key]
                except Exception as e:
                    logger.error(f"Error processing {opinion_type}: {e}")
    
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    print(f"Total cases: {total_stats['total']}")
    print(f"Downloaded: {total_stats['downloaded']}")
    print(f"Skipped: {total_stats['skipped']}")
    print(f"Failed: {total_stats['failed']}")


if __name__ == "__main__":
    main()
