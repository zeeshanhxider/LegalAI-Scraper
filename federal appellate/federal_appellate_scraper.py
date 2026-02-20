"""
Federal Appellate Court Scraper for CourtListener API

A unified scraper supporting both synchronous and asynchronous modes.

Usage:
    # Synchronous mode (default, stable)
    python federal_appellate_scraper.py --api-token YOUR_TOKEN --mode full
    
    # Asynchronous mode (faster, parallel requests)
    python federal_appellate_scraper.py --api-token YOUR_TOKEN --mode full --async

Features:
- Token-based authentication with rate limiting
- Cursor/page-based pagination for unlimited data access
- Automatic retry with exponential backoff
- Progress tracking and checkpoint resume
- Comprehensive metadata extraction (ALL fields)
- Sync mode: Sequential requests (stable, predictable)
- Async mode: Parallel requests (faster, 4-5x speedup)
"""

import os
import sys
import json
import time
import logging
import argparse
import csv
from datetime import datetime
from typing import Dict, List, Optional, Any, Generator
from pathlib import Path

# Sync imports
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Async imports (optional - only loaded if async mode is used)
try:
    import asyncio
    import aiohttp
    from aiohttp import ClientSession, ClientTimeout, TCPConnector
    ASYNC_AVAILABLE = True
except ImportError:
    ASYNC_AVAILABLE = False


# =============================================================================
# SHARED CONSTANTS
# =============================================================================

CIRCUIT_COURTS = [
    'ca1', 'ca2', 'ca3', 'ca4', 'ca5', 'ca6',
    'ca7', 'ca8', 'ca9', 'ca10', 'ca11',
    'cadc',  # DC Circuit
    'cafc',  # Federal Circuit
]

BASE_URL = "https://www.courtlistener.com/api/rest/v4"
RATE_LIMIT_PER_HOUR = 5000


# =============================================================================
# SYNCHRONOUS API CLIENT
# =============================================================================

class SyncAPI:
    """Synchronous client for CourtListener REST API."""
    
    def __init__(self, api_token: str, rate_limit_delay: float = 0.75):
        self.api_token = api_token
        self.rate_limit_delay = rate_limit_delay
        self.session = self._create_session()
        self.request_count = 0
        self.start_time = time.time()
        
    def _create_session(self) -> requests.Session:
        """Create requests session with retry logic."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        session.headers.update({
            'Authorization': f'Token {self.api_token}',
            'Accept': 'application/json',
            'User-Agent': 'FederalAppellateScraper/2.0'
        })
        
        return session
    
    def _rate_limit(self):
        """Enforce rate limiting."""
        time.sleep(self.rate_limit_delay)
        self.request_count += 1
        
        if self.request_count % 100 == 0:
            elapsed = time.time() - self.start_time
            rate = self.request_count / (elapsed / 3600) if elapsed > 0 else 0
            logging.info(f"Made {self.request_count} requests. Rate: {rate:.0f}/hour")
    
    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make GET request to API."""
        self._rate_limit()
        url = f"{BASE_URL}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {url} - {e}")
            raise
    
    def get_paginated(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        max_results: Optional[int] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """Get all results from paginated endpoint."""
        params = params or {}
        count = 0
        
        while True:
            data = self.get(endpoint, params)
            results = data.get('results', [])
            
            for item in results:
                yield item
                count += 1
                if max_results and count >= max_results:
                    return
            
            next_url = data.get('next')
            if not next_url:
                break
            
            # Support both cursor and page-based pagination
            if 'cursor=' in next_url:
                params['cursor'] = next_url.split('cursor=')[1].split('&')[0]
            elif 'page=' in next_url:
                params['page'] = next_url.split('page=')[1].split('&')[0]
            else:
                break


# =============================================================================
# ASYNCHRONOUS API CLIENT
# =============================================================================

class AsyncAPI:
    """Asynchronous client for CourtListener REST API."""
    
    MAX_CONCURRENT = 20
    
    def __init__(self, api_token: str, rate_limit_delay: float = 0.75):
        self.api_token = api_token
        self.rate_limit_delay = rate_limit_delay
        self.request_count = 0
        self.start_time = time.time()
        self.semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)
        
        self.headers = {
            'Authorization': f'Token {self.api_token}',
            'Accept': 'application/json',
            'User-Agent': 'FederalAppellateScraper/2.0-Async'
        }
    
    async def create_session(self) -> ClientSession:
        """Create aiohttp session."""
        timeout = ClientTimeout(total=60, connect=30)
        connector = TCPConnector(
            limit=self.MAX_CONCURRENT,
            ttl_dns_cache=300,
            ssl=False  # Disable SSL verification for compatibility
        )
        return ClientSession(headers=self.headers, timeout=timeout, connector=connector)
    
    async def _rate_limit(self):
        """Enforce rate limiting."""
        await asyncio.sleep(self.rate_limit_delay)
        self.request_count += 1
        
        if self.request_count % 100 == 0:
            elapsed = time.time() - self.start_time
            rate = self.request_count / (elapsed / 3600) if elapsed > 0 else 0
            logging.info(f"Made {self.request_count} requests. Rate: {rate:.0f}/hour")
    
    async def get(
        self,
        session: ClientSession,
        endpoint: str,
        params: Optional[Dict] = None,
        retries: int = 5
    ) -> Dict[str, Any]:
        """Make async GET request with retries."""
        url = f"{BASE_URL}/{endpoint}"
        params = params or {}
        
        async with self.semaphore:
            await self._rate_limit()
            
            for attempt in range(retries):
                try:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:
                            wait_time = (2 ** attempt) * 2
                            logging.warning(f"Rate limited, waiting {wait_time}s")
                            await asyncio.sleep(wait_time)
                        elif response.status >= 500:
                            wait_time = 2 ** attempt
                            logging.warning(f"Server error {response.status}, waiting {wait_time}s")
                            await asyncio.sleep(wait_time)
                        else:
                            logging.error(f"HTTP {response.status} for {url}")
                            return {}
                except asyncio.TimeoutError:
                    logging.warning(f"Timeout attempt {attempt + 1}/{retries}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    logging.error(f"Request error: {e}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 ** attempt)
            
            logging.error(f"Failed after {retries} attempts: {url}")
            return {}
    
    async def get_paginated(
        self,
        session: ClientSession,
        endpoint: str,
        params: Optional[Dict] = None,
        max_results: Optional[int] = None
    ) -> List[Dict]:
        """Get all results from paginated endpoint."""
        params = params or {}
        all_results = []
        
        while True:
            data = await self.get(session, endpoint, params)
            
            if not data or 'results' not in data:
                break
            
            results = data['results']
            if not results:
                break
            
            all_results.extend(results)
            
            if max_results and len(all_results) >= max_results:
                return all_results[:max_results]
            
            next_url = data.get('next')
            if not next_url:
                break
            
            # Support both cursor and page-based pagination
            if 'cursor=' in next_url:
                params['cursor'] = next_url.split('cursor=')[1].split('&')[0]
            elif 'page=' in next_url:
                params['page'] = next_url.split('page=')[1].split('&')[0]
            else:
                break
        
        return all_results


# =============================================================================
# UNIFIED SCRAPER
# =============================================================================

class FederalAppellateScraper:
    """Unified scraper supporting both sync and async modes."""
    
    def __init__(
        self,
        api_token: str,
        output_dir: str = "downloads",
        use_async: bool = False,
        rate_limit_delay: float = 0.75
    ):
        self.api_token = api_token
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.use_async = use_async and ASYNC_AVAILABLE
        self.rate_limit_delay = rate_limit_delay
        
        # Initialize appropriate API client
        if self.use_async:
            self.api = AsyncAPI(api_token, rate_limit_delay)
        else:
            self.api = SyncAPI(api_token, rate_limit_delay)
        
        # Checkpoint for resume capability
        self.checkpoint_file = self.output_dir / "checkpoint.json"
        self.checkpoint = self._load_checkpoint()
        
        self._setup_logging()
    
    def _setup_logging(self):
        """Configure logging."""
        mode = "Async" if self.use_async else "Sync"
        log_file = self.output_dir / f"scraper_{datetime.now():%Y%m%d_%H%M%S}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        logging.info("=" * 80)
        logging.info(f"Federal Appellate Scraper Started ({mode} Mode)")
        logging.info("=" * 80)
    
    def _load_checkpoint(self) -> Dict:
        """Load checkpoint from file."""
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r') as f:
                return json.load(f)
        return {'completed_courts': []}
    
    def _save_checkpoint(self):
        """Save checkpoint to file."""
        with open(self.checkpoint_file, 'w') as f:
            json.dump(self.checkpoint, f, indent=2)
    
    def _save_batch(self, data: List[Dict], data_type: str, court_id: Optional[str] = None):
        """Save batch of data to JSON file."""
        if not data:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = f"_{court_id}" if court_id else ""
        filename = self.output_dir / f"{data_type}{suffix}_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        logging.info(f"Saved {len(data)} {data_type} to {filename}")
    
    # -------------------------------------------------------------------------
    # SYNCHRONOUS METHODS
    # -------------------------------------------------------------------------
    
    def _scrape_dockets_sync(
        self,
        court_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_dockets: Optional[int] = None
    ) -> List[Dict]:
        """Scrape dockets synchronously."""
        params = {'order_by': '-date_filed,id'}
        
        if court_id:
            params['court'] = court_id
            logging.info(f"Scraping dockets for court: {court_id}")
        else:
            params['court__jurisdiction'] = 'F'
            logging.info("Scraping dockets for all federal appellate courts")
        
        if start_date:
            params['date_filed__gte'] = start_date
        if end_date:
            params['date_filed__lte'] = end_date
        
        logging.info("Starting docket scrape...")
        
        dockets = []
        for i, docket in enumerate(self.api.get_paginated('dockets/', params, max_dockets), 1):
            dockets.append(docket)
            
            if i % 100 == 0:
                logging.info(f"Fetched {i:,} dockets")
                self._save_batch(dockets, 'dockets', court_id)
                dockets = []
        
        if dockets:
            self._save_batch(dockets, 'dockets', court_id)
        
        logging.info("Completed docket scrape")
        return dockets
    
    def _scrape_clusters_sync(
        self,
        court_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_clusters: Optional[int] = None
    ) -> List[Dict]:
        """Scrape opinion clusters synchronously."""
        params = {'order_by': '-date_filed,id'}
        
        if court_id:
            params['docket__court'] = court_id
        else:
            params['docket__court__jurisdiction'] = 'F'
        
        if start_date:
            params['date_filed__gte'] = start_date
        if end_date:
            params['date_filed__lte'] = end_date
        
        logging.info("Starting cluster scrape...")
        
        clusters = []
        for i, cluster in enumerate(self.api.get_paginated('clusters/', params, max_clusters), 1):
            clusters.append(cluster)
            
            if i % 100 == 0:
                logging.info(f"Fetched {i:,} clusters")
                self._save_batch(clusters, 'clusters', court_id)
                clusters = []
        
        if clusters:
            self._save_batch(clusters, 'clusters', court_id)
        
        logging.info("Completed cluster scrape")
        return clusters
    
    def _scrape_opinions_sync(
        self,
        court_id: Optional[str] = None,
        max_opinions: Optional[int] = None
    ) -> List[Dict]:
        """Scrape opinions synchronously."""
        params = {'order_by': 'id'}
        
        if court_id:
            params['cluster__docket__court'] = court_id
        else:
            params['cluster__docket__court__jurisdiction'] = 'F'
        
        logging.info("Starting opinion scrape...")
        
        opinions = []
        for i, opinion in enumerate(self.api.get_paginated('opinions/', params, max_opinions), 1):
            opinions.append(opinion)
            
            if i % 50 == 0:  # More frequent saves for large text data
                logging.info(f"Fetched {i:,} opinions")
                self._save_batch(opinions, 'opinions', court_id)
                opinions = []
        
        if opinions:
            self._save_batch(opinions, 'opinions', court_id)
        
        logging.info("Completed opinion scrape")
        return opinions
    
    # -------------------------------------------------------------------------
    # ASYNCHRONOUS METHODS
    # -------------------------------------------------------------------------
    
    async def _scrape_court_async(
        self,
        session: ClientSession,
        court_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_dockets: Optional[int] = None
    ) -> List[Dict]:
        """Scrape dockets for single court asynchronously."""
        params = {'order_by': '-date_filed,id', 'court': court_id}
        
        if start_date:
            params['date_filed__gte'] = start_date
        if end_date:
            params['date_filed__lte'] = end_date
        
        logging.info(f"Starting async docket scrape for {court_id}...")
        
        dockets = await self.api.get_paginated(session, 'dockets/', params, max_dockets)
        
        logging.info(f"Fetched {len(dockets)} dockets from {court_id}")
        self._save_batch(dockets, 'dockets', court_id)
        
        return dockets
    
    async def _scrape_all_courts_async(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_per_court: Optional[int] = None
    ) -> Dict[str, List[Dict]]:
        """Scrape all courts in parallel."""
        async with await self.api.create_session() as session:
            tasks = [
                self._scrape_court_async(session, court_id, start_date, end_date, max_per_court)
                for court_id in CIRCUIT_COURTS
            ]
            
            logging.info(f"Starting parallel scrape of {len(CIRCUIT_COURTS)} courts...")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            court_dockets = {}
            for court_id, result in zip(CIRCUIT_COURTS, results):
                if isinstance(result, Exception):
                    logging.error(f"Error scraping {court_id}: {result}")
                    court_dockets[court_id] = []
                else:
                    court_dockets[court_id] = result
            
            total = sum(len(d) for d in court_dockets.values())
            logging.info(f"Completed parallel scrape: {total} total dockets")
            
            return court_dockets
    
    async def _scrape_clusters_async(
        self,
        session: ClientSession,
        dockets: List[Dict]
    ) -> List[Dict]:
        """Scrape clusters for dockets asynchronously."""
        docket_ids = [d.get('id') for d in dockets if d.get('id')]
        
        if not docket_ids:
            return []
        
        logging.info(f"Starting async cluster scrape for {len(docket_ids)} dockets...")
        
        batch_size = 50
        all_clusters = []
        
        for i in range(0, len(docket_ids), batch_size):
            batch_ids = docket_ids[i:i + batch_size]
            params = {'docket__in': ','.join(map(str, batch_ids)), 'order_by': 'id'}
            
            clusters = await self.api.get_paginated(session, 'clusters/', params)
            all_clusters.extend(clusters)
            
            if (i // batch_size + 1) % 10 == 0:
                logging.info(f"Fetched clusters for {i + len(batch_ids)} dockets")
        
        logging.info(f"Fetched {len(all_clusters)} total clusters")
        self._save_batch(all_clusters, 'clusters')
        
        return all_clusters
    
    async def _scrape_opinions_async(
        self,
        session: ClientSession,
        clusters: List[Dict]
    ) -> List[Dict]:
        """Scrape opinions for clusters asynchronously."""
        cluster_ids = [c.get('id') for c in clusters if c.get('id')]
        
        if not cluster_ids:
            return []
        
        logging.info(f"Starting async opinion scrape for {len(cluster_ids)} clusters...")
        
        batch_size = 50
        all_opinions = []
        
        for i in range(0, len(cluster_ids), batch_size):
            batch_ids = cluster_ids[i:i + batch_size]
            params = {'cluster__in': ','.join(map(str, batch_ids)), 'order_by': 'id'}
            
            opinions = await self.api.get_paginated(session, 'opinions/', params)
            all_opinions.extend(opinions)
            
            if (i // batch_size + 1) % 10 == 0:
                logging.info(f"Fetched opinions for {i + len(batch_ids)} clusters")
        
        logging.info(f"Fetched {len(all_opinions)} total opinions")
        self._save_batch(all_opinions, 'opinions')
        
        return all_opinions
    
    # -------------------------------------------------------------------------
    # PUBLIC METHODS
    # -------------------------------------------------------------------------
    
    def scrape_dockets(self, **kwargs) -> List[Dict]:
        """Scrape dockets (auto-selects sync/async)."""
        if self.use_async:
            return asyncio.run(self._scrape_all_courts_async(
                start_date=kwargs.get('start_date'),
                end_date=kwargs.get('end_date'),
                max_per_court=kwargs.get('max_dockets')
            ))
        else:
            return self._scrape_dockets_sync(**kwargs)
    
    def scrape_clusters(self, **kwargs) -> List[Dict]:
        """Scrape clusters (sync only, async uses scrape_full)."""
        return self._scrape_clusters_sync(**kwargs)
    
    def scrape_opinions(self, **kwargs) -> List[Dict]:
        """Scrape opinions (sync only, async uses scrape_full)."""
        return self._scrape_opinions_sync(**kwargs)
    
    def scrape_full(
        self,
        courts: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_per_court: Optional[int] = None
    ):
        """Scrape complete dataset: dockets, clusters, and opinions."""
        courts = courts or CIRCUIT_COURTS
        start_time = time.time()
        
        logging.info(f"Starting full scrape for {len(courts)} courts")
        logging.info(f"Date range: {start_date or 'ALL'} to {end_date or 'ALL'}")
        logging.info(f"Mode: {'Async' if self.use_async else 'Sync'}")
        
        if self.use_async:
            asyncio.run(self._scrape_full_async(courts, start_date, end_date, max_per_court))
        else:
            self._scrape_full_sync(courts, start_date, end_date, max_per_court)
        
        elapsed = time.time() - start_time
        logging.info("=" * 80)
        logging.info(f"Full scrape completed in {elapsed / 3600:.2f} hours")
        logging.info("=" * 80)
    
    def _scrape_full_sync(
        self,
        courts: List[str],
        start_date: Optional[str],
        end_date: Optional[str],
        max_per_court: Optional[int]
    ):
        """Full scrape using synchronous requests."""
        for court_id in courts:
            if court_id in self.checkpoint.get('completed_courts', []):
                logging.info(f"Skipping {court_id} (already completed)")
                continue
            
            logging.info("=" * 80)
            logging.info(f"Processing court: {court_id}")
            logging.info("=" * 80)
            
            try:
                self._scrape_dockets_sync(court_id, start_date, end_date, max_per_court)
                self._scrape_clusters_sync(court_id, start_date, end_date, max_per_court)
                self._scrape_opinions_sync(court_id, max_per_court)
                
                self.checkpoint['completed_courts'].append(court_id)
                self._save_checkpoint()
                
                logging.info(f"Completed {court_id}")
                
            except Exception as e:
                logging.error(f"Error processing {court_id}: {e}", exc_info=True)
    
    async def _scrape_full_async(
        self,
        courts: List[str],
        start_date: Optional[str],
        end_date: Optional[str],
        max_per_court: Optional[int]
    ):
        """Full scrape using asynchronous parallel requests."""
        # Phase 1: Scrape all courts in parallel
        court_dockets = await self._scrape_all_courts_async(start_date, end_date, max_per_court)
        
        all_dockets = []
        for dockets in court_dockets.values():
            all_dockets.extend(dockets)
        
        logging.info(f"Phase 1 complete: {len(all_dockets)} dockets")
        
        # Phase 2 & 3: Scrape clusters and opinions
        async with await self.api.create_session() as session:
            all_clusters = await self._scrape_clusters_async(session, all_dockets)
            logging.info(f"Phase 2 complete: {len(all_clusters)} clusters")
            
            all_opinions = await self._scrape_opinions_async(session, all_clusters)
            logging.info(f"Phase 3 complete: {len(all_opinions)} opinions")
    
    def export_to_csv(self, json_files: List[Path], output_csv: Path):
        """Export JSON data to CSV format."""
        logging.info(f"Exporting {len(json_files)} files to CSV...")
        
        all_data = []
        for json_file in json_files:
            with open(json_file, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    all_data.append(data)
        
        if not all_data:
            logging.warning("No data to export")
            return
        
        # Get all unique keys
        fieldnames = set()
        for item in all_data:
            fieldnames.update(self._flatten_dict(item).keys())
        
        fieldnames = sorted(fieldnames)
        
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for item in all_data:
                writer.writerow(self._flatten_dict(item))
        
        logging.info(f"Exported {len(all_data)} records to {output_csv}")
    
    def _flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """Flatten nested dictionary for CSV export."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
        
        return dict(items)


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Federal Appellate Court Scraper for CourtListener API"
    )
    
    parser.add_argument(
        '--api-token',
        help='CourtListener API token (or set COURTLISTENER_TOKEN env var)'
    )
    
    parser.add_argument(
        '--output-dir',
        default='downloads',
        help='Output directory for downloaded data'
    )
    
    parser.add_argument(
        '--courts',
        nargs='+',
        help='Specific court IDs to scrape (e.g., ca1 ca9 cadc)'
    )
    
    parser.add_argument(
        '--start-date',
        help='Start date in YYYY-MM-DD format'
    )
    
    parser.add_argument(
        '--end-date',
        help='End date in YYYY-MM-DD format'
    )
    
    parser.add_argument(
        '--max-per-court',
        type=int,
        help='Maximum items to fetch per court'
    )
    
    parser.add_argument(
        '--mode',
        choices=['full', 'dockets', 'clusters', 'opinions'],
        default='full',
        help='Scraping mode'
    )
    
    parser.add_argument(
        '--rate-limit',
        type=float,
        default=0.75,
        help='Delay between requests in seconds'
    )
    
    parser.add_argument(
        '--async',
        dest='use_async',
        action='store_true',
        help='Use async mode for parallel requests (faster)'
    )
    
    args = parser.parse_args()
    
    # Get API token
    api_token = args.api_token or os.getenv('COURTLISTENER_TOKEN')
    if not api_token:
        print("Error: API token required. Use --api-token or set COURTLISTENER_TOKEN env var")
        sys.exit(1)
    
    # Check async availability
    if args.use_async and not ASYNC_AVAILABLE:
        print("Warning: aiohttp not installed. Falling back to sync mode.")
        print("Install with: pip install aiohttp")
        args.use_async = False
    
    # Initialize scraper
    scraper = FederalAppellateScraper(
        api_token=api_token,
        output_dir=args.output_dir,
        use_async=args.use_async,
        rate_limit_delay=args.rate_limit
    )
    
    # Execute based on mode
    if args.mode == 'dockets':
        scraper.scrape_dockets(
            court_id=args.courts[0] if args.courts else None,
            start_date=args.start_date,
            end_date=args.end_date,
            max_dockets=args.max_per_court
        )
    
    elif args.mode == 'clusters':
        scraper.scrape_clusters(
            court_id=args.courts[0] if args.courts else None,
            start_date=args.start_date,
            end_date=args.end_date,
            max_clusters=args.max_per_court
        )
    
    elif args.mode == 'opinions':
        scraper.scrape_opinions(
            court_id=args.courts[0] if args.courts else None,
            max_opinions=args.max_per_court
        )
    
    elif args.mode == 'full':
        scraper.scrape_full(
            courts=args.courts,
            start_date=args.start_date,
            end_date=args.end_date,
            max_per_court=args.max_per_court
        )
    
    logging.info("Scraper finished successfully!")


if __name__ == '__main__':
    main()
