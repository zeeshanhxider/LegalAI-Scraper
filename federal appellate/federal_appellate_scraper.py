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

Output structure:
    downloads/
      {Court Name}/           (e.g. '1st Circuit', 'DC Circuit')
        {year}/
          {docket_number}_{case_slug}/
            docket.json
            cluster.json       (one per cluster, suffixed if multiple)
            opinion.json       (one per opinion, suffixed if multiple)
"""

import os
import sys
import json
import time
import re
import logging
import argparse
import csv
from collections import OrderedDict
from datetime import datetime
from typing import Dict, List, Optional, Any, Generator
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

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
# BOUNDED LRU CACHE
# =============================================================================

class BoundedCache(OrderedDict):
    """OrderedDict that evicts the oldest entry when ``maxsize`` is exceeded."""

    def __init__(self, maxsize: int = 20_000):
        super().__init__()
        self.maxsize = maxsize

    def __setitem__(self, key, value):
        # Move to end if already present (LRU refresh)
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        # Evict oldest entries if we've exceeded capacity
        while len(self) > self.maxsize:
            self.popitem(last=False)


# =============================================================================
# SHARED CONSTANTS
# =============================================================================

CIRCUIT_COURTS = [
    'ca1', 'ca2', 'ca3', 'ca4', 'ca5', 'ca6',
    'ca7', 'ca8', 'ca9', 'ca10', 'ca11',
    'cadc',  # DC Circuit
    'cafc',  # Federal Circuit
]

# Human-readable folder names for each circuit court
COURT_FOLDER_NAMES = {
    'ca1':  '1st Circuit',
    'ca2':  '2nd Circuit',
    'ca3':  '3rd Circuit',
    'ca4':  '4th Circuit',
    'ca5':  '5th Circuit',
    'ca6':  '6th Circuit',
    'ca7':  '7th Circuit',
    'ca8':  '8th Circuit',
    'ca9':  '9th Circuit',
    'ca10': '10th Circuit',
    'ca11': '11th Circuit',
    'cadc': 'DC Circuit',
    'cafc': 'Federal Circuit',
}

BASE_URL = "https://www.courtlistener.com/api/rest/v4"


# =============================================================================
# HELPERS
# =============================================================================

def _slugify(text: str, max_len: int = 60) -> str:
    """Convert arbitrary text into a safe filesystem-friendly slug."""
    if not text:
        return "unknown"
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)          # strip non-word chars
    text = re.sub(r'[\s_]+', '-', text.strip())    # spaces → hyphens
    text = re.sub(r'-+', '-', text)                # collapse multiple hyphens
    return text[:max_len].strip('-') or "unknown"


def _case_folder_name(docket: Dict) -> str:
    """
    Build a deterministic folder name for a case from its docket record.
    Format:  {docket_number}_{case_slug}
    Example: 22-50116_smith-v-jones
    """
    docket_number = docket.get('docket_number') or docket.get('docket_number_raw') or str(docket.get('id', 'unknown'))
    case_name     = docket.get('case_name') or docket.get('case_name_short') or ''
    safe_dn       = _slugify(docket_number, max_len=30)
    safe_cn       = _slugify(case_name,     max_len=60)
    return f"{safe_dn}_{safe_cn}" if safe_cn != "unknown" else safe_dn


def _year_from_docket(docket: Dict) -> Optional[str]:
    """
    Extract the filing year from a docket record.
    Tries, in order:
      1. date_filed field (YYYY-MM-DD)
      2. date_argued, date_reargued, date_reargument_denied
      3. docket_number prefix (e.g. '20-1234' → '2020', '2020-1234' → '2020')
    Returns None if no year can be determined.
    """
    # 1. Explicit date fields
    for field in ('date_filed', 'date_argued', 'date_reargued', 'date_reargument_denied'):
        val = docket.get(field) or ''
        if val and len(val) >= 4:
            return val[:4]

    # 2. Parse year from docket number (handles both '20-1234' and '2020-1234')
    dn = docket.get('docket_number') or docket.get('docket_number_raw') or ''
    m = re.match(r'^(\d{4})-', dn)          # full 4-digit year prefix
    if m:
        return m.group(1)
    m = re.match(r'^(\d{2})-', dn)          # 2-digit year prefix (e.g. 20- → 2020)
    if m:
        yy = int(m.group(1))
        # Heuristic: 00-29 → 2000-2029, 30-99 → 1930-1999
        century = 2000 if yy < 30 else 1900
        return str(century + yy)

    return None


# =============================================================================
# SYNCHRONOUS API CLIENT
# =============================================================================

class SyncAPI:
    """Synchronous client for CourtListener REST API."""
    
    def __init__(self, api_token: str, rate_limit_delay: float = 0.75, rate_limiter=None):
        self.api_token = api_token
        self.rate_limit_delay = rate_limit_delay
        self.rate_limiter = rate_limiter  # optional SharedRateLimiter (cross-process)
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
        """Enforce rate limiting — uses shared cross-process limiter if provided."""
        if self.rate_limiter is not None:
            self.rate_limiter.acquire()
        else:
            time.sleep(self.rate_limit_delay)
        self.request_count += 1
        if self.request_count % 50 == 0:
            elapsed = time.time() - self.start_time
            rate = self.request_count / (elapsed / 3600) if elapsed > 0 else 0
            logging.info(f"Requests this process: {self.request_count} | rate: {rate:.0f}/hr")
    
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
    
    MAX_CONCURRENT = 10  # 10 workers per year-process
    
    def __init__(self, api_token: str, rate_limit_delay: float = 0.75, rate_limiter=None):
        self.api_token = api_token
        self.rate_limit_delay = rate_limit_delay
        self.rate_limiter = rate_limiter  # optional SharedRateLimiter (cross-process)
        self.request_count = 0
        self.start_time = time.time()
        self._semaphore: Optional[asyncio.Semaphore] = None  # lazy-init inside event loop
        
        self.headers = {
            'Authorization': f'Token {self.api_token}',
            'Accept': 'application/json',
            'User-Agent': 'FederalAppellateScraper/2.0-Async'
        }

    @property
    def semaphore(self) -> asyncio.Semaphore:
        """Lazily created inside the running event loop."""
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)
        return self._semaphore
    
    async def create_session(self) -> ClientSession:
        """Create aiohttp session with proper connection pooling."""
        timeout = ClientTimeout(total=60, connect=30)
        connector = TCPConnector(
            limit=self.MAX_CONCURRENT,
            limit_per_host=self.MAX_CONCURRENT,  # pool per host matches worker count
            ttl_dns_cache=300,
            ssl=False,
            enable_cleanup_closed=True,
        )
        return ClientSession(headers=self.headers, timeout=timeout, connector=connector)
    
    async def _rate_limit(self):
        """Enforce rate limiting — uses shared cross-process limiter if provided."""
        if self.rate_limiter is not None:
            # Blocking acquire; run in thread pool so async event loop isn't blocked
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.rate_limiter.acquire)
        else:
            await asyncio.sleep(self.rate_limit_delay)
        self.request_count += 1
        if self.request_count % 50 == 0:
            elapsed = time.time() - self.start_time
            rate = self.request_count / (elapsed / 3600) if elapsed > 0 else 0
            logging.info(f"Requests this process: {self.request_count} | rate: {rate:.0f}/hr")
    
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
        
        for attempt in range(retries):
            # Rate-limit BEFORE acquiring the semaphore so we don't
            # hold a connection slot while sleeping.
            await self._rate_limit()

            async with self.semaphore:
                try:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:
                            # Prefer server-supplied Retry-After header
                            retry_after = response.headers.get('Retry-After')
                            if retry_after:
                                try:
                                    wait_time = float(retry_after)
                                except ValueError:
                                    wait_time = (2 ** attempt) * 2
                            else:
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
        page_num = 0
        
        while True:
            data = await self.get(session, endpoint, params)
            
            if not data or 'results' not in data:
                break
            
            results = data['results']
            if not results:
                break
            
            all_results.extend(results)
            page_num += 1
            
            # Progress logging every 10 pages (200 results)
            if page_num % 10 == 0:
                logging.info(
                    f"  [{endpoint}] paginating: {len(all_results)} results "
                    f"fetched ({page_num} pages)..."
                )
            
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
        rate_limit_delay: float = 0.75,
        rate_limiter=None,  # SharedRateLimiter instance from orchestrator
        year: Optional[int] = None,  # when set, checkpoint is isolated per year
    ):
        self.api_token = api_token
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.use_async = use_async and ASYNC_AVAILABLE
        self.rate_limit_delay = rate_limit_delay
        self.rate_limiter = rate_limiter

        # Initialize appropriate API client
        if self.use_async:
            self.api = AsyncAPI(api_token, rate_limit_delay, rate_limiter=rate_limiter)
        else:
            self.api = SyncAPI(api_token, rate_limit_delay, rate_limiter=rate_limiter)

        # Per-year checkpoint so parallel year-processes don't interfere with each other
        checkpoint_name = f"checkpoint_{year}.json" if year is not None else "checkpoint.json"
        self.checkpoint_file = self.output_dir / checkpoint_name
        self.checkpoint = self._load_checkpoint()

        # In-memory caches so lookups work even when phases run independently
        # Bounded to 20k entries each (~50-100 MB max) to avoid unbounded growth
        self._docket_cache: BoundedCache = BoundedCache(maxsize=20_000)
        self._cluster_cache: BoundedCache = BoundedCache(maxsize=20_000)
        
        self._setup_logging()
    
    def _setup_logging(self):
        """Configure logging.

        If the root logger already has handlers (e.g. set up by run_all_years.py's
        year_worker), we skip adding new handlers to avoid duplicate output.
        """
        mode = "Async" if self.use_async else "Sync"
        root = logging.getLogger()

        if not root.handlers:
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
        """Load checkpoint from file.

        New schema stores per-court-per-phase state::

            {
                "court_phases": {
                    "ca1": "completed",   # all 3 phases done
                    "ca2": "opinions",    # dockets + clusters done, opinions in progress
                    "ca3": "clusters",    # dockets done, clusters in progress
                },
                "completed_courts": ["ca1"]  # backward-compat summary list
            }
        """
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)
            # Migrate old format → new format
            if 'court_phases' not in data:
                data['court_phases'] = {
                    c: 'completed' for c in data.get('completed_courts', [])
                }
            return data
        return {'completed_courts': [], 'court_phases': {}}
    
    def _save_checkpoint(self):
        """Save checkpoint to file."""
        with open(self.checkpoint_file, 'w') as f:
            json.dump(self.checkpoint, f, indent=2)

    def _mark_court_phase(self, court_id: str, phase: str):
        """Mark a court as having completed a specific phase and persist.

        phase values: 'dockets', 'clusters', 'opinions', 'completed'
        """
        self.checkpoint.setdefault('court_phases', {})[court_id] = phase
        if phase == 'completed':
            completed = self.checkpoint.setdefault('completed_courts', [])
            if court_id not in completed:
                completed.append(court_id)
        self._save_checkpoint()

    def _court_phase(self, court_id: str) -> Optional[str]:
        """Return the last completed phase for a court, or None."""
        return self.checkpoint.get('court_phases', {}).get(court_id)

    # -------------------------------------------------------------------------
    # PARENT RECORD RESOLUTION (cache + on-demand API fetch)
    # -------------------------------------------------------------------------

    def _fetch_docket(self, docket_id: int) -> Optional[Dict]:
        """
        Return the docket record for docket_id.
        Checks the in-memory cache first; falls back to a live **sync** API call.
        Returns None if the fetch fails.

        NOTE: This is a synchronous helper.  The async code-path should use
        ``_fetch_docket_async`` instead.
        """
        if docket_id in self._docket_cache:
            return self._docket_cache[docket_id]
        if self.use_async:
            # Cannot make a sync HTTP call when we are in async mode.
            # Caller should have populated the cache or used the async variant.
            logging.warning(
                f"_fetch_docket called in async mode for docket {docket_id} — cache miss"
            )
            return None
        try:
            record = self.api.get(f'dockets/{docket_id}/')
            self._docket_cache[docket_id] = record
            return record
        except Exception as e:
            logging.warning(f"Could not fetch docket {docket_id}: {e}")
            return None

    def _fetch_cluster(self, cluster_id: int) -> Optional[Dict]:
        """
        Return the cluster record for cluster_id.
        Checks the in-memory cache first; falls back to a live **sync** API call.
        Returns None if the fetch fails.

        NOTE: This is a synchronous helper.  The async code-path should use
        ``_fetch_cluster_async`` instead.
        """
        if cluster_id in self._cluster_cache:
            return self._cluster_cache[cluster_id]
        if self.use_async:
            logging.warning(
                f"_fetch_cluster called in async mode for cluster {cluster_id} — cache miss"
            )
            return None
        try:
            record = self.api.get(f'clusters/{cluster_id}/')
            self._cluster_cache[cluster_id] = record
            return record
        except Exception as e:
            logging.warning(f"Could not fetch cluster {cluster_id}: {e}")
            return None

    async def _fetch_docket_async(self, session, docket_id: int) -> Optional[Dict]:
        """Async version of _fetch_docket — uses the aiohttp session."""
        if docket_id in self._docket_cache:
            return self._docket_cache[docket_id]
        try:
            record = await self.api.get(session, f'dockets/{docket_id}/')
            if record:
                self._docket_cache[docket_id] = record
            return record or None
        except Exception as e:
            logging.warning(f"Could not fetch docket {docket_id}: {e}")
            return None

    async def _fetch_cluster_async(self, session, cluster_id: int) -> Optional[Dict]:
        """Async version of _fetch_cluster — uses the aiohttp session."""
        if cluster_id in self._cluster_cache:
            return self._cluster_cache[cluster_id]
        try:
            record = await self.api.get(session, f'clusters/{cluster_id}/')
            if record:
                self._cluster_cache[cluster_id] = record
            return record or None
        except Exception as e:
            logging.warning(f"Could not fetch cluster {cluster_id}: {e}")
            return None

    # -------------------------------------------------------------------------
    # PER-CASE FOLDER SAVING
    # -------------------------------------------------------------------------

    def _case_dir(self, court_id: str, year: Optional[str], docket: Dict,
                  create: bool = True) -> Path:
        """
        Return the per-case directory path:
            {output_dir}/{Court Name}/{year}/{docket_number}_{case_slug}/
        If year is None, the year segment is omitted:
            {output_dir}/{Court Name}/{docket_number}_{case_slug}/

        When create=True (default), the directory is created on disk.
        Use create=False for existence checks to avoid creating empty folders.
        """
        court_folder = COURT_FOLDER_NAMES.get(court_id, court_id)
        folder = _case_folder_name(docket)
        if year:
            path = self.output_dir / court_folder / year / folder
        else:
            path = self.output_dir / court_folder / folder
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def _cache_docket(self, docket: Dict):
        """Populate the in-memory cache for a docket without writing to disk."""
        docket_id = docket.get('id')
        if docket_id is not None:
            self._docket_cache[docket_id] = docket

    def _save_docket(self, docket: Dict, court_id: str):
        """Save a single docket record to its case folder and populate the cache."""
        self._cache_docket(docket)
        year = _year_from_docket(docket)
        case_dir = self._case_dir(court_id, year, docket)
        out_path = case_dir / "docket.json"
        with open(out_path, 'w') as f:
            json.dump(docket, f, indent=2)

    def _resolve_docket_for_cluster(self, cluster: Dict,
                                     docket_map: Dict[int, Dict]) -> Optional[Dict]:
        """Look up the parent docket for a cluster via map → cache → sync fetch."""
        docket_id = cluster.get('docket_id')
        if docket_id is None:
            return None
        return (
            docket_map.get(docket_id)
            or self._docket_cache.get(docket_id)
            or self._fetch_docket(docket_id)
        )

    async def _resolve_docket_for_cluster_async(
        self, session, cluster: Dict, docket_map: Dict[int, Dict]
    ) -> Optional[Dict]:
        """Look up the parent docket for a cluster via map → cache → async fetch."""
        docket_id = cluster.get('docket_id')
        if docket_id is None:
            return None
        docket = docket_map.get(docket_id) or self._docket_cache.get(docket_id)
        if docket is not None:
            return docket
        return await self._fetch_docket_async(session, docket_id)

    def _cluster_case_dir(self, cluster: Dict, docket: Optional[Dict],
                          court_id: str, create: bool = True) -> Path:
        """Determine the case directory for a cluster given its resolved docket."""
        docket_id = cluster.get('docket_id')
        if docket is not None:
            year = _year_from_docket(docket)
            return self._case_dir(court_id, year, docket, create=create)
        # No docket available — derive year from cluster and use docket_id as folder name
        year = None
        for field in ('date_filed', 'date_blocked'):
            val = cluster.get(field) or ''
            if val and len(val) >= 4:
                year = val[:4]
                break
        fallback_docket = {
            'id': docket_id or cluster.get('id'),
            'docket_number': str(docket_id or cluster.get('id', '')),
            'case_name': cluster.get('case_name') or cluster.get('case_name_short', ''),
            'date_filed': cluster.get('date_filed', ''),
        }
        return self._case_dir(court_id, year, fallback_docket, create=create)

    def _write_cluster(self, cluster: Dict, case_dir: Path):
        """Write cluster JSON to disk with suffix handling for multiple clusters."""
        base = case_dir / "cluster.json"
        if not base.exists():
            out_path = base
        else:
            idx = 2
            while True:
                candidate = case_dir / f"cluster_{idx}.json"
                if not candidate.exists():
                    out_path = candidate
                    break
                idx += 1
        with open(out_path, 'w') as f:
            json.dump(cluster, f, indent=2)

    def _save_cluster(self, cluster: Dict, docket_map: Dict[int, Dict], court_id: str):
        """
        Save a single cluster to the case folder it belongs to (sync).
        Resolves the parent docket via: docket_map → _docket_cache → live API fetch.
        If multiple clusters exist for the same docket, they are suffixed:
            cluster.json, cluster_2.json, cluster_3.json, ...
        """
        cluster_id = cluster.get('id')
        if cluster_id is not None:
            self._cluster_cache[cluster_id] = cluster

        docket = self._resolve_docket_for_cluster(cluster, docket_map)
        case_dir = self._cluster_case_dir(cluster, docket, court_id)
        self._write_cluster(cluster, case_dir)

    async def _save_cluster_async(self, session, cluster: Dict,
                                   docket_map: Dict[int, Dict], court_id: str):
        """
        Save a single cluster to the case folder it belongs to (async).
        Resolves the parent docket via: docket_map → _docket_cache → async API fetch.
        """
        cluster_id = cluster.get('id')
        if cluster_id is not None:
            self._cluster_cache[cluster_id] = cluster

        docket = await self._resolve_docket_for_cluster_async(session, cluster, docket_map)
        case_dir = self._cluster_case_dir(cluster, docket, court_id)
        self._write_cluster(cluster, case_dir)

    def _resolve_opinion_parents(
        self, opinion: Dict, cluster_map: Dict[int, Dict],
        docket_map: Dict[int, Dict]
    ) -> tuple:
        """Resolve (cluster, docket) for an opinion via maps → caches → sync fetch.
        Returns (cluster_or_None, docket_or_None).
        """
        cluster_id = opinion.get('cluster_id')
        cluster = None
        if cluster_id is not None:
            cluster = (
                cluster_map.get(cluster_id)
                or self._cluster_cache.get(cluster_id)
                or self._fetch_cluster(cluster_id)
            )

        docket_id = cluster.get('docket_id') if cluster is not None else None
        docket = None
        if docket_id is not None:
            docket = (
                docket_map.get(docket_id)
                or self._docket_cache.get(docket_id)
                or self._fetch_docket(docket_id)
            )
        return cluster, docket

    async def _resolve_opinion_parents_async(
        self, session, opinion: Dict, cluster_map: Dict[int, Dict],
        docket_map: Dict[int, Dict]
    ) -> tuple:
        """Resolve (cluster, docket) for an opinion via maps → caches → async fetch.
        Returns (cluster_or_None, docket_or_None).
        """
        cluster_id = opinion.get('cluster_id')
        cluster = None
        if cluster_id is not None:
            cluster = (
                cluster_map.get(cluster_id)
                or self._cluster_cache.get(cluster_id)
            )
            if cluster is None:
                cluster = await self._fetch_cluster_async(session, cluster_id)

        docket_id = cluster.get('docket_id') if cluster is not None else None
        docket = None
        if docket_id is not None:
            docket = (
                docket_map.get(docket_id)
                or self._docket_cache.get(docket_id)
            )
            if docket is None:
                docket = await self._fetch_docket_async(session, docket_id)
        return cluster, docket

    def _opinion_case_dir(self, opinion: Dict, cluster: Optional[Dict],
                          docket: Optional[Dict], court_id: str) -> Path:
        """Determine the case directory for an opinion given its resolved parents."""
        cluster_id = opinion.get('cluster_id')
        docket_id = cluster.get('docket_id') if cluster is not None else None

        if docket is not None:
            year = _year_from_docket(docket)
            return self._case_dir(court_id, year, docket)
        elif cluster is not None:
            # Have cluster but not docket
            year = None
            for field in ('date_filed', 'date_blocked'):
                val = cluster.get(field) or ''
                if val and len(val) >= 4:
                    year = val[:4]
                    break
            fallback_docket = {
                'id': docket_id or cluster.get('id'),
                'docket_number': str(docket_id or cluster.get('id', '')),
                'case_name': cluster.get('case_name') or cluster.get('case_name_short', ''),
                'date_filed': cluster.get('date_filed', ''),
            }
            return self._case_dir(court_id, year, fallback_docket)
        else:
            # Last resort: only opinion data available
            year = None
            for field in ('date_created', 'date_modified'):
                val = opinion.get(field) or ''
                if val and len(val) >= 4:
                    year = val[:4]
                    break
            fallback_docket = {
                'id': cluster_id or opinion.get('id'),
                'docket_number': str(cluster_id or opinion.get('id', '')),
                'case_name': '',
                'date_filed': '',
            }
            return self._case_dir(court_id, year, fallback_docket)

    def _write_opinion(self, opinion: Dict, case_dir: Path):
        """Write opinion JSON to disk with suffix handling for multiple opinions."""
        base = case_dir / "opinion.json"
        if not base.exists():
            out_path = base
        else:
            idx = 2
            while True:
                candidate = case_dir / f"opinion_{idx}.json"
                if not candidate.exists():
                    out_path = candidate
                    break
                idx += 1
        with open(out_path, 'w') as f:
            json.dump(opinion, f, indent=2)

    def _save_opinion(self, opinion: Dict, cluster_map: Dict[int, Dict],
                      docket_map: Dict[int, Dict], court_id: str):
        """
        Save a single opinion to the case folder it belongs to (sync).
        Resolves the parent chain via: maps → instance caches → live API fetches.
        If multiple opinions exist for the same case, they are suffixed.
        """
        cluster, docket = self._resolve_opinion_parents(opinion, cluster_map, docket_map)
        case_dir = self._opinion_case_dir(opinion, cluster, docket, court_id)
        self._write_opinion(opinion, case_dir)

    async def _save_opinion_async(self, session, opinion: Dict,
                                   cluster_map: Dict[int, Dict],
                                   docket_map: Dict[int, Dict], court_id: str):
        """
        Save a single opinion to the case folder it belongs to (async).
        Resolves the parent chain via: maps → instance caches → async API fetches.
        """
        cluster, docket = await self._resolve_opinion_parents_async(
            session, opinion, cluster_map, docket_map
        )
        case_dir = self._opinion_case_dir(opinion, cluster, docket, court_id)
        self._write_opinion(opinion, case_dir)

    # -------------------------------------------------------------------------
    # SYNCHRONOUS METHODS
    # -------------------------------------------------------------------------
    
    def _scrape_dockets_sync(
        self,
        court_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_dockets: Optional[int] = None,
        save_to_disk: bool = True
    ) -> List[Dict]:
        """Scrape dockets synchronously.

        Args:
            save_to_disk: If True (default), writes docket.json immediately.
                          If False, only populates the in-memory cache (used by
                          ``_scrape_full_sync`` which defers saving until after
                          the cluster check so docket-only cases are skipped).
        """
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
            cid = docket.get('court_id') or court_id or 'unknown'
            if save_to_disk:
                self._save_docket(docket, cid)
            else:
                self._cache_docket(docket)
            
            if i % 100 == 0:
                logging.info(f"Fetched {i:,} dockets")
        
        action = "saved" if save_to_disk else "cached"
        logging.info(f"Completed docket scrape: {len(dockets)} dockets {action}")
        return dockets
    
    def _scrape_clusters_sync(
        self,
        court_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_clusters: Optional[int] = None,
        docket_map: Optional[Dict[int, Dict]] = None,
        docket_ids: Optional[List[int]] = None,
    ) -> List[Dict]:
        """Scrape opinion clusters synchronously, saving each to its own case folder.

        When docket_ids is provided, queries one docket at a time using
        docket=<id> so clusters land in the same folders as their dockets.
        Falls back to a court-wide query otherwise.
        """
        docket_map = docket_map or {}
        logging.info("Starting cluster scrape...")
        clusters: List[Dict] = []

        if docket_ids:
            for docket_id in docket_ids:
                params: Dict = {'docket': docket_id, 'order_by': 'id'}
                for cluster in self.api.get_paginated('clusters/', params):
                    clusters.append(cluster)
                    cid = court_id or 'unknown'
                    self._save_cluster(cluster, docket_map, cid)
        else:
            params = {'order_by': '-date_filed,id'}
            if court_id:
                params['docket__court'] = court_id
            else:
                params['docket__court__jurisdiction'] = 'F'
            if start_date:
                params['date_filed__gte'] = start_date
            if end_date:
                params['date_filed__lte'] = end_date
            for i, cluster in enumerate(self.api.get_paginated('clusters/', params, max_clusters), 1):
                clusters.append(cluster)
                cid = court_id or 'unknown'
                self._save_cluster(cluster, docket_map, cid)
                if i % 100 == 0:
                    logging.info(f"Fetched {i:,} clusters")

        logging.info(f"Completed cluster scrape: {len(clusters)} clusters saved")
        return clusters

    def _scrape_opinions_sync(
        self,
        court_id: Optional[str] = None,
        max_opinions: Optional[int] = None,
        cluster_map: Optional[Dict[int, Dict]] = None,
        docket_map: Optional[Dict[int, Dict]] = None,
        cluster_ids: Optional[List[int]] = None,
    ) -> List[Dict]:
        """Scrape opinions synchronously, saving each to its own case folder.

        When cluster_ids is provided, queries one cluster at a time using
        cluster=<id> so opinions land in the same folders as their clusters.
        Falls back to a court-wide query otherwise.
        """
        cluster_map = cluster_map or {}
        docket_map  = docket_map  or {}
        logging.info("Starting opinion scrape...")
        opinions: List[Dict] = []

        if cluster_ids:
            for cluster_id in cluster_ids:
                params: Dict = {'cluster': cluster_id, 'order_by': 'id'}
                for opinion in self.api.get_paginated('opinions/', params):
                    opinions.append(opinion)
                    cid = court_id or 'unknown'
                    self._save_opinion(opinion, cluster_map, docket_map, cid)
        else:
            params = {'order_by': 'id'}
            if court_id:
                params['cluster__docket__court'] = court_id
            else:
                params['cluster__docket__court__jurisdiction'] = 'F'
            for i, opinion in enumerate(self.api.get_paginated('opinions/', params, max_opinions), 1):
                opinions.append(opinion)
                cid = court_id or 'unknown'
                self._save_opinion(opinion, cluster_map, docket_map, cid)
                if i % 50 == 0:
                    logging.info(f"Fetched {i:,} opinions")
        
        logging.info(f"Completed opinion scrape: {len(opinions)} opinions saved")
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
        max_dockets: Optional[int] = None,
        save_to_disk: bool = True
    ) -> List[Dict]:
        """Scrape dockets for single court asynchronously.

        Args:
            save_to_disk: If True (default), writes docket.json immediately.
                          If False, only populates the in-memory cache (used by
                          ``_scrape_full_async`` which defers saving until after
                          the cluster check so docket-only cases are skipped).
        """
        params = {'order_by': '-date_filed,id', 'court': court_id}
        
        if start_date:
            params['date_filed__gte'] = start_date
        if end_date:
            params['date_filed__lte'] = end_date
        
        logging.info(f"Starting async docket scrape for {court_id}...")
        
        dockets = await self.api.get_paginated(session, 'dockets/', params, max_dockets)
        
        logging.info(f"Fetched {len(dockets)} dockets from {court_id}")

        for docket in dockets:
            if save_to_disk:
                self._save_docket(docket, court_id)
            else:
                self._cache_docket(docket)
        
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
        court_id: str,
        docket_map: Dict[int, Dict],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict]:
        """Scrape clusters for a court using a single paginated court-wide query.

        Instead of querying clusters/?docket=<id> one-by-one for every docket
        (which consumes one API request per docket), this fetches all clusters
        for the court+date range in a single paginated stream.  For a typical
        court-year with 1,000 clusters this uses ~50 page requests instead of
        ~2,500 per-docket requests — a ~50× reduction in API calls.

        Returns all clusters (does NOT save to disk — the caller handles saving
        after determining which dockets have clusters).
        """
        params: Dict[str, Any] = {'docket__court': court_id, 'order_by': 'id'}
        if start_date:
            params['date_filed__gte'] = start_date
        if end_date:
            params['date_filed__lte'] = end_date

        logging.info(f"Cluster scrape for {court_id}: fetching court-wide paginated stream...")

        phase_start = time.time()
        all_clusters = await self.api.get_paginated(session, 'clusters/', params)

        # Populate the cluster cache
        for cluster in all_clusters:
            cluster_id = cluster.get('id')
            if cluster_id is not None:
                self._cluster_cache[cluster_id] = cluster

        duration = time.time() - phase_start
        logging.info(
            f"Fetched {len(all_clusters)} clusters for {court_id} "
            f"in {duration:.1f}s (paginated stream)"
        )
        return all_clusters

    async def _scrape_opinions_async(
        self,
        session: ClientSession,
        court_id: str,
        cluster_map: Dict[int, Dict],
        docket_map: Dict[int, Dict],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict]:
        """Scrape opinions for a court using a single paginated court-wide query.

        Instead of querying opinions/?cluster=<id> one-by-one for every cluster
        (which consumes one API request per cluster), this fetches all opinions
        for the court+date range in a single paginated stream.  Each opinion's
        cluster_id is used to look up its parent cluster and docket from the
        in-memory maps for folder organisation.
        """
        params: Dict[str, Any] = {
            'cluster__docket__court': court_id,
            'order_by': 'id',
        }
        if start_date:
            params['cluster__date_filed__gte'] = start_date
        if end_date:
            params['cluster__date_filed__lte'] = end_date

        logging.info(f"Opinion scrape for {court_id}: fetching court-wide paginated stream...")

        phase_start = time.time()
        all_opinions = await self.api.get_paginated(session, 'opinions/', params)

        # Save each opinion to disk (only if its cluster is in our map)
        saved_count = 0
        skipped_count = 0
        for opinion in all_opinions:
            cluster_id = opinion.get('cluster_id')
            if cluster_id is not None and cluster_id not in cluster_map and cluster_id not in self._cluster_cache:
                skipped_count += 1
                continue  # opinion's cluster not in our set
            cluster, docket = await self._resolve_opinion_parents_async(
                session, opinion, cluster_map, docket_map
            )
            case_dir = self._opinion_case_dir(opinion, cluster, docket, court_id)
            self._write_opinion(opinion, case_dir)
            saved_count += 1

        duration = time.time() - phase_start
        logging.info(
            f"Fetched {len(all_opinions)} opinions for {court_id} "
            f"({saved_count} saved, {skipped_count} skipped) "
            f"in {duration:.1f}s (paginated stream)"
        )
        return all_opinions

    async def _enrich_dockets_async(
        self,
        session,
        court_id: str,
        docket_map: Dict[int, Dict],
    ) -> int:
        """Fetch full docket records from the API to replace partial dockets.

        The async cluster-first approach constructs partial docket.json files
        with only ~8 fields.  This method fetches the complete docket record
        for each docket_id and overwrites the partial file on disk with the
        full record (20+ fields: docket_number, nature_of_suit, panel info,
        date_argued, date_terminated, etc.).

        Returns the number of dockets successfully enriched.
        """
        docket_ids = list(docket_map.keys())
        if not docket_ids:
            return 0

        logging.info(
            f"Docket enrichment for {court_id}: {len(docket_ids)} dockets to fetch..."
        )
        phase_start = time.time()
        enriched = 0

        for i, docket_id in enumerate(docket_ids, 1):
            try:
                full_docket = await self.api.get(session, f'dockets/{docket_id}/')
                if not full_docket or not full_docket.get('id'):
                    logging.warning(f"Empty response for docket {docket_id}")
                    continue

                # Update the cache and the in-memory map
                self._docket_cache[docket_id] = full_docket
                docket_map[docket_id] = full_docket

                # Overwrite the partial docket.json on disk
                year = _year_from_docket(full_docket)
                case_dir = self._case_dir(court_id, year, full_docket, create=False)
                docket_path = case_dir / "docket.json"
                if case_dir.exists():
                    with open(docket_path, 'w') as f:
                        json.dump(full_docket, f, indent=2)
                    enriched += 1
                else:
                    # Case dir might have a slightly different name if the full
                    # docket has different docket_number formatting.  Save anyway.
                    case_dir.mkdir(parents=True, exist_ok=True)
                    with open(docket_path, 'w') as f:
                        json.dump(full_docket, f, indent=2)
                    enriched += 1

                if i % 50 == 0:
                    logging.info(
                        f"  [{court_id}] enriched {i}/{len(docket_ids)} dockets..."
                    )
            except Exception as e:
                logging.warning(f"Failed to enrich docket {docket_id}: {e}")

        duration = time.time() - phase_start
        logging.info(
            f"Docket enrichment for {court_id}: {enriched}/{len(docket_ids)} "
            f"enriched in {duration:.1f}s"
        )
        return enriched
    
    # -------------------------------------------------------------------------
    # PUBLIC METHODS
    # -------------------------------------------------------------------------
    
    def scrape_dockets(self, **kwargs) -> List[Dict]:
        """Scrape dockets (auto-selects sync/async). Always returns a flat list."""
        if self.use_async:
            court_dict = asyncio.run(self._scrape_all_courts_async(
                start_date=kwargs.get('start_date'),
                end_date=kwargs.get('end_date'),
                max_per_court=kwargs.get('max_dockets')
            ))
            # Flatten Dict[str, List[Dict]] → List[Dict] for consistent return type
            return [d for dockets in court_dict.values() for d in dockets]
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
        """Full scrape using synchronous requests, one court at a time.

        Docket-only cases (no clusters/opinions) are skipped — docket.json is
        only written to disk for dockets that have at least one cluster.
        """
        scrape_start = time.time()
        summary: List[Dict] = []

        for court_id in courts:
            if court_id in self.checkpoint.get('completed_courts', []):
                logging.info(f"Skipping {court_id} (already completed)")
                continue
            
            logging.info("=" * 80)
            logging.info(f"Processing court: {court_id}")
            logging.info("=" * 80)
            court_start = time.time()
            
            try:
                # Phase 1: fetch dockets (cache only — do NOT write to disk yet)
                p1_start = time.time()
                dockets    = self._scrape_dockets_sync(court_id, start_date, end_date, max_per_court,
                                                       save_to_disk=False)
                docket_map = {d['id']: d for d in dockets if d.get('id')}
                docket_ids = list(docket_map.keys())
                p1_dur = time.time() - p1_start
                logging.info(f"  Phase 1 (dockets) for {court_id}: {len(dockets)} in {p1_dur:.1f}s")
                self._mark_court_phase(court_id, 'dockets')

                # Phase 2: clusters — scoped to the exact dockets we just fetched
                p2_start = time.time()
                clusters    = self._scrape_clusters_sync(
                    court_id=court_id,
                    start_date=start_date,
                    end_date=end_date,
                    docket_map=docket_map,
                    docket_ids=docket_ids,
                )
                cluster_map = {c['id']: c for c in clusters if c.get('id')}
                cluster_ids = list(cluster_map.keys())

                # Determine which dockets had at least one cluster
                docket_ids_with_clusters = {c.get('docket_id') for c in clusters if c.get('docket_id')}
                skipped = len(docket_map) - len(docket_ids_with_clusters)

                # Now save docket.json ONLY for dockets that have clusters
                for docket_id in docket_ids_with_clusters:
                    docket = docket_map.get(docket_id)
                    if docket:
                        self._save_docket(docket, court_id)

                p2_dur = time.time() - p2_start
                logging.info(
                    f"  Phase 2 (clusters) for {court_id}: {len(clusters)} clusters in {p2_dur:.1f}s — "
                    f"saved {len(docket_ids_with_clusters)} dockets, "
                    f"skipped {skipped} docket-only"
                )
                self._mark_court_phase(court_id, 'clusters')

                # Phase 3: opinions — scoped to the exact clusters we just fetched
                p3_start = time.time()
                self._scrape_opinions_sync(
                    court_id=court_id,
                    cluster_map=cluster_map,
                    docket_map=docket_map,
                    cluster_ids=cluster_ids,
                )
                p3_dur = time.time() - p3_start
                logging.info(f"  Phase 3 (opinions) for {court_id}: completed in {p3_dur:.1f}s")
                
                self._mark_court_phase(court_id, 'completed')

                court_dur = time.time() - court_start
                court_name = COURT_FOLDER_NAMES.get(court_id, court_id)
                summary.append({
                    'court': court_name, 'dockets': len(dockets),
                    'clusters': len(clusters), 'duration': court_dur,
                })
                logging.info(
                    f"Completed {court_name}: {len(dockets)} dockets, "
                    f"{len(clusters)} clusters in {court_dur:.1f}s"
                )
                
            except Exception as e:
                logging.error(f"Error processing {court_id}: {e}", exc_info=True)

        # C3: Overall summary
        scrape_dur = time.time() - scrape_start
        if summary:
            logging.info("=" * 80)
            logging.info("SCRAPE SUMMARY")
            logging.info("=" * 80)
            for s in summary:
                logging.info(
                    f"  {s['court']:<20s}  dockets={s['dockets']:<6d}  "
                    f"clusters={s['clusters']:<6d}  time={s['duration']:.1f}s"
                )
            logging.info(f"  Total duration: {scrape_dur:.1f}s ({scrape_dur/3600:.2f}h)")
            logging.info("=" * 80)
    
    async def _scrape_full_async(
        self,
        courts: List[str],
        start_date: Optional[str],
        end_date: Optional[str],
        max_per_court: Optional[int]
    ):
        """Full scrape using cluster-first approach.

        Instead of fetching dockets first (which have mismatched date_filed
        semantics), this fetches clusters first via court-wide paginated
        queries, constructs a partial docket.json from cluster metadata,
        then fetches opinions.

        Two phases only — no individual docket API calls needed:
          Phase 1: clusters (paginated) → save cluster.json + constructed docket.json
          Phase 2: opinions (paginated) → save opinion.json
          Phase 3: docket enrichment → replace partial docket.json with full records

        Each phase is parallelised across courts (A1).
        Per-court-per-phase checkpointing survives crashes (B1).
        """
        scrape_start = time.time()

        # Filter out courts already fully completed in this year's checkpoint
        completed = set(self.checkpoint.get('completed_courts', []))
        pending_courts = [c for c in courts if c not in completed]
        if len(pending_courts) < len(courts):
            skipped = [c for c in courts if c in completed]
            logging.info(f"Skipping already-completed courts: {', '.join(skipped)}")
        if not pending_courts:
            logging.info("All courts already completed for this year.")
            return

        # Determine which courts still need each phase (B1 resume)
        courts_need_clusters = [c for c in pending_courts
                                if self._court_phase(c) in (None, 'dockets')]
        courts_need_opinions = [c for c in pending_courts
                                if self._court_phase(c) in (None, 'dockets', 'clusters')]

        async with await self.api.create_session() as session:
            # ==== Phase 1: clusters — ALL courts in parallel ====
            phase1_start = time.time()
            if courts_need_clusters:
                async def phase1_court(court_id: str) -> tuple:
                    """Fetch clusters for one court via paginated stream."""
                    clusters = await self._scrape_clusters_async(
                        session, court_id, {},
                        start_date=start_date, end_date=end_date,
                    )
                    cmap = {c['id']: c for c in clusters if c.get('id')}
                    return court_id, cmap

                logging.info(
                    f"Phase 1: scraping clusters for {len(courts_need_clusters)} "
                    f"courts in parallel..."
                )
                p1_tasks = [phase1_court(c) for c in courts_need_clusters]
                p1_results = await asyncio.gather(*p1_tasks, return_exceptions=True)
            else:
                logging.info("Phase 1: all courts already have clusters checkpointed.")
                p1_results = []

            court_cluster_map: Dict[str, Dict[int, Dict]] = {}
            for item in p1_results:
                if isinstance(item, Exception):
                    logging.error(f"Error in Phase 1: {item}")
                    continue
                court_id, cmap = item
                court_cluster_map[court_id] = cmap

            total_clusters = sum(len(m) for m in court_cluster_map.values())
            phase1_dur = time.time() - phase1_start
            logging.info(
                f"Phase 1 complete: {total_clusters} clusters in {phase1_dur:.1f}s"
            )

            # Build docket map from cluster data and save cluster.json + docket.json
            court_docket_map: Dict[str, Dict[int, Dict]] = {}
            total_clusters_saved = 0
            total_dockets_saved = 0
            for court_id in courts_need_clusters:
                cluster_map = court_cluster_map.get(court_id, {})
                clusters = list(cluster_map.values())

                # Group clusters by docket_id
                docket_clusters: Dict[int, List[Dict]] = {}
                for cluster in clusters:
                    docket_id = cluster.get('docket_id')
                    if docket_id is not None:
                        docket_clusters.setdefault(docket_id, []).append(cluster)

                # For each unique docket_id, construct a partial docket and save
                docket_map: Dict[int, Dict] = {}
                dockets_saved = 0
                clusters_saved = 0
                for docket_id, d_clusters in docket_clusters.items():
                    # Use the first cluster to derive docket metadata
                    first_cluster = d_clusters[0]
                    constructed_docket = {
                        'id': docket_id,
                        'resource_uri': first_cluster.get('docket', ''),
                        'case_name': first_cluster.get('case_name', ''),
                        'case_name_short': first_cluster.get('case_name_short', ''),
                        'date_filed': first_cluster.get('date_filed', ''),
                        'court_id': court_id,
                        '_partial': True,  # marker for future backfill
                        '_source': 'constructed_from_cluster',
                    }
                    docket_map[docket_id] = constructed_docket

                    # Save docket.json
                    self._save_docket(constructed_docket, court_id)
                    dockets_saved += 1

                    # Save cluster.json for each cluster of this docket
                    for cluster in d_clusters:
                        case_dir = self._cluster_case_dir(
                            cluster, constructed_docket, court_id
                        )
                        self._write_cluster(cluster, case_dir)
                        clusters_saved += 1

                court_docket_map[court_id] = docket_map
                total_clusters_saved += clusters_saved
                total_dockets_saved += dockets_saved
                logging.info(
                    f"{court_id}: saved {dockets_saved} dockets + "
                    f"{clusters_saved} clusters"
                )

                # B1: Mark clusters phase complete per court
                self._mark_court_phase(court_id, 'clusters')

            logging.info(
                f"Phase 1 save complete: {total_dockets_saved} dockets, "
                f"{total_clusters_saved} clusters saved"
            )

            # ==== Phase 2: opinions — ALL courts in parallel ====
            phase2_start = time.time()
            if courts_need_opinions:
                async def phase2_court(court_id: str) -> tuple:
                    """Fetch opinions for one court via paginated stream."""
                    cluster_map = court_cluster_map.get(court_id, {})
                    docket_map  = court_docket_map.get(court_id, {})
                    opinions = await self._scrape_opinions_async(
                        session, court_id, cluster_map, docket_map,
                        start_date=start_date, end_date=end_date,
                    )
                    return court_id, len(opinions)

                logging.info(
                    f"Phase 2: scraping opinions for {len(courts_need_opinions)} "
                    f"courts in parallel..."
                )
                p2_tasks = [phase2_court(c) for c in courts_need_opinions]
                p2_results = await asyncio.gather(*p2_tasks, return_exceptions=True)
            else:
                logging.info("Phase 2: all courts already have opinions checkpointed.")
                p2_results = []

            total_opinions = 0
            for item in p2_results:
                if isinstance(item, Exception):
                    logging.error(f"Error in Phase 2: {item}")
                    continue
                court_id, n_opinions = item
                total_opinions += n_opinions
                # B1: Mark opinions phase complete per court
                self._mark_court_phase(court_id, 'opinions')

            phase2_dur = time.time() - phase2_start
            logging.info(f"Phase 2 complete: {total_opinions} opinions in {phase2_dur:.1f}s")

            # ==== Phase 3: docket enrichment — ALL courts in parallel ====
            # Replace partial docket.json files (constructed from cluster metadata)
            # with full docket records from the API.
            courts_need_enrichment = [c for c in pending_courts
                                      if self._court_phase(c) in (None, 'dockets', 'clusters', 'opinions')]
            phase3_start = time.time()
            if courts_need_enrichment:
                async def phase3_court(court_id: str) -> tuple:
                    """Fetch full docket records to replace partial ones."""
                    docket_map = court_docket_map.get(court_id, {})
                    enriched = await self._enrich_dockets_async(
                        session, court_id, docket_map,
                    )
                    return court_id, enriched

                logging.info(
                    f"Phase 3: enriching dockets for {len(courts_need_enrichment)} "
                    f"courts in parallel..."
                )
                p3_tasks = [phase3_court(c) for c in courts_need_enrichment]
                p3_results = await asyncio.gather(*p3_tasks, return_exceptions=True)
            else:
                logging.info("Phase 3: all courts already have enriched dockets.")
                p3_results = []

            total_enriched = 0
            for item in p3_results:
                if isinstance(item, Exception):
                    logging.error(f"Error in Phase 3: {item}")
                    continue
                court_id, n_enriched = item
                total_enriched += n_enriched
                # B1: Mark completed per court
                self._mark_court_phase(court_id, 'completed')

            phase3_dur = time.time() - phase3_start
            logging.info(f"Phase 3 complete: {total_enriched} dockets enriched in {phase3_dur:.1f}s")

            # Summary
            scrape_dur = time.time() - scrape_start
            logging.info("=" * 80)
            logging.info("SCRAPE SUMMARY")
            logging.info("=" * 80)
            for court_id in pending_courts:
                n_clusters = len(court_cluster_map.get(court_id, {}))
                n_dockets  = len(court_docket_map.get(court_id, {}))
                court_name = COURT_FOLDER_NAMES.get(court_id, court_id)
                logging.info(
                    f"  {court_name:<20s}  dockets={n_dockets:<6d}  "
                    f"clusters={n_clusters:<6d}"
                )
            logging.info(
                f"  {'TOTAL':<20s}  dockets={total_dockets_saved:<6d}  "
                f"clusters={total_clusters:<6d}  opinions={total_opinions:<6d}  "
                f"enriched={total_enriched:<6d}"
            )
            logging.info(f"  Duration: {scrape_dur:.1f}s ({scrape_dur/3600:.2f}h)")
            logging.info("=" * 80)

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
        default=os.environ.get('COURTLISTENER_TOKEN', ''),
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
        '--years',
        nargs='+',
        help='Specific years to scrape (e.g., 2013 2015 2020) or "all" for 2013-present. Overrides --start-date/--end-date.'
    )
    
    parser.add_argument(
        '--max-per-court',
        type=int,
        help='Maximum items to fetch per court'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum items to fetch per court (alias for --max-per-court)'
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
    
    # Handle --years argument (converts to start_date/end_date)
    if args.years:
        if 'all' in [y.lower() for y in args.years]:
            args.start_date = args.start_date or '2013-01-01'
            # end_date stays None (up to present)
        else:
            years = sorted([int(y) for y in args.years])
            args.start_date = args.start_date or f'{years[0]}-01-01'
            args.end_date = args.end_date or f'{years[-1]}-12-31'
    
    # Handle --limit alias for --max-per-court
    if args.limit and not args.max_per_court:
        args.max_per_court = args.limit
    
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
    
    # Determine which courts to iterate over
    courts_to_scrape = args.courts or CIRCUIT_COURTS
    
    # Execute based on mode
    if args.mode == 'dockets':
        for court_id in courts_to_scrape:
            scraper.scrape_dockets(
                court_id=court_id,
                start_date=args.start_date,
                end_date=args.end_date,
                max_dockets=args.max_per_court
            )
    
    elif args.mode == 'clusters':
        for court_id in courts_to_scrape:
            scraper.scrape_clusters(
                court_id=court_id,
                start_date=args.start_date,
                end_date=args.end_date,
                max_clusters=args.max_per_court
            )
    
    elif args.mode == 'opinions':
        for court_id in courts_to_scrape:
            scraper.scrape_opinions(
                court_id=court_id,
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
