"""
Configuration settings for the Washington Courts Opinion Scraper
"""

# Base URLs
BASE_URL = "https://www.courts.wa.gov"
OPINIONS_URL = f"{BASE_URL}/opinions/index.cfm"

# URL patterns
# Main page: ?fa=opinions.displayAll
# Year page: ?fa=opinions.byYear&fileYear=2013&crtLevel=S&pubStatus=PUB
# Case info: ?fa=opinions.showOpinion&filename=871051MAJ
# PDF: /opinions/pdf/871051.pdf

# Request settings
REQUEST_DELAY_MIN = 1.0  # Minimum seconds between requests
REQUEST_DELAY_MAX = 2.0  # Maximum seconds between requests
REQUEST_TIMEOUT = 30  # Timeout for requests in seconds

# Retry settings
MAX_RETRIES = 5
RETRY_DELAY = 10  # Base seconds to wait before retrying (uses exponential backoff)
MAX_RETRY_DELAY = 300  # Maximum retry delay (5 minutes)

# Checkpoint settings
CHECKPOINT_FILE = "scraper_checkpoint.json"

# Headers to mimic a browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}

# Output settings
OUTPUT_DIR = "downloads"
METADATA_FILENAME = "metadata.csv"

# Opinion Types Configuration
# Each type has: court_level, pub_status, folder_name, display_name
OPINION_TYPES = {
    "supreme_court": {
        "court_level": "S",
        "pub_status": "PUB",
        "folder": "Supreme_Court_Opinions",
        "display_name": "Supreme Court Opinions"
    },
    "appeals_published": {
        "court_level": "C",
        "pub_status": "PUB",
        "folder": "Court_of_Appeals_Published",
        "display_name": "Court of Appeals - Published Opinions"
    },
    "appeals_partial": {
        "court_level": "C",
        "pub_status": "PAR",
        "folder": "Court_of_Appeals_Published_in_Part",
        "display_name": "Court of Appeals - Opinions Published in Part"
    },
    "appeals_unpublished": {
        "court_level": "C",
        "pub_status": "UNP",
        "folder": "Court_of_Appeals_Unpublished",
        "display_name": "Court of Appeals - Unpublished Opinions"
    }
}

# Court levels (legacy - for backwards compatibility)
COURT_LEVELS = {
    "supreme_court": "S",           # Supreme Court
    "court_of_appeals": "C",        # Court of Appeals
}

# Publication status (legacy - for backwards compatibility)
PUB_STATUS = {
    "published": "PUB",
    "unpublished": "UNP",
    "partial": "PIP",  # Published in Part
}

# Years to scrape (will be dynamically detected, but can be overridden)
YEARS_TO_SCRAPE = None  # None means scrape all available years
