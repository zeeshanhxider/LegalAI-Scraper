# Pennsylvania Scraper Help

This folder contains the Pennsylvania (PACourts) opinions scraper.

## Run

```bash
python pennsylvania_scraper.py
```

## Output

- CSV: `pennsylvania/pacourts_pdfs.csv`
- PDFs: `pennsylvania/downloads/`
- Debug artifacts (optional): `pennsylvania/debug/`

## Common Options (env vars)

```bash
# Run headed for debugging
HEADLESS=0 python pennsylvania_scraper.py

# Use Chrome instead of bundled Chromium
BROWSER_CHANNEL=chrome python pennsylvania_scraper.py

# Increase result wait timeout (milliseconds)
WAIT_TIMEOUT_MS=120000 python pennsylvania_scraper.py

# Parallel PDF downloads
MAX_DOWNLOADS=8 MAX_DOWNLOAD_QUEUE=24 python pennsylvania_scraper.py

# Dump debug HTML/JSON for the first skipped result on each page
DUMP_SKIPS=1 python pennsylvania_scraper.py
```

## Notes

- The site opens PDFs in a new tab. The scraper detects PDFs via network response headers,
  popup content, and Coveo metadata.
- If results time out or PDFs are skipped, check `pennsylvania/debug/` for HTML/JSON
  and share those files for troubleshooting.
