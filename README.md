# Washington State Courts Opinion Scraper

A Python scraper for downloading Supreme Court opinions from the Washington State Courts website.

## Features

- Downloads all Supreme Court opinions (2012-2025)
- Saves PDFs organized by Year/Month folders
- Generates metadata CSV with case details
- **Resume capability** - automatically continues from where it stopped
- **Failproof** - retries failed downloads, handles network issues
- **Graceful shutdown** - press Ctrl+C to stop safely

## Installation

```bash
# Clone the repository
git clone <repo-url>
cd LegalAI_Scraper

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

```bash
# Scrape all available years (with auto-resume)
python run_scraper.py

# Scrape specific years
python run_scraper.py --years 2023 2024 2025

# Scrape a single year
python run_scraper.py --years 2024

# List available years without downloading
python run_scraper.py --list-years

# Start fresh (ignore previous progress)
python run_scraper.py --no-resume

# Custom output directory
python run_scraper.py --output ./my_downloads
```

## Output Structure

```
downloads/
├── 2025/
│   ├── January/
│   │   ├── 102586-6_Case Title.pdf
│   │   └── ...
│   ├── February/
│   └── ...
├── 2024/
├── metadata.csv
└── scraper_checkpoint.json
```

## Metadata CSV Columns

| Column          | Description                            |
| --------------- | -------------------------------------- |
| year            | Filing year                            |
| month           | Filing month                           |
| file_date       | Original file date                     |
| case_number     | Court case number                      |
| case_title      | Case name (e.g., "State v. Smith")     |
| file_contains   | Opinion type (Majority, Dissent, etc.) |
| case_info_url   | Link to case info page                 |
| pdf_url         | Direct PDF download URL                |
| pdf_filename    | Local filename                         |
| download_status | Success/Failed                         |
| scraped_at      | Timestamp                              |

## Running Overnight

The scraper is designed for long-running sessions:

```bash
# Just run it - it will resume automatically if stopped
python run_scraper.py
```

- **Network issues?** Auto-retries with exponential backoff
- **Need to stop?** Press Ctrl+C - saves progress and exits gracefully
- **Power failure?** Just run again - resumes from checkpoint
- **Failed downloads?** Automatically retried on next run

## Configuration

Edit `config.py` to adjust:

- Request delays (default: 1-2 seconds between requests)
- Retry settings (default: 5 retries with exponential backoff)
- Timeout settings (default: 30 seconds)

## Files

- `scraper.py` - Main scraper class
- `config.py` - Configuration settings
- `run_scraper.py` - CLI runner script
- `test_scraper.py` - Test script for development

## License

MIT
