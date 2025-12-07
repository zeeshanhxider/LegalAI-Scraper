# Washington State Courts Opinion Scraper

A Python scraper for downloading court opinions from the Washington State Courts website.

## Features

- Downloads **all opinion types**:
  - Supreme Court Opinions
  - Court of Appeals - Published Opinions
  - Court of Appeals - Opinions Published in Part
  - Court of Appeals - Unpublished Opinions
- Saves PDFs organized by Opinion Type/Year/Month folders
- Generates metadata CSV with case details (including opinion type)
- **Resume capability** - automatically continues from where it stopped
- **Failproof** - retries failed downloads, handles network issues
- **Graceful shutdown** - press Ctrl+C to stop safely

## Case Counts (as of December 2025)

| Opinion Type                         | Cases      | Years     |
| ------------------------------------ | ---------- | --------- |
| Supreme Court Opinions               | 1,114      | 2013-2025 |
| Court of Appeals - Published         | 2,849      | 2012-2025 |
| Court of Appeals - Published in Part | 379        | 2013-2025 |
| Court of Appeals - Unpublished       | 15,182     | 2013-2025 |
| **GRAND TOTAL**                      | **19,524** |           |

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

### Basic Commands

```bash
# Scrape Supreme Court opinions (default)
python run_scraper.py

# Scrape ALL opinion types
python run_scraper.py --type all

# Scrape a specific opinion type
python run_scraper.py --type appeals_published
python run_scraper.py --type appeals_partial
python run_scraper.py --type appeals_unpublished

# Count cases for all opinion types (no download)
python run_scraper.py --count-all
```

### Opinion Type Options

| Option                | Description                                   |
| --------------------- | --------------------------------------------- |
| `supreme_court`       | Supreme Court Opinions (default)              |
| `appeals_published`   | Court of Appeals - Published Opinions         |
| `appeals_partial`     | Court of Appeals - Opinions Published in Part |
| `appeals_unpublished` | Court of Appeals - Unpublished Opinions       |
| `all`                 | Scrape all opinion types                      |

### Additional Options

```bash
# Scrape specific years
python run_scraper.py --type supreme_court --years 2023 2024 2025

# List available years for an opinion type
python run_scraper.py --type appeals_published --list-years

# Test mode - only scrape the most recent year
python run_scraper.py --type all --test

# Start fresh (ignore previous progress)
python run_scraper.py --no-resume

# Custom output directory
python run_scraper.py --output ./my_downloads
```

## Output Structure

```
downloads/
├── Supreme_Court_Opinions/
│   ├── 2025/
│   │   ├── January/
│   │   │   ├── 102586-6_Case Title.pdf
│   │   │   └── ...
│   │   ├── February/
│   │   └── ...
│   ├── 2024/
│   ├── metadata.csv
│   └── scraper_checkpoint.json
├── Court_of_Appeals_Published/
│   ├── 2025/
│   ├── metadata.csv
│   └── ...
├── Court_of_Appeals_Published_in_Part/
│   └── ...
└── Court_of_Appeals_Unpublished/
    └── ...
```

## Metadata CSV Columns

| Column             | Description                                        |
| ------------------ | -------------------------------------------------- |
| opinion_type       | Court type: "Supreme Court" or "Court of Appeals"  |
| publication_status | "Published", "Unpublished", or "Published in Part" |
| year               | Filing year                                        |
| month              | Filing month                                       |
| file_date          | Original file date                                 |
| case_number        | Court case number                                  |
| case_title         | Case name (e.g., "State v. Smith")                 |
| file_contains      | Opinion type (Majority, Dissent, etc.)             |
| case_info_url      | Link to case info page                             |
| pdf_url            | Direct PDF download URL                            |
| pdf_filename       | Local filename                                     |
| download_status    | Success/Failed                                     |
| scraped_at         | Timestamp                                          |

## Running Overnight

The scraper is designed for long-running sessions:

```bash
# Just run it - it will resume automatically if stopped
python run_scraper.py --type all
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
