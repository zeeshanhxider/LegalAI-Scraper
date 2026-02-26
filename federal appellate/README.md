# Federal Appellate Court Scraper

A production-grade scraper for downloading federal appellate case law and comprehensive metadata from the CourtListener API.

## Features

- ✅ **Comprehensive Coverage**: Scrapes all 13 federal circuit courts (CA1-CA11, CADC, CAFC)
- ✅ **Complete Metadata**: Captures dockets, opinion clusters, and full opinion text
- ✅ **Production-Ready**: Rate limiting, retry logic, error handling, and progress tracking
- ✅ **Efficient**: Field selection, cursor-based pagination, and batch processing
- ✅ **Resumable**: Checkpoint system allows resuming interrupted scrapes
- ✅ **Flexible**: Multiple modes for targeted scraping
- ✅ **Export Options**: JSON and CSV output formats

## Prerequisites

1. **Python 3.8+** 
2. **CourtListener API Token**: Get your free token at https://www.courtlistener.com/sign-in/
3. **Python packages**: See requirements.txt

## Installation

```bash
# Clone or navigate to this directory
cd "federal appellate"

# Install required packages
pip install -r requirements.txt
```

## Quick Start

### 1. Set your API token

```bash
export COURTLISTENER_TOKEN="your-api-token-here"
```

Or pass it directly with `--api-token` flag.

### 2. Production Run: Multiprocess Orchestrator (Recommended)

The multiprocess orchestrator (`run_all_years.py`) distributes scraping across 13 parallel year-processes with async workers, achieving ~4-5x speedup:

```bash
# Full production run: all years (2013-2025), all courts, all cases
python run_all_years.py --all

# Test run: 1 case per court per year (fast verification, ~5 min)
python run_all_years.py --limit 1

# Specific years and limit
python run_all_years.py --limit 100 --years 2020 2021 2022

# Specific courts only
python run_all_years.py --limit 50 --courts ca1 ca9 cadc

# Conservative rate budget (half quota, ~2400 req/hr)
python run_all_years.py --all --rate-budget 2400
```

**Note**: Either `--all` or `--limit N` is required (mutually exclusive). Omitting both is an error to prevent accidental unlimited runs.

**Output structure** (per-year per-court per-case):
```
downloads/
├── 1st Circuit/2024/22-1234_case-name/
│   ├── docket.json
│   ├── cluster.json      (or cluster_2.json, cluster_3.json for multiple)
│   └── opinion.json      (or opinion_2.json, opinion_3.json for multiple)
├── 1st Circuit/2023/...
├── 2nd Circuit/2024/...
├── DC Circuit/2024/...
├── Federal Circuit/2024/...
└── checkpoint_2020.json  (per-year checkpoint for resume capability)
    checkpoint_2021.json
    ...
```

### 3. Direct Scraper (Single Process, Simpler)

For smaller scrapes or single-court operations, use the direct scraper:

```bash
# Scrape recent cases from the Ninth Circuit (limited to 100 dockets)
python federal_appellate_scraper.py \
  --api-token YOUR_TOKEN \
  --mode full \
  --courts ca9 \
  --start-date 2024-01-01 \
  --max-per-court 100
```

## Usage Examples

### Get Court Information

```bash
python federal_appellate_scraper.py \
  --api-token YOUR_TOKEN \
  --mode courts
```

### Scrape Only Dockets

```bash
# All federal appellate courts
python federal_appellate_scraper.py \
  --api-token YOUR_TOKEN \
  --mode dockets \
  --start-date 2024-01-01

# Specific courts only
python federal_appellate_scraper.py \
  --api-token YOUR_TOKEN \
  --mode dockets \
  --courts ca1 ca2 ca9 \
  --start-date 2024-01-01
```

### Scrape Opinion Clusters

```bash
python federal_appellate_scraper.py \
  --api-token YOUR_TOKEN \
  --mode clusters \
  --courts cadc \
  --start-date 2023-01-01 \
  --end-date 2023-12-31
```

### Scrape Full Opinion Text

```bash
# This downloads the complete HTML text with citations
python federal_appellate_scraper.py \
  --api-token YOUR_TOKEN \
  --mode opinions \
  --courts ca9 \
  --start-date 2024-01-01 \
  --max-per-court 500
```

### Full Dataset with Date Range

```bash
# Scrape everything from the past year
python federal_appellate_scraper.py \
  --api-token YOUR_TOKEN \
  --mode full \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

## Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--api-token` | CourtListener API token (required) | - |
| `--output-dir` | Output directory for downloads | `downloads` |
| `--mode` | Scraping mode: `full`, `dockets`, `clusters`, `opinions`, `courts` | `full` |
| `--courts` | Specific court IDs (e.g., ca1 ca9 cadc) | All 13 circuits |
| `--start-date` | Start date (YYYY-MM-DD) | None (all dates) |
| `--end-date` | End date (YYYY-MM-DD) | None (all dates) |
| `--max-per-court` | Maximum items per court | None (unlimited) |
| `--rate-limit` | Delay between requests (seconds) | 0.75 |

## Federal Appellate Court IDs

| Court ID | Court Name |
|----------|------------|
| `ca1` | First Circuit |
| `ca2` | Second Circuit |
| `ca3` | Third Circuit |
| `ca4` | Fourth Circuit |
| `ca5` | Fifth Circuit |
| `ca6` | Sixth Circuit |
| `ca7` | Seventh Circuit |
| `ca8` | Eighth Circuit |
| `ca9` | Ninth Circuit |
| `ca10` | Tenth Circuit |
| `ca11` | Eleventh Circuit |
| `cadc` | DC Circuit |
| `cafc` | Federal Circuit |

## Output Structure

### Multiprocess Orchestrator Output (`run_all_years.py`)

```
downloads/
├── 1st Circuit/
│   ├── 2024/
│   │   ├── 22-1234_case-name/
│   │   │   ├── docket.json                 # case docket record
│   │   │   ├── cluster.json                # opinion cluster (or cluster_2.json, etc)
│   │   │   └── opinion.json                # opinion text (or opinion_2.json, etc)
│   │   └── 22-5678_another-case/
│   │       ├── docket.json
│   │       └── opinion.json                # (no cluster if not published)
│   └── 2023/...
├── 2nd Circuit/2024/...
├── DC Circuit/2024/...
├── Federal Circuit/2024/...
├── checkpoint_2024.json                    # resume checkpoint for year 2024
├── checkpoint_2023.json                    # resume checkpoint for year 2023
└── ... (one checkpoint per year)
```

**Key features**:
- Each case folder contains all 3 files (docket, cluster, opinion) when available
- Multiple clusters/opinions per docket get suffixed: `cluster_2.json`, `opinion_3.json`, etc.
- Year derived from docket's `date_filed`, `date_argued`, or docket number prefix
- Per-year checkpoint files enable parallel year-processes without interference
- Cases with no published opinion in CourtListener only have `docket.json`

## Data Structure

### Dockets
- Case identifiers (docket number, ID)
- Case name
- Court
- Important dates (filed, argued, terminated)
- Panel information
- Nature of suit
- Links to clusters

### Clusters (Opinion Groups)
- Cluster ID (used in CourtListener URLs)
- Case name (fixed version)
- Filing date
- Citations (parallel citations)
- Judge information
- Precedential status
- Syllabus, headnotes, summary
- Links to individual opinions

### Opinions
- Opinion ID
- Opinion type (lead, concurrence, dissent)
- Author information
- **Full text**: `html_with_citations` (preferred) or `plain_text`
- Page count
- Opinions cited (citation network)
- Download URL and local path

## Rate Limiting

- **API Limit**: 5,000 requests/hour for authenticated users
- **Default orchestrator budget**: 4,000 req/hr (shared across all 13 processes)
- **Per-process effective**: ~308 req/hr (4,000 ÷ 13)
- **Built-in buffer**: 1,000-request safety margin below API cap
- **Burst protection**: Token bucket starts with 60 tokens (not full) to prevent spike on launch
- **Process stagger**: 8 seconds between each child process launch
- **Automatic rate limiting**: Token-bucket limiter ensures compliance across processes
- The scraper automatically tracks and logs request rates

### Adjusting Rate Limit

If you want a more conservative budget (e.g., to avoid peak times):

```bash
# Use only 2,400 requests/hour (half quota) — safer but slower
python run_all_years.py --all --rate-budget 2400

# Use full 5,000 requests/hour (riskier, may trigger throttling)
python run_all_years.py --all --rate-budget 5000
```

## Performance & Architecture

### Multiprocess Orchestrator (`run_all_years.py`) — Recommended for Large Runs

**Architecture**:
- **13 processes** (one per year, 2013–2025)
- **10 async workers** per process (up to 130 concurrent connections)
- **Single shared rate limiter** (across-process, ensures API compliance)
- **Per-year checkpoints** (safe parallel execution)

**Performance**:
- ~4-5x speedup vs. sequential (when async module available)
- ~4,000 req/hr total rate limit (shared across all processes)
- Each process gets ~308 req/hr effective quota
- Test run (1 case/court/year): ~26 cases in <1 minute
- Full run (all cases, 2013-2025): hours to days depending on data volume

**When to use**:
- Full production runs across multiple years
- Need to maximize throughput
- Can tolerate multiprocess overhead

### Direct Scraper (`federal_appellate_scraper.py`) — Simple Alternative

**Architecture**:
- Single process, sequential court processing
- Optional async mode (requires `aiohttp`)
- Simple checkpoint system

**Performance**:
- Slower than multiprocess for large runs
- Good for testing, specific courts, or small date ranges

**When to use**:
- Testing a single court or date range
- Simple, single-process operation preferred
- Debugging or development

## Resume Capability

### Multiprocess Orchestrator (`run_all_years.py`)

Each year-process maintains its own checkpoint file (`checkpoint_2024.json`, `checkpoint_2025.json`, etc.). This allows:

1. **Parallel safety**: Multiple year-processes don't interfere with each other's progress
2. **Resuming after interruption**: Simply re-run the same command; already-completed courts will be skipped
3. **Granular tracking**: See which courts are done per year

```bash
# Start a run (will be interrupted)
python run_all_years.py --all

# Later, resume: already-completed courts are skipped
python run_all_years.py --all

# Clean restart: delete checkpoints
rm downloads/checkpoint_*.json
python run_all_years.py --all
```

### Direct Scraper (`federal_appellate_scraper.py`)

The scraper saves a single checkpoint file (`checkpoint.json`). To resume:

1. The checkpoint file tracks completed courts
2. Simply run the same command again
3. Already-completed courts will be skipped

To start fresh, delete `downloads/checkpoint.json`.

## Error Handling

- **Automatic retries**: Failed requests retry with exponential backoff
- **Rate limit handling**: Built-in 429 status code handling
- **Timeout protection**: 30-second timeout per request
- **Detailed logging**: All errors logged to file and console

## Maintenance Windows

CourtListener has maintenance windows:
- **Weekly**: Thursday 21:00-23:59 PT
- Avoid scheduling cron jobs during this time

## Advanced Usage

### Multiprocess Orchestrator Options

```bash
# Monitor status updates every 10 seconds (default: 30)
python run_all_years.py --all --status-interval 10

# Use different API token
python run_all_years.py --all --api-token YOUR_TOKEN

# Save logs to custom directory
python run_all_years.py --all --log-dir my_logs

# Save downloads to custom directory
python run_all_years.py --all --output-dir my_downloads
```

**Live monitoring**: Follow a single year's progress in real-time:
```bash
# Terminal 1: start the orchestrator
python run_all_years.py --all

# Terminal 2: watch year 2020's log
tail -f logs/scrape_2020.log
```

### Incremental Updates

Track the most recent `date_modified` and use it for incremental updates:

```bash
# First run - get everything
python federal_appellate_scraper.py --api-token TOKEN --mode full --start-date 2024-01-01

# Later runs - only get modified records
python federal_appellate_scraper.py --api-token TOKEN --mode full --start-date 2024-12-01
```

### Custom Rate Limiting

Adjust rate limiting based on your needs:

```bash
# Slower (safer): 1 second between requests
python federal_appellate_scraper.py --api-token TOKEN --rate-limit 1.0

# Faster (riskier): 0.5 seconds between requests
python federal_appellate_scraper.py --api-token TOKEN --rate-limit 0.5
```

## Exporting to CSV

The scraper saves data as JSON by default. To convert to CSV:

```python
# Example conversion script
from federal_appellate_scraper import FederalAppellateScraper
from pathlib import Path

scraper = FederalAppellateScraper(api_token="YOUR_TOKEN")

# Find all docket JSON files
json_files = list(Path("downloads").rglob("docket.json"))

# Export to CSV
scraper.export_to_csv(json_files, Path("downloads/all_dockets.csv"))
```

## Troubleshooting

### "401 Unauthorized"
- Check your API token
- Ensure token is properly formatted in header

### "429 Too Many Requests"
- Increase `--rate-limit` value
- Check your usage at https://www.courtlistener.com/profile/api/

### Script hangs or runs slowly
- Check your internet connection
- Verify CourtListener API is operational
- Review logs for specific errors

### Missing data
- Some courts have limited historical data
- Check CourtListener coverage: https://www.courtlistener.com/help/coverage/opinions/

## API Documentation

For complete API documentation:
- General API docs: See `courtlistener_api_rest_general.html`
- Case law API docs: See `courtlistener_api_case_law.html`
- API analysis: See `API_ANALYSIS.md`
- Online docs: https://www.courtlistener.com/help/api/rest/

## Contributing

This is a Free Law Project initiative. To contribute:
1. Report issues or suggestions
2. Submit pull requests
3. Consider donating to Free Law Project: https://donate.free.law/forms/membership

## License

The data from CourtListener is free of known copyright restrictions (Public Domain).

This scraper code is provided as-is for educational and research purposes.

## Support

- CourtListener GitHub: https://github.com/freelawproject/courtlistener
- Discussions: https://github.com/freelawproject/courtlistener/discussions
- Contact: https://www.courtlistener.com/contact/

## Citation

If you use this data in research, please cite:

```
Free Law Project. (2025). CourtListener Federal Appellate Case Law Database. 
Retrieved from https://www.courtlistener.com/
```

---

**Maintained by**: Free Law Project Community  
**Last Updated**: February 2026  
**API Version**: v4.3
