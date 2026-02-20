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

### 2. Test with a single court

```bash
# Scrape recent cases from the Ninth Circuit (limited to 100 dockets)
python federal_appellate_scraper.py \
  --api-token YOUR_TOKEN \
  --mode full \
  --courts ca9 \
  --start-date 2024-01-01 \
  --max-per-court 100
```

### 3. Full scrape of all federal appellate courts

```bash
# This will take hours/days depending on date range
python federal_appellate_scraper.py \
  --api-token YOUR_TOKEN \
  --mode full \
  --start-date 2020-01-01 \
  --end-date 2024-12-31
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

```
downloads/
├── federal_appellate_courts.json          # Court information
├── dockets_ca1_20250220_123456.json      # Docket batches per court
├── dockets_ca2_20250220_123500.json
├── clusters_ca1_20250220_123600.json     # Cluster batches per court
├── opinions_ca1_20250220_123700.json     # Opinion batches per court
├── scraper_20250220_120000.log           # Detailed log file
└── scraper_checkpoint.json                # Progress checkpoint
```

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

- **Default**: 0.75 seconds between requests = ~4,800 requests/hour
- **API Limit**: 5,000 requests/hour for authenticated users
- **Buffer**: Built-in 200-request buffer for safety
- The scraper automatically tracks and logs request rates

## Performance Tips

1. **Use date ranges**: Narrow your date range for faster completion
2. **Target specific courts**: Focus on courts of interest
3. **Use max-per-court**: Limit results for testing
4. **Run during off-peak hours**: Better API performance
5. **Monitor logs**: Check progress and identify issues

## Resume Capability

The scraper saves checkpoints automatically. If interrupted:

1. The checkpoint file tracks completed courts
2. Simply run the same command again
3. Already-completed courts will be skipped
4. Scraping continues from where it stopped

To start fresh, delete `scraper_checkpoint.json`.

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
from federal_appellate_scraper import FederalAppellateScraper, CourtListenerAPI
from pathlib import Path

api = CourtListenerAPI("YOUR_TOKEN")
scraper = FederalAppellateScraper(api)

# Find all docket JSON files
json_files = list(Path("downloads").glob("dockets_*.json"))

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
