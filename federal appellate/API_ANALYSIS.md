# CourtListener API Analysis for Federal Appellate Scraper

## Overview
This document summarizes the CourtListener REST API (v4.3) for building a production-grade scraper to collect federal appellate case law and metadata.

## Authentication
- **Method**: Token-based authentication
- **Header Format**: `Authorization: Token <your-token-here>`
- **Alternative Methods**: Cookie/Session Auth, HTTP Basic Auth
- **Rate Limits**: 5,000 queries per hour for authenticated users
- **Note**: Must authenticate to avoid anonymous user throttling

## Data Model Architecture

The CourtListener case law data follows this hierarchical structure:

```
Courts → Dockets → Clusters → Opinions
```

### 1. Courts (`/api/rest/v4/courts/`)
- Contains information about federal and state courts
- Fields: name, abbreviation, jurisdiction, court level, founding date
- **Key Field**: `id` (court abbreviation like "ca1", "ca2", "scotus")
- Can be cached (rarely changes)

### 2. Dockets (`/api/rest/v4/dockets/`)
- Top level of case hierarchy
- **Key Fields**:
  - `id`: Unique docket identifier
  - `court`: Foreign key to court
  - `docket_number`: Case number (not unique across courts)
  - `case_name`: Current case name (can change over time)
  - `date_filed`: Date case was filed
  - `date_terminated`: Date case was terminated
  - `date_argued`: Date case was argued
  - `date_cert_granted/denied`: Cert petition dates
  - `clusters`: List of opinion cluster URLs
  - `appeal_from`: Court case was appealed from
  - `panel`: List of judges on the panel

### 3. Clusters (`/api/rest/v4/clusters/`)
- Groups related opinions from same case (majority, dissent, concurrence)
- **Key Fields**:
  - `id`: Cluster ID (used in CourtListener URLs)
  - `docket`: Foreign key to docket
  - `case_name`: Case name (fixed, doesn't change like docket name)
  - `sub_opinions`: List of opinion URLs
  - `date_filed`: Filing date
  - `citations`: List of parallel citations (e.g., official, West, etc.)
  - `judges`: String of judge names
  - `panel`: List of judge records
  - `nature_of_suit`: Type of case
  - `precedential_status`: Published, Unpublished, etc.
  - `syllabus`: Case syllabus
  - `headnotes`: Legal headnotes
  - `summary`: Case summary

### 4. Opinions (`/api/rest/v4/opinions/`)
- Individual opinion text and metadata
- **Key Fields**:
  - `id`: Opinion ID
  - `cluster`: Foreign key to cluster
  - `type`: Opinion type (lead, concurrence, dissent, etc.)
  - `author`: Judge who wrote the opinion
  - `joined_by`: Judges who joined
  - `html_with_citations`: **BEST FIELD** - HTML text with citations linked
  - `plain_text`: Plain text version
  - `html`, `html_columbia`, `html_lawbox`, `xml_harvard`: Source-specific formats
  - `download_url`: Original source URL
  - `local_path`: Path to original binary file
  - `opinions_cited`: List of opinions cited by this opinion
  - `extracted_by_ocr`: Whether OCR was used
  - `page_count`: Number of pages

## Critical Filtering for Federal Appellate Cases

### Jurisdiction Codes
- `F` = Federal jurisdiction
- `FD` = Federal District
- `FB` = Federal Bankruptcy  
- `FS` = Federal Special
- `FDC` = Federal District Columbia
- `S` = State

### Court Level Codes
- `A` = Appellate
- `T` = Trial
- `S` = Supreme

### Filter Examples for Federal Appellate Courts

**Get all federal appellate dockets:**
```
/api/rest/v4/dockets/?court__jurisdiction=F
```

**Filter by specific circuit:**
```
/api/rest/v4/dockets/?court=ca1           # First Circuit
/api/rest/v4/dockets/?court=ca2           # Second Circuit
...
/api/rest/v4/dockets/?court=ca9           # Ninth Circuit
/api/rest/v4/dockets/?court=cadc          # DC Circuit
/api/rest/v4/dockets/?court=cafc          # Federal Circuit
```

**Get opinions from federal appellate courts:**
```
/api/rest/v4/opinions/?cluster__docket__court__jurisdiction=F
```

**Date range filtering (ISO-8601 format):**
```
/api/rest/v4/dockets/?date_filed__gte=2024-01-01&date_filed__lte=2024-12-31
```

**Combined filters:**
```
/api/rest/v4/dockets/?court__jurisdiction=F&date_filed__gte=2024-01-01&order_by=-date_filed
```

## Django-Style Field Lookups

Use double underscores (`__`) to traverse relationships and apply lookups:

### Lookup Types
- `exact`: Exact match (default)
- `iexact`: Case-insensitive exact match
- `contains`: Contains substring
- `icontains`: Case-insensitive contains
- `startswith` / `istartswith`: Starts with
- `endswith` / `iendswith`: Ends with
- `gt` / `gte`: Greater than / greater than or equal
- `lt` / `lte`: Less than / less than or equal
- `range`: Within range (inclusive)
- `in`: In list of values
- `isnull`: Is null (true/false)

### Exclusion Filters
Prepend `!` to exclude: `?court__jurisdiction!=F` (non-federal cases)

### Related Filters
Join across APIs: `?cluster__docket__court=ca9`

## Pagination

### Standard Pagination (Limited to 100 pages)
```
?page=1
?page=2
```

### Deep Pagination (Cursor-based)
When ordering by `id`, `date_created`, or `date_modified`:
- Use `next` and `previous` URLs from response
- `page` parameter won't work
- Supports unlimited pagination

**Example:**
```python
next_url = response['next']
# https://www.courtlistener.com/api/rest/v4/dockets/?cursor=cD0xMjM0NTY%3D%3D
```

## Ordering

Use `order_by` parameter with field names:
- Ascending: `?order_by=date_filed`
- Descending: `?order_by=-date_filed`
- Multiple fields: `?order_by=-date_filed,id`

**Important**: Always include a tie-breaker field (like `id`) for deterministic ordering.

## Field Selection (Critical for Performance)

### Select specific fields only
```
?fields=id,case_name,date_filed,docket_number
```

### Select nested fields
```
?fields=clusters__id,clusters__date_filed
```

### Omit fields
```
?omit=html_with_citations    # Skip large text fields when not needed
```

**Performance Tip**: Always use field selection to minimize payload size and improve serialization speed.

## Counting

Get total count without fetching data:
```
?count=on
```

Returns: `{"count": 12345}`

Standard responses include count URL:
```json
{
  "count": "https://.../?count=on",
  "next": "...",
  "results": [...]
}
```

## Rate Limiting & Performance

### Rate Limits
- 5,000 queries/hour for authenticated users
- Multiple accounts per project forbidden
- Check usage at: https://www.courtlistener.com/profile/api/#usage

### Performance Best Practices
1. **Use field selection** to limit payload size
2. **Avoid `__id` joins**: Use `court=xyz` not `court__id=xyz`
3. **Precise queries**: Filter to smallest result set first
4. **Cache court data**: Courts don't change often
5. **Batch processing**: Process in chunks with delays
6. **Use cursor pagination**: For large result sets

## Serialization Formats

- **JSON** (default): `Accept: application/json`
- **XML**: `Accept: application/xml`
- **HTML**: `Accept: text/html` (for browsing)

## OPTIONS Requests

Get metadata about any endpoint:
```bash
curl -X OPTIONS \
  --header 'Authorization: Token <token>' \
  "https://www.courtlistener.com/api/rest/v4/dockets/"
```

Returns:
- Available filters
- Ordering fields
- Field descriptions
- Filter lookup types

## Federal Appellate Court IDs

### Circuit Courts
- `ca1` - First Circuit
- `ca2` - Second Circuit
- `ca3` - Third Circuit
- `ca4` - Fourth Circuit
- `ca5` - Fifth Circuit
- `ca6` - Sixth Circuit
- `ca7` - Seventh Circuit
- `ca8` - Eighth Circuit
- `ca9` - Ninth Circuit
- `ca10` - Tenth Circuit
- `ca11` - Eleventh Circuit
- `cadc` - DC Circuit
- `cafc` - Federal Circuit

### Supreme Court
- `scotus` - Supreme Court of the United States

## Example Scraping Workflow

### 1. Get Federal Appellate Courts
```python
GET /api/rest/v4/courts/?jurisdiction=F
# Cache the results
```

### 2. Get Recent Dockets
```python
GET /api/rest/v4/dockets/
  ?court__jurisdiction=F
  &date_filed__gte=2024-01-01
  &order_by=-date_filed,id
  &fields=id,docket_number,case_name,date_filed,court,clusters
```

### 3. Get Clusters for Docket
```python
# Follow cluster URLs from docket response
GET /api/rest/v4/clusters/{cluster_id}/
  ?fields=id,case_name,date_filed,citations,sub_opinions,precedential_status
```

### 4. Get Opinions
```python
# Follow sub_opinions URLs from cluster
GET /api/rest/v4/opinions/{opinion_id}/
  ?fields=id,type,author,html_with_citations,page_count
```

## Metadata to Capture

### Essential Fields
- Case identifiers (docket number, cluster ID, opinion ID)
- Case name
- Court
- Dates (filed, argued, decided)
- Judge information (panel, author)
- Citations
- Precedential status
- Opinion text (html_with_citations preferred)

### Extended Metadata
- Nature of suit
- Syllabus and headnotes
- Parties and attorneys (from PACER endpoints)
- Appeal information
- Citation network (opinions cited)
- Page count
- Source information

## Error Handling

- **429 Too Many Requests**: Rate limit exceeded
- **404 Not Found**: Resource doesn't exist
- **401 Unauthorized**: Authentication failed
- **500 Server Error**: Retry with exponential backoff

## Maintenance Windows

- **Weekly**: Thursday 21:00-23:59 PT
- **Bulk tasks**: Check public calendar
- Schedule cron jobs to avoid these windows

## Data Freshness

- New opinions added daily
- PACER dockets updated continuously
- Use `date_modified` field to track updates
- Consider incremental updates vs. full refresh

## Additional Resources

- API Base URL: https://www.courtlistener.com/api/rest/v4/
- Documentation: https://www.courtlistener.com/help/api/rest/
- GitHub: https://github.com/freelawproject/courtlistener
- Support: https://github.com/freelawproject/courtlistener/discussions
- Coverage Info: https://www.courtlistener.com/help/coverage/opinions/

## Copyright & Terms

- Data is free of known copyright restrictions (Public Domain)
- Sponsored by Free Law Project (non-profit)
- Consider donating to support the service
