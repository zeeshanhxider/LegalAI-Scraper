# Federal Appellate Scraper - Data Format & Complete Field Coverage

## 📦 DATA STORAGE FORMAT

### Primary Format: JSON
All data is stored as **JSON files** with the following characteristics:

```json
{
  "key": "value",
  "nested": {
    "field": "data"
  },
  "array": [1, 2, 3]
}
```

- **Pretty-printed** with 2-space indentation for human readability
- **UTF-8 encoding**
- **Valid JSON** parseable by any JSON library

### File Naming Convention

```
downloads/
├── federal_appellate_courts.json              # Court information (one-time)
├── dockets_{court_id}_{timestamp}.json        # Docket batches
├── clusters_{court_id}_{timestamp}.json       # Cluster batches  
├── opinions_{court_id}_{timestamp}.json       # Opinion batches
└── scraper_checkpoint.json                    # Progress tracking
```

**Timestamp Format**: `YYYYMMDD_HHMMSS` (e.g., `20260220_143052`)

**Court ID Examples**:
- `ca1`, `ca2`, ... `ca11` (Circuit courts)
- `cadc` (DC Circuit)
- `cafc` (Federal Circuit)
- `all` (when scraping all courts together)

### Batch Processing

Data is saved in **batches** to manage memory:
- **Dockets**: Every 100 records
- **Clusters**: Every 100 records
- **Opinions**: Every 50 records (smaller due to large text content)

This means you'll get multiple timestamped files per court if scraping large datasets.

### Example File Contents

**dockets_ca9_20260220_143052.json**:
```json
[
  {
    "id": 123456,
    "docket_number": "23-1234",
    "case_name": "Smith v. Jones",
    "court": "https://www.courtlistener.com/api/rest/v4/courts/ca9/",
    "court_id": "ca9",
    "date_filed": "2023-06-15",
    "date_argued": "2023-12-10",
    "date_terminated": null,
    "nature_of_suit": "Civil Rights",
    "jurisdiction_type": "Federal Question",
    "panel": [...],
    "clusters": [...],
    ... (ALL other available fields)
  },
  {
    "id": 123457,
    ... (next docket)
  }
]
```

---

## ✅ COMPLETE FIELD COVERAGE - NOW ENABLED

### 🔧 What Changed

**BEFORE** (Limited fields):
```python
params = {
    'fields': 'id,docket_number,case_name,date_filed,...'  # Only 13-14 fields
}
```

**NOW** (All fields):
```python
params = {
    'order_by': '-date_filed,id'  # NO field filter = ALL fields
}
```

### 📊 Fields Now Captured

#### DOCKETS: ~60 FIELDS
✅ **ALL available docket metadata** including:

**Identifiers** (7 fields):
- id, resource_uri, absolute_url, docket_number, docket_number_core, pacer_case_id, slug

**Case Names** (4 fields):
- case_name, case_name_short, case_name_full

**Court & Jurisdiction** (4 fields):
- court, court_id, original_court_info, jurisdiction_type

**ALL Dates** (14 fields):
- date_created, date_modified, date_filed, date_terminated, date_last_filing
- date_argued, date_reargued, date_reargument_denied
- date_cert_granted, date_cert_denied
- date_last_index, date_blocked, ia_date_first_change

**Case Classification** (5 fields):
- source, nature_of_suit, cause, jury_demand, mdl_status

**Appeal Information** (3 fields):
- appeal_from, appeal_from_str, appellate_fee_status, appellate_case_type_information

**Judges** (6 fields):
- assigned_to, assigned_to_str, referred_to, referred_to_str, panel, panel_str

**Related Objects** (6 fields):
- clusters, audio_files, tags, parties, docket_entries, idb_data

**PACER/Archive** (7 fields):
- filepath_ia, filepath_ia_json, ia_upload_failure_count, ia_needs_upload
- blocked, bankruptcy_information

**Plus**: Any other fields the API provides (future-proof!)

---

#### CLUSTERS: ~40 FIELDS
✅ **ALL available cluster metadata** including:

**Identifiers** (4 fields):
- id, resource_uri, absolute_url, slug

**Case Names** (3 fields):
- case_name, case_name_short, case_name_full

**Relationships** (2 fields):
- docket (URL), docket_id

**Dates** (3 fields):
- date_created, date_modified, date_filed, date_filed_is_approximate

**Citations** (1 field, array of objects):
- citations: [
    {volume, reporter, page, type, cite},
    {volume, reporter, page, type, cite},
    ...
  ]

**Publication** (2 fields):
- precedential_status, slug

**Content** (7 fields):
- syllabus, headnotes, summary, history, other_dates, cross_reference, correction

**Judges** (5 fields):
- judges, panel, panel_ids, non_participating_judges, per_curiam, panel_str, non_participating_judge_str

**Case Info** (4 fields):
- nature_of_suit, posture, attorneys, headmatter

**Opinions** (1 field):
- sub_opinions (array)

**Source** (6 fields):
- source, scdb_id, scdb_decision_direction, scdb_votes_majority, scdb_votes_minority
- filepath_json_harvard

**Plus**: Any other fields the API provides

---

#### OPINIONS: ~30 FIELDS
✅ **ALL available opinion metadata** including:

**Identifiers** (3 fields):
- id, resource_uri, absolute_url

**Relationships** (2 fields):
- cluster (URL), cluster_id

**Type** (1 field):
- type (combined/lead/concurrence/dissent/etc.)

**Authors** (6 fields):
- author (URL), author_id, author_str, per_curiam, joined_by (array), joined_by_ids

**ALL Text Formats** (7 fields):
- html_with_citations ⭐
- plain_text
- html
- html_lawbox
- html_columbia
- html_anon_2020
- xml_harvard

**Document Info** (3 fields):
- page_count, sha1, extracted_by_ocr

**Source** (2 fields):
- download_url, local_path

**Dates** (2 fields):
- date_created, date_modified

**Citation Network** (1 field):
- opinions_cited (array of IDs)

**Sorting** (1 field):
- ordering_key

**Plus**: Any other fields the API provides

---

## 🎯 COMPLETE DATA EXTRACTION

### What This Means

1. **Every field the API returns** is now captured
2. **No data loss** - you get the complete record
3. **Future-proof** - new fields automatically included
4. **Maximum flexibility** - filter/analyze afterwards
5. **Research-ready** - all metadata for any analysis

### File Size Impact

⚠️ **Files will be LARGER**:
- **Dockets**: ~2-5 KB per record (was ~1 KB)
- **Clusters**: ~3-8 KB per record (was ~2 KB)
- **Opinions**: ~20-200 KB per record depending on opinion length (unchanged)

**Estimate**: For 1,000 opinions with full metadata:
- Before: ~100-150 MB
- Now: ~150-300 MB

### Performance Impact

⚠️ **Scraping will be slightly SLOWER**:
- Larger payloads to transfer
- More data to serialize
- But: Same API rate limits apply (0.75s delay between requests)
- Overall: ~10-20% more time due to larger payloads

---

## 📁 SECONDARY FORMAT: CSV EXPORT

### Converting JSON to CSV

The scraper includes a CSV export function:

```python
from federal_appellate_scraper import FederalAppellateScraper, CourtListenerAPI
from pathlib import Path

api = CourtListenerAPI("YOUR_TOKEN")
scraper = FederalAppellateScraper(api)

# Find all JSON files
json_files = list(Path("downloads").glob("dockets_*.json"))

# Export to CSV
scraper.export_to_csv(json_files, Path("downloads/all_dockets.csv"))
```

### CSV Format Characteristics

- **Flattened structure**: Nested objects become `field_subfield` columns
- **Arrays as JSON strings**: Lists stored as JSON strings
- **UTF-8 encoding**
- **All fields as columns**

**Example CSV columns**:
```
id,docket_number,case_name,date_filed,court_id,panel_str,...
123456,"23-1234","Smith v. Jones","2023-06-15","ca9","Judge A, Judge B",...
```

### When to Use CSV vs JSON

**Use JSON when**:
- ✅ Preserving nested data structures
- ✅ Working with APIs or databases
- ✅ Need to maintain data types
- ✅ Complex citation arrays

**Use CSV when**:
- ✅ Excel/spreadsheet analysis
- ✅ SQL database imports
- ✅ Simple statistical analysis
- ✅ Sharing with non-technical users

---

## 🔄 DATA WORKFLOW

### Recommended Workflow

1. **Scrape** → JSON files (complete data)
2. **Store** → Keep JSON as master copy
3. **Process** → Convert to CSV for specific analyses
4. **Archive** → Compress JSON files for long-term storage

### Example: Complete Data Pipeline

```bash
# 1. Scrape everything (JSON)
python federal_appellate_scraper.py \
  --api-token TOKEN \
  --mode full \
  --courts ca9 \
  --start-date 2024-01-01

# Output: downloads/dockets_ca9_*.json
#         downloads/clusters_ca9_*.json
#         downloads/opinions_ca9_*.json

# 2. Combine and export to CSV (if needed)
python -c "
from federal_appellate_scraper import FederalAppellateScraper, CourtListenerAPI
from pathlib import Path
api = CourtListenerAPI('TOKEN')
scraper = FederalAppellateScraper(api)
scraper.export_to_csv(
    list(Path('downloads').glob('dockets_ca9_*.json')),
    Path('downloads/ca9_dockets.csv')
)
"

# 3. Compress original JSON for archival
tar -czf downloads_ca9_20260220.tar.gz downloads/*_ca9_*.json
```

---

## 📊 DATA STRUCTURE EXAMPLES

### JSON Structure: Docket

```json
{
  "id": 4214664,
  "resource_uri": "https://www.courtlistener.com/api/rest/v4/dockets/4214664/",
  "absolute_url": "/docket/4214664/smith-v-jones/",
  "docket_number": "23-1234",
  "docket_number_core": "231234",
  "case_name": "Smith v. Jones",
  "case_name_short": "Smith",
  "case_name_full": "Jane Smith, Appellant v. John Jones, Appellee",
  "court": "https://www.courtlistener.com/api/rest/v4/courts/ca9/",
  "court_id": "ca9",
  "date_filed": "2023-06-15",
  "date_argued": "2023-12-10",
  "date_terminated": null,
  "nature_of_suit": "Civil Rights - Employment",
  "jurisdiction_type": "Federal Question",
  "appeal_from_str": "N.D. Cal.",
  "panel": [
    "https://www.courtlistener.com/api/rest/v4/people/123/",
    "https://www.courtlistener.com/api/rest/v4/people/456/"
  ],
  "panel_str": "Judge Smith, Judge Williams, Judge Davis",
  "clusters": [
    "https://www.courtlistener.com/api/rest/v4/clusters/9876543/"
  ],
  ... (50+ more fields)
}
```

### JSON Structure: Cluster with Citations

```json
{
  "id": 9876543,
  "case_name": "Smith v. Jones",
  "date_filed": "2024-01-15",
  "citations": [
    {
      "volume": "999",
      "reporter": "F.3d",
      "page": "123",
      "type": "federal",
      "cite": "999 F.3d 123"
    },
    {
      "volume": "2024",
      "reporter": "WL",
      "page": "123456",
      "type": "neutral",
      "cite": "2024 WL 123456"
    }
  ],
  "precedential_status": "Published",
  "syllabus": "This case involves...",
  "headnotes": "1. Civil Rights - Employment discrimination...",
  "judges": "Smith, Williams, Davis",
  "panel": [...],
  "sub_opinions": [
    "https://www.courtlistener.com/api/rest/v4/opinions/12345678/"
  ],
  ... (30+ more fields)
}
```

### JSON Structure: Opinion with Full Text

```json
{
  "id": 12345678,
  "type": "020lead",
  "author_str": "Judge Smith",
  "html_with_citations": "<div class=\"opinion\"><p>This appeal concerns...</p><p>We hold that <a href=\"...\">Smith v. Prior Case</a>...</p></div>",
  "plain_text": "This appeal concerns... We hold that Smith v. Prior Case...",
  "page_count": 15,
  "extracted_by_ocr": false,
  "opinions_cited": [11111, 22222, 33333],
  "joined_by": [...],
  ... (20+ more fields)
}
```

---

## 🎓 ACCESSING THE DATA

### Python

```python
import json

# Load dockets
with open('downloads/dockets_ca9_20260220_143052.json', 'r') as f:
    dockets = json.load(f)

# Access fields
for docket in dockets:
    print(f"Case: {docket['case_name']}")
    print(f"Filed: {docket['date_filed']}")
    print(f"Court: {docket['court_id']}")
    print(f"Nature: {docket['nature_of_suit']}")
    print(f"Panel: {docket['panel_str']}")
```

### JavaScript/Node.js

```javascript
const fs = require('fs');

// Load JSON
const dockets = JSON.parse(
  fs.readFileSync('downloads/dockets_ca9_20260220_143052.json', 'utf8')
);

// Access data
dockets.forEach(docket => {
  console.log(`${docket.case_name} (${docket.docket_number})`);
});
```

### Command Line (jq)

```bash
# Get all case names
jq '.[].case_name' downloads/dockets_ca9_*.json

# Filter by date
jq '.[] | select(.date_filed >= "2024-01-01")' downloads/dockets_ca9_*.json

# Count by court
jq -s 'group_by(.court_id) | map({court: .[0].court_id, count: length})' downloads/dockets_*.json
```

---

## ✅ VERIFICATION

To verify you're getting all fields, check the JSON file:

```bash
# Count fields in first docket
jq '.[0] | keys | length' downloads/dockets_ca9_*.json

# List all field names
jq '.[0] | keys' downloads/dockets_ca9_*.json

# Should see 50-60+ fields for dockets
# Should see 35-45+ fields for clusters  
# Should see 25-35+ fields for opinions
```

---

## 🎯 SUMMARY

| Aspect | Details |
|--------|---------|
| **Primary Format** | JSON (pretty-printed, indent=2) |
| **Secondary Format** | CSV (via export_to_csv method) |
| **Field Coverage** | 🟢 **ALL fields** - no restrictions |
| **File Naming** | `{type}_{court}_{timestamp}.json` |
| **Batch Size** | 100 dockets, 100 clusters, 50 opinions |
| **Encoding** | UTF-8 |
| **Total Fields** | ~130-150 per case (60 docket + 40 cluster + 30 opinion) |
| **File Size** | ~2-200 KB per record (depends on opinion length) |
| **Future-Proof** | ✅ New API fields automatically included |

---

**Updated**: February 20, 2026  
**Scraper Version**: 1.0 (Complete Field Coverage)  
**API Version**: CourtListener v4.3
