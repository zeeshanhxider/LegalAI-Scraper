Pipeline Documentation

## Overview

This scraper downloads court opinions (PDF files) from the Washington State Courts website and organizes them with metadata. It's designed to be resilient, resumable, and handle thousands of cases efficiently.

## What We Scrape

### Data Sources

- **Website**: https://www.courts.wa.gov/opinions/
- **Opinion Types**:
  - Supreme Court Opinions
  - Court of Appeals - Published Opinions
  - Court of Appeals - Published in Part
  - Court of Appeals - Unpublished Opinions

### Data Collected

For each case, we scrape:

- **File Date**: When the opinion was filed (e.g., "Jan. 16, 2025")
- **Case Number**: Court case identifier (e.g., "102,586-6")
- **Division**: For Court of Appeals only (I, II, or III)
- **Case Title**: Party names (e.g., "State v. Morgan")
- **File Contains**: Type of opinions included (e.g., "Maj., and Con. Opinions")
- **PDF URL**: Direct link to the PDF file
- **Case Info URL**: Link to the case details page

---

## What to Scrape from Washington Courts Website - Detailed Guide

### Step-by-Step Scraping Process

#### 1. Navigate to Year Page

**URL Pattern**: `https://www.courts.wa.gov/opinions/index.cfm?fa=opinions.byYear&fileYear=YYYY&crtLevel=X&pubStatus=XXX`

**Parameters**:

- `fileYear`: Year to scrape (e.g., 2025)
- `crtLevel`:
  - `S` = Supreme Court
  - `A` = Court of Appeals
- `pubStatus`:
  - `PUB` = Published Opinions
  - `PAR` = Published in Part
  - `UNP` = Unpublished

**Example**: For Supreme Court 2025:

```
https://www.courts.wa.gov/opinions/index.cfm?fa=opinions.byYear&fileYear=2025&crtLevel=S&pubStatus=PUB
```

#### 2. Extract Data from HTML Tables

Each year page contains HTML tables with case information. Parse each table row to extract:

| Field Position | HTML Location            | Field Name                  | Example Value                                  | Notes                             |
| -------------- | ------------------------ | --------------------------- | ---------------------------------------------- | --------------------------------- |
| Column 1       | `<td>` cell 0            | **File Date**               | "Jan. 16, 2025"                                | Date opinion was filed            |
| Column 2       | `<td>` cell 1 with `<a>` | **Case Number**             | "102,586-6"                                    | Get from link text                |
| Column 2       | `<a href="...">`         | **Case Info URL**           | `?fa=opinions.showOpinion&filename=1025866MAJ` | Get href attribute, make absolute |
| Column 3       | `<td>` cell 2            | **Division** (Appeals only) | "I", "II", or "III"                            | Empty for Supreme Court           |
| Column 3/4     | `<td>` cell 2 or 3       | **Case Title**              | "State v. Morgan"                              | Party names                       |
| Column 4/5     | `<td>` cell 3 or 4       | **File Contains**           | "Maj., and Con. Opinions"                      | Types of opinions in PDF          |

**Note**: Court of Appeals has 5 columns (includes Division), Supreme Court has 4 columns.

#### 3. Extract PDF URL

From the Case Info URL, extract the filename parameter and construct PDF URL:

**Case Info URL**:

```
https://www.courts.wa.gov/opinions/index.cfm?fa=opinions.showOpinion&filename=1025866MAJ
```

**Extract filename**: `1025866MAJ`

**Remove suffix** (MAJ, CON, DIS, etc.): `1025866`

**Construct PDF URL**:

```
https://www.courts.wa.gov/opinions/pdf/1025866.pdf
```

**PDF URL Pattern**: `https://www.courts.wa.gov/opinions/pdf/[case_number_no_suffix].pdf`

#### 4. Download the PDF

- Download the PDF from the constructed PDF URL
- Save with filename: `[case_number]_[case_title].pdf`
- Example: `102,586-6_Pub. Util. Dist. No. 1 of Snohomish County v. Stat.pdf`
- Store in folder: `Opinion_Type/Year/Month/`

### Complete Metadata Fields to Capture

After scraping each case, save these 14 fields to CSV:

| Field                  | Source                        | Example                         | Description                                |
| ---------------------- | ----------------------------- | ------------------------------- | ------------------------------------------ |
| **opinion_type**       | Configuration                 | "Supreme Court"                 | Type of court (fixed per scraper run)      |
| **publication_status** | Configuration                 | "Published"                     | Publication status (fixed per scraper run) |
| **year**               | Extracted from file_date      | "2025"                          | Year parsed from date                      |
| **month**              | Extracted from file_date      | "January"                       | Month name parsed from date                |
| **file_date**          | Table column 1                | "Jan. 16, 2025"                 | Raw date string from website               |
| **case_number**        | Table column 2 (link text)    | "102,586-6"                     | Case identifier with commas                |
| **division**           | Table column 3 (Appeals only) | "I" or ""                       | Division for Appeals, empty for Supreme    |
| **case_title**         | Table column 3 or 4           | "State v. Morgan"               | Party names                                |
| **file_contains**      | Table column 4 or 5           | "Majority Opinion"              | Types of opinions in PDF                   |
| **case_info_url**      | Table column 2 (href)         | Full URL to case details        | Link to opinion details page               |
| **pdf_url**            | Constructed from filename     | Full URL to PDF                 | Direct download link                       |
| **pdf_filename**       | Generated                     | "102,586-6_State v. Morgan.pdf" | Local filename                             |
| **download_status**    | After download attempt        | "Success" or error              | Whether PDF downloaded successfully        |
| **scraped_at**         | Current timestamp             | "2025-12-05T11:05:07.856494"    | ISO format timestamp                       |

### Example Scraped Record

```csv
opinion_type,publication_status,year,month,file_date,case_number,division,case_title,file_contains,case_info_url,pdf_url,pdf_filename,download_status,scraped_at
Supreme Court,Published,2025,January,"Jan. 16, 2025","102,586-6",,Pub. Util. Dist. No. 1 of Snohomish County v. State,Majority Opinion,https://www.courts.wa.gov/opinions/index.cfm?fa=opinions.showOpinion&filename=1025866MAJ,https://www.courts.wa.gov/opinions/pdf/1025866.pdf,"102,586-6_Pub. Util. Dist. No. 1 of Snohomish County v. Stat.pdf",Success,2025-12-05T11:05:07.856494
```

### Important Implementation Details

1. **URL Construction**: Always use absolute URLs by joining with `BASE_URL`
2. **Filename Sanitization**: Remove invalid characters from case titles for filenames
3. **Month Extraction**: Parse month name from date string (e.g., "Jan." → "January")
4. **Division Handling**: Check if cell contains "I", "II", or "III" to determine table structure
5. **Error Handling**: If PDF download fails, record error in `download_status` but continue
6. **Checkpoint Progress**: Save checkpoint after each successful case to enable resume

## How the Pipeline Works

### 1. Discovery Phase

- Fetches the main opinions page to discover available years
- For each year, constructs a URL with parameters:
  - `crtLevel`: Court level (S = Supreme Court, A = Appeals)
  - `pubStatus`: Publication status (PUB, PAR, UNP)
  - `fileYear`: The year to scrape

### 2. Scraping Phase

- Parses HTML tables containing case information
- Extracts metadata from each table row (date, case number, title, etc.)
- Downloads PDF files from discovered URLs
- Uses polite delays (1-2 seconds) between requests
- Implements retry logic with exponential backoff for failed requests

### 3. Storage Phase

- **PDF Files**: Saved to organized folder structure (see below)
- **Metadata**: Appended to CSV file in real-time as cases are processed
- **Checkpoint**: JSON file tracks progress for resume capability

### 4. Resume Capability

- If interrupted (Ctrl+C, network issue, etc.), the scraper can resume
- Checkpoint file (`scraper_checkpoint.json`) stores last processed year/month/case
- Skips already downloaded PDFs automatically

## Folder Structure

```
downloads/
├── Supreme_Court_Opinions/
│   ├── metadata.csv                    # All case metadata
│   ├── scraper_checkpoint.json         # Resume progress
│   ├── 2013/
│   │   ├── January/
│   │   │   └── [case_number]_[title].pdf
│   │   ├── February/
│   │   └── ...
│   ├── 2014/
│   └── ...
├── Court_of_Appeals_Published/
│   ├── metadata.csv
│   ├── scraper_checkpoint.json
│   ├── 2012/
│   └── ...
├── Court_of_Appeals_Published_in_Part/
│   └── ...
└── Court_of_Appeals_Unpublished/
    └── ...
```

**Key Points:**

- Each opinion type has its own root folder
- PDFs organized by Year → Month → PDF files
- Each folder has its own `metadata.csv` with all cases for that opinion type
- PDF filenames: `[case_number]_[case_title].pdf`

## Metadata CSV Structure

The `metadata.csv` file contains these columns:

| Column             | Description                                            |
| ------------------ | ------------------------------------------------------ |
| opinion_type       | Type of court (e.g., "Supreme Court")                  |
| publication_status | Publication status (Published, Published in Part, etc) |
| year               | Year extracted from file date                          |
| month              | Month name (e.g., "January")                           |
| file_date          | Original date string (e.g., "Jan. 16, 2025")           |
| case_number        | Court case number                                      |
| division           | Court of Appeals division (I/II/III) or empty          |
| case_title         | Party names                                            |
| file_contains      | Types of opinions in the PDF                           |
| case_info_url      | URL to case details page                               |
| pdf_url            | Direct URL to PDF                                      |
| pdf_filename       | Local filename of saved PDF                            |
| download_status    | "Success" or error message                             |
| scraped_at         | ISO timestamp when scraped                             |

## Running the Scraper

```bash
# Scrape all opinion types for all available years
python run_scraper.py --type all

# Scrape only 2025 data for a specific opinion type
python run_scraper.py --type supreme_court --years 2025

# If interrupted, just re-run the same command - it will resume automatically
```

## Adapting for Other States (2025 Only)

To adapt this for other states:

1. **Update URLs** in `config.py`:
   - Change `BASE_URL` to the target state's court website
   - Update `OPINIONS_URL` to match their opinions page structure

2. **Adjust HTML Parsing** in `scraper.py`:
   - Modify `get_cases_for_year()` to match the target site's HTML structure
   - Update selectors for tables, rows, and cells based on their layout

3. **Configure Opinion Types** in `config.py`:
   - Update `OPINION_TYPES` dictionary with relevant court levels for that state
   - Adjust URL parameters to match their query string format

4. **Limit to 2025** in `run_scraper.py`:
   - Use `--years 2025` flag or modify code to only fetch 2025 cases

5. **Test First**:
   - Use `--test` flag to scrape only most recent data
   - Verify folder structure and metadata format before full run

## Key Features for Production Use

- **Resilient**: Automatic retries with exponential backoff
- **Resumable**: Can stop/start without losing progress
- **Organized**: Clean folder structure with comprehensive metadata
- **Polite**: Respects rate limits with configurable delays
- **Logging**: Detailed logs to `scraper.log` for debugging
