# Complete Metadata Available for Federal Appellate Cases

This document provides a comprehensive catalog of all metadata fields available through the CourtListener API for federal appellate cases.

---

## 1. COURT METADATA (`/api/rest/v4/courts/{court_id}/`)

### Basic Information
- **`id`**: Court identifier (e.g., "ca1", "ca9", "cadc")
- **`full_name`**: Complete court name (e.g., "United States Court of Appeals for the Ninth Circuit")
- **`short_name`**: Abbreviated name
- **`citation_string`**: How to cite this court (e.g., "9th Cir.")

### Classification
- **`jurisdiction`**: Court jurisdiction code
  - `F` = Federal
  - `FD` = Federal District
  - `FB` = Federal Bankruptcy
  - `S` = State
- **`position`**: Numerical position for sorting
- **`in_use`**: Whether court is currently active
- **`has_opinion_scraper`**: Whether automated scraping is configured
- **`has_oral_argument_scraper`**: Oral argument availability

### Location
- **`url`**: Court's official website
- **`start_date`**: When court was established
- **`end_date`**: When court ceased operation (if applicable)

---

## 2. DOCKET METADATA (`/api/rest/v4/dockets/{docket_id}/`)

### Core Identifiers
- **`id`**: Unique docket ID (integer)
- **`resource_uri`**: API URL for this docket
- **`absolute_url`**: CourtListener page URL
- **`docket_number`**: Official case number (e.g., "23-1234")
- **`docket_number_core`**: Normalized docket number
- **`pacer_case_id`**: PACER identifier (if from PACER)

### Case Names
- **`case_name`**: Current case name (can change)
- **`case_name_short`**: Abbreviated case name
- **`case_name_full`**: Complete case name with all parties
- **`slug`**: URL-friendly case name

### Court & Jurisdiction
- **`court`**: Court API URL
- **`court_id`**: Court identifier (e.g., "ca9")
- **`original_court_info`**: Original court information (text)
- **`jurisdiction_type`**: Type of jurisdiction
  - "Federal Question"
  - "Diversity of Citizenship"
  - "U.S. Government Defendant"
  - "U.S. Government Plaintiff"

### Important Dates
- **`date_created`**: When record was created in database
- **`date_modified`**: When record was last updated
- **`date_filed`**: Date case was filed
- **`date_terminated`**: Date case was terminated/closed
- **`date_last_filing`**: Date of most recent filing
- **`date_argued`**: Date of oral arguments
- **`date_reargued`**: Date of re-argument (if any)
- **`date_reargument_denied`**: Date re-argument was denied
- **`date_cert_granted`**: Date certiorari was granted
- **`date_cert_denied`**: Date certiorari was denied
- **`date_last_index`**: Last time indexed for search
- **`date_blocked`**: Date case was blocked (if applicable)

### Case Classification
- **`source`**: Data source identifier (integer code)
- **`nature_of_suit`**: Legal category of case
  - "Civil Rights"
  - "Contract"
  - "Copyright"
  - "Labor"
  - "Administrative Law"
  - "Other Statutory Actions"
  - Many more...
- **`cause`**: Legal basis for suit (e.g., "28:1331 Federal Question")
- **`jury_demand`**: Jury demand status
  - "None"
  - "Plaintiff"
  - "Defendant"
  - "Both"

### Appeal Information
- **`appeal_from`**: Court case was appealed from (API URL)
- **`appeal_from_str`**: Appeal source as text
- **`appellate_fee_status`**: Fee status for appellate cases
- **`appellate_case_type_information`**: Type of appellate case

### Judge Information
- **`assigned_to`**: Judge assigned to case (API URL)
- **`assigned_to_str`**: Judge name as string
- **`referred_to`**: Judge case was referred to (API URL)
- **`referred_to_str`**: Referred judge name as string
- **`panel`**: List of panel judge URLs
- **`panel_str`**: Panel judges as comma-separated string

### Related Objects
- **`clusters`**: List of opinion cluster URLs
- **`audio_files`**: List of oral argument URLs
- **`tags`**: List of tag URLs
- **`parties`**: List of party URLs (PACER data)
- **`docket_entries`**: List of docket entry URLs (PACER data)

### MDL & Special Designations
- **`mdl_status`**: Multi-District Litigation status
- **`blocked`**: Whether case is blocked from public access
- **`idb_data`**: Integrated Database data

### PACER/Archive Information
- **`filepath_ia`**: Internet Archive XML file path
- **`filepath_ia_json`**: Internet Archive JSON file path
- **`ia_upload_failure_count`**: Upload failure tracking
- **`ia_needs_upload`**: Whether needs upload to Internet Archive
- **`ia_date_first_change`**: First change date for IA

### Bankruptcy Information (if applicable)
- **`bankruptcy_information`**: Nested bankruptcy data
  - Chapter
  - Trustee information
  - Debtor details

---

## 3. CLUSTER METADATA (`/api/rest/v4/clusters/{cluster_id}/`)

### Core Identifiers
- **`id`**: Unique cluster ID (used in CourtListener URLs)
- **`resource_uri`**: API URL for this cluster
- **`absolute_url`**: CourtListener page URL
- **`panel_str`**: Panel members as string
- **`non_participating_judge_str`**: Non-participating judges

### Case Names (Fixed - Don't Change)
- **`case_name`**: Official case name
- **`case_name_short`**: Short form
- **`case_name_full`**: Full case name with all parties

### Docket Relationship
- **`docket`**: Docket API URL
- **`docket_id`**: Docket ID (integer)

### Dates
- **`date_created`**: Database creation date
- **`date_modified`**: Last modification date
- **`date_filed`**: Date opinion was filed
- **`date_filed_is_approximate`**: Whether date is approximate

### Citations
- **`citations`**: Array of citation objects
  - **`volume`**: Citation volume number
  - **`reporter`**: Reporter abbreviation (e.g., "F.3d", "F. App'x")
  - **`page`**: Starting page
  - **`type`**: Citation type
    - "federal" (official U.S. reporter)
    - "west" (West Publishing)
    - "neutral" (vendor-neutral)
    - "specialty" (specialized reporters)
  - **`cite`**: Full citation string

### Publication Status
- **`precedential_status`**: Publication/precedential status
  - "Published" - Citable precedent
  - "Unpublished" - Non-precedential
  - "Errata" - Corrections
  - "Separate Opinion"
  - "In-chambers"
  - "Relating-to"
  - "Unknown"
- **`slug`**: URL slug

### Case Content
- **`syllabus`**: Case syllabus (summary of legal issues)
- **`headnotes`**: Legal headnotes
- **`summary`**: Case summary
- **`history`**: Procedural history
- **`other_dates`**: Other significant dates (text)
- **`cross_reference`**: Cross-reference to other cases

### Judges
- **`judges`**: Comma-separated string of judge names
- **`panel`**: Array of judge API URLs
- **`panel_ids`**: Array of judge IDs
- **`non_participating_judges`**: Array of non-participating judge URLs
- **`per_curiam`**: Whether opinion is per curiam

### Case Classification
- **`nature_of_suit`**: Legal nature of case
- **`posture`**: Procedural posture
- **`attorneys`**: Attorney information (text)

### Opinions
- **`sub_opinions`**: Array of opinion API URLs

### Source Information
- **`source`**: Data source code
- **`scdb_id`**: Supreme Court Database ID (SCOTUS cases)
- **`scdb_decision_direction`**: SCDB decision direction
- **`scdb_votes_majority`**: Majority vote count
- **`scdb_votes_minority`**: Minority vote count

### File Information
- **`filepath_json_harvard`**: Harvard CAP JSON file path
- **`correction`**: Any corrections noted
- **`headmatter`**: Court-generated headmatter

---

## 4. OPINION METADATA (`/api/rest/v4/opinions/{opinion_id}/`)

### Core Identifiers
- **`id`**: Unique opinion ID
- **`resource_uri`**: API URL for this opinion
- **`absolute_url`**: CourtListener page URL

### Cluster Relationship
- **`cluster`**: Cluster API URL
- **`cluster_id`**: Cluster ID

### Opinion Type
- **`type`**: Type of opinion (with sorting priority)
  - "010combined" - Combined opinion
  - "020lead" - Lead opinion
  - "030concurrence" - Concurring opinion
  - "040dissent" - Dissenting opinion
  - "050addendum" - Addendum
  - "060remittur" - Remittitur
  - "070rehearing" - Rehearing
  - "080on-the-merits" - On the merits
  - "090on-motion-to-strike" - Motion to strike

### Author Information
- **`author`**: Author judge API URL
- **`author_id`**: Author judge ID
- **`author_str`**: Author name as string
- **`per_curiam`**: Whether opinion is per curiam
- **`joined_by`**: Array of joining judge URLs
- **`joined_by_ids`**: Array of joining judge IDs

### Opinion Text (Multiple Formats)
- **`html_with_citations`**: ⭐ **BEST FIELD** - HTML with hyperlinked citations
- **`plain_text`**: Plain text version
- **`html`**: Generic HTML (from court websites)
- **`html_lawbox`**: HTML from Lawbox donation
- **`html_columbia`**: HTML from Columbia collaboration
- **`html_anon_2020`**: HTML from anonymous 2020 source
- **`xml_harvard`**: XML from Harvard CAP (may have OCR errors)

### Document Information
- **`page_count`**: Number of pages
- **`sha1`**: SHA-1 hash of document
- **`extracted_by_ocr`**: Whether extracted via OCR

### Source/Download
- **`download_url`**: Original source URL
- **`local_path`**: Path to local binary file (if available)
  - Format: `pdf/{court}/{year}/{file}.pdf`
  - Access via CourtListener storage

### Dates
- **`date_created`**: Database creation date
- **`date_modified`**: Last modification date

### Citation Network
- **`opinions_cited`**: Array of opinion IDs cited
  - Used to build citation graph
  - Track influence and precedent

### Sorting
- **`ordering_key`**: Order within cluster (Harvard/Columbia only)

---

## 5. JUDGE METADATA (`/api/rest/v4/people/{judge_id}/`)

Available when following judge URLs from dockets/clusters:

### Basic Information
- **`id`**: Unique judge ID
- **`name_first`**: First name
- **`name_middle`**: Middle name/initial
- **`name_last`**: Last name
- **`name_suffix`**: Suffix (Jr., Sr., III, etc.)
- **`date_dob`**: Date of birth
- **`date_dod`**: Date of death
- **`gender`**: Gender

### Career Information
- **`positions`**: Array of judicial position URLs
  - Court served on
  - Date nominated
  - Date confirmed
  - Date started service
  - Date retired/terminated
  - Appointment type
  - Nominating president

### Education
- **`educations`**: Array of education URLs
  - Institution
  - Degree type
  - Degree year

### Political Information
- **`political_affiliations`**: Array of political affiliations
- **`aba_ratings`**: ABA ratings

---

## 6. CITATION NETWORK (`/api/rest/v4/citations/`)

### Citation Relationships
- **`id`**: Citation ID
- **`citing_opinion`**: Opinion that cites (URL)
- **`cited_opinion`**: Opinion being cited (URL)
- **`depth`**: Citation depth (1-5, indicates treatment)
  - 1: Mentioned
  - 2: Discussed
  - 3: Treatment unclear
  - 4: Applied/followed
  - 5: Distinguished/criticized

---

## 7. ADDITIONAL METADATA FROM SEARCH API

When using the search endpoint (`/api/rest/v4/search/`):

### Additional Search Fields
- **`snippet`**: Text snippet showing search match
- **`suitNature`**: Nature of suit
- **`citeCount`**: Number of times cited
- **`status`**: Precedential status
- **`court_id`**: Court identifier
- **`court_citation_string`**: How to cite the court

---

## METADATA SUMMARY BY USE CASE

### For Basic Case Research
✅ Required Fields:
- Docket: `id`, `case_name`, `docket_number`, `court_id`, `date_filed`
- Cluster: `id`, `case_name`, `citations`, `precedential_status`
- Opinion: `id`, `html_with_citations`, `type`

### For Citation Analysis
✅ Required Fields:
- Opinion: `opinions_cited`
- Citation: `citing_opinion`, `cited_opinion`, `depth`
- Cluster: `citations`

### For Judge Research
✅ Required Fields:
- Docket: `panel`, `assigned_to`
- Cluster: `panel`, `judges`
- Opinion: `author`, `joined_by`
- Judge: All biographical fields

### For Temporal Analysis
✅ Required Fields:
- Docket: `date_filed`, `date_argued`, `date_terminated`
- Cluster: `date_filed`
- Opinion: `date_created`, `date_modified`

### For Jurisdictional Analysis
✅ Required Fields:
- Court: `jurisdiction`, `full_name`
- Docket: `jurisdiction_type`, `nature_of_suit`, `appeal_from`

### For Full-Text Analysis
✅ Required Fields:
- Opinion: `html_with_citations` or `plain_text`
- Cluster: `syllabus`, `headnotes`, `summary`
- Opinion: `page_count`

---

## DATA QUALITY NOTES

### Field Completeness Varies By:
1. **Court**: Federal appellate courts have better coverage than some state courts
2. **Date**: Recent cases (2010+) have more complete metadata
3. **Source**: Harvard CAP data has excellent coverage for older cases
4. **Publication Status**: Published opinions have more metadata than unpublished

### Known Gaps:
- Not all opinions have `syllabus` or `headnotes` (court-dependent)
- `panel` information may be missing for older cases
- OCR-extracted text may have errors (`extracted_by_ocr` flag)
- Some `download_url` fields may point to dead links (courts change websites)

### Best Practices:
1. Always check `precedential_status` for precedential weight
2. Use `html_with_citations` over `plain_text` when available
3. Follow `date_modified` for incremental updates
4. Use field selection to only fetch needed metadata (performance)
5. Check `extracted_by_ocr` flag when doing NLP/text analysis

---

## ESTIMATED FIELD COUNTS

**Per Case (Complete):**
- Docket: ~60 metadata fields
- Cluster: ~40 metadata fields  
- Opinion: ~30 metadata fields per opinion (typically 1-3 opinions per cluster)
- Total: **~130-150 fields of metadata per case**

**Federal Appellate Coverage:**
- 13 Circuit Courts
- Coverage: 1891-present (varies by court)
- Millions of opinions available
- Daily updates with new cases

---

This comprehensive metadata enables:
- 📊 Quantitative legal research
- 🔍 Citation network analysis
- 👨‍⚖️ Judge voting pattern studies
- ⚖️ Precedential impact tracking
- 📈 Temporal legal trend analysis
- 🗺️ Jurisdictional comparisons
- 🤖 Machine learning on legal text
- 📚 Building custom legal databases
