"""
Quick test script to scrape a single month from a single year

URL Structure:
- Main page: ?fa=opinions.displayAll
- Year page: ?fa=opinions.byYear&fileYear=2013&crtLevel=S&pubStatus=PUB
- Case info: ?fa=opinions.showOpinion&filename=871051MAJ
- PDF: /opinions/pdf/871051.pdf
"""

from scraper import WashingtonCourtsScraper
import logging

logging.basicConfig(level=logging.INFO)

def test_single_month():
    # Use S for Supreme Court, PUB for Published
    scraper = WashingtonCourtsScraper(output_dir="test_downloads", court_level="S", pub_status="PUB")
    
    # Test with 2013 (we have screenshots for this year)
    year = "2013"
    
    print(f"\n=== Testing scraper for {year} ===\n")
    
    # Get cases for the year
    cases = scraper.get_cases_for_year(year)
    
    print(f"Found {len(cases)} total cases")
    
    if not cases:
        print("No data found! Check if the website structure has changed.")
        return
    
    # Group by month for display
    months = {}
    for case in cases:
        month = case['month']
        if month not in months:
            months[month] = []
        months[month].append(case)
    
    print(f"\nCases by month:")
    for month, month_cases in months.items():
        print(f"  - {month}: {len(month_cases)} cases")
    
    # Test first 3 cases only
    test_cases = cases[:3]
    metadata_list = []
    
    print(f"\n=== Testing with first 3 cases ===\n")
    
    for case in test_cases:
        print(f"\nCase: {case['case_number']} - {case['case_title']}")
        print(f"  Month: {case['month']}")
        print(f"  File Date: {case['file_date']}")
        print(f"  File Contains: {case['file_contains']}")
        print(f"  Case Info URL: {case['case_info_url']}")
        
        # Try to get PDF URL
        pdf_url = scraper.get_pdf_url(case['case_info_url'])
        print(f"  PDF URL: {pdf_url}")
        
        if pdf_url:
            # Download the PDF
            safe_title = scraper.sanitize_filename(case['case_title'])[:50]
            pdf_filename = f"{case['case_number']}_{safe_title}.pdf"
            month = case['month'] or "Unknown"
            save_path = f"test_downloads/{year}/{month}/{pdf_filename}"
            
            success = scraper.download_pdf(pdf_url, save_path)
            print(f"  Download: {'SUCCESS' if success else 'FAILED'}")
            
            # Add to metadata
            from scraper import OpinionMetadata
            from datetime import datetime
            metadata = OpinionMetadata(
                year=year,
                month=month,
                file_date=case['file_date'],
                case_number=case['case_number'],
                case_title=case['case_title'],
                file_contains=case['file_contains'],
                case_info_url=case['case_info_url'],
                pdf_url=pdf_url,
                pdf_filename=pdf_filename if success else "",
                download_status="Success" if success else "Download failed",
                scraped_at=datetime.now().isoformat()
            )
            metadata_list.append(metadata)
    
    # Save metadata to CSV
    if metadata_list:
        scraper.save_metadata(metadata_list, "test_downloads/metadata.csv")
        print(f"\nMetadata saved to: test_downloads/metadata.csv")
    
    print("\n=== Test complete! ===")
    print("Check the 'test_downloads' folder for downloaded PDFs")


def get_cases_for_year(scraper, year: str):
    """Get all cases for a given year - extracted for clarity"""
    url = scraper.get_year_page_url(year)
    print(f"Fetching: {url}")
    
    response = scraper._make_request(url)
    if not response:
        return []
    
    from bs4 import BeautifulSoup
    import re
    from urllib.parse import urljoin
    from config import BASE_URL
    
    soup = BeautifulSoup(response.text, 'html.parser')
    all_cases = []
    current_month = None
    
    # Find all tables with case data
    tables = soup.find_all('table')
    print(f"Found {len(tables)} tables")
    
    for table in tables:
        # Try to find the month from a preceding element
        prev = table.find_previous(['p', 'h2', 'h3', 'strong', 'b'])
        while prev:
            text = prev.get_text(strip=True)
            month_match = re.match(r'^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}$', text)
            if month_match:
                current_month = month_match.group(1)
                break
            prev = prev.find_previous(['p', 'h2', 'h3', 'strong', 'b'])
        
        # Parse the table rows
        rows = table.find_all('tr')
        
        for row in rows[1:]:  # Skip header row
            cells = row.find_all('td')
            if len(cells) >= 3:
                try:
                    file_date = cells[0].get_text(strip=True)
                    
                    # Always extract month from file_date for accuracy
                    case_month = scraper._extract_month_from_date(file_date) or current_month or "Unknown"
                    
                    # Get case number and link
                    case_cell = cells[1]
                    case_link = case_cell.find('a')
                    case_number = case_link.get_text(strip=True) if case_link else case_cell.get_text(strip=True)
                    case_info_url = urljoin(BASE_URL, case_link.get('href')) if case_link else ""
                    
                    case_title = cells[2].get_text(strip=True)
                    case_title = re.sub(r'^\*\s*', '', case_title)
                    
                    file_contains = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                    
                    if case_number and case_info_url:
                        all_cases.append({
                            'year': year,
                            'month': case_month,
                            'file_date': file_date,
                            'case_number': case_number,
                            'case_title': case_title,
                            'file_contains': file_contains,
                            'case_info_url': case_info_url
                        })
                except Exception as e:
                    print(f"Error parsing row: {e}")
                    continue
    
    return all_cases


# Add the get_cases_for_year method to scraper dynamically for testing
WashingtonCourtsScraper.get_cases_for_year = lambda self, year: get_cases_for_year(self, year)


if __name__ == "__main__":
    test_single_month()
