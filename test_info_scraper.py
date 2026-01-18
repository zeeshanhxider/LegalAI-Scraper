"""Test the info sheet scraper"""
from scrape_info_sheets import InfoSheetScraper

# Test with Supreme Court
scraper = InfoSheetScraper('supreme_court', resume=True)
cases = scraper.load_metadata()[:3]  # Just first 3

for case in cases:
    print(f"Case: {case['case_number']}")
    filename = scraper.get_filename_from_url(case['case_info_url'])
    print(f"Filename: {filename}")
    print(f"Info URL: {scraper.get_info_sheet_url(filename)}")
    
    # Try downloading one
    success = scraper.download_info_sheet(case)
    print(f"Download success: {success}")
    print('---')
