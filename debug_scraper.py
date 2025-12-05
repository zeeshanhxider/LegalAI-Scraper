"""
Debug script to see what the website HTML looks like
"""

import requests
from bs4 import BeautifulSoup
from config import HEADERS, BASE_URL

def debug_page():
    session = requests.Session()
    session.headers.update(HEADERS)
    
    # Fetch the year page
    url = "https://www.courts.wa.gov/opinions/index.cfm?fa=opinions.opindisp&year=2013"
    print(f"Fetching: {url}\n")
    
    response = session.get(url, timeout=30)
    print(f"Status: {response.status_code}")
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Save the HTML for inspection
    with open("debug_page.html", "w", encoding="utf-8") as f:
        f.write(response.text)
    print("Saved full HTML to debug_page.html\n")
    
    # Look for tables
    tables = soup.find_all('table')
    print(f"Found {len(tables)} tables\n")
    
    for i, table in enumerate(tables):
        rows = table.find_all('tr')
        print(f"Table {i+1}: {len(rows)} rows")
        if rows:
            # Print first row (header)
            first_row = rows[0]
            cells = first_row.find_all(['td', 'th'])
            print(f"  First row cells: {[c.get_text(strip=True)[:30] for c in cells]}")
        print()
    
    # Look for month headers
    print("Looking for month headers...")
    for tag in ['h2', 'h3', 'strong', 'b']:
        elements = soup.find_all(tag)
        for el in elements:
            text = el.get_text(strip=True)
            if any(month in text for month in ['January', 'February', 'March', 'April', 'May', 'June', 
                                                 'July', 'August', 'September', 'October', 'November', 'December']):
                print(f"  Found month in <{tag}>: {text}")
    
    # Look for links that might be to case pages
    print("\nLooking for case links...")
    links = soup.find_all('a', href=True)
    case_links = [l for l in links if 'opindisp' in l.get('href', '').lower() or 'case' in l.get('href', '').lower()]
    print(f"Found {len(case_links)} potential case links")
    for link in case_links[:5]:
        print(f"  {link.get_text(strip=True)}: {link.get('href')}")


if __name__ == "__main__":
    debug_page()
