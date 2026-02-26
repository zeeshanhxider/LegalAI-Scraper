"""Quick script to find the printer friendly link"""
import requests
from bs4 import BeautifulSoup

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
}

url = "https://www.courts.wa.gov/opinions/index.cfm?fa=opinions.showOpinion&filename=1025866MAJ"
r = requests.get(url, headers=HEADERS)
soup = BeautifulSoup(r.text, 'html.parser')

print("=== All links on page ===")
for link in soup.find_all('a', href=True):
    text = link.get_text(strip=True)
    href = link.get('href', '')
    if 'print' in text.lower() or 'print' in href.lower() or 'friendly' in text.lower():
        print(f"Text: {text}")
        print(f"Href: {href}")
        print("---")

print("\n=== Looking for images with print ===")
for img in soup.find_all('img'):
    src = img.get('src', '')
    if 'print' in src.lower():
        parent = img.parent
        print(f"Image src: {src}")
        print(f"Parent tag: {parent.name}")
        if parent.name == 'a':
            print(f"Parent href: {parent.get('href', '')}")
        # Check sibling or nearby text
        next_sibling = img.find_next_sibling()
        if next_sibling:
            print(f"Next sibling: {next_sibling}")
        print("---")

print("\n=== Raw HTML around printSmall ===")
if 'printSmall' in r.text:
    idx = r.text.find('printSmall')
    print(r.text[idx-200:idx+300])
