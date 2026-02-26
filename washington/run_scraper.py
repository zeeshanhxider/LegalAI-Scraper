"""
Runner script for the Washington Courts Opinion Scraper

This script provides a simple interface to run the scraper with various options.
Supports all opinion types: Supreme Court, Court of Appeals (Published, Partial, Unpublished)
"""

import argparse
import sys
from scraper import WashingtonCourtsScraper
from config import OPINION_TYPES


def main():
    parser = argparse.ArgumentParser(
        description='Washington State Courts Opinion Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Opinion Types:
  supreme_court      - Supreme Court Opinions
  appeals_published  - Court of Appeals - Published Opinions
  appeals_partial    - Court of Appeals - Opinions Published in Part
  appeals_unpublished - Court of Appeals - Unpublished Opinions
  all                - Scrape all opinion types

Examples:
  # Scrape all Supreme Court opinions (default)
  python run_scraper.py

  # Scrape all opinion types
  python run_scraper.py --type all

  # Scrape Court of Appeals published opinions
  python run_scraper.py --type appeals_published

  # Scrape specific years for a specific type
  python run_scraper.py --type supreme_court --years 2023 2024 2025

  # Specify custom output directory
  python run_scraper.py --output ./my_downloads

  # Test mode - scrape only the most recent year
  python run_scraper.py --test
  
  # Start fresh (ignore previous progress)
  python run_scraper.py --no-resume
  
  # List available years for an opinion type
  python run_scraper.py --type appeals_published --list-years
  
  # Count cases for all opinion types
  python run_scraper.py --count-all
        """
    )
    
    parser.add_argument(
        '--type',
        choices=list(OPINION_TYPES.keys()) + ['all'],
        default='supreme_court',
        help='Type of opinions to scrape (default: supreme_court)'
    )
    
    parser.add_argument(
        '--years',
        nargs='+',
        help='Specific years to scrape (e.g., 2023 2024)'
    )
    
    parser.add_argument(
        '--output',
        default='downloads',
        help='Base output directory for downloaded PDFs (default: downloads)'
    )
    
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode: only scrape the most recent year'
    )
    
    parser.add_argument(
        '--list-years',
        action='store_true',
        help='Only list available years without downloading'
    )
    
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Start fresh, ignoring any previous progress'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=5,
        help='Number of parallel workers for downloading (default: 5)'
    )
    
    parser.add_argument(
        '--count-all',
        action='store_true',
        help='Count cases for all opinion types without downloading'
    )
    
    args = parser.parse_args()
    
    # Count all opinion types
    if args.count_all:
        count_all_opinion_types(args.output)
        return
    
    # Determine which opinion types to scrape
    if args.type == 'all':
        opinion_types = list(OPINION_TYPES.keys())
    else:
        opinion_types = [args.type]
    
    # Process each opinion type
    for opinion_type in opinion_types:
        print(f"\n{'='*60}")
        type_config = OPINION_TYPES[opinion_type]
        print(f"Processing: {type_config['opinion_type']} - {type_config['publication_status']}")
        print(f"{'='*60}")
        
        # Initialize scraper with resume option
        resume = not args.no_resume
        scraper = WashingtonCourtsScraper(
            output_dir=args.output, 
            opinion_type=opinion_type,
            resume=resume,
            workers=args.workers
        )
        
        # List years only
        if args.list_years:
            years = scraper.get_available_years()
            print(f"\nAvailable years: {', '.join(years)}")
            print(f"Total: {len(years)} years")
            continue
        
        # Determine years to scrape
        years_to_scrape = None
        
        if args.years:
            years_to_scrape = args.years
            print(f"Scraping specified years: {years_to_scrape}")
        elif args.test:
            all_years = scraper.get_available_years()
            if all_years:
                years_to_scrape = [all_years[0]]  # Most recent year
                print(f"Test mode: scraping only {years_to_scrape[0]}")
            else:
                print("Error: Could not fetch available years")
                continue
        else:
            print("Scraping all available years...")
        
        # Run the scraper
        scraper.run(years=years_to_scrape)
        
        print(f"\nCompleted: {OPINION_TYPES[opinion_type]['opinion_type']} - {OPINION_TYPES[opinion_type]['publication_status']}")
        print(f"PDFs saved to: {scraper.output_dir}/")
        print(f"Metadata saved to: {scraper.output_dir}/metadata.csv")
    
    print("\n" + "="*60)
    print("All scraping complete!")
    print("="*60)


def count_all_opinion_types(base_output_dir: str):
    """Count cases for all opinion types without downloading"""
    import requests
    from bs4 import BeautifulSoup
    import re
    
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    BASE_URL = "https://www.courts.wa.gov/opinions/index.cfm"
    
    grand_total = 0
    
    print("\n" + "="*70)
    print("CASE COUNT FOR ALL OPINION TYPES")
    print("="*70)
    
    for type_key, type_config in OPINION_TYPES.items():
        court_level = type_config["court_level"]
        pub_status = type_config["pub_status"]
        opinion_type = type_config["opinion_type"]
        pub_status_name = type_config["publication_status"]
        
        print(f"\n{opinion_type} - {pub_status_name}")
        print("-" * 50)
        
        # Get years for this type
        r = requests.get(f"{BASE_URL}?fa=opinions.displayAll", headers=HEADERS)
        soup = BeautifulSoup(r.text, 'html.parser')
        
        years = []
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True).replace('|', '').strip()
            if 'byYear' in href and f'crtLevel={court_level}' in href and f'pubStatus={pub_status}' in href:
                if re.match(r'^\d{4}$', text):
                    years.append(text)
        
        years = sorted(list(set(years)), reverse=True)
        
        type_total = 0
        for year in years:
            url = f"{BASE_URL}?fa=opinions.byYear&fileYear={year}&crtLevel={court_level}&pubStatus={pub_status}"
            r = requests.get(url, headers=HEADERS)
            soup = BeautifulSoup(r.text, 'html.parser')
            
            case_links = [l for l in soup.find_all('a', href=True) if 'showOpinion' in l.get('href', '')]
            count = len(case_links)
            type_total += count
            print(f"  {year}: {count} cases")
        
        print(f"  {'SUBTOTAL'}: {type_total} cases")
        grand_total += type_total
    
    print("\n" + "="*70)
    print(f"GRAND TOTAL: {grand_total} cases across all opinion types")
    print("="*70)


if __name__ == "__main__":
    main()
