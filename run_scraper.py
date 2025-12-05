"""
Runner script for the Washington Courts Opinion Scraper

This script provides a simple interface to run the scraper with various options.
"""

import argparse
import sys
from scraper import WashingtonCourtsScraper


def main():
    parser = argparse.ArgumentParser(
        description='Washington State Courts Opinion Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape all available years (with auto-resume)
  python run_scraper.py

  # Scrape specific years
  python run_scraper.py --years 2023 2024 2025

  # Scrape a single year
  python run_scraper.py --years 2024

  # Specify custom output directory
  python run_scraper.py --output ./my_downloads

  # Test mode - scrape only the most recent year
  python run_scraper.py --test
  
  # Start fresh (ignore previous progress)
  python run_scraper.py --no-resume
  
  # Run overnight (recommended)
  python run_scraper.py 2>&1 | tee scraper_output.log
        """
    )
    
    parser.add_argument(
        '--years',
        nargs='+',
        help='Specific years to scrape (e.g., 2023 2024)'
    )
    
    parser.add_argument(
        '--output',
        default='downloads',
        help='Output directory for downloaded PDFs (default: downloads)'
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
    
    args = parser.parse_args()
    
    # Initialize scraper with resume option
    resume = not args.no_resume
    scraper = WashingtonCourtsScraper(output_dir=args.output, resume=resume)
    
    # List years only
    if args.list_years:
        years = scraper.get_available_years()
        print(f"\nAvailable years: {', '.join(years)}")
        print(f"Total: {len(years)} years")
        return
    
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
            sys.exit(1)
    else:
        print("Scraping all available years...")
    
    # Run the scraper
    scraper.run(years=years_to_scrape)
    
    print("\nScraping complete!")
    print(f"PDFs saved to: {args.output}/")
    print(f"Metadata saved to: {args.output}/metadata.csv")


if __name__ == "__main__":
    main()
