#!/usr/bin/env python3
"""
Test script to verify federal appellate scraper on a few sample cases.

Tests all courts with a small limit to verify:
  - Docket data completeness
  - Cluster data completeness  
  - Opinion data completeness
  - Proper error handling
  - Data format validation
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

from federal_appellate_scraper import FederalAppellateScraper

# API token from environment variable
API_TOKEN = os.environ.get("COURTLISTENER_TOKEN", "")

def print_section(title):
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")

def test_single_court(scraper, court_id, limit=3):
    """Test scraping a single court with a small limit."""
    print_section(f"Testing {court_id.upper()} - Scraping {limit} items per type")
    
    # Use recent date range (last 30 days) for faster results
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    print(f"Date range: {start_date} to {end_date}")
    
    try:
        # Scrape dockets
        print(f"\n→ Scraping dockets...")
        dockets = scraper.scrape_dockets(
            court_id=court_id,
            start_date=start_date,
            end_date=end_date,
            max_dockets=limit
        )
        docket_count = len(dockets)
        print(f"✓ Scraped {docket_count} dockets")
        
        # Scrape clusters
        print(f"\n→ Scraping clusters...")
        clusters = scraper.scrape_clusters(
            court_id=court_id,
            start_date=start_date,
            end_date=end_date,
            max_clusters=limit
        )
        cluster_count = len(clusters)
        print(f"✓ Scraped {cluster_count} clusters")
        
        # Scrape opinions
        print(f"\n→ Scraping opinions...")
        opinions = scraper.scrape_opinions(
            court_id=court_id,
            max_opinions=limit
        )
        opinion_count = len(opinions)
        print(f"✓ Scraped {opinion_count} opinions")
        
        return {
            'court': court_id,
            'dockets': docket_count,
            'clusters': cluster_count,
            'opinions': opinion_count,
            'status': 'success'
        }
    
    except Exception as e:
        print(f"✗ Error scraping {court_id}: {str(e)}")
        return {
            'court': court_id,
            'status': 'error',
            'error': str(e)
        }

def analyze_sample_data(scraper, output_dir):
    """Analyze the sampled data to verify completeness.

    The new folder layout is:
        {output_dir}/{court}/{year}/{docket_number}_{slug}/docket.json
        {output_dir}/{court}/{year}/{docket_number}_{slug}/cluster.json
        {output_dir}/{court}/{year}/{docket_number}_{slug}/opinion.json
    """
    print_section("ANALYZING SAMPLE DATA")

    # Recursively find all case directories (dirs containing a docket.json)
    docket_files = sorted(output_dir.rglob("docket.json"))
    cluster_files = sorted(output_dir.rglob("cluster*.json"))
    opinion_files = sorted(output_dir.rglob("opinion*.json"))

    # Exclude checkpoint files
    cluster_files = [f for f in cluster_files if 'checkpoint' not in f.name]
    opinion_files = [f for f in opinion_files if 'checkpoint' not in f.name]

    if not docket_files:
        print("Warning: No docket.json files found in output directory")
        return

    # Count case completeness
    case_dirs = set(f.parent for f in docket_files)
    complete = 0
    docket_only = 0
    partial = 0

    for case_dir in sorted(case_dirs):
        has_docket  = (case_dir / "docket.json").exists()
        has_cluster = any(case_dir.glob("cluster*.json"))
        has_opinion = any(case_dir.glob("opinion*.json"))

        if has_docket and has_cluster and has_opinion:
            complete += 1
        elif has_docket and not has_cluster and not has_opinion:
            docket_only += 1
        else:
            partial += 1

    print(f"\n  Total case folders : {len(case_dirs)}")
    print(f"  Complete (3 files) : {complete}")
    print(f"  Docket-only        : {docket_only}")
    print(f"  Partial            : {partial}")
    print(f"  Total docket files : {len(docket_files)}")
    print(f"  Total cluster files: {len(cluster_files)}")
    print(f"  Total opinion files: {len(opinion_files)}")

    # Show a sample docket record
    if docket_files:
        sample_file = docket_files[-1]
        print(f"\n  Sample docket: {sample_file.relative_to(output_dir)}")
        try:
            with open(sample_file) as f:
                sample = json.load(f)
            print(f"  Fields ({len(sample.keys())}):")
            for i, field in enumerate(sorted(sample.keys())[:10], 1):
                value = sample[field]
                value_str = str(value)[:60] if value else "(null)"
                print(f"    {i:2}. {field:30} = {value_str}")
            if len(sample.keys()) > 10:
                print(f"    ... and {len(sample.keys()) - 10} more fields")
        except Exception as e:
            print(f"  Error reading file: {e}")

def main():
    print_section("FEDERAL APPELLATE SCRAPER - TEST")
    print(f"Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create output directory
    output_dir = Path("tests")
    output_dir.mkdir(exist_ok=True, parents=True)
    
    print(f"Output directory: {output_dir.absolute()}")
    
    # Initialize scraper (sync mode for clarity during testing)
    scraper = FederalAppellateScraper(
        api_token=API_TOKEN,
        output_dir=str(output_dir),
        use_async=False  # Sync for clearer test output
    )
    
    # Test a subset of courts
    test_courts = ['ca9', 'cadc']  # 9th Circuit and DC Circuit
    results = []
    
    for court_id in test_courts:
        result = test_single_court(scraper, court_id, limit=5)
        results.append(result)
    
    # Summary
    print_section("TEST SUMMARY")
    for result in results:
        if result['status'] == 'success':
            print(f"✓ {result['court'].upper():8} | Dockets: {result['dockets']:3} | Clusters: {result['clusters']:3} | Opinions: {result['opinions']:3}")
        else:
            print(f"✗ {result['court'].upper():8} | Error: {result.get('error', 'Unknown')}")
    
    # Analyze the data
    analyze_sample_data(scraper, output_dir)
    
    print_section("TEST COMPLETE")
    print(f"Test completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Check the 'tests' directory for JSON output files")

if __name__ == '__main__':
    main()
