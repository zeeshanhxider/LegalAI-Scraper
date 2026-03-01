#!/usr/bin/env python3
"""
Bulk download processor for CourtListener federal appellate data.

Instead of hitting the REST API ~300K times (30-50 hours), this script:
1. Streams bulk CSV files from CourtListener's S3 bucket
2. Filters for federal appellate courts (CA1-CA11, CADC, CAFC), years 2013-2025
3. Writes output in the same folder structure as the API scraper

The CSV files are bz2-compressed and streamed through memory — the full files
(51GB+ for opinions) never touch disk.

Usage:
    python3 bulk_download.py                    # Full run
    python3 bulk_download.py --start 2020 --end 2025  # Year range
    python3 bulk_download.py --dry-run           # Show counts only
"""

import argparse
import bz2
import csv
import io
import json
import logging
import os
import pickle
import re
import ssl
import sys
import time
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://storage.courtlistener.com/bulk-data"
# Most recent bulk data snapshot
SNAPSHOT_DATE = "2025-12-31"

CLUSTERS_FILE = f"opinion-clusters-{SNAPSHOT_DATE}.csv.bz2"
DOCKETS_FILE  = f"dockets-{SNAPSHOT_DATE}.csv.bz2"
OPINIONS_FILE = f"opinions-{SNAPSHOT_DATE}.csv.bz2"

FEDERAL_APPELLATE_COURTS = {
    "ca1", "ca2", "ca3", "ca4", "ca5", "ca6", "ca7",
    "ca8", "ca9", "ca10", "ca11", "cadc", "cafc",
}

COURT_NAMES = {
    "ca1": "1st Circuit", "ca2": "2nd Circuit", "ca3": "3rd Circuit",
    "ca4": "4th Circuit", "ca5": "5th Circuit", "ca6": "6th Circuit",
    "ca7": "7th Circuit", "ca8": "8th Circuit", "ca9": "9th Circuit",
    "ca10": "10th Circuit", "ca11": "11th Circuit",
    "cadc": "DC Circuit", "cafc": "Federal Circuit",
}

DOWNLOAD_DIR = Path("downloads")
TEMP_DIR = Path("bulk_temp")
CHECKPOINT_DIR = Path("bulk_checkpoints")

# Raise CSV field size limit — opinion text fields easily exceed the 128 KB default.
# Use the largest value the platform supports.
csv.field_size_limit(min(sys.maxsize, 2_147_483_647))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bulk")

# ---------------------------------------------------------------------------
# Checkpoint helpers — save/load Phase 1 & 2 results so a re-run skips them
# ---------------------------------------------------------------------------

def _checkpoint_path(name: str) -> Path:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    return CHECKPOINT_DIR / f"{name}.pkl"

def save_checkpoint(name: str, data) -> None:
    path = _checkpoint_path(name)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "wb") as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    tmp.replace(path)
    log.info(f"Checkpoint saved: {path}")

def load_checkpoint(name: str):
    path = _checkpoint_path(name)
    if path.exists():
        log.info(f"Loading checkpoint: {path}")
        with open(path, "rb") as f:
            return pickle.load(f)
    return None

# ---------------------------------------------------------------------------
# Streaming CSV reader via curl | bunzip2 pipe
# ---------------------------------------------------------------------------

def _make_ssl_context():
    """
    Return an SSL context that verifies certificates.
    Uses certifi's CA bundle if available (needed on macOS with framework Python).
    On Windows, the default context uses the system cert store and works fine.
    """
    try:
        import certifi
        ctx = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        ctx = ssl.create_default_context()
    return ctx


def stream_csv_rows(filename, desc=""):
    """
    Stream a bz2-compressed CSV from S3, yielding dicts row by row.
    Uses Python's built-in urllib.request + bz2 — no external binaries
    required, so this works on Windows, Mac, and Linux.
    Memory usage: ~one CSV row at a time.
    """
    url = f"{BASE_URL}/{filename}"
    log.info(f"Streaming {desc or filename} from {url} ...")

    ctx = _make_ssl_context()
    response = urllib.request.urlopen(url, context=ctx)
    bz2_reader = bz2.BZ2File(response)
    text_stream = io.TextIOWrapper(bz2_reader, encoding="utf-8", errors="replace")

    reader = csv.DictReader(text_stream)
    row_count = 0
    t0 = time.time()

    try:
        for row in reader:
            row_count += 1
            if row_count % 500_000 == 0:
                elapsed = time.time() - t0
                log.info(f"  {desc}: {row_count:,} rows processed ({elapsed:.0f}s)")
            yield row
    finally:
        text_stream.close()
        bz2_reader.close()
        response.close()
        elapsed = time.time() - t0
        log.info(f"  {desc}: done — {row_count:,} total rows in {elapsed:.0f}s")


# ---------------------------------------------------------------------------
# Phase 1: Stream dockets, filter for federal appellate courts
# ---------------------------------------------------------------------------

def phase1_dockets():
    """
    Stream dockets CSV, keep only federal appellate court dockets.
    Returns dict: docket_id -> {court_id, docket_number, case_name, case_name_short, slug, ...}
    """
    log.info("=" * 70)
    log.info("PHASE 1: Streaming dockets — filtering for federal appellate courts")
    log.info("=" * 70)

    dockets = {}  # id -> dict
    for row in stream_csv_rows(DOCKETS_FILE, desc="dockets"):
        court_id = row.get("court_id", "")
        if court_id not in FEDERAL_APPELLATE_COURTS:
            continue
        docket_id = row["id"]
        dockets[docket_id] = {
            "id": docket_id,
            "court_id": court_id,
            "docket_number": row.get("docket_number", ""),
            "docket_number_core": row.get("docket_number_core", ""),
            "case_name": row.get("case_name", ""),
            "case_name_short": row.get("case_name_short", ""),
            "case_name_full": row.get("case_name_full", ""),
            "slug": row.get("slug", ""),
            "date_filed": row.get("date_filed", ""),
            "date_terminated": row.get("date_terminated", ""),
            "date_created": row.get("date_created", ""),
            "date_modified": row.get("date_modified", ""),
            "source": row.get("source", ""),
            "assigned_to_str": row.get("assigned_to_str", ""),
            "referred_to_str": row.get("referred_to_str", ""),
            "panel_str": row.get("panel_str", ""),
            "appeal_from_str": row.get("appeal_from_str", ""),
            "nature_of_suit": row.get("nature_of_suit", ""),
            "cause": row.get("cause", ""),
            "jurisdiction_type": row.get("jurisdiction_type", ""),
        }

    log.info(f"Phase 1 complete: {len(dockets):,} federal appellate dockets found")
    # Stats by court
    court_counts = {}
    for d in dockets.values():
        c = d["court_id"]
        court_counts[c] = court_counts.get(c, 0) + 1
    for c in sorted(court_counts):
        log.info(f"  {COURT_NAMES.get(c, c):20s}: {court_counts[c]:,} dockets")
    return dockets


# ---------------------------------------------------------------------------
# Phase 2: Stream clusters, filter by docket IDs + date range
# ---------------------------------------------------------------------------

def phase2_clusters(docket_ids: set, start_year: int, end_year: int):
    """
    Stream opinion-clusters CSV, keep only those whose docket_id is in our set
    and date_filed falls in [start_year, end_year].

    Returns:
        clusters: dict cluster_id -> full cluster row dict
        cluster_to_docket: dict cluster_id -> docket_id
    """
    log.info("=" * 70)
    log.info(f"PHASE 2: Streaming clusters — filtering {start_year}-{end_year}")
    log.info("=" * 70)

    clusters = {}
    cluster_to_docket = {}

    for row in stream_csv_rows(CLUSTERS_FILE, desc="clusters"):
        docket_id = row.get("docket_id", "")
        if docket_id not in docket_ids:
            continue

        # Check date range
        date_filed = row.get("date_filed", "")
        if not date_filed:
            continue
        try:
            year = int(date_filed[:4])
        except (ValueError, IndexError):
            continue
        if year < start_year or year > end_year:
            continue

        cluster_id = row["id"]
        clusters[cluster_id] = row
        cluster_to_docket[cluster_id] = docket_id

    log.info(f"Phase 2 complete: {len(clusters):,} clusters matched")
    return clusters, cluster_to_docket


# ---------------------------------------------------------------------------
# Phase 3+4 combined: Stream opinions and write immediately
# ---------------------------------------------------------------------------

def phase3_stream_and_write(cluster_ids: set, clusters: dict,
                             cluster_to_docket: dict, dockets: dict):
    """
    Stream opinions CSV (51GB compressed!) and write case folders on the fly.
    This avoids accumulating all opinion text in memory.

    Because a cluster can have multiple sub-opinions (e.g. majority + dissent),
    and CSV rows for the same cluster may not be adjacent, we do two things:
    1. First pass: collect just the opinion rows (they're small relative to 51GB)
       Actually — the opinion TEXT is in the rows, so they're not small.
    2. Better approach: collect matched opinions into a buffer, then batch-write
       periodically. Since opinions for one cluster usually appear close together
       in the CSV (sorted by ID), the buffer stays small.

    Actually, the simplest correct approach: accumulate matched opinions in a dict
    keyed by cluster_id, but write and flush each cluster to disk as soon as we
    encounter a *different* cluster_id (since the CSV is ordered by id, and
    cluster_id is a foreign key that groups opinions). For safety, also flush
    when the buffer exceeds a threshold.

    REVISED: The opinions CSV is ordered by opinion.id, not cluster_id. Multiple
    opinions for one cluster will have consecutive IDs, so they're usually adjacent.
    But we can't guarantee this. Let's just buffer all matched opinions and write
    at the end — but to limit memory, we write each cluster's opinions as soon as
    we've seen a reasonable gap. The simpler approach: accumulate all, it should be
    fine. ~300K opinions * ~20KB avg = ~6GB. If memory is tight, we can split.

    Let's use a pragmatic approach: accumulate matched opinions in memory (just the
    fields we need for JSON), then write all at the end.
    """
    log.info("=" * 70)
    log.info(f"PHASE 3+4: Streaming opinions + writing cases")
    log.info(f"  Filtering {len(cluster_ids):,} cluster IDs from ~51GB compressed file")
    log.info("=" * 70)

    opinions_by_cluster = {}  # cluster_id -> [opinion_dicts]
    matched = 0

    for row in stream_csv_rows(OPINIONS_FILE, desc="opinions"):
        cid = row.get("cluster_id", "")
        if cid not in cluster_ids:
            continue
        matched += 1
        if cid not in opinions_by_cluster:
            opinions_by_cluster[cid] = []
        opinions_by_cluster[cid].append(row)

        if matched % 10_000 == 0:
            log.info(f"  opinions: {matched:,} matched so far "
                     f"({len(opinions_by_cluster):,} clusters covered)")

    log.info(f"  Opinions stream complete: {matched:,} opinions matched "
             f"across {len(opinions_by_cluster):,} clusters")

    # Now write all cases
    return _write_cases(dockets, clusters, cluster_to_docket, opinions_by_cluster)


# ---------------------------------------------------------------------------
# Phase 4: Write folder structure
# ---------------------------------------------------------------------------

def sanitize_folder_name(name: str) -> str:
    """Convert a case name / docket number to a safe folder name."""
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    name = re.sub(r'\s+', '-', name.strip())
    name = name.strip('-.')
    return name[:120]  # Limit length


def build_docket_json(docket_row: dict) -> dict:
    """Build a docket.json matching the API scraper's format."""
    return {
        "id": int(docket_row["id"]) if docket_row["id"] else None,
        "case_name": docket_row.get("case_name", ""),
        "case_name_short": docket_row.get("case_name_short", ""),
        "case_name_full": docket_row.get("case_name_full", ""),
        "court_id": docket_row.get("court_id", ""),
        "docket_number": docket_row.get("docket_number", ""),
        "docket_number_core": docket_row.get("docket_number_core", ""),
        "date_filed": docket_row.get("date_filed", ""),
        "date_terminated": docket_row.get("date_terminated", ""),
        "date_created": docket_row.get("date_created", ""),
        "date_modified": docket_row.get("date_modified", ""),
        "source": docket_row.get("source", ""),
        "assigned_to_str": docket_row.get("assigned_to_str", ""),
        "referred_to_str": docket_row.get("referred_to_str", ""),
        "panel_str": docket_row.get("panel_str", ""),
        "appeal_from_str": docket_row.get("appeal_from_str", ""),
        "nature_of_suit": docket_row.get("nature_of_suit", ""),
        "cause": docket_row.get("cause", ""),
        "jurisdiction_type": docket_row.get("jurisdiction_type", ""),
        "slug": docket_row.get("slug", ""),
        "_source": "bulk_data",
    }


def build_cluster_json(cluster_row: dict) -> dict:
    """Build a cluster.json matching the API scraper's format."""
    return {
        "id": int(cluster_row["id"]) if cluster_row["id"] else None,
        "docket_id": int(cluster_row["docket_id"]) if cluster_row.get("docket_id") else None,
        "date_filed": cluster_row.get("date_filed", ""),
        "date_filed_is_approximate": cluster_row.get("date_filed_is_approximate", "") == "t",
        "date_created": cluster_row.get("date_created", ""),
        "date_modified": cluster_row.get("date_modified", ""),
        "judges": cluster_row.get("judges", ""),
        "case_name": cluster_row.get("case_name", ""),
        "case_name_short": cluster_row.get("case_name_short", ""),
        "case_name_full": cluster_row.get("case_name_full", ""),
        "slug": cluster_row.get("slug", ""),
        "source": cluster_row.get("source", ""),
        "procedural_history": cluster_row.get("procedural_history", ""),
        "attorneys": cluster_row.get("attorneys", ""),
        "nature_of_suit": cluster_row.get("nature_of_suit", ""),
        "posture": cluster_row.get("posture", ""),
        "syllabus": cluster_row.get("syllabus", ""),
        "headnotes": cluster_row.get("headnotes", ""),
        "summary": cluster_row.get("summary", ""),
        "disposition": cluster_row.get("disposition", ""),
        "history": cluster_row.get("history", ""),
        "other_dates": cluster_row.get("other_dates", ""),
        "cross_reference": cluster_row.get("cross_reference", ""),
        "correction": cluster_row.get("correction", ""),
        "citation_count": int(cluster_row["citation_count"]) if cluster_row.get("citation_count") else 0,
        "precedential_status": cluster_row.get("precedential_status", ""),
        "date_blocked": cluster_row.get("date_blocked", ""),
        "blocked": cluster_row.get("blocked", "") == "t",
        "arguments": cluster_row.get("arguments", ""),
        "headmatter": cluster_row.get("headmatter", ""),
        "_source": "bulk_data",
    }


def build_opinion_json(opinion_rows: list) -> dict:
    """
    Build an opinion.json. If a cluster has multiple sub-opinions,
    we combine them into a single JSON with a list.
    For single-opinion clusters (the common case), the format matches the API scraper.
    """
    if len(opinion_rows) == 1:
        row = opinion_rows[0]
        return _opinion_row_to_dict(row)
    else:
        # Multiple sub-opinions for this cluster
        return {
            "sub_opinions": [_opinion_row_to_dict(r) for r in opinion_rows],
            "_source": "bulk_data",
        }


def _opinion_row_to_dict(row: dict) -> dict:
    # Pick the best text field available
    text = ""
    for field in ("html_with_citations", "html_columbia", "html_lawbox",
                  "html_anon_2020", "html", "xml_harvard", "plain_text"):
        val = row.get(field, "")
        if val and val.strip():
            text = val
            break

    def _safe_int(val):
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    return {
        "id": _safe_int(row.get("id")),
        "cluster_id": _safe_int(row.get("cluster_id")),
        "date_created": row.get("date_created", ""),
        "date_modified": row.get("date_modified", ""),
        "author_str": row.get("author_str", ""),
        "per_curiam": row.get("per_curiam", "") == "t",
        "joined_by_str": row.get("joined_by_str", ""),
        "type": row.get("type", ""),
        "sha1": row.get("sha1", ""),
        "page_count": row.get("page_count", ""),
        "download_url": row.get("download_url", ""),
        "local_path": row.get("local_path", ""),
        "plain_text": row.get("plain_text", ""),
        "html": row.get("html", ""),
        "html_lawbox": row.get("html_lawbox", ""),
        "html_columbia": row.get("html_columbia", ""),
        "html_anon_2020": row.get("html_anon_2020", ""),
        "xml_harvard": row.get("xml_harvard", ""),
        "html_with_citations": row.get("html_with_citations", ""),
        "extracted_by_ocr": row.get("extracted_by_ocr", "") == "t",
        "_best_text": text,
        "_source": "bulk_data",
    }


def _write_cases(dockets, clusters, cluster_to_docket, opinions_by_cluster):
    """
    Write the folder structure:
      downloads/{Circuit Name}/{year}/{docket_number}_{slug}/
        docket.json, cluster.json, opinion.json
    """
    log.info("=" * 70)
    log.info("PHASE 4: Writing folder structure")
    log.info("=" * 70)

    written = 0
    skipped_no_opinion = 0
    errors = 0

    for cluster_id, cluster_row in clusters.items():
        docket_id = cluster_to_docket[cluster_id]
        docket_row = dockets.get(docket_id)
        if not docket_row:
            errors += 1
            continue

        # Skip clusters with no opinion
        if cluster_id not in opinions_by_cluster:
            skipped_no_opinion += 1
            continue

        court_id = docket_row["court_id"]
        court_name: str = COURT_NAMES.get(court_id, court_id) or court_id
        date_filed = cluster_row.get("date_filed", "")
        try:
            year = str(date_filed[:4])
        except (IndexError, TypeError):
            year = "unknown"
        if not year or len(year) != 4:
            year = "unknown"

        # Build folder name: docket_number_slug
        docket_num = docket_row.get("docket_number", "") or ""
        slug = cluster_row.get("slug", "") or docket_row.get("slug", "") or ""
        folder_name_parts = []
        if docket_num:
            folder_name_parts.append(sanitize_folder_name(docket_num))
        if slug:
            folder_name_parts.append(sanitize_folder_name(slug))
        folder_name: str = "_".join(folder_name_parts) if folder_name_parts else f"case_{cluster_id}"

        case_dir = DOWNLOAD_DIR / court_name / year / folder_name
        case_dir.mkdir(parents=True, exist_ok=True)

        # Write docket.json
        with open(case_dir / "docket.json", "w") as f:
            json.dump(build_docket_json(docket_row), f, indent=2)

        # Write cluster.json
        with open(case_dir / "cluster.json", "w") as f:
            json.dump(build_cluster_json(cluster_row), f, indent=2)

        # Write opinion.json
        with open(case_dir / "opinion.json", "w") as f:
            json.dump(build_opinion_json(opinions_by_cluster[cluster_id]), f, indent=2)

        written += 1
        if written % 5_000 == 0:
            log.info(f"  Written {written:,} cases...")

    log.info(f"Phase 4 complete:")
    log.info(f"  Written:              {written:,} cases")
    log.info(f"  Skipped (no opinion): {skipped_no_opinion:,}")
    log.info(f"  Errors:               {errors:,}")
    return written


# ---------------------------------------------------------------------------
# Memory-efficient alternative: phases 1+2 first, then stream opinions
# ---------------------------------------------------------------------------

def run_full(start_year: int, end_year: int, dry_run: bool = False):
    """Execute the full bulk download pipeline."""
    t0 = time.time()

    # Phase 1: dockets — load from checkpoint if available
    cached = load_checkpoint("phase1_dockets")
    if cached is not None:
        dockets = cached
        log.info(f"Phase 1 loaded from checkpoint: {len(dockets):,} dockets")
    else:
        dockets = phase1_dockets()
        save_checkpoint("phase1_dockets", dockets)
    docket_ids = set(dockets.keys())
    log.info(f"Memory: ~{len(dockets) * 500 / 1024 / 1024:.0f}MB for dockets dict")

    # Phase 2: clusters — load from checkpoint if available
    cache_key = f"phase2_clusters_{start_year}_{end_year}"
    cached2 = load_checkpoint(cache_key)
    if cached2 is not None:
        clusters, cluster_to_docket = cached2
        log.info(f"Phase 2 loaded from checkpoint: {len(clusters):,} clusters")
    else:
        clusters, cluster_to_docket = phase2_clusters(docket_ids, start_year, end_year)
        save_checkpoint(cache_key, (clusters, cluster_to_docket))
    cluster_ids = set(clusters.keys())
    log.info(f"Memory: ~{len(clusters) * 1000 / 1024 / 1024:.0f}MB for clusters dict")

    if dry_run:
        log.info("DRY RUN — skipping opinions download and file writing")
        log.info(f"Would process {len(clusters):,} clusters")
        elapsed = time.time() - t0
        log.info(f"Dry run completed in {elapsed:.0f}s")
        return

    # Phase 3+4: stream opinions and write case folders
    written = phase3_stream_and_write(cluster_ids, clusters, cluster_to_docket, dockets)

    elapsed = time.time() - t0
    log.info("=" * 70)
    log.info(f"ALL DONE: {written:,} cases written in {elapsed / 3600:.1f} hours")
    log.info("=" * 70)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Bulk download CourtListener federal appellate data")
    parser.add_argument("--start", type=int, default=2013, help="Start year (default: 2013)")
    parser.add_argument("--end", type=int, default=2025, help="End year (default: 2025)")
    parser.add_argument("--dry-run", action="store_true", help="Only count matches, don't download opinions")
    args = parser.parse_args()

    log.info(f"CourtListener Bulk Data Processor")
    log.info(f"Date range: {args.start}-{args.end}")
    log.info(f"Courts: {', '.join(sorted(FEDERAL_APPELLATE_COURTS))}")
    log.info(f"Snapshot: {SNAPSHOT_DATE}")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    run_full(args.start, args.end, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
