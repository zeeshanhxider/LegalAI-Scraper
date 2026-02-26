#!/usr/bin/env python3
"""
run_all_years.py — Multiprocess + async orchestrator for the federal appellate scraper.

Architecture
────────────
   Main process
    └─ spawns N child processes  (one per year, via multiprocessing.spawn)
         └─ each child runs an asyncio event loop
              └─ up to 10 concurrent aiohttp workers per process
                   └─ all workers share ONE cross-process token-bucket rate limiter

Rate limiting
─────────────
  Total API budget : 4000 req/hr  (hard cap shared across ALL processes + workers)
  Cross-process safety via multiprocessing.Manager Lock + dict
  Initial burst protection: bucket starts with 60 tokens (not full) to prevent spikes
  Process stagger: 8 seconds between each child launch

Checkpoints
───────────
  Each year-process writes its own  downloads/checkpoint_{year}.json
  so that parallel processes never interfere with each other's progress.
  Re-running after a partial failure will skip already-completed courts
  for that year only.

Logging
───────
  Each year-process writes to:  logs/scrape_{year}.log
  Main process prints a live status table every N seconds.
  Follow a single year:  tail -f logs/scrape_2018.log

Usage
─────
  # Run all 13 years, unlimited cases (full production run)
  python run_all_years.py --all

  # Test run: 1 case per court per year  (fast verification)
  python run_all_years.py --limit 1

  # Specific years, 100 cases/court/year
  python run_all_years.py --years 2020 2021 --limit 100

  # Specific courts only
  python run_all_years.py --limit 10 --courts ca1 ca9 cadc

  # Conservative rate budget (half quota)
  python run_all_years.py --all --rate-budget 2400
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_TOKEN       = os.environ.get("COURTLISTENER_TOKEN", "")
ALL_YEARS           = list(range(2013, 2026))   # 2013 … 2025  →  13 years
WORKERS_PER_PROCESS = 10
PROCESS_STAGGER_SEC = 8    # seconds between launching each child process


# ══════════════════════════════════════════════════════════════════════════════
#  SHARED CROSS-PROCESS TOKEN-BUCKET RATE LIMITER
# ══════════════════════════════════════════════════════════════════════════════

class SharedRateLimiter:
    """
    Token-bucket limiter safe across processes AND asyncio workers.

    All child processes (each with up to 10 concurrent workers) funnel
    every outgoing HTTP request through this single shared pool.  The bucket
    refills at `max_per_hour / 3600` tokens per second; if the bucket is empty
    the caller blocks until a token is available.

    When the API returns HTTP 429, any worker can call ``pause_until(seconds)``
    to freeze ALL token grants across every process for the Retry-After
    duration.  This prevents a 429 cascade where hundreds of workers each
    independently hammer the API and each receive their own 429.
    """

    def __init__(self, manager, max_per_hour: float = 4000):
        # Start with a small initial pool (60 tokens) to prevent burst spikes
        # when all processes start simultaneously.  The bucket refills steadily.
        initial_tokens = min(60.0, float(max_per_hour))
        self._lock  = manager.Lock()
        self._state = manager.dict({
            "tokens"     : initial_tokens,
            "last_time"  : time.time(),
            "max_tokens" : float(max_per_hour),
            "rate"       : max_per_hour / 3600.0,   # tokens / second
            "paused_until": 0.0,  # epoch time; 0 = not paused
        })

    def pause_until(self, seconds: float) -> None:
        """Pause ALL token grants for ``seconds`` from now.

        Called when any worker receives an HTTP 429 with a Retry-After header.
        Only extends the pause — never shortens an existing one.
        """
        with self._lock:
            s = self._state
            resume_at = time.time() + seconds
            if resume_at > s["paused_until"]:
                s["paused_until"] = resume_at
                logging.warning(
                    f"Rate limiter PAUSED globally for {seconds:.0f}s "
                    f"(until {time.strftime('%H:%M:%S', time.localtime(resume_at))})"
                )

    def acquire(self) -> None:
        """
        Blocking call.  Returns only when a token has been consumed.
        Runs in a thread-pool executor when called from async code so it
        never blocks the event loop.

        Respects global pause set by ``pause_until()``.
        """
        while True:
            with self._lock:
                now = time.time()
                s   = self._state

                # If globally paused, compute remaining wait and release lock
                if now < s["paused_until"]:
                    wait = s["paused_until"] - now
                    # Cap sleep to 5s chunks so we re-check periodically
                    time.sleep(min(wait, 5.0))
                    continue

                elapsed = now - s["last_time"]
                tokens  = min(s["max_tokens"], s["tokens"] + elapsed * s["rate"])
                s["last_time"] = now
                if tokens >= 1.0:
                    s["tokens"] = tokens - 1.0
                    return
                # Not enough tokens yet — compute wait and release lock
                s["tokens"] = tokens
                wait = (1.0 - tokens) / s["rate"]
            time.sleep(min(wait, 0.5))   # re-check at most every 0.5 s


# ══════════════════════════════════════════════════════════════════════════════
#  CHILD-PROCESS WORKER  (must be a top-level function for multiprocessing spawn)
# ══════════════════════════════════════════════════════════════════════════════

def year_worker(
    year: int,
    args_dict: dict,
    rate_limiter: SharedRateLimiter,
    log_dir: str,
) -> None:
    """
    Runs inside a dedicated child process.

    Sets up file-based logging for this year, then drives the async scraper
    (up to 10 concurrent aiohttp workers) against the CourtListener API.
    Each process uses its own checkpoint_{year}.json so processes don't
    interfere with each other.
    """
    # ── per-year logging ────────────────────────────────────────────────────
    log_path = Path(log_dir) / f"scrape_{year}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    fmt  = logging.Formatter(
        f"[{year}] %(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)

    fh = logging.FileHandler(str(log_path), mode="a", encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    # ── import scraper from same directory ──────────────────────────────────
    here = str(Path(__file__).parent)
    if here not in sys.path:
        sys.path.insert(0, here)

    from federal_appellate_scraper import FederalAppellateScraper, CIRCUIT_COURTS  # noqa

    output_dir = str(Path(args_dict["output_dir"]))
    courts     = args_dict.get("courts") or CIRCUIT_COURTS

    logging.info("")
    logging.info("=" * 80)
    logging.info(f"=== Year {year} started  (PID {os.getpid()}) ===")
    logging.info(f"    Resumed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"    courts     : {', '.join(courts)}")
    logging.info(f"    limit      : {args_dict.get('limit') or 'unlimited (--all)'}")
    logging.info(f"    output     : {output_dir}/{{Circuit Name}}/{year}/{{case}}/")
    logging.info(f"    checkpoint : {output_dir}/checkpoint_{year}.json")

    scraper = FederalAppellateScraper(
        api_token    = args_dict["api_token"],
        output_dir   = output_dir,
        use_async    = True,           # always async inside each process
        rate_limiter = rate_limiter,   # shared cross-process bucket
        year         = year,           # per-year checkpoint isolation
    )

    try:
        scraper.scrape_full(
            courts        = courts,
            start_date    = f"{year}-01-01",
            end_date      = f"{year}-12-31",
            max_per_court = args_dict.get("limit"),  # None = unlimited
        )
        logging.info(f"=== Year {year} COMPLETE ===")
    except Exception as exc:
        logging.exception(f"=== Year {year} FAILED: {exc} ===")
        raise


# ══════════════════════════════════════════════════════════════════════════════
#  LIVE STATUS MONITOR  (runs in main process between sleeps)
# ══════════════════════════════════════════════════════════════════════════════

def _tail_log(log_file: Path, chars: int = 90) -> str:
    """Return the last `chars` characters of a log file (single line, stripped)."""
    try:
        with open(log_file, "rb") as f:
            f.seek(0, 2)
            f.seek(max(0, f.tell() - 400))
            tail = f.read().decode("utf-8", errors="replace")
        last = tail.strip().split("\n")[-1].strip()
        return last[-chars:] if len(last) > chars else last
    except Exception:
        return ""


def _read_checkpoint_progress(output_dir: str, year: int) -> str:
    """Read checkpoint file and return a short progress string like '5/13 courts'."""
    try:
        cp_file = Path(output_dir) / f"checkpoint_{year}.json"
        if not cp_file.exists():
            return "0/13"
        with open(cp_file, 'r') as f:
            data = json.load(f)
        phases = data.get('court_phases', {})
        completed = sum(1 for v in phases.values() if v == 'completed')
        in_progress = sum(1 for v in phases.values() if v != 'completed')
        if in_progress > 0:
            return f"{completed}+{in_progress}/13"
        return f"{completed}/13"
    except Exception:
        return "?"


def print_status(
    years: list[int],
    log_dir: str,
    processes: list,
    start_time: float,
    output_dir: str = "downloads",
) -> None:
    W = 100
    print(f"\n{'─' * W}")
    print(f"  {'YEAR':<7}{'PID':<9}{'STATUS':<12}{'COURTS':<14}LAST LOG LINE")
    print(f"{'─' * W}")
    for year, proc in zip(years, processes):
        if proc.is_alive():
            status = "RUNNING"
        elif proc.exitcode == 0:
            status = "DONE"
        else:
            status = f"FAIL({proc.exitcode})"
        log_file = Path(log_dir) / f"scrape_{year}.log"
        last     = _tail_log(log_file, chars=70)
        courts   = _read_checkpoint_progress(output_dir, year)
        print(f"  {year:<7}{str(proc.pid):<9}{status:<12}{courts:<14}{last}")
    elapsed = time.time() - start_time
    done    = sum(1 for p in processes if not p.is_alive())
    print(f"{'─' * W}")
    print(f"  Elapsed: {elapsed/3600:.2f}h  |  "
          f"Done: {done}/{len(processes)}  |  "
          f"Log dir: {log_dir}/")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Multiprocess + async orchestrator — "
            "13 processes x 10 async workers, shared rate limiter, per-year checkpoints"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
  # Full production run (unlimited cases, all years, all courts)
  python run_all_years.py --all

  # Quick test: 1 case per court per year
  python run_all_years.py --limit 1

  # Specific years, 100 cases/court/year
  python run_all_years.py --years 2020 2021 2022 --limit 100

  # Specific courts only
  python run_all_years.py --limit 10 --courts ca1 ca9 cadc

  # Conservative rate budget (half quota, ~2400 req/hr)
  python run_all_years.py --all --rate-budget 2400
        """,
    )

    # ── required: exactly one of --all or --limit ────────────────────────────
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument(
        "--all",
        action="store_true",
        help="Scrape all available cases (no limit). Use for production runs.",
    )
    scope.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="Max cases per court per year. Use for testing (e.g. --limit 1).",
    )

    parser.add_argument("--api-token",   default=DEFAULT_TOKEN)
    parser.add_argument(
        "--years",
        nargs="+",
        default=["all"],
        help='Specific years to scrape, or "all" (default). Example: --years 2020 2021',
    )
    parser.add_argument(
        "--courts",
        nargs="+",
        default=None,
        help="Specific court IDs to scrape (default: all 13). Example: --courts ca1 ca9 cadc",
    )
    parser.add_argument("--output-dir",  default="downloads")
    parser.add_argument("--log-dir",     default="logs")
    parser.add_argument(
        "--rate-budget",
        type=float,
        default=4000,
        help="Total API requests/hour shared across ALL processes (default: 4000)",
    )
    parser.add_argument(
        "--status-interval",
        type=int,
        default=30,
        help="Seconds between live status table prints (default: 30)",
    )
    args = parser.parse_args()

    # ── validate API token ───────────────────────────────────────────────────
    if not args.api_token:
        sys.exit(
            "Error: API token required. Use --api-token TOKEN or set "
            "COURTLISTENER_TOKEN env var."
        )

    # ── resolve limit ────────────────────────────────────────────────────────
    limit = None if args.all else args.limit

    # ── resolve years ────────────────────────────────────────────────────────
    if "all" in [y.lower() for y in args.years]:
        years = ALL_YEARS
    else:
        try:
            years = sorted(set(int(y) for y in args.years))
        except ValueError:
            sys.exit("Error: --years must be integers or 'all'")

    # ── build picklable args dict ────────────────────────────────────────────
    args_dict = {
        "api_token"  : args.api_token,
        "output_dir" : args.output_dir,
        "limit"      : limit,
        "courts"     : args.courts,
    }

    Path(args.log_dir).mkdir(parents=True, exist_ok=True)

    courts_display = ", ".join(args.courts) if args.courts else f"all {len(__import__('federal_appellate_scraper').CIRCUIT_COURTS) if False else 13}"
    print("=" * 72)
    print("  Federal Appellate Scraper  —  Multiprocess Orchestrator")
    print("=" * 72)
    print(f"  Processes     : {len(years)} (one per year)")
    print(f"  Workers/proc  : {WORKERS_PER_PROCESS} async workers")
    print(f"  Total slots   : {len(years) * WORKERS_PER_PROCESS} concurrent connections")
    print(f"  Rate budget   : {int(args.rate_budget):,} req/hr  SHARED across all processes")
    print(f"  Per-process   : ~{int(args.rate_budget / max(len(years), 1)):,} req/hr effective")
    print(f"  Limit         : {limit or 'unlimited (--all)'} per court per year")
    print(f"  Years         : {', '.join(str(y) for y in years)}")
    print(f"  Courts        : {', '.join(args.courts) if args.courts else 'all 13'}")
    print(f"  Output        : {args.output_dir}/<Circuit Name>/<year>/<case>/")
    print(f"  Checkpoints   : {args.output_dir}/checkpoint_<year>.json  (one per year)")
    print(f"  Logs          : {args.log_dir}/scrape_<year>.log")
    print("=" * 72)
    print(f"\nTip: tail -f {args.log_dir}/scrape_{years[0]}.log   (follow any year live)\n")

    # ── shared rate limiter via Manager ─────────────────────────────────────
    mp_ctx   = multiprocessing.get_context("spawn")
    manager  = mp_ctx.Manager()
    rate_lim = SharedRateLimiter(manager, max_per_hour=args.rate_budget)

    start_time = time.time()
    processes: list = []

    for year in years:
        p = mp_ctx.Process(
            target = year_worker,
            args   = (year, args_dict, rate_lim, args.log_dir),
            name   = f"scraper-{year}",
            daemon = False,
        )
        p.start()
        print(f"  Started PID {str(p.pid):<7} → year {year}   "
              f"log: {args.log_dir}/scrape_{year}.log")
        processes.append(p)
        time.sleep(PROCESS_STAGGER_SEC)

    print(f"\n{len(processes)} processes running. "
          f"Status update every {args.status_interval}s  (Ctrl-C to stop all)\n")

    # ── monitor loop ─────────────────────────────────────────────────────────
    try:
        while any(p.is_alive() for p in processes):
            time.sleep(args.status_interval)
            print_status(years, args.log_dir, processes, start_time, args.output_dir)
    except KeyboardInterrupt:
        print("\n\nInterrupt — terminating all child processes …")
        for p in processes:
            if p.is_alive():
                p.terminate()

    for p in processes:
        p.join(timeout=15)

    elapsed = time.time() - start_time
    success = sum(1 for p in processes if p.exitcode == 0)
    failed  = len(processes) - success

    print("\n" + "=" * 72)
    print(f"  Finished in {elapsed / 3600:.2f} hours")
    print(f"  Success: {success}/{len(processes)}    Failed: {failed}/{len(processes)}")
    print(f"  Logs: {args.log_dir}/")
    print("=" * 72)


if __name__ == "__main__":
    main()
