#!/usr/bin/env python3
"""
run_all_years.py — Orchestrator for the federal appellate scraper.

Modes
─────
  SEQUENTIAL (default, recommended):
    Runs years one at a time in the main process.
    Each year gets the FULL rate budget (4000 req/hr) with 10 async workers.
    Much faster per-year completion. No multiprocessing overhead.

  PARALLEL (--parallel):
    Spawns N child processes (one per year) sharing the rate budget.
    Each process effectively gets budget/N req/hr — rarely useful since the
    API rate limit (not parallelism) is the bottleneck.

Rate limiting
─────────────
  Total API budget : 4000 req/hr  (hard cap)
  Sequential mode  : single-process async limiter (no cross-process overhead)
  Parallel mode    : cross-process token-bucket via multiprocessing.Manager

Checkpoints
───────────
  Each year writes its own  downloads/checkpoint_{year}.json
  Re-running after a partial failure will skip already-completed courts
  for that year only.

Logging
───────
  Each year writes to:  logs/scrape_{year}.log
  Follow a single year:  tail -f logs/scrape_2018.log

Usage
─────
  # Full production run, sequential (recommended)
  python run_all_years.py --all

  # Test run: 1 case per court per year
  python run_all_years.py --limit 1

  # Specific years only
  python run_all_years.py --years 2020 2021 --all

  # Specific courts only
  python run_all_years.py --limit 10 --courts ca1 ca9 cadc

  # Parallel mode (not recommended — splits rate budget across processes)
  python run_all_years.py --all --parallel

  # Conservative rate budget
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
WORKERS_PER_PROCESS = 5
PROCESS_STAGGER_SEC = 8    # seconds between launching each child process


# ══════════════════════════════════════════════════════════════════════════════
#  SHARED CROSS-PROCESS TOKEN-BUCKET RATE LIMITER
# ══════════════════════════════════════════════════════════════════════════════

class LocalRateLimiter:
    """Single-process token-bucket rate limiter for sequential mode.

    Functionally identical to SharedRateLimiter but without multiprocessing
    overhead.  Safe for use from multiple asyncio workers within one process
    (via ``run_in_executor``).

    Also exposes ``pause_until(seconds)`` so the 429 handler in AsyncAPI
    works identically in both sequential and parallel mode.
    """

    def __init__(self, max_per_hour: float = 4000):
        import threading
        self._lock = threading.Lock()
        initial_tokens = min(60.0, float(max_per_hour))
        self._tokens       = initial_tokens
        self._last_time    = time.time()
        self._max_tokens   = float(max_per_hour)
        self._rate         = max_per_hour / 3600.0
        self._paused_until = 0.0

    def pause_until(self, seconds: float) -> None:
        with self._lock:
            resume_at = time.time() + seconds
            if resume_at > self._paused_until:
                self._paused_until = resume_at
                logging.warning(
                    f"Rate limiter PAUSED for {seconds:.0f}s "
                    f"(until {time.strftime('%H:%M:%S', time.localtime(resume_at))})"
                )

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.time()
                if now < self._paused_until:
                    wait = self._paused_until - now
                    time.sleep(min(wait, 5.0))
                    continue
                elapsed = now - self._last_time
                self._tokens = min(self._max_tokens, self._tokens + elapsed * self._rate)
                self._last_time = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate
            time.sleep(min(wait, 0.5))


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
#  YEAR WORKERS
# ══════════════════════════════════════════════════════════════════════════════

def _setup_year_logging(year: int, log_dir: str) -> None:
    """Configure per-year file + console logging."""
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


def _run_year(
    year: int,
    args_dict: dict,
    log_dir: str,
    rate_limiter=None,
) -> None:
    """Run the scraper for a single year.

    Works both as a standalone call (sequential mode) and inside a child
    process (parallel mode).  When *rate_limiter* is ``None`` a local
    single-process token-bucket limiter is created automatically.
    """
    _setup_year_logging(year, log_dir)

    here = str(Path(__file__).parent)
    if here not in sys.path:
        sys.path.insert(0, here)

    from federal_appellate_scraper import FederalAppellateScraper, CIRCUIT_COURTS  # noqa

    output_dir = str(Path(args_dict["output_dir"]))
    courts     = args_dict.get("courts") or CIRCUIT_COURTS

    # Create a local rate limiter for sequential mode (full budget, single process)
    if rate_limiter is None:
        budget = args_dict.get("rate_budget", 4000)
        rate_limiter = LocalRateLimiter(max_per_hour=budget)
        limiter_mode = f"local ({int(budget)} req/hr)"
    else:
        limiter_mode = "shared (cross-process)"

    logging.info("")
    logging.info("=" * 80)
    logging.info(f"=== Year {year} started  (PID {os.getpid()}) ===")
    logging.info(f"    Resumed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"    courts     : {', '.join(courts)}")
    logging.info(f"    limit      : {args_dict.get('limit') or 'unlimited (--all)'}")
    logging.info(f"    output     : {output_dir}/{{Circuit Name}}/{year}/{{case}}/")
    logging.info(f"    checkpoint : {output_dir}/checkpoint_{year}.json")
    logging.info(f"    limiter    : {limiter_mode}")

    scraper = FederalAppellateScraper(
        api_token    = args_dict["api_token"],
        output_dir   = output_dir,
        use_async    = True,
        rate_limiter = rate_limiter,
        year         = year,
    )

    try:
        scraper.scrape_full(
            courts        = courts,
            start_date    = f"{year}-01-01",
            end_date      = f"{year}-12-31",
            max_per_court = args_dict.get("limit"),
        )
        logging.info(f"=== Year {year} COMPLETE ===")
    except Exception as exc:
        logging.exception(f"=== Year {year} FAILED: {exc} ===")
        raise


def year_worker(
    year: int,
    args_dict: dict,
    rate_limiter: SharedRateLimiter,
    log_dir: str,
) -> None:
    """Parallel-mode child-process entry point (multiprocessing.spawn)."""
    _run_year(year, args_dict, log_dir, rate_limiter=rate_limiter)


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
            "Orchestrator for the federal appellate scraper — "
            "sequential (default) or parallel mode, async workers, per-year checkpoints"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
  # Full production run, sequential (recommended — full rate budget per year)
  python run_all_years.py --all

  # Quick test: 1 case per court
  python run_all_years.py --limit 1 --years 2025 --courts ca9

  # Specific years, 50 cases/court/year
  python run_all_years.py --years 2020 2021 --limit 50

  # Specific courts only
  python run_all_years.py --limit 10 --courts ca1 ca9 cadc

  # Parallel mode (splits rate budget across processes — rarely useful)
  python run_all_years.py --all --parallel

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
        help="Seconds between live status table prints in parallel mode (default: 30)",
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help=(
            "Run years in parallel (one process per year, shared rate budget). "
            "Default is sequential — each year gets the full rate budget."
        ),
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
        "api_token"   : args.api_token,
        "output_dir"  : args.output_dir,
        "limit"       : limit,
        "courts"      : args.courts,
        "rate_budget" : args.rate_budget,
    }

    courts_display = ", ".join(args.courts) if args.courts else f"all {len(__import__('federal_appellate_scraper').CIRCUIT_COURTS) if False else 13}"
    mode_str = "PARALLEL" if args.parallel else "SEQUENTIAL"
    print("=" * 72)
    print("  Federal Appellate Scraper  —  Orchestrator")
    print("=" * 72)
    print(f"  Mode          : {mode_str}")
    if args.parallel:
        print(f"  Processes     : {len(years)} (one per year)")
        print(f"  Workers/proc  : {WORKERS_PER_PROCESS} async workers")
        print(f"  Total slots   : {len(years) * WORKERS_PER_PROCESS} concurrent connections")
        print(f"  Rate budget   : {int(args.rate_budget):,} req/hr  SHARED across all processes")
        print(f"  Per-process   : ~{int(args.rate_budget / max(len(years), 1)):,} req/hr effective")
    else:
        print(f"  Workers       : {WORKERS_PER_PROCESS} async workers")
        print(f"  Rate budget   : {int(args.rate_budget):,} req/hr  (full budget per year)")
    print(f"  Limit         : {limit or 'unlimited (--all)'} per court per year")
    print(f"  Years         : {', '.join(str(y) for y in years)}")
    print(f"  Courts        : {', '.join(args.courts) if args.courts else 'all 13'}")
    print(f"  Output        : {args.output_dir}/<Circuit Name>/<year>/<case>/")
    print(f"  Checkpoints   : {args.output_dir}/checkpoint_<year>.json  (one per year)")
    print(f"  Logs          : {args.log_dir}/scrape_<year>.log")
    print("=" * 72)
    print(f"\nTip: tail -f {args.log_dir}/scrape_{years[0]}.log   (follow any year live)\n")

    Path(args.log_dir).mkdir(parents=True, exist_ok=True)

    start_time = time.time()

    if args.parallel:
        # ── PARALLEL MODE: multiprocessing with shared rate limiter ───────
        mp_ctx   = multiprocessing.get_context("spawn")
        manager  = mp_ctx.Manager()
        rate_lim = SharedRateLimiter(manager, max_per_hour=args.rate_budget)

        processes: list = []
        for year in years:
            p = mp_ctx.Process(
                target = year_worker,
                args   = (year, args_dict, rate_lim, args.log_dir),
                name   = f"scraper-{year}",
                daemon = True,   # die with parent — prevents orphaned processes
            )
            p.start()
            print(f"  Started PID {str(p.pid):<7} → year {year}   "
                  f"log: {args.log_dir}/scrape_{year}.log")
            processes.append(p)
            time.sleep(PROCESS_STAGGER_SEC)

        print(f"\n{len(processes)} processes running. "
              f"Status update every {args.status_interval}s  (Ctrl-C to stop all)\n")

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

    else:
        # ── SEQUENTIAL MODE: one year at a time, full rate budget ─────────
        completed = 0
        failed_years: list[int] = []

        for i, year in enumerate(years, 1):
            print(f"\n{'─' * 72}")
            print(f"  [{i}/{len(years)}]  Year {year}   "
                  f"(rate budget: {int(args.rate_budget):,} req/hr)")
            print(f"{'─' * 72}")

            try:
                _run_year(year, args_dict, args.log_dir, rate_limiter=None)
                completed += 1
            except KeyboardInterrupt:
                print(f"\n\nInterrupt during year {year} — stopping.")
                print(f"  Resume with: python run_all_years.py "
                      f"{'--all' if limit is None else f'--limit {limit}'} "
                      f"--years {' '.join(str(y) for y in years[i-1:])}")
                break
            except Exception as exc:
                logging.error(f"Year {year} failed: {exc}")
                failed_years.append(year)
                continue

        elapsed = time.time() - start_time
        print("\n" + "=" * 72)
        print(f"  Finished in {elapsed / 3600:.2f} hours")
        print(f"  Completed: {completed}/{len(years)}")
        if failed_years:
            print(f"  Failed: {', '.join(str(y) for y in failed_years)}")
            print(f"  Re-run: python run_all_years.py "
                  f"{'--all' if limit is None else f'--limit {limit}'} "
                  f"--years {' '.join(str(y) for y in failed_years)}")
        print(f"  Logs: {args.log_dir}/")
        print("=" * 72)


if __name__ == "__main__":
    main()
