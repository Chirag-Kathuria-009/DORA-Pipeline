"""End-to-end smoke test: ingestion → streaming → Iceberg.

Standalone script (NOT a pytest test — the filename intentionally does not match
pytest's test_*.py pattern, so a 100-second subprocess run is never auto-collected).
Run it directly:

    python -m tests.e2e_smoke_test

What it does:
  1. Launches the streaming job (processing.streaming_job) as a subprocess FIRST and
     lets Spark boot + subscribe. This ordering is required: the streaming job reads
     Kafka with startingOffsets="latest", so any records produced before it subscribes
     would be missed.
  2. Launches the incident generator (ingestion.simulator.incident_generator) for a
     fixed window so its output lands in the already-running stream.
  3. Drains (waits one+ trigger interval) so the final 10-second micro-batch commits,
     then stops both subprocesses.
  4. Queries dora.incidents_classified (and dora.audit_log) via PyIceberg — filtered to
     THIS run only (timestamp >= run start) so prior data never inflates the result.
  5. Asserts: >=100 records, no null dora_severity, every CRITICAL has
     bafin_notification_required=True, no null record_hash.
  6. Prints a summary table and exits 0 (pass) / 1 (fail).

NOTE on record_hash: it is NOT a column on incidents_classified. Per decisions.md
(2026-06-05 | streaming), audit metadata lives in dora.audit_log. The record_hash
assertion is therefore evaluated against audit_log, the table that actually holds it.

Prerequisites (this script does NOT set them up):
  - Docker stack running:        docker compose up -d   (kafka, minio healthy)
  - Kafka topics created:        python -m ingestion.kafka.topics_setup
  - MinIO bucket created:        python -m storage.s3_config
  - PySpark installed in .venv:  pip install pyspark==3.5.1
  - Iceberg tables exist:        created automatically by the streaming job at startup
"""

import os
import pathlib
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from storage.iceberg_tables import _load_catalog

# ── Tunable timing / thresholds ─────────────────────────────────────────────────
# Total active window ≈ WARMUP + GENERATOR + DRAIN (plus Spark boot). Longer than a
# flat 70s because Spark must subscribe before the generator runs (latest offsets)
# and the final 10s-trigger micro-batch must be allowed to commit.
STREAM_WARMUP_SECS = 25      # let Spark boot + the streaming query subscribe to Kafka
GENERATOR_SECS     = 60      # how long the generator produces events
DRAIN_SECS         = 20      # wait >1 trigger interval so the last micro-batch commits
GENERATOR_RATE     = 3.0     # events/sec → ~180 events, comfortable margin over MIN_RECORDS
MIN_RECORDS        = 100     # assertion threshold

_PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
_LOG_DIR      = _PROJECT_ROOT / "tests" / "_smoke_logs"


# ── Subprocess helpers ───────────────────────────────────────────────────────────

def _launch(module: str, extra_args: list[str], log_path: pathlib.Path) -> subprocess.Popen:
    """Launch `python -m <module>` from the project root, streaming output to a log file.

    Uses the same interpreter running this script (sys.executable) so the job runs
    inside the active virtualenv. stdout and stderr are redirected to log_path so a
    failure can be diagnosed without interleaving on the console.

    Args:
        module:     Dotted module path to run with -m (e.g. "processing.streaming_job").
        extra_args: Additional CLI arguments appended after the module.
        log_path:   File to capture combined stdout/stderr into.

    Returns:
        The running Popen handle.
    """
    log_handle = open(log_path, "w", encoding="utf-8")
    return subprocess.Popen(
        [sys.executable, "-m", module, *extra_args],
        cwd=str(_PROJECT_ROOT),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )


def _fail_fast_if_dead(proc: subprocess.Popen, name: str, log_path: pathlib.Path) -> None:
    """Abort the smoke test immediately if a subprocess has already exited.

    Catches the common case where a job dies on startup (e.g. pyspark not installed,
    Kafka unreachable) so the test reports the real error instead of waiting out the
    full window only to find zero rows.

    Args:
        proc:     The subprocess to check.
        name:     Human-readable name for the error message.
        log_path: Log file whose tail is printed on early death.

    Raises:
        SystemExit: with code 1 if the process has already terminated.
    """
    if proc.poll() is not None:
        print(f"\n[FATAL] {name} exited early with code {proc.returncode}. Last log lines:")
        print(_tail(log_path, 30))
        sys.exit(1)


def _terminate(proc: subprocess.Popen, name: str) -> None:
    """Stop a subprocess, preferring a graceful terminate then a hard kill.

    proc.terminate() sends SIGTERM on POSIX (the streaming job's graceful-shutdown
    handler catches it) and TerminateProcess on Windows. Either way, micro-batches
    already committed to Iceberg persist. Falls back to kill() if the process does
    not exit within the grace period.

    Args:
        proc: The subprocess to stop.
        name: Human-readable name for status output.
    """
    if proc.poll() is not None:
        return
    print(f"  stopping {name} ...", flush=True)
    proc.terminate()
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        print(f"  {name} did not stop gracefully — killing.", flush=True)
        proc.kill()
        proc.wait(timeout=10)


def _tail(path: pathlib.Path, n: int) -> str:
    """Return the last n lines of a text file, or a placeholder if it is empty/missing.

    Args:
        path: File to read.
        n:    Number of trailing lines to return.

    Returns:
        The joined tail lines, or a short note if nothing is available.
    """
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except FileNotFoundError:
        return f"  (no log file at {path})"
    return "\n".join(lines[-n:]) if lines else "  (log empty)"


# ── Iceberg queries (PyIceberg, no Spark needed) ─────────────────────────────────

def _query_rows(identifier: str, since: datetime, time_col: str) -> list[dict]:
    """Load an Iceberg table and return rows produced at/after `since`.

    Filtering is done in Python over the scanned Arrow rows (a few hundred at most),
    which avoids any dependency on PyIceberg expression-API specifics and keeps the
    result scoped to the current run.

    Args:
        identifier: Fully-qualified table name, e.g. "dora.incidents_classified".
        since:      tz-aware UTC cutoff; rows with time_col < since are dropped.
        time_col:   Name of the timestamp column to filter on.

    Returns:
        A list of row dicts (one per matching record).
    """
    catalog = _load_catalog()
    table = catalog.load_table(identifier)
    rows = table.scan().to_arrow().to_pylist()
    return [r for r in rows if r.get(time_col) is not None and r[time_col] >= since]


# ── Assertions & reporting ───────────────────────────────────────────────────────

def _run_assertions(classified: list[dict], audit: list[dict]) -> list[tuple[str, bool, str]]:
    """Evaluate the four smoke-test assertions and return per-check results.

    Args:
        classified: This run's rows from dora.incidents_classified.
        audit:      This run's rows from dora.audit_log (source of record_hash).

    Returns:
        A list of (description, passed, detail) tuples, one per assertion.
    """
    results: list[tuple[str, bool, str]] = []

    # 1. At least MIN_RECORDS records exist
    results.append((
        f"records >= {MIN_RECORDS}",
        len(classified) >= MIN_RECORDS,
        f"found {len(classified)}",
    ))

    # 2. No null dora_severity
    null_sev = [r for r in classified if r.get("dora_severity") is None]
    results.append((
        "no null dora_severity",
        len(null_sev) == 0,
        f"{len(null_sev)} null",
    ))

    # 3. Every CRITICAL has bafin_notification_required = True
    bad_critical = [
        r for r in classified
        if r.get("dora_severity") == "critical" and r.get("bafin_notification_required") is not True
    ]
    results.append((
        "all CRITICAL → bafin_notification_required=True",
        len(bad_critical) == 0,
        f"{len(bad_critical)} violations",
    ))

    # 4. No null record_hash (in audit_log — see module docstring / decisions.md)
    null_hash = [r for r in audit if not r.get("record_hash")]
    results.append((
        "no null record_hash (audit_log)",
        len(audit) > 0 and len(null_hash) == 0,
        f"{len(audit)} audit rows, {len(null_hash)} null hash",
    ))

    return results


def _print_summary(classified: list[dict]) -> None:
    """Print the total / by-severity / avg-financial-impact summary table.

    Args:
        classified: This run's rows from dora.incidents_classified.
    """
    by_sev = Counter(r.get("dora_severity") for r in classified)
    impacts = [r["financial_impact_eur"] for r in classified if r.get("financial_impact_eur") is not None]
    avg_impact = sum(impacts) / len(impacts) if impacts else 0.0

    print("\n" + "=" * 52)
    print("  SMOKE TEST SUMMARY — dora.incidents_classified")
    print("=" * 52)
    print(f"  {'total records':<28} {len(classified):>20,}")
    for sev in ("critical", "major", "minor"):
        print(f"  {'  ' + (sev or 'unknown'):<28} {by_sev.get(sev, 0):>20,}")
    unknown = by_sev.get(None, 0)
    if unknown:
        print(f"  {'  unknown/null':<28} {unknown:>20,}")
    print(f"  {'avg financial_impact_eur':<28} {avg_impact:>20,.2f}")
    print("=" * 52)


# ── Orchestration ────────────────────────────────────────────────────────────────

def run_smoke_test() -> int:
    """Run the full ingestion→streaming→Iceberg smoke test and return an exit code.

    Launches the streaming job, then the generator, drains the final micro-batch,
    stops both, queries Iceberg for this run's records, prints a summary, and
    evaluates the assertions.

    Returns:
        0 if every assertion passed, 1 otherwise.
    """
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    stream_log = _LOG_DIR / "streaming_job.log"
    gen_log    = _LOG_DIR / "incident_generator.log"

    run_start = datetime.now(timezone.utc)
    print(f"Run start (UTC): {run_start.isoformat()}")
    print(f"Logs: {_LOG_DIR}\n")

    stream_proc = None
    gen_proc = None
    try:
        # 1. Streaming first so it subscribes before any events are produced.
        print(f"[1/5] launching streaming job (warmup {STREAM_WARMUP_SECS}s for Spark boot+subscribe) ...", flush=True)
        stream_proc = _launch("processing.streaming_job", [], stream_log)
        time.sleep(STREAM_WARMUP_SECS)
        _fail_fast_if_dead(stream_proc, "streaming job", stream_log)

        # 2. Generator for a fixed window.
        print(f"[2/5] launching generator for {GENERATOR_SECS}s at {GENERATOR_RATE}/s ...", flush=True)
        gen_proc = _launch("ingestion.simulator.incident_generator", ["--rate", str(GENERATOR_RATE)], gen_log)
        time.sleep(2)
        _fail_fast_if_dead(gen_proc, "incident generator", gen_log)
        time.sleep(GENERATOR_SECS)

        # 3. Stop generator, then drain so the last micro-batch commits.
        print("[3/5] stopping generator, draining final micro-batch ...", flush=True)
        _terminate(gen_proc, "generator")
        time.sleep(DRAIN_SECS)

        # 4. Stop streaming.
        print("[4/5] stopping streaming job ...", flush=True)
        _terminate(stream_proc, "streaming job")
    finally:
        # Ensure nothing is left running if we error out mid-window.
        if gen_proc is not None:
            _terminate(gen_proc, "generator")
        if stream_proc is not None:
            _terminate(stream_proc, "streaming job")

    # 5. Query Iceberg (this run only) and evaluate.
    print("[5/5] querying Iceberg tables (this run only) ...", flush=True)
    classified = _query_rows("dora.incidents_classified", run_start, "timestamp")
    audit      = _query_rows("dora.audit_log", run_start, "processed_at")

    _print_summary(classified)

    results = _run_assertions(classified, audit)
    print("\nAssertions:")
    all_passed = True
    for desc, passed, detail in results:
        mark = "PASS" if passed else "FAIL"
        all_passed = all_passed and passed
        print(f"  [{mark}] {desc:<48} ({detail})")

    if all_passed:
        print("\nSMOKE TEST PASSED ✅")
        return 0

    print("\nSMOKE TEST FAILED ❌")
    print(f"  streaming log tail:\n{_tail(stream_log, 15)}")
    return 1


def main() -> None:
    """Entry point: run the smoke test and exit with its result code."""
    sys.exit(run_smoke_test())


if __name__ == "__main__":
    main()
