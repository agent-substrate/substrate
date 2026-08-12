#!/usr/bin/env python3

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
ci-failure-analysis.py — categorize CI failures by root cause and surface flaky tests.

Instead of treating all CI failures as "flakiness", this script fetches job logs
from failed runs and classifies each failure into one of several categories:

  no_free_workers   — envtest/integration tests fail because no worker is assigned
                      ("no free workers available") — resource contention in tests
  e2e_timeout       — e2e tests time out waiting for actor/service responses or
                      kubectl wait conditions; upstream connect errors
  gcs_access        — GCS/S3 bucket access denied or not found when fetching
                      sandbox assets (credential/IAM issue)
  named_test_fail   — a specific Go test emitted "--- FAIL: TestName"
  verify_fail       — hack/verify-all.sh reported "Verification failed" or a diff
  license_check     — hack/verify/licenses.sh specifically produced a diff
  log_fetch_failed  — job log could not be fetched (expired, rate-limited, etc.)
  unknown           — log fetched but could not be classified

Usage
─────
  # Analyse the last N runs (fetches up to --fetch-limit runs, processes --sample failed ones)
  python hack/metrics/ci-failure-analysis.py \\
    --repo agent-substrate/substrate \\
    --fetch-limit 300 --sample 30

  # Save raw results for diffing later
  python hack/metrics/ci-failure-analysis.py \\
    --repo agent-substrate/substrate \\
    --fetch-limit 300 --sample 40 \\
    --save ci-failure-breakdown.json
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from typing import Optional


# ── GitHub helpers ────────────────────────────────────────────────────────────

def _gh_json(*args: str):
    cmd = ["gh"] + list(args)
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"gh error: {r.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return json.loads(r.stdout)


def _gh_text(*args: str) -> Optional[str]:
    """Return stdout as plain text, or None if the request failed (expired logs, rate limit, etc.)."""
    cmd = ["gh"] + list(args)
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        return None
    # strip ANSI codes and GitHub Actions timestamp prefixes
    text = r.stdout
    text = re.sub(r'\x1b\[[0-9;]*m', '', text)
    text = re.sub(r'^\d{4}-\d{2}-\d{2}T[\d:.Z]+ ', '', text, flags=re.MULTILINE)
    return text


# ── failure classification ────────────────────────────────────────────────────

# Ordered: first match wins.
#
# - Infrastructure patterns (no_free_workers, e2e_timeout, gcs_access) come
#   first so that an envtest run that dies on "no free workers available" is not
#   mis-attributed to the Go test that printed "--- FAIL:" before crashing.
# - named_test_fail comes before verify_fail: if a specific test name is
#   available it is more useful than a generic "verify failed". verify_fail's
#   old `hack/verify` substring matched the Actions command-echo line
#   ("Run hack/verify-all.sh"), causing almost every verify job to be swallowed
#   as verify_fail even when a named Go test was the real cause. The pattern
#   is now narrowed to match only explicit failure output from verify scripts.
# - Generic timeout strings ("deadline exceeded", "connection reset") are
#   intentionally omitted from e2e_timeout: they appear in ordinary unit-test
#   output and would over-trigger when infra patterns fire before named_test_fail.
_PATTERNS = [
    ("no_free_workers", re.compile(r'no free workers available', re.IGNORECASE)),
    ("e2e_timeout",     re.compile(
        r'timed out waiting for actor response'
        r'|timed out waiting for the condition'
        r'|connection timeout'
        r'|upstream connect error', re.IGNORECASE)),
    ("gcs_access",      re.compile(r'AccessDenied|NoSuchBucket|storage\.objects\.get', re.IGNORECASE)),
    ("license_check",   re.compile(r'verify/licenses\.sh resulted in a diff', re.IGNORECASE)),
    ("named_test_fail", re.compile(r'^--- FAIL: (\S+)', re.MULTILINE)),
    ("verify_fail",     re.compile(r'Verification failed|resulted in a diff', re.IGNORECASE)),
]


# Extracted so analyse() can findall() all failing test names without
# re-searching the log after classify() already determined the category.
_NAMED_TEST_RE = next(p for _, p in _PATTERNS if _ == "named_test_fail")


def classify(log: str) -> tuple[str, Optional[str]]:
    """Return (category, detail). detail is the first failing test for named_test_fail."""
    for category, pattern in _PATTERNS:
        m = pattern.search(log)
        if m:
            detail = m.group(1) if category == "named_test_fail" else None
            return category, detail
    return "unknown", None


# ── main analysis ─────────────────────────────────────────────────────────────

def analyse(repo: str, fetch_limit: int, sample: int, workflow: str) -> dict:
    print(f"Fetching last {fetch_limit} {workflow} runs ...", file=sys.stderr)
    runs = _gh_json("run", "list", "--repo", repo, "--workflow", workflow,
                    "--limit", str(fetch_limit),
                    "--json", "databaseId,conclusion,event,headBranch,createdAt")
    failed = [r for r in runs if r["conclusion"] == "failure"]
    print(f"  {len(failed)} failed runs found. Sampling {min(sample, len(failed))} ...",
          file=sys.stderr)
    failed = failed[:sample]

    results = []
    category_counts: Counter = Counter()
    test_fail_counts: Counter = Counter()
    by_category: dict[str, list] = defaultdict(list)

    for i, run in enumerate(failed, 1):
        run_id = run["databaseId"]
        branch = run["headBranch"]
        event  = run["event"]
        print(f"  [{i}/{len(failed)}] run {run_id} ({branch}) ...", file=sys.stderr)

        try:
            jobs = _gh_json("api",
                            f"repos/{repo}/actions/runs/{run_id}/jobs?per_page=100",
                            "--jq", "[.jobs[] | select(.conclusion==\"failure\") | {id,name}]")
        except SystemExit:
            results.append({"run_id": run_id, "branch": branch, "event": event,
                            "category": "log_fetch_failed", "detail": None, "job": None})
            category_counts["log_fetch_failed"] += 1
            continue

        if not jobs:
            results.append({"run_id": run_id, "branch": branch, "event": event,
                            "category": "unknown", "detail": None, "job": "no_failed_job"})
            category_counts["unknown"] += 1
            continue

        # Only the first failed job is inspected — sufficient for attributing the
        # run's root cause; a single infrastructure failure typically cascades.
        job = jobs[0]
        log = _gh_text("api", f"repos/{repo}/actions/jobs/{job['id']}/logs")
        if log is None:
            category, detail = "log_fetch_failed", None
        else:
            category, detail = classify(log)
        category_counts[category] += 1
        if category == "named_test_fail" and log is not None:
            for test in _NAMED_TEST_RE.findall(log):
                test_fail_counts[test] += 1
        by_category[category].append({"run_id": run_id, "branch": branch,
                                      "event": event, "job": job["name"],
                                      "detail": detail})
        results.append({"run_id": run_id, "branch": branch, "event": event,
                        "job": job["name"], "category": category, "detail": detail})

    return {
        "total_sampled": len(failed),
        "category_counts": dict(category_counts),
        "test_fail_counts": dict(test_fail_counts),
        "by_category": dict(by_category),
        "raw": results,
    }


# ── output ────────────────────────────────────────────────────────────────────

def print_report(data: dict) -> None:
    n = data["total_sampled"]
    counts = data["category_counts"]
    test_counts = data["test_fail_counts"]

    labels = {
        "no_free_workers":  "No free workers         (envtest resource contention)",
        "e2e_timeout":      "E2e timeout             (actor/condition wait; upstream connect)",
        "gcs_access":       "GCS/S3 access denied    (credential / IAM)",
        "license_check":    "License check diff      (hack/verify/licenses.sh)",
        "verify_fail":      "Other verify failure    (hack/verify-all.sh)",
        "named_test_fail":  "Named Go test failure   (--- FAIL: TestName)",
        "log_fetch_failed": "Log fetch failed        (expired / rate-limited)",
        "unknown":          "Unclassified",
    }

    print(f"\n{'─' * 66}")
    print(f"  CI failure breakdown  (sampled {n} most-recent failed runs)")
    print(f"{'─' * 66}")
    for key, label in labels.items():
        c = counts.get(key, 0)
        if c == 0:
            continue
        pct = c / n * 100
        bar = "█" * int(pct / 3)
        print(f"  {label}")
        print(f"    {c:3d} / {n}  ({pct:4.0f}%)  {bar}")
    print()

    if test_counts:
        print(f"  Named test failures (unique tests):")
        for test, count in sorted(test_counts.items(), key=lambda x: -x[1]):
            print(f"    {count}x  {test}")
        print()
    else:
        print("  No named Go test failures found in this sample.\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--repo", default="agent-substrate/substrate")
    parser.add_argument("--workflow", default="pr-workflow")
    parser.add_argument("--fetch-limit", type=int, default=300,
                        help="How many recent runs to fetch before filtering (default: 300)")
    parser.add_argument("--sample", type=int, default=30,
                        help="How many failed runs to pull logs for (default: 30; "
                             "each costs one API call per job)")
    parser.add_argument("--format", choices=["table", "json"], default="table")
    parser.add_argument("--save", metavar="FILE",
                        help="Save full results to JSON for later diffing")
    args = parser.parse_args()

    data = analyse(args.repo, args.fetch_limit, args.sample, args.workflow)

    if args.format == "json":
        print(json.dumps(data, indent=2))
    else:
        print_report(data)

    if args.save:
        with open(args.save, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Saved → {args.save}", file=sys.stderr)


if __name__ == "__main__":
    main()
