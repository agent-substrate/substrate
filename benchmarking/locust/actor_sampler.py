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

"""Samples boomer's locust_actors gauge into a CSV and summarizes it.

boomer counts the actors it drives by the lifecycle state it last observed
(internal/benchmarking/boomer/metrics). The runner polls boomer's /metrics
endpoint while the test runs, so the actor population reaches the uploaded
results without a Prometheus server in the loop.
"""

import csv
import threading
import time
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

from prometheus_client.parser import text_string_to_metric_families

ACTORS_METRIC = "locust_actors"
HISTORY_COLUMNS = ("Timestamp", "User Class", "State", "Count")
FETCH_TIMEOUT_SECONDS = 2

# Keys summarize_actor_history() always returns, so a trial_summary row has
# the same shape whether or not the test drove actors through boomer.
EMPTY_ACTOR_SUMMARY: dict[str, Any] = {
    "running_actors_peak": None,
    "running_actors_p50": None,
    "running_actors_p90": None,
    "live_actors_peak": None,
    "hibernate_pending_actors_peak": None,
    "crashed_actors_final": None,
}


def parse_actor_counts(text: str) -> dict[tuple[str, str], float]:
    """Returns {(user_class, state): count} from a Prometheus text scrape."""
    counts: dict[tuple[str, str], float] = {}
    for family in text_string_to_metric_families(text):
        if family.name != ACTORS_METRIC:
            continue
        for sample in family.samples:
            key = (sample.labels.get("user_class", ""), sample.labels.get("state", ""))
            counts[key] = sample.value
    return counts


class ActorSampler:
    """Polls url every interval seconds and appends locust_actors to out_csv.

    A failed scrape writes nothing for that tick, so a gap in the history is
    a missed sample, never a zero. Only the first failure is logged: boomer
    takes a moment to bind its port, and a test type without boomer never
    starts a sampler at all.
    """

    def __init__(self, url: str, out_csv: Path, interval: float,
                 log: Callable[[str], None]):
        self._url = url
        self._out_csv = out_csv
        self._interval = interval
        self._log = log
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=FETCH_TIMEOUT_SECONDS + self._interval)

    def _fetch(self) -> dict[tuple[str, str], float]:
        with urllib.request.urlopen(self._url, timeout=FETCH_TIMEOUT_SECONDS) as resp:
            return parse_actor_counts(resp.read().decode("utf-8"))

    def _run(self) -> None:
        failure_logged = False
        with open(self._out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(HISTORY_COLUMNS)
            while not self._stop.is_set():
                try:
                    counts = self._fetch()
                except Exception as e:
                    if not failure_logged:
                        self._log(f"Notice: could not sample {self._url}: {e}")
                        failure_logged = True
                else:
                    ts = int(time.time())
                    for (user_class, state), count in sorted(counts.items()):
                        writer.writerow((ts, user_class, state, int(count)))
                    f.flush()
                self._stop.wait(self._interval)


def _percentile(sorted_values: list[float], q: float) -> float:
    return sorted_values[min(int(len(sorted_values) * q), len(sorted_values) - 1)]


def summarize_actor_history(history_csv: Path | None) -> dict[str, Any]:
    """Reduces an ActorSampler CSV to trial-level actor counts.

    Counts are summed across user classes at each timestamp. Returns
    EMPTY_ACTOR_SUMMARY when there is no file or it holds no samples.
    """
    summary = dict(EMPTY_ACTOR_SUMMARY)
    if history_csv is None or not history_csv.exists():
        return summary

    # timestamp -> state -> count
    by_ts: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    with open(history_csv, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                by_ts[int(row["Timestamp"])][row["State"]] += float(row["Count"])
            except (KeyError, TypeError, ValueError):
                continue
    if not by_ts:
        return summary

    samples = [by_ts[ts] for ts in sorted(by_ts)]
    running = sorted(s.get("running", 0.0) for s in samples)
    summary["running_actors_peak"] = int(running[-1])
    summary["running_actors_p50"] = int(_percentile(running, 0.50))
    summary["running_actors_p90"] = int(_percentile(running, 0.90))
    summary["live_actors_peak"] = int(max(sum(s.values()) for s in samples))
    summary["hibernate_pending_actors_peak"] = int(
        max(s.get("hibernate_pending", 0.0) for s in samples))
    summary["crashed_actors_final"] = int(samples[-1].get("crashed", 0.0))
    return summary
