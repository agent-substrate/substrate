# Copyright 2026 The Agent Substrate Authors
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

"""Where runs are stored: opening stored runs, and finding the run another run is paired with.

The runner stores each run under
<dest>/runs/<test>/run_date=<YYYY-MM-DD>/run_ts=<unix s>/run_tag=<commit>/, with
<dest> a local directory or gs://bucket/path. This module is the only one that
knows that layout or touches gs://: it opens a stored run as a
BenchmarkRunContext whose identity comes from the layout and whose artifacts
are read from the store, a gs:// one downloaded on first use (read-only on the
store). GCS is read through the google-cloud-storage client when it is
installed (the runner image), else through `gcloud storage`.
"""

import re
import shutil
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

from .sources.base import ArtifactStore, BenchmarkRunContext, LocalArtifacts, RunIdentity
from .sources.run_results import read_stats

RUN_PATH = re.compile(r"^(?P<root>.*)/runs/(?P<test>[^/]+)/run_date=(?P<date>[^/]+)/run_ts=(?P<ts>\d+)"
                      r"/run_tag=(?P<tag>[^/]+)/?$")
DEFAULT_MAX_AGE_S = 6 * 3600


def identity_from_uri(uri: str) -> RunIdentity:
    """The identity of the run stored at uri: test_name, tag, run_date and run_ts too when it is in the runner's layout."""
    uri = uri.rstrip("/")
    m = RUN_PATH.match(uri)
    if not m:
        return RunIdentity(uri=uri)
    return RunIdentity(uri=uri, test_name=m["test"], tag=m["tag"], run_date=m["date"], run_ts=int(m["ts"]))


def run_uri(dest: str, test: str, run_date: str, run_ts: str, tag: str) -> str:
    """The uri of a run stored in the runner's layout; RUN_PATH parses it."""
    return f"{dest.rstrip('/')}/runs/{test}/run_date={run_date}/run_ts={run_ts}/run_tag={tag}"


class GcsArtifacts:
    """A run's artifacts under a gs:// prefix, each downloaded on first use into tmp. Never writes to GCS."""

    def __init__(self, uri: str, tmp: Path):
        self.uri = uri.rstrip("/")
        self.tmp = tmp
        self._fetched: dict[str, Optional[Path]] = {}

    def fetch(self, name: str) -> Optional[Path]:
        """Raises OSError on a read error."""
        if name not in self._fetched:
            self._fetched[name] = _gcs_download(f"{self.uri}/{name}", self.tmp / name)
        return self._fetched[name]


def open_run(uri: str, tmp: Path) -> BenchmarkRunContext:
    """A BenchmarkRunContext for a local run directory, or a gs:// run whose artifacts are downloaded under tmp."""
    uri = uri.rstrip("/")
    if not uri.startswith("gs://"):
        if not Path(uri).is_dir():
            raise FileNotFoundError(f"no run directory {uri}")
        return BenchmarkRunContext(identity_from_uri(uri), LocalArtifacts(Path(uri)))
    if not _gcs_list(f"{uri}/"):
        raise FileNotFoundError(f"no objects under {uri}/")
    tmp.mkdir(parents=True, exist_ok=True)
    local = Path(tempfile.mkdtemp(prefix="run-", dir=tmp))
    return BenchmarkRunContext(identity_from_uri(uri), GcsArtifacts(uri, local))


def dest_of(uri: str) -> Optional[str]:
    """The <dest> a run stored in the runner's layout lives under; None otherwise."""
    m = RUN_PATH.match(uri.rstrip("/"))
    return m["root"] if m else None


def find_paired_run(dest: str, test_name: str, tag: str, before_ts: int, max_age_s: int = DEFAULT_MAX_AGE_S) -> str:
    """The stored run of test_name paired with a run of tag at before_ts; raises LookupError when there is none.

    The pair is the run of test_name under dest with the same tag (commit) and
    the latest run_ts at or before before_ts, at most max_age_s earlier: in a
    suite, the earlier test of the same invocation. It must have a logs.txt or
    a stats.jsonl.
    """
    first = datetime.fromtimestamp(before_ts - max_age_s, tz=timezone.utc).date()
    last = datetime.fromtimestamp(before_ts, tz=timezone.utc).date()
    dates = [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]
    candidates = []
    for date in dates:
        for found in _list_runs(f"{dest.rstrip('/')}/runs/{test_name}/run_date={date}/"):
            other = identity_from_uri(found)
            if other.tag == tag and other.run_ts is not None and before_ts - max_age_s <= other.run_ts <= before_ts:
                candidates.append((other.run_ts, found))
    if not candidates:
        raise LookupError(f"no run of {test_name} with tag {tag} under {dest.rstrip('/')}/runs/ in the "
                          f"{max_age_s / 3600:g} h before run_ts {before_ts}")
    return max(candidates)[1]


def cluster_name(artifacts: ArtifactStore) -> str:
    """The cluster a run ran on (trial_summary's metadata_cluster_name); "" when not recorded.
    Raises ValueError when stats.jsonl is corrupt, OSError when it can't be read."""
    trial = read_stats(artifacts.fetch("stats.jsonl")).get("trial_summary") or {}
    measurements = trial.get("measurements")
    name = measurements.get("metadata_cluster_name") if isinstance(measurements, dict) else None
    return str(name) if name else ""


def data_timestamp(artifacts: ArtifactStore) -> str:
    """The timestamp the run's stats.jsonl rows carry (trial_summary's); "" without one
    or when stats.jsonl is corrupt (the evaluation then says why)."""
    try:
        stats = read_stats(artifacts.fetch("stats.jsonl"))
    except ValueError:
        return ""
    row = stats.get("trial_summary") or next(iter(stats.values()), {})
    return str(row.get("timestamp") or "")


def paired_run_finder(dest: str, tag: str, run_ts: int, artifacts: ArtifactStore, tmp: Path,
                      max_age_s: int = DEFAULT_MAX_AGE_S) -> Callable[[str], BenchmarkRunContext]:
    """BenchmarkRunContext.paired_run for the run of tag at run_ts under dest, whose artifacts are artifacts.

    The paired run of a test is find_paired_run's, opened under tmp and looked
    up once. It must have run on the same cluster as this run when both record
    one, checked on each call because this run's stats.jsonl exists only after
    the run. Any failure (no such run, a listing or permission error, an
    unreadable run or stats.jsonl) raises LookupError saying why.
    """
    looked_up: dict[str, BenchmarkRunContext | str] = {}

    def paired_run(test: str) -> BenchmarkRunContext:
        if test not in looked_up:
            try:
                looked_up[test] = open_run(find_paired_run(dest, test, tag, run_ts, max_age_s), tmp)
            except Exception as e:  # pylint: disable=broad-except
                # KeyError and IndexError are LookupErrors too, but not ours.
                looked_up[test] = str(e) if type(e) is LookupError else f"{type(e).__name__}: {e}"
        run = looked_up[test]
        if isinstance(run, str):
            raise LookupError(run)
        try:
            mine, theirs = cluster_name(artifacts), cluster_name(run.artifacts)
        except (ValueError, OSError) as e:
            raise LookupError(f"can't compare the clusters: {e}") from None
        if mine and theirs and mine != theirs:
            raise LookupError(f"cluster differs ({mine} vs {theirs})")
        return run

    return paired_run


def _list_runs(prefix: str) -> list[str]:
    """Run directories (with a logs.txt or stats.jsonl) under prefix, which ends in run_date=<date>/."""
    marks = ("/logs.txt", "/stats.jsonl")
    if not prefix.startswith("gs://"):
        found = [p for m in marks for p in Path(prefix).glob(f"run_ts=*/run_tag=*{m}")]
        return sorted({str(p.parent) for p in found})
    names = _gcs_list(prefix)
    return sorted({n.rsplit("/", 1)[0] for n in names if n.endswith(marks)})


def _split(uri: str) -> tuple[str, str]:
    bucket, _, path = uri[len("gs://"):].partition("/")
    return bucket, path


def _storage_client():
    """A google-cloud-storage client, or None when the library is not installed."""
    try:
        from google.cloud import storage  # pylint: disable=import-outside-toplevel
    except ImportError:
        return None
    return storage.Client()


def _gcs_list(prefix: str) -> list[str]:
    """gs:// URIs of the objects under prefix. Raises OSError on a listing error."""
    client = _storage_client()
    bucket, path = _split(prefix)
    if client is not None:
        return [f"gs://{bucket}/{b.name}" for b in client.list_blobs(bucket, prefix=path)]
    if not shutil.which("gcloud"):
        raise OSError("reading gs:// needs google-cloud-storage or gcloud")
    res = subprocess.run(["gcloud", "storage", "ls", "-r", prefix], capture_output=True, text=True, check=False)
    if res.returncode != 0:
        if "matched no objects" in res.stderr or "One or more URLs matched no objects" in res.stderr:
            return []
        raise OSError(f"gcloud storage ls {prefix}: {res.stderr.strip()[:300]}")
    return [line.strip() for line in res.stdout.splitlines() if line.strip().startswith("gs://")]


def _gcs_download(uri: str, dest: Path) -> Optional[Path]:
    """Downloads the object uri to dest; None when there is no such object. Never writes to GCS.
    Raises OSError on a read error."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    client = _storage_client()
    if client is not None:
        bucket, path = _split(uri)
        blob = client.bucket(bucket).get_blob(path)
        if blob is None:
            return None
        blob.download_to_filename(str(dest))
        return dest
    if not shutil.which("gcloud"):
        raise OSError("reading gs:// needs google-cloud-storage or gcloud")
    res = subprocess.run(["gcloud", "storage", "cp", uri, str(dest)], capture_output=True, text=True, check=False)
    if res.returncode != 0:
        if "matched no objects" in res.stderr or "not found" in res.stderr.lower():
            return None
        raise OSError(f"gcloud storage cp {uri}: {res.stderr.strip()[:300]}")
    return dest
