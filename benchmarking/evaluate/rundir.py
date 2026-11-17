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

"""The runner's storage layout, and the runs of one invocation.

The runner stores each run under
<dest>/runs/<test>/run_date=<YYYY-MM-DD>/run_ts=<unix s>/run_tag=<commit>/, with
<dest> a local directory or gs://bucket/path. run_ts is the orchestrator's
timestamp for the invocation, which all its runs share, so the run of another
test from the same invocation is stored at the same path with that test's name.

This module is the only one that knows that layout or touches gs://: it opens
a stored run as a BenchmarkRunContext whose identity comes from the layout and
whose artifacts are read from the store, a gs:// one downloaded on first use
(read-only on the store), through the google-cloud-storage library.
"""

import functools
import re
import tempfile
from pathlib import Path
from typing import Callable, Optional

from .sources.base import BenchmarkRunContext, LocalArtifacts, RunIdentity, no_other_runs

RUN_PATH = re.compile(r"^(?P<root>.*)/runs/(?P<test>[^/]+)/run_date=(?P<date>[^/]+)/run_ts=(?P<ts>\d+)"
                      r"/run_tag=(?P<tag>[^/]+)/?$")


def identity_from_uri(uri: str) -> RunIdentity:
    """The identity of the run stored at uri: test_name, tag, run_date and run_ts too when it is in the runner's layout."""
    uri = uri.rstrip("/")
    m = RUN_PATH.match(uri)
    if not m:
        return RunIdentity(uri=uri)
    return RunIdentity(uri=uri, test_name=m["test"], tag=m["tag"], run_date=m["date"], run_ts=int(m["ts"]))


class GcsArtifacts:
    """A run's artifacts under a gs:// prefix, each downloaded on first use into tmp. Never writes to GCS."""

    def __init__(self, uri: str, tmp: Path):
        self.uri = uri.rstrip("/")
        self.tmp = tmp
        self._fetched: dict[str, Optional[Path]] = {}

    def fetch(self, name: str) -> Optional[Path]:
        """Raises google-cloud-storage's exception on a read error."""
        if name not in self._fetched:
            self._fetched[name] = _gcs_download(f"{self.uri}/{name}", self.tmp / name)
        return self._fetched[name]


def open_run(uri: str, tmp: Path) -> BenchmarkRunContext:
    """The run stored at uri, a local directory or gs://, whose gs:// artifacts are downloaded under tmp, and
    whose run_of is same_invocation's. Raises FileNotFoundError when nothing is stored there."""
    uri = uri.rstrip("/")
    if uri.startswith("gs://"):
        if not _gcs_list(f"{uri}/"):
            raise FileNotFoundError(f"no objects under {uri}/")
        tmp.mkdir(parents=True, exist_ok=True)
        artifacts = GcsArtifacts(uri, Path(tempfile.mkdtemp(prefix="run-", dir=tmp)))
    else:
        if not Path(uri).is_dir():
            raise FileNotFoundError(f"no run directory {uri}")
        artifacts = LocalArtifacts(Path(uri))
    return BenchmarkRunContext(identity_from_uri(uri), artifacts, run_of=same_invocation(uri, tmp))


def same_invocation(uri: str, tmp: Path) -> Callable[[str], BenchmarkRunContext]:
    """BenchmarkRunContext.run_of for the run stored at uri: the run of test stored at the same path with
    test in place of uri's test, opened under tmp once, or LookupError when there is none. A uri outside
    the runner's layout has no other runs."""
    m = RUN_PATH.match(uri.rstrip("/"))
    if not m:
        return no_other_runs

    @functools.cache
    def run_of(test: str) -> BenchmarkRunContext:
        other = f"{m['root']}/runs/{test}/run_date={m['date']}/run_ts={m['ts']}/run_tag={m['tag']}"
        try:
            return open_run(other, tmp)
        except FileNotFoundError:
            raise LookupError(f"no run of {test} at {other}") from None

    return run_of


def _split(uri: str) -> tuple[str, str]:
    bucket, _, path = uri[len("gs://"):].partition("/")
    return bucket, path


@functools.cache
def _storage_client():
    """The google-cloud-storage client, created on first use. Raises OSError when the library is not installed."""
    try:
        from google.cloud import storage  # pylint: disable=import-outside-toplevel
    except ImportError as e:
        raise OSError("reading gs:// needs google-cloud-storage (pip install google-cloud-storage)") from e
    return storage.Client()


def _gcs_list(prefix: str) -> list[str]:
    """gs:// URIs of the objects under prefix. A listing error raises google-cloud-storage's exception (a
    google.api_core or google.auth error), as does every gs:// read here."""
    bucket, path = _split(prefix)
    return [f"gs://{bucket}/{b.name}" for b in _storage_client().list_blobs(bucket, prefix=path)]


def _gcs_download(uri: str, dest: Path) -> Optional[Path]:
    """Downloads the object uri to dest; None when there is no such object. Never writes to GCS."""
    bucket, path = _split(uri)
    blob = _storage_client().bucket(bucket).get_blob(path)
    if blob is None:
        return None
    dest.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(str(dest))
    return dest
