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

"""Unit tests for rundir: stored runs, and finding the run another run is paired with.

Runs are stand-in directories in the runner's layout; GCS is mocked.
"""

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from evaluate import rundir  # noqa: E402
from evaluate.sources.base import LocalArtifacts, RunIdentity  # noqa: E402

T0 = 1791700000  # 2026-10-11T07:06:40Z
TAG = "abc123"


def write_run(d: Path, cluster: str = "c1") -> Path:
    d.mkdir(parents=True, exist_ok=True)
    (d / "logs.txt").write_text("[boomer] {}\n")
    trial = {"timestamp": "2026-10-11T07:06:40Z", "tag": TAG, "test_name": "t", "metric": "trial_summary",
             "measurements": {"metadata_cluster_name": cluster} if cluster else {}}
    (d / "stats.jsonl").write_text(json.dumps(trial) + "\n")
    return d


def stored(root: Path, test: str, run_ts: int, tag: str = TAG, cluster: str = "c1") -> Path:
    date = datetime.fromtimestamp(run_ts, tz=timezone.utc).strftime("%Y-%m-%d")
    return write_run(Path(rundir.run_uri(str(root), test, date, str(run_ts), tag)), cluster)


class FindPairedRunTest(unittest.TestCase):
    def test_latest_run_with_the_tag_before_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            want = stored(root, "t1", T0 + 100)
            stored(root, "t1", T0 + 50)  # older
            stored(root, "t1", T0 + 200)  # after the run
            stored(root, "t1", T0 + 120, tag="other")  # another commit, e.g. a fork job reusing the name
            stored(root, "t1", T0 - 8 * 3600)  # too old
            self.assertEqual(rundir.find_paired_run(str(root), "t1", TAG, T0 + 150), str(want))
            with self.assertRaisesRegex(LookupError, f"^no run of t1 with tag abc123 under {tmp}/runs/ in the "
                                                     f"6 h before run_ts {T0 + 10}$"):
                rundir.find_paired_run(str(root), "t1", TAG, T0 + 10)

    def test_pairing_crosses_midnight(self):
        midnight = 1791763200  # 2026-10-12T00:00:00Z
        with tempfile.TemporaryDirectory() as tmp:
            want = stored(Path(tmp), "t1", midnight - 600)
            self.assertEqual(rundir.find_paired_run(tmp, "t1", TAG, midnight + 600), str(want))

    def test_gcs_dest(self):
        prefix = "gs://b/p/runs/t1/run_date=2026-10-11"
        objects = [f"{prefix}/run_ts={T0 + 100}/run_tag={TAG}/logs.txt",
                   f"{prefix}/run_ts={T0 + 100}/run_tag={TAG}/stats.jsonl",
                   f"{prefix}/run_ts={T0 + 50}/run_tag={TAG}/stats.jsonl",
                   f"{prefix}/run_ts={T0 + 120}/run_tag={TAG}/status.json",  # no logs.txt or stats.jsonl
                   f"{prefix}/run_ts={T0 + 130}/run_tag=other/logs.txt"]
        with mock.patch.object(rundir, "_gcs_list", return_value=objects) as listing:
            found = rundir.find_paired_run("gs://b/p/", "t1", TAG, T0 + 150)
        self.assertEqual(found, f"{prefix}/run_ts={T0 + 100}/run_tag={TAG}")
        listing.assert_called_once_with(f"{prefix}/")

    def test_identity_from_uri(self):
        self.assertEqual(rundir.identity_from_uri(f"gs://b/p/runs/t1/run_date=2026-10-11/run_ts={T0}/run_tag=x/"),
                         RunIdentity(uri=f"gs://b/p/runs/t1/run_date=2026-10-11/run_ts={T0}/run_tag=x",
                                     test_name="t1", tag="x", run_date="2026-10-11", run_ts=T0))
        self.assertEqual(rundir.identity_from_uri("/tmp/run"), RunIdentity(uri="/tmp/run"))


class OpenRunTest(unittest.TestCase):
    def test_local_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = write_run(Path(tmp) / "run")
            run = rundir.open_run(f"{d}/", Path(tmp) / "x")
            self.assertEqual(run.identity, RunIdentity(uri=str(d)))
            self.assertEqual(run.artifact("logs.txt"), d / "logs.txt")
            self.assertIsNone(run.artifact("server_summary.json"))
            with self.assertRaisesRegex(FileNotFoundError, "no run directory"):
                rundir.open_run(str(Path(tmp) / "missing"), Path(tmp) / "x")

    def test_gcs_run_downloads_each_artifact_on_first_use(self):
        uri = f"gs://b/p/runs/t1/run_date=2026-10-11/run_ts={T0}/run_tag={TAG}"
        path = uri[len("gs://b/"):]
        stored_names = {f"{path}/logs.txt", f"{path}/custom/extra.json"}
        downloads = []

        class Blob:
            def __init__(self, name):
                self.name = name

            def download_to_filename(self, dest):
                Path(dest).write_text(self.name)

        def get_blob(name):
            downloads.append(name)
            return Blob(name) if name in stored_names else None

        client = mock.Mock()
        client.list_blobs.return_value = [Blob(n) for n in stored_names]
        client.bucket.return_value.get_blob.side_effect = get_blob
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(rundir, "_storage_client", return_value=client):
                run = rundir.open_run(uri, Path(tmp))
                self.assertEqual(run.identity, rundir.identity_from_uri(uri))
                self.assertEqual(downloads, [])  # nothing is read until a source asks
                # A file the framework knows nothing about is there too, for the source that reads it.
                self.assertEqual(run.artifact("custom/extra.json").read_text(), f"{path}/custom/extra.json")
                self.assertEqual(run.artifact("logs.txt").read_text(), f"{path}/logs.txt")
                self.assertEqual(run.artifact("logs.txt").parent.parent, Path(tmp))
                self.assertIsNone(run.artifact("server_summary.json"))
                self.assertIsNone(run.artifact("server_summary.json"))
                with self.assertRaisesRegex(ValueError, "outside the run"):
                    run.artifact("../escape.txt")
            self.assertEqual(downloads, [f"{path}/custom/extra.json", f"{path}/logs.txt",
                                         f"{path}/server_summary.json"])  # each once
            client.list_blobs.return_value = []
            with mock.patch.object(rundir, "_storage_client", return_value=client), \
                 self.assertRaisesRegex(FileNotFoundError, f"no objects under {uri}/"):
                rundir.open_run(uri, Path(tmp))


class PairedRunFinderTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.run_dir = stored(self.root, "t150", T0 + 3600)

    def finder(self, run_dir=None):
        return rundir.paired_run_finder(str(self.root), TAG, T0 + 3600, LocalArtifacts(run_dir or self.run_dir),
                                        self.root / "tmp")

    def test_finds_the_paired_run(self):
        want = stored(self.root, "t1", T0)
        paired = self.finder()("t1")
        self.assertEqual(paired.identity, rundir.identity_from_uri(str(want)))
        self.assertEqual(paired.artifact("stats.jsonl"), want / "stats.jsonl")

    def test_lookup_errors(self):
        stored(self.root, "t1", T0, cluster="sweperf-load-test")
        stored(self.root, "t2", T0, cluster="")
        stored(self.root, "t3", T0, cluster="c1")
        finder = self.finder()
        with self.assertRaisesRegex(LookupError, "^cluster differs \\(c1 vs sweperf-load-test\\)$"):
            finder("t1")
        self.assertEqual(finder("t2").identity.test_name, "t2")  # no cluster recorded
        self.assertEqual(finder("t3").identity.test_name, "t3")
        with self.assertRaisesRegex(LookupError, "^no run of t4 with tag abc123 under "):
            finder("t4")
        # Anything else is a LookupError too, saying what it was.
        for error, why in ((PermissionError("403 list"), "PermissionError: 403 list"),
                           (KeyError("run_ts"), "KeyError: 'run_ts'")):
            with self.subTest(why=why), mock.patch.object(rundir, "find_paired_run", side_effect=error), \
                 self.assertRaises(LookupError) as raised:
                self.finder()("t1")
            self.assertEqual((type(raised.exception), str(raised.exception)), (LookupError, why))

    def test_a_corrupt_stats_jsonl_is_a_lookup_error(self):
        stored(self.root, "t1", T0)
        with open(self.run_dir / "stats.jsonl", "a", encoding="utf-8") as f:
            f.write("not json\n")
        with self.assertRaisesRegex(LookupError, "^can't compare the clusters: stats.jsonl line 2 isn't JSON"):
            self.finder()("t1")

    def test_each_test_is_looked_up_once(self):
        stored(self.root, "t1", T0)
        finder = self.finder()
        with mock.patch.object(rundir, "find_paired_run", wraps=rundir.find_paired_run) as find:
            self.assertIs(finder("t1"), finder("t1"))
            for _ in range(2):
                with self.assertRaisesRegex(LookupError, "^no run of t9 "):
                    finder("t9")
        self.assertEqual([c.args[1] for c in find.call_args_list], ["t1", "t9"])

    def test_cluster_is_read_when_asked(self):
        """The runner makes the finder before its run writes stats.jsonl."""
        stored(self.root, "t1", T0, cluster="c1")
        later = self.root / "work"
        finder = self.finder(later)
        write_run(later, cluster="c2")
        with self.assertRaisesRegex(LookupError, "^cluster differs \\(c2 vs c1\\)$"):
            finder("t1")


if __name__ == "__main__":
    unittest.main()
