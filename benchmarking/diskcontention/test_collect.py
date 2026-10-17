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

"""Tests for collect.py, with Prometheus faked. No cluster needed:

    cd benchmarking/diskcontention && python3 -m pytest -q .
"""

import contextlib
import io
import json
import os
import re
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import collect  # noqa: E402


def sample(value, **labels) -> dict:
    """One instant-vector sample as the Prometheus HTTP API returns it."""
    return {"metric": labels, "value": [0, str(value)]}


class FakeProm:
    """Answers collect.prom from node_filesystem samples and fixed answers.

    mounts maps each mount point to its device. answers maps a regex to the
    result of every query it matches; queries that match none get [].
    """

    def __init__(self, mounts: dict[str, str] | None = None, answers: dict[str, list] | None = None):
        self.mounts = mounts or {}
        self.answers = answers or {}
        self.queries: list[str] = []

    def __call__(self, url: str, query: str, at: float) -> list:
        self.queries.append(query)
        m = re.fullmatch(r'node_filesystem_avail_bytes\{mountpoint=~"\.\*(.*)\$"\}', query)
        if m:
            return [sample(1, mountpoint=mp, device=dev) for mp, dev in self.mounts.items()
                    if mp.endswith(m.group(1))]
        m = re.fullmatch(r'node_filesystem_avail_bytes\{mountpoint="(.*)"\}', query)
        if m:
            return [sample(1, mountpoint=mp, device=dev) for mp, dev in self.mounts.items()
                    if mp == m.group(1)]
        for pattern, result in self.answers.items():
            if re.search(pattern, query):
                return result
        return []


class WholeDiskTest(unittest.TestCase):
    def test_strips_partitions(self):
        for dev, want in {
            "/dev/nvme0n1p1": "nvme0n1",
            "/dev/nvme0n3": "nvme0n3",
            "sda1": "sda",
            "/dev/vdb2": "vdb",
            "xvda1": "xvda",
            "mmcblk0p2": "mmcblk0",
            "md0": "md0",
            "dm-0": "dm-0",
        }.items():
            self.assertEqual(collect._whole_disk(dev), want, dev)


class ScalarTest(unittest.TestCase):
    def test_values(self):
        self.assertIsNone(collect.scalar([]))
        self.assertEqual(collect.scalar([sample("2.5")]), 2.5)
        self.assertIsNone(collect.scalar([sample("NaN")]))

    def test_refuses_more_than_one_node(self):
        with self.assertRaisesRegex(SystemExit, "returned 2 series; the suite expects every actor on one node"):
            collect.scalar([sample(1, instance="a"), sample(2, instance="b")])

    def test_by_label(self):
        result = [sample(3, phase="persist"), sample("NaN", phase="total"), sample(1)]
        self.assertEqual(collect.by_label(result, "phase"), {"persist": 3.0, "total": None, "": 1.0})


class DetectDeviceTest(unittest.TestCase):
    ACTORS = ("/var/lib/ate/actors", "/var/lib/ate")

    def detect(self, mounts: dict[str, str]) -> tuple[str, str]:
        with mock.patch.object(collect, "prom", FakeProm(mounts)):
            boot = collect.detect_device("http://prom", (), 0)
            return boot, collect.detect_device("http://prom", self.ACTORS, 0, boot)

    def test_layouts(self):
        boot = {"/": "/dev/root", "/mnt/stateful_partition": "/dev/nvme0n1p1"}
        for name, extra, want in (
            ("boot disk only", {}, "nvme0n1"),
            ("disk at /var/lib/ate/actors", {"/var/lib/ate/actors": "/dev/nvme0n3"}, "nvme0n3"),
            ("disk at /var/lib/ate", {"/var/lib/ate": "/dev/nvme0n2"}, "nvme0n2"),
            # Guest data stays on the boot disk when only snapshots move.
            ("disk at /var/lib/ate/snapshots", {"/var/lib/ate/snapshots": "/dev/nvme0n4"}, "nvme0n1"),
        ):
            with self.subTest(name):
                self.assertEqual(self.detect({**boot, **extra}), ("nvme0n1", want))

    def test_falls_back_to_root_then_default(self):
        self.assertEqual(self.detect({"/": "/dev/sda1"}), ("sda", "sda"))
        # Devices outside /dev (tmpfs, overlay) are not block devices.
        self.assertEqual(self.detect({"/": "overlay"}), ("nvme0n1", "nvme0n1"))

    def test_mounted_device_only_reports_that_mount(self):
        fake = FakeProm({"/mnt/stateful_partition": "/dev/nvme0n1p1"})
        with mock.patch.object(collect, "prom", fake):
            self.assertIsNone(collect.mounted_device("http://prom", "/var/lib/ate/snapshots", 0))
            self.assertEqual(collect.mounted_device("http://prom", "/mnt/stateful_partition", 0), "nvme0n1")

    def test_prometheus_errors_fall_back_to_the_default(self):
        def broken(url, query, at):
            raise OSError("connection refused")
        with mock.patch.object(collect, "prom", broken):
            self.assertEqual(collect.detect_device("http://prom", self.ACTORS, 0, "sdb"), "sdb")


class PhaseStatsTest(unittest.TestCase):
    def test_joins_counts_sums_and_quantiles(self):
        lbl = "ate_snapshot_phase"
        fake = FakeProm(answers={
            r"^sum by .*_count": [sample(4, **{lbl: "persist"}), sample(0, **{lbl: "total"})],
            r"^sum by .*_sum": [sample(10, **{lbl: "persist"}), sample(0, **{lbl: "total"})],
            r"histogram_quantile\(0\.95,": [sample(3.5, **{lbl: "persist"})],
        })
        with mock.patch.object(collect, "prom", fake):
            stats = collect.phase_stats("http://prom", collect.CHECKPOINT, 600, 0)
        self.assertEqual(stats["persist"], {"count": 4.0, "mean_s": 2.5, "p50_s": None, "p95_s": 3.5, "p99_s": None})
        self.assertIsNone(stats["total"]["mean_s"])
        self.assertTrue(all('ate_template_name="glutton"' in q for q in fake.queries))


class SeriesTest(unittest.TestCase):
    def test_refuses_more_than_one_node(self):
        two = [{"metric": {}, "values": [[0, "1"]]}] * 2
        with mock.patch.object(collect, "prom_range", lambda *a: two):
            with self.assertRaisesRegex(SystemExit, "returned 2 series"):
                collect.series("http://prom", 0, 60, "nvme0n1")

    def test_values(self):
        one = [{"metric": {}, "values": [[10, "1.5"], [25, "2"]]}]
        with mock.patch.object(collect, "prom_range", lambda *a: one):
            out = collect.series("http://prom", 0, 60, "nvme0n1")
        self.assertEqual(out["disk_write_MBps"], [[10.0, 1.5], [25.0, 2.0]])


class MainTest(unittest.TestCase):
    def run_main(self, fake: FakeProm, *args: str) -> tuple[int, dict]:
        with tempfile.TemporaryDirectory() as d:
            out = os.path.join(d, "collect.json")
            argv = ["collect.py", "--start", "1000", "--end", "1900", "--warmup", "180", "--out", out, *args]
            with mock.patch.object(sys, "argv", argv), \
                    mock.patch.object(collect, "prom", fake), \
                    mock.patch.object(collect, "prom_range", lambda *a: []), \
                    contextlib.redirect_stdout(io.StringIO()), \
                    contextlib.redirect_stderr(io.StringIO()):
                rc = collect.main()
            if not os.path.exists(out):
                return rc, {}
            with open(out) as f:
                return rc, json.load(f)

    def test_actor_disk_and_boot_disk(self):
        fake = FakeProm(
            mounts={"/mnt/stateful_partition": "/dev/nvme0n1p1", "/var/lib/ate": "/dev/nvme0n2"},
            answers={r'node_disk_written_bytes_total\{device="nvme0n2"\}\[720s\]\) / 720': [sample(150)]},
        )
        rc, res = self.run_main(fake)
        self.assertEqual(rc, 0)
        self.assertEqual(res["window"], {"start": 1180.0, "end": 1900.0, "seconds": 720, "warmup": 180.0})
        self.assertEqual(res["node"]["device"], "nvme0n2")
        self.assertEqual(res["node"]["write_MBps_avg"], 150.0)
        # The boot disk is reported too when actor data is elsewhere.
        self.assertIn("boot_node", res)
        self.assertNotIn("snapshots_node", res)
        self.assertNotIn("gce_disk", res)

    def test_snapshot_disk(self):
        fake = FakeProm(
            mounts={"/mnt/stateful_partition": "/dev/nvme0n1p1", "/var/lib/ate/snapshots": "/dev/nvme0n4"},
            answers={r'node_disk_written_bytes_total\{device="nvme0n4"\}\[720s\]\) / 720': [sample(90)]},
        )
        rc, res = self.run_main(fake)
        self.assertEqual(rc, 0)
        # Guest data stays on the boot disk; snapshot staging has its own.
        self.assertEqual(res["node"]["device"], "nvme0n1")
        self.assertNotIn("boot_node", res)
        self.assertEqual(res["snapshots_node"]["device"], "nvme0n4")
        self.assertEqual(res["snapshots_node"]["write_MBps_avg"], 90.0)

    def test_device_flag_skips_detection(self):
        rc, res = self.run_main(FakeProm(), "--device", "sdc")
        self.assertEqual(rc, 0)
        self.assertEqual(res["node"]["device"], "sdc")
        self.assertIn("boot_node", res)

    def test_window_too_short(self):
        rc, res = self.run_main(FakeProm(), "--warmup", "860")
        self.assertEqual((rc, res), (1, {}))


if __name__ == "__main__":
    unittest.main()
