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

"""Summarize repeat_matplotlib.sh output: median / min / max per metric.

    profiling/aggregate_repeats.py profiling/results/v3/repeat > summary.md

Cold boot rows are per arm. Lifecycle rows are bucketed by
(arm, step, placement, image path) so that, e.g., a cross-node cold
resume #1 is never averaged with a same-node stat-hit resume #1.
All values are printed in ms except rows marked (s).
"""
import glob
import json
import os
import re
import statistics
import sys
from collections import defaultdict

IM = "matplotlib__matplotlib-23476"


def node_of(notes):
    for n in notes or []:
        m = re.search(r" on (\S+)$", n)
        if m:
            return m.group(1)
    return ""


def image_path(step):
    s = step.get("streaming", {}).get("swebench-verified path")
    if s:
        if s.startswith("FALLBACK"):
            return "stream FALLBACK"
        if "cold" in s:
            return "stream cold"
        if "stat hit" in s:
            return "stream stat-hit"
        return "stream " + s.split(",")[0]
    img = step.get("images", {}).get("swebench-verified")
    return f"pull {img}" if img else "no image record"


def add(bucket, key, value, scale=1000.0):
    if isinstance(value, (int, float)):
        bucket[key].append(value * scale)


def main(root):
    cold = defaultdict(lambda: defaultdict(list))   # arm -> metric -> [ms]
    life = defaultdict(lambda: defaultdict(list))   # (arm, step, placement, path) -> metric -> [ms]
    problems = []
    for rep in sorted(glob.glob(os.path.join(root, "rep-*"))):
        for arm in ("stream", "pull"):
            cb_f = os.path.join(rep, f"{arm}-coldboot-{IM}.json")
            lc_f = os.path.join(rep, f"{arm}-lifecycle-{IM}.json")
            if not (os.path.exists(cb_f) and os.path.exists(lc_f)):
                problems.append(f"{rep} {arm}: missing JSON")
                continue
            cb, lc = json.load(open(cb_f)), json.load(open(lc_f))
            path = image_path(cb)
            if path not in ("stream cold", "pull MISS"):
                problems.append(f"{rep} {arm}: cold boot was '{path}', excluded")
                continue
            b = cold[arm]
            add(b, "build_wall (s)", cb.get("build_wall"), 1)
            for k, v in cb.get("build_segments", {}).items():
                add(b, k.strip() + " (s)", v, 1)
            for k, v in cb.get("images", {}).items():
                if k.startswith("pull "):
                    add(b, "pull took (s)", v, 1)
            for k, v in cb.get("streaming", {}).items():
                add(b, k.strip(), v)
            for k, v in cb.get("ateom_boot", {}).items():
                add(b, "boot " + k, v)
            add(b, "boot ateom_total", cb.get("ateom_boot_meta", {}).get("ateom_total"))
            for k, v in cb.get("ateom_checkpoint", {}).items():
                add(b, "checkpoint " + k, v)
            add(b, "checkpoint ateom_total", cb.get("ateom_checkpoint_meta", {}).get("ateom_total"))

            golden = node_of(cb.get("notes"))
            prev = golden
            for st in lc.get("steps", []):
                name = st["step"]
                node = st.get("ateapi", {}).get("_node", "")
                if "resume" in name:
                    place = "same-node" if node and node == prev else "cross-node"
                    prev = node or prev
                else:
                    place = "-"
                key = (arm, name, place, image_path(st) if "resume" in name else "-")
                m = life[key]
                add(m, "client_wall", st.get("client_wall"))
                for k, v in st.get("ateapi", {}).items():
                    add(m, k, v)
                for k, v in st.get("atelet_restore", {}).items():
                    add(m, "restore " + k, v)
                ar = st.get("atelet_restore", {})
                if ar.get("total") is not None:
                    parts = (ar.get("manifest_fetch", 0) + max(ar.get("download", 0), ar.get("sandbox_assets", 0) + ar.get("oci_unpack", 0))
                             + ar.get("ateom_restore", 0))
                    add(m, "restore untimed", ar["total"] - parts)
                for k, v in st.get("streaming", {}).items():
                    add(m, k.strip(), v)
                for k, v in st.get("ateom", {}).items():
                    add(m, "ateom " + k, v)
                add(m, "ateom_total", st.get("ateom_meta", {}).get("ateom_total"))

    def table(title, metrics):
        print(f"\n### {title}\n")
        print("| metric | n | median | min | max | meaning (compare?) |\n|---|---:|---:|---:|---:|---|")
        for k, vals in metrics.items():
            if vals:
                print(f"| {k} | {len(vals)} | {statistics.median(vals):.1f} | {min(vals):.1f} | {max(vals):.1f} | {describe(k)} |")

    print("# matplotlib repeat summary (ms unless marked s)")
    print()
    print("compare?: ✅ measured work that can differ by arm; ⚠️ real work but includes a fixed part, "
          "client overhead or overlaps another row; ❌ fixed or timer-dependent wait, or script overhead. "
          "ateom rows run from the named marker to the next one.")
    for arm in ("stream", "pull"):
        table(f"Cold boot, {arm}", cold[arm])
    for key in sorted(life):
        table("Lifecycle, " + " / ".join(key), life[key])
    if problems:
        print("\n### Excluded or incomplete\n")
        for p in problems:
            print(f"- {p}")


# Metric meanings, keyed by row name. ateom rows are matched on the marker
# label with the step number removed.
DESCRIPTIONS = {
    "build_wall (s)": "⚠️ wall clock of RECREATE=1 swebench-template.sh until the golden snapshot is ready; includes the resync wait and polling below",
    "create -> golden snapshot (s)": "⚠️ same span as build_wall",
    "harness prologue (crane, atespace) (s)": "❌ script overhead: crane digest, atespace check, deleting the previous template",
    "template created -> reconciler noticed (s)": "❌ wait for the reconciler's periodic 20 s resync; 0-20 s depending on timing, unrelated to arm",
    "reconciler noticed -> snapshot deadline (s)": "✅ golden actor placed + image fetch + boot, plus a fixed 20 s warmup (no readyz probe); subtract 20 s",
    "template created -> snapshot deadline (s)": "⚠️ the two rows above combined; only used when the work-queue log is missing",
    "snapshot deadline -> golden tag (s)": "❌ checkpoint and upload plus up to one 5 s script poll; dominated by the poll",
    "pull took (s)": "✅ atelet registry pull + unpack of the image into the node layer cache",
    "prepare_layers.resolve": "✅ resolve image digest, config and layers from the registry; 0 when atelet's metadata cache hits",
    "prepare_layers.stat": "✅ per-layer check for a committed snapshot, summed over layers",
    "prepare_layers.prepare": "✅ snapshotter Prepare for missing layers (Riptide provides them); 0 on a stat hit",
    "prepare_layers.view": "✅ snapshotter View per layer (mountable read-only view)",
    "prepare_layers.listable": "✅ wait until each layer mount can be listed",
    "prepare_layers.wrapper": "✅ per-layer wrapper dir, symlink and marker, plus lease metadata write",
    "prepare_layers.total": "✅ whole streaming PrepareLayers until the rootfs is usable; bytes keep downloading in the background",
    "prepare_layers.unattributed": "⚠️ total minus the named phases (client connect, gcfsd credential push, lease bookkeeping); ~7 ms on stat hits vs ~1.5 cold, not attributed",
    "stream swebench-verified (atelet total)": "✅ atelet's whole streaming call; wraps prepare_layers.total",
    "client_wall": "⚠️ local timing of the kubectl ate call; includes ~0.4 s client overhead (same on both arms)",
    "ateapi_rpc_total": "✅ server-side RPC time (Handle RPC elapsed-time); the main per-step latency",
    "finalize_suspended.get_actor": "✅ store call at the end of suspend; tiny, not arm-dependent",
    "finalize_suspended.release_worker": "✅ store call at the end of suspend; tiny, not arm-dependent",
    "finalize_suspended.refetch_actor": "✅ store call at the end of suspend; tiny, not arm-dependent",
    "finalize_suspended.release_snapshot": "✅ store call at the end of suspend; tiny, not arm-dependent",
    "finalize_suspended.update_actor": "✅ store call at the end of suspend; tiny, not arm-dependent",
    "finalize_suspended.total": "✅ whole FinalizeSuspended step (store calls)",
    "restore volume_mount": "❌ external volume mount; ~0, the template has no volumes",
    "restore manifest_fetch": "✅ fetch the snapshot manifest from GCS; ~2x on the first restore on a node",
    "restore sandbox_assets": "✅ sandbox asset prep; runs in parallel with download",
    "restore download": "✅ download the snapshot from GCS; runs in parallel with assets + oci_unpack",
    "restore oci_unpack": "✅ prepareOCIBundles incl. getting the image (stream PrepareLayers or cache lookup/pull); overlaps the streaming/pull rows",
    "restore ateom_restore": "✅ the ateom restore RPC; wraps ateom_total",
    "restore total": "✅ whole atelet Restore",
    "restore untimed": "⚠️ derived: total - manifest_fetch - max(download, assets + oci_unpack) - ateom_restore; code outside the timed phases",
    "ateom_total": "✅ end marker minus start marker in ateom",
}
ATEOM_MARKERS = {
    "Actor starting": "✅ ateom setup before the first runsc call",
    "Actor restoring": "✅ ateom setup before the first runsc call",
    "Actor checkpointing": "✅ ateom setup before runsc checkpoint (~0)",
    "runsc create[_pause]": "✅ create the pause (sandbox) container",
    "runsc start[_pause]": "✅ start the sandbox",
    "runsc restore[_pause]": "✅ gVisor restore of the sandbox",
    "runsc create[testbed]": "✅ create the workload container",
    "runsc start[testbed]": "✅ start the workload until Actor started; reads the rootfs",
    "runsc restore[testbed]": "✅ restore the workload until Actor restored",
    "runsc checkpoint[_pause]": "✅ gVisor checkpoint of the sandbox",
    "runsc kill[testbed]": "✅ signal the workload container",
    "runsc kill[_pause]": "✅ signal the pause container",
    "runsc wait[testbed]": "✅ wait for the workload container to exit",
    "runsc wait[_pause]": "✅ wait for the pause container to exit, until Actor checkpointed",
}


def describe(metric):
    if metric in DESCRIPTIONS:
        return DESCRIPTIONS[metric]
    for prefix in ("boot ", "checkpoint ", "ateom "):
        if metric.startswith(prefix):
            rest = metric[len(prefix):]
            if rest == "ateom_total":
                return DESCRIPTIONS["ateom_total"]
            return ATEOM_MARKERS.get(re.sub(r"^\d+\. ", "", rest), "")
    return ""


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "profiling/results/v3/repeat")
