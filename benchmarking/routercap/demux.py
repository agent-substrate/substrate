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

"""Splits one arm's pod output into the files a local run writes directly.

The generator runs in a distroless container, so ``kubectl cp`` cannot
retrieve what it writes.  Instead the binary tags every record with its
stream and writes them all to stdout, and this puts them back:

    kubectl logs -f job/... --all-containers | demux.py OUTDIR

    samples.jsonl   aligned records: load, latency, CPU and memory over one
                    cAdvisor-defined window
    run.json        the run header
    job.log         everything else, which is the binary's own stderr

1s generator-only records (stream "fine") are counted for the closing summary
line and discarded; nothing downstream reads them.  Writes are flushed per
line so ``kubectl logs -f`` piped through here still behaves as a stream.
"""

import argparse
import json
import os
import sys


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("outdir", help="Directory to write into; created if absent.")
    ap.add_argument(
        "--quiet",
        action="store_true",
        help="Do not echo a progress line per aligned record to stderr.",
    )
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    paths = {
        "sample": os.path.join(args.outdir, "samples.jsonl"),
    }
    header_path = os.path.join(args.outdir, "run.json")
    log_path = os.path.join(args.outdir, "job.log")

    files = {k: open(v, "w", encoding="utf-8") for k, v in paths.items()}
    log = open(log_path, "w", encoding="utf-8")
    counts = {"sample": 0, "fine": 0, "header": 0, "log": 0}

    try:
        for line in sys.stdin:
            line = line.rstrip("\n")
            if not line:
                continue
            rec = None
            if line.startswith("{"):
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    rec = None
            stream = rec.get("stream") if isinstance(rec, dict) else None
            if stream in files:
                # Unwrap: what lands in samples.jsonl is byte-identical in shape
                # to what a local --output-dir run writes, so charts.py cannot
                # tell the two apart and does not have to.
                files[stream].write(json.dumps(rec["record"], separators=(",", ":")) + "\n")
                files[stream].flush()
                counts[stream] += 1
                if stream == "sample" and not args.quiet:
                    progress(rec["record"])
            elif stream == "fine":
                counts["fine"] += 1
            elif stream == "header":
                with open(header_path, "w", encoding="utf-8") as fh:
                    json.dump(rec["record"], fh, indent=2)
                    fh.write("\n")
                counts["header"] += 1
            else:
                log.write(line + "\n")
                log.flush()
                counts["log"] += 1
    finally:
        for f in files.values():
            f.close()
        log.close()

    print(
        "[demux] {sample} samples, {fine} fine, {header} header, {log} log lines -> {d}".format(
            d=args.outdir, **counts
        ),
        file=sys.stderr,
    )
    # A run whose header never arrived is one nobody can interpret later, so
    # say so loudly rather than leave a directory that looks complete.
    if counts["header"] == 0:
        print("[demux] WARNING: no run header in this arm's output", file=sys.stderr)
        return 1
    return 0


def progress(rec: dict) -> None:
    """Echoes one aligned record as a human-readable line."""
    load = rec.get("load") or {}
    lat = load.get("latency") or {}
    containers = rec.get("containers") or {}

    def cores(role: str) -> str:
        """Cores for role, or a dash when this window never sampled it.

        cAdvisor can close a window before a given container has ticked;
        printing 0.00c there would read as "went idle" when it means "nobody
        looked".
        """
        v = (containers.get(role) or {}).get("cpu_cores")
        return "    -" if v is None else "%5.2f" % v

    trips = ",".join(g.get("guard", "?") for g in (rec.get("guards") or []))
    print(
        "[{arm:>3}c rung {rung:>2}{warm}] offered {off:>7.0f}  achieved {ach:>7.0f}  "
        "inflight {inf:>6d}  p50 {p50:>6.1f}ms  p95 {p95:>7.1f}ms  "
        "envoy {envoy}c  sidecar {side}c{guards}".format(
            arm=rec.get("arm_cores", 0),
            rung=rec.get("rung", 0),
            warm="w" if rec.get("warmup") else " ",
            off=load.get("offered_qps", 0.0),
            ach=load.get("achieved_qps", 0.0),
            inf=int(load.get("in_flight_max", 0)),
            p50=lat.get("p50_ms", 0.0),
            p95=lat.get("p95_ms", 0.0),
            envoy=cores("envoy"),
            side=cores("atenet-router"),
            guards="  GUARD:" + trips if trips else "",
        ),
        file=sys.stderr,
    )


if __name__ == "__main__":
    sys.exit(main())
