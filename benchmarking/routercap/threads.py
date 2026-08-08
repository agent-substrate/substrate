# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Turns threads.sh's /proc stat stream into per-worker CPU, one JSON line per
sampling interval.

Input: blocks of "T <epoch>" followed by /proc/<pid>/task/<tid>/stat lines.
Output (stdout): {"t0": .., "t1": .., "workers": {"0": cores, ...},
"other_cores": .., "max_worker": .., "mean_worker": ..} — workers keyed by the
index in the thread's "wrk:worker_N" name, every other envoy thread (main,
dns, watchdog) folded into other_cores.

Cores are (utime+stime deltas in jiffies) / USER_HZ / elapsed. USER_HZ is 100
on every kernel GKE ships; if that ever changes the absolute level shifts but
the skew between workers — the number this file exists for — does not.
"""

import json
import re
import sys

USER_HZ = 100.0

# tid (comm) state ... utime is field 14, stime 15, 1-indexed after the comm.
# comm can contain spaces but never a ')' for the threads envoy names.
STAT = re.compile(r"^(\d+) \((.*)\) \S (?:\S+ ){10}(\d+) (\d+) ")


def parse_blocks(lines):
    """Yields (epoch, {tid: (comm, jiffies)}) per sample block."""
    t, threads = None, {}
    for line in lines:
        if line.startswith("T "):
            if t is not None and threads:
                yield t, threads
            try:
                t = int(line.split()[1])
            except (IndexError, ValueError):
                t = None
            threads = {}
            continue
        m = STAT.match(line)
        if m:
            tid, comm, ut, st = m.group(1), m.group(2), int(m.group(3)), int(m.group(4))
            threads[tid] = (comm, ut + st)
    if t is not None and threads:
        yield t, threads


def main(path):
    with open(path) as f:
        blocks = list(parse_blocks(f))
    prev_t, prev = None, None
    for t, threads in blocks:
        if prev is not None and t > prev_t:
            dt = t - prev_t
            workers, other = {}, 0.0
            for tid, (comm, j) in threads.items():
                pj = prev.get(tid)
                # A thread that appeared mid-interval has no baseline; skip it
                # this round rather than crediting its lifetime total.
                if pj is None or pj[0] != comm:
                    continue
                cores = (j - pj[1]) / USER_HZ / dt
                if comm.startswith("wrk:worker_"):
                    workers[comm[len("wrk:worker_"):]] = round(cores, 4)
                else:
                    other += cores
            if workers:
                vals = list(workers.values())
                print(json.dumps({
                    "t0": prev_t, "t1": t, "workers": workers,
                    "other_cores": round(other, 4),
                    "max_worker": max(vals),
                    "mean_worker": round(sum(vals) / len(vals), 4),
                }))
        prev_t, prev = t, threads


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: threads.py <threads.log>")
    main(sys.argv[1])
