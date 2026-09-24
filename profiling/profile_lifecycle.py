#!/usr/bin/env python3
"""Profile one actor lifecycle: create -> boot -> suspend -> resume.

Drives kubectl-ate with --trace so every step has a trace ID, then joins the
logs of ate-api-server, atelet and the ateom worker pods on that ID. Only reads
telemetry Substrate already emits; nothing here requires a code change.

Phase sources (all verified against the tree):
  ate-api-server  "Handle RPC"                  elapsed-time, Go duration string
                  "Picked worker"               worker assignment
                  "Actor has ... snapshot"      boot path taken
                  "FinalizeSuspended store ..." slog.Duration, int64 ns
  atelet          "Image cache hit|miss"        per image reference
                  "Image pulled into layer cache" took, int64 ns: the registry pull
                  "Restore timing breakdown"    ate.actor.restore.duration.*, float64 s
  ateom worker    "Actor starting|started"      lifecycle envelope, RFC3339Nano
                  "Actor restoring|restored"
                  "Actor checkpointing|checkpointed"
                  "About to run runsc ..."      per-container step markers
                  "Readyz reached 200"          elapsed, int64 ns
atelet's checkpoint path records metrics only, so suspend granularity comes from
the ateom markers and the ateapi tail breakdown.
"""

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime

ATE_SYSTEM = "ate-system"
RESTORE_PREFIX = "ate.actor.restore.duration."
# The phase names atelet records, used to read the older bare-key spelling.
RESTORE_PHASES = (
    "volume_mount", "manifest_fetch", "sandbox_assets",
    "download", "oci_unpack", "ateom_restore", "total",
)
# Lifecycle pairs bounding an ateom window, in the order the markers appear.
ATEOM_WINDOWS = {
    "boot": ("Actor starting", "Actor started"),
    "suspend": ("Actor checkpointing", "Actor checkpointed"),
    "restore": ("Actor restoring", "Actor restored"),
}


def run(cmd, check=True):
    """Run a shell command and capture it.

    check=False is the norm here: a log scrape against a selector that matches
    no pods is an empty result, not a failure worth ending the run over.
    """
    p = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and p.returncode != 0:
        raise SystemExit(f"command failed: {cmd}\n{p.stderr.strip()}")
    return p


def parse_ts(value):
    """RFC3339 with any fractional precision -> epoch seconds."""
    if not value:
        return None
    s = value.replace("Z", "+00:00")
    # fromisoformat caps the fraction at 6 digits; RFC3339Nano writes up to 9.
    s = re.sub(r"\.(\d{6})\d+", r".\1", s)
    try:
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        return None


def parse_go_duration(value):
    """'1.5s', '310ms', '644.935µs' -> float seconds."""
    if value is None:
        return None
    units = {"ns": 1e-9, "us": 1e-6, "µs": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0}
    total, matched = 0.0, False
    for num, unit in re.findall(r"([\d.]+)(ns|us|µs|ms|h|m|s)", str(value)):
        total += float(num) * units[unit]
        matched = True
    return total if matched else None


def ns_to_s(value):
    """slog.Duration serializes to int64 nanoseconds in JSON."""
    return float(value) / 1e9 if isinstance(value, (int, float)) else None


def msg_of(rec):
    """ateom lifecycle logs use 'message'; component slog logs use 'msg'."""
    return rec.get("msg") or rec.get("message") or ""


def actor_name_of(rec):
    """Actor name from a lifecycle envelope.

    The label group has two spellings and the choice is environmental, not
    historical: actorlog.LabelsKey writes logging.googleapis.com/labels on GCE,
    because that is the key Cloud Logging promotes into LogEntry.labels, and
    plain "labels" everywhere else. Both are read so this works against a kind
    cluster as well as GKE.

    The key inside is ateattr.ActorNameKey, which is the only spelling emitted.
    """
    labels = rec.get("labels") or rec.get("logging.googleapis.com/labels") or {}
    if not isinstance(labels, dict):
        return ""
    return labels.get("ate.actor.name") or ""


def logs(selector, namespace, since, prefix=False):
    """Scrape the JSON log records of every pod matching a label selector.

    Each record gains three synthetic keys the rest of the script reads:
    _pod (empty unless prefix, which is how a worker record is tied back to the
    pod that wrote it), _ts (epoch seconds) and _raw (the line as received, for
    substring matching a trace ID whatever key it was written under).

    Anything that is not a JSON object is dropped: kubectl's own prefix lines
    and any component that logged in text are noise here, not errors.
    """
    cmd = f"kubectl logs -l {selector} -n {namespace} --since={since}s --tail=-1"
    if prefix:
        cmd += " --prefix"
    out = run(cmd, check=False).stdout
    records = []
    for line in out.splitlines():
        pod = ""
        if prefix:
            m = re.match(r"\[pod/([^/]+)/[^]]+\]\s*(.*)", line)
            if not m:
                continue
            pod, line = m.group(1), m.group(2)
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        rec["_pod"] = pod
        rec["_ts"] = parse_ts(rec.get("time") or rec.get("timestamp"))
        rec["_raw"] = line
        records.append(rec)
    return records


def for_trace(records, trace_id):
    """Match on the raw line: the trace ID is spelled trace_id in component
    slog records and ate.dev/trace-id in the ateapi RPC log."""
    hits = [r for r in records if trace_id and trace_id in r["_raw"]]
    return sorted(hits, key=lambda r: r["_ts"] or 0.0)


def ate(args, sub, retries=0):
    """Run a kubectl-ate subcommand, returning (wall_seconds, trace_id, stdout).

    A worker record can outlive its pod, and ateapi fails the call rather than
    picking again, so retry that one error: the next assignment may be live.
    Each attempt is timed on its own, so the returned latency is the attempt
    that did the work, not the dead ones before it.
    """
    cmd = f"{args.kubectl_ate} {sub} -a {args.atespace} --trace"
    for attempt in range(retries + 1):
        t0, t_wall = time.perf_counter(), time.time()
        p = run(cmd, check=False)
        wall = time.perf_counter() - t0
        m = re.search(r"Trace ID:\s*([0-9a-f]+)", p.stderr)
        if p.returncode == 0:
            return wall, (m.group(1) if m else ""), (t_wall, time.time())
        if attempt < retries and "worker pod not found" in p.stderr:
            print(f"  retrying ({attempt + 1}/{retries}): assigned worker pod is gone")
            time.sleep(2)
            continue
        raise SystemExit(f"command failed: {cmd}\n{p.stderr.strip()}")



def ateom_timeline(records, window, actor):
    """Inter-marker deltas inside an ateom lifecycle window.

    The worker's lifecycle logs name the actor but carry no trace ID, so the
    window is selected by actor and the step markers inside it by time and pod.
    A pod multiplexes actors, so restricting to the pod that logged the window
    is what keeps a neighbour's runsc step out of this breakdown.

    Reports every marker the worker emitted rather than a fixed phase list, so
    the breakdown stays as granular as the logs allow.
    """
    start_msg, end_msg = ATEOM_WINDOWS[window]
    start = next((r for r in records
                  if msg_of(r) == start_msg and actor_name_of(r) == actor), None)
    end = next((r for r in reversed(records)
                if msg_of(r) == end_msg and actor_name_of(r) == actor), None)
    if not start or not end or start["_ts"] is None or end["_ts"] is None:
        return {}, {}

    pod = start.get("_pod", "")
    marks = []
    for r in records:
        if r["_ts"] is None or not (start["_ts"] <= r["_ts"] <= end["_ts"]):
            continue
        if r.get("_pod") != pod:
            continue
        m = msg_of(r)
        named = actor_name_of(r)
        if named and named != actor:
            continue
        if m in (start_msg, end_msg) or m.startswith("About to run runsc ") or m == "Readyz reached 200":
            label = m.replace("About to run ", "")
            if r.get("container"):
                label = f"{label}[{r['container']}]"
            marks.append((label, r["_ts"], r))

    phases = {}
    for (label, ts, _), (_, next_ts, _) in zip(marks, marks[1:]):
        # Markers repeat per container, so number them to keep each delta.
        phases[f"{len(phases) + 1:02d}. {label}"] = next_ts - ts
    for _, _, r in marks:
        if msg_of(r) == "Readyz reached 200":
            phases[f"readyz[{r.get('container', '')}] (reported)"] = ns_to_s(r.get("elapsed"))

    return phases, {"worker_pod": pod, "ateom_total": end["_ts"] - start["_ts"]}


def atelet_restore(records):
    """Phases from atelet's restore record, normalized to seconds.

    Newer atelet writes ate.actor.restore.duration.<phase> as float seconds
    because the matching instrument declares unit s; older builds write the bare
    phase name as the int64 nanoseconds slog.Duration produces. Read both so the
    profiler does not silently report nothing against a lagging control plane.
    """
    rec = next((r for r in reversed(records) if msg_of(r) == "Restore timing breakdown"), None)
    if not rec:
        return {}
    phases = {}
    for k, v in rec.items():
        if k.startswith(RESTORE_PREFIX):
            phases[k[len(RESTORE_PREFIX):]] = float(v)
        elif k in RESTORE_PHASES and isinstance(v, (int, float)):
            phases.setdefault(k, ns_to_s(v))
    return phases


def atelet_images(records):
    """Per-image cache outcome and, on a miss, how long the pull took.

    The layer cache is node-local, so the same image is a miss the first time
    an actor lands on a node and a hit forever after; the outcome is what makes
    the pull duration readable. Both the cold-boot and the restore path go
    through here, which is the only place either one times the registry fetch.

    Returns (table, notes). A miss with no pull under it is called out rather
    than left to be read as a free one: atelet dedupes concurrent requests for
    a digest through a singleflight group, so a request that loses the race
    logs its miss, blocks on the winner's fetch, and never logs a pull. The
    step still paid for the wait, and the wait is not in this table.
    """
    out, pulls, misses = {}, [], 0
    for r in records:
        m = msg_of(r)
        if m in ("Image cache hit", "Image cache miss"):
            out[short_ref(r.get("ref", ""))] = m.rsplit(" ", 1)[1].upper()
            misses += m == "Image cache miss"
        elif m == "Image pulled into layer cache":
            pulls.append(r)
    for r in pulls:
        out[f"pull {r.get('digest', '')[7:19]} ({r.get('layers')} layers)"] = ns_to_s(r.get("took"))
    notes = []
    if misses > len(pulls):
        n = misses - len(pulls)
        notes.append(
            f"{n} cache miss{'es' if n > 1 else ''} with no pull logged: deduped into "
            "another actor's in-flight fetch (atelet singleflight). This step waited "
            "on that fetch; the wait is inside ateapi_rpc_total, not in the pull rows."
        )
    return out, notes


def short_ref(ref):
    """Last path element of an image reference, minus the digest."""
    return ref.rsplit("/", 1)[-1].split("@")[0] or ref


def ateapi_facts(records, method):
    """Server-side total, plus the placement and path facts ateapi logs.

    Returns (facts, notes). Numbers go in facts and are printed as a table;
    notes are messages worth reading verbatim, chiefly which snapshot the
    resume chose, because that is what decides whether the step is a boot from
    spec or a restore and therefore which other tables can exist at all.

    Keys prefixed with _ are working values rather than timings: the caller
    compares _node across steps to classify a resume as same-node or a
    migration, and table() hides them.
    """
    facts, notes = {}, []
    for r in records:
        m = msg_of(r)
        if m == "Handle RPC" and r.get("method", "").endswith(method):
            facts["ateapi_rpc_total"] = parse_go_duration(r.get("elapsed-time"))
        elif m == "Picked worker":
            w = str(r.get("worker", ""))
            pod = re.search(r'worker_pod:"([^"]+)"', w)
            node = re.search(r'node_name:"([^"]+)"', w)
            if pod:
                facts["_worker_pod"] = pod.group(1)
            if node:
                facts["_node"] = node.group(1)
            notes.append(f"worker {pod.group(1) if pod else '?'} on {node.group(1) if node else '?'}")
        elif "Restoring from snapshot" in m or "Booting from ActorTemplate spec" in m:
            # The clause, not an "Actor has " prefix: the unrelated "Actor has
            # no assigned worker pod during delete" warning shares that prefix.
            notes.append(m)
        elif m == "FinalizeSuspended store call durations":
            for k in ("get_actor", "release_worker", "refetch_actor", "release_snapshot", "update_actor", "total"):
                if k in r:
                    facts[f"finalize_suspended.{k}"] = ns_to_s(r[k])
    return facts, notes


def collect(args, step, method, window, trace_id, since, actor, span):
    """Join one lifecycle step's records from all three components.

    Scraping happens per step rather than once at the end so that `since` stays
    short and the worker stream stays small enough to filter by timestamp.

    ateapi and atelet are selected by trace ID. ateom is not: its lifecycle
    envelope names the actor but carries no trace ID, so the call's own
    wall-clock span selects its records, padded by a second on each side for
    clock skew between the client and the nodes.
    """
    api = for_trace(logs("app=ate-api-server", ATE_SYSTEM, since), trace_id)
    let = for_trace(logs("app=atelet", ATE_SYSTEM, since), trace_id)
    begin, end = span
    wrk = [r for r in logs(args.worker_selector, args.worker_namespace, since, prefix=True)
           if r["_ts"] is not None and begin - 1 <= r["_ts"] <= end + args.settle + 1]
    wrk.sort(key=lambda r: r["_ts"])

    facts, notes = ateapi_facts(api, method)
    images, image_notes = atelet_images(let)
    phases, meta = ateom_timeline(wrk, window, actor)
    return {
        "step": step,
        "trace_id": trace_id,
        "notes": notes + image_notes,
        "ateapi": facts,
        "images": images,
        "atelet_restore": atelet_restore(let),
        "ateom": phases,
        "ateom_meta": meta,
    }


def table(title, data):
    """Print one block of measurements, or nothing if there are none.

    A float is seconds and renders as milliseconds; anything else prints as it
    is, which is how the image cache reports HIT and MISS alongside durations.
    Underscore-prefixed keys are internal to the join and stay hidden.

    Printing nothing for an empty block is deliberate: a component that logged
    no timings for a step should leave no heading behind suggesting it did.
    """
    rows = [(k, v) for k, v in data.items() if v is not None and not k.startswith("_")]
    if not rows:
        return
    print(f"\n  {title}")
    for k, v in rows:
        print(f"    {k:<44} {v * 1000:>10.1f} ms" if isinstance(v, float) else f"    {k:<44} {v}")


def report(result, placement):
    """Print one step: its trace ID, what happened, then a table per component.

    The tables read outside-in — client, then ate-api-server, then atelet, then
    the worker — so each row's remainder over the one below it is the overhead
    that layer added.
    """
    print("\n" + "=" * 68)
    print(f" {result['step'].upper()}   (trace {result['trace_id'] or 'n/a'})")
    print("=" * 68)
    for n in result["notes"]:
        print(f"  - {n}")
    if result["ateom_meta"].get("worker_pod"):
        print(f"  - worker pod {result['ateom_meta']['worker_pod']}")
    if placement:
        print(f"  - {placement}")
    table("client + ate-api-server", {"client_wall": result["client_wall"], **result["ateapi"]})
    table("atelet image cache (pull from the registry)", result["images"])
    table("atelet restore phases (concurrent: download || assets+oci_unpack)", result["atelet_restore"])
    table("ateom worker steps", {**result["ateom"], "ateom_total": result["ateom_meta"].get("ateom_total")})


def main():
    """Drive one actor through its lifecycle, reporting each step as it goes.

    create (born suspended, no worker) -> resume (boot from spec, or a restore
    if the template has a golden snapshot) -> suspend (checkpoint to object
    storage, release the worker) -> resume (restore what suspend just wrote).

    Whether the last step lands on the same node is the scheduler's choice, so
    it is classified after the fact rather than forced.
    """
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--atespace", required=True)
    p.add_argument("--template", required=True, help="<atespace>/<name> or <name>")
    p.add_argument("--worker-namespace", required=True, help="namespace of the WorkerPool pods")
    p.add_argument("--worker-selector", default="", help="defaults to ate.dev/worker-pool=<--worker-pool>")
    p.add_argument("--worker-pool", default="", help="WorkerPool name")
    p.add_argument("--actor", default="", help="actor name; defaults to a timestamped one")
    p.add_argument("--kubectl-ate", default="kubectl-ate")
    p.add_argument("--settle", type=float, default=3.0, help="seconds to wait before scraping logs")
    p.add_argument("--retries", type=int, default=5, help="retries when the assigned worker pod is gone")
    p.add_argument("--json", default="", help="write the full result to this path")
    p.add_argument("--keep", action="store_true", help="do not delete the actor afterwards")
    args = p.parse_args()

    if not args.worker_selector:
        if not args.worker_pool:
            p.error("one of --worker-selector or --worker-pool is required")
        args.worker_selector = f"ate.dev/worker-pool={args.worker_pool}"

    actor = args.actor or f"prof-{int(time.time())}"
    results, t_origin = [], time.time()

    def since():
        """Seconds of history to ask kubectl for, widening as the run goes on.

        Always reaches back to the start of the run, so a step whose records
        landed late is still in the window, with a floor for the first step.
        """
        return max(int(time.time() - t_origin) + 5, 10)

    # 1. create: metadata only, the actor is born suspended and holds no worker.
    try:
        wall, trace, span = ate(args, f"create actor {actor} --template {args.template}")
        time.sleep(args.settle)
        r = collect(args, "create actor record", "CreateActor", "boot", trace, since(), actor, span)
        r["client_wall"] = wall
        results.append((r, ""))

        # 2. first resume: cold boot from the template spec, or a golden restore
        #    if the template has a golden snapshot. ateapi logs which it chose.
        wall, trace, span = ate(args, f"resume actor {actor}", retries=args.retries)
        time.sleep(args.settle)
        boot = collect(args, "cold boot (resume #1)", "ResumeActor", "boot", trace, since(), actor, span)
        if not boot["ateom"]:  # a golden restore lands in the restore window
            boot.update(collect(args, "initial activation (resume #1)", "ResumeActor", "restore", trace, since(), actor, span))
        boot["client_wall"] = wall
        node_before = boot["ateapi"].get("_node", "")
        results.append((boot, f"node {node_before or 'unknown'}"))

        # 3. live suspend: checkpoint to object storage and release the worker.
        wall, trace, span = ate(args, f"suspend actor {actor}", retries=args.retries)
        time.sleep(args.settle)
        r = collect(args, "live suspend", "SuspendActor", "suspend", trace, since(), actor, span)
        r["client_wall"] = wall
        results.append((r, ""))

        # 4. second resume: restore from the snapshot just written. Whether it
        #    lands on the same node is the pool's choice, so classify it rather
        #    than force it.
        wall, trace, span = ate(args, f"resume actor {actor}", retries=args.retries)
        time.sleep(args.settle)
        r = collect(args, "resume from snapshot", "ResumeActor", "restore", trace, since(), actor, span)
        r["client_wall"] = wall
        node_after = r["ateapi"].get("_node", "")
        kind = "same-node warm resume" if node_after and node_after == node_before else "cross-node migration"
        results.append((r, f"{kind}: {node_before or '?'} -> {node_after or '?'}"))
    finally:
        # A step that fails still measured the ones before it, and still leaves
        # an actor behind, so report and clean up on the way out either way.
        for r, placement in results:
            r["placement"] = placement
            report(r, placement)

        if args.json and results:
            with open(args.json, "w") as f:
                json.dump({"actor": actor, "steps": [r for r, _ in results]}, f, indent=2)
            print(f"\nwrote {args.json}")

        if not args.keep:
            # Deletion requires the actor to be suspended; a run that died
            # mid-resume leaves it running.
            run(f"{args.kubectl_ate} suspend actor {actor} -a {args.atespace}", check=False)
            run(f"{args.kubectl_ate} delete actor {actor} -a {args.atespace}", check=False)
            print(f"cleaned up actor {actor}")


if __name__ == "__main__":
    sys.exit(main())
