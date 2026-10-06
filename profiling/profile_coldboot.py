#!/usr/bin/env python3
"""Profile the boot from spec, and its image fetch, inside template creation.

profile_lifecycle.py never sees a boot from spec. Every actor it creates comes
from a template that already has a golden snapshot, so every activation is a
restore. A restore still fetches the image if it lands on a node that does not
have it yet, and profile_lifecycle.py reports that, but the boot itself is
always a restore.

The boot from spec happens to the template's *golden actor*, not to an actor
the user creates. Creating a template makes the reconciler boot the golden
actor from the template spec, fetching the image (registry pull, or image
streaming) if the node does not have it. The golden actor is then checkpointed
into the golden snapshot and deleted
(cmd/ateapi/internal/controlapi/template_reconciler.go).

This script (re)creates the template through swebench-template.sh and scrapes
the ate-api-server, atelet and worker logs over the exact wall-clock window of
that build. Container logs rotate, so they cannot be read back reliably later.

    profiling/profile_coldboot.py --instance matplotlib__matplotlib-23476

The image fetch row is the number to compare: it is measured directly, while
the build wall clock also includes a resync wait and a fixed warmup.

A fetch is only cold the first time a digest reaches a node. Re-running on the
same nodes recreates the template but not the node's image cache, so it reports
a cache HIT (pull) or a stat hit (stream). To get a cold fetch again, run on
fresh nodes (repeat_matplotlib.sh recreates the node pools every rep) or use an
instance the cluster has not seen.
"""

import argparse
import json
import os
import re
import sys
import time

from profile_lifecycle import (
    ATE_SYSTEM,
    atelet_flags,
    atelet_streaming,
    ateom_timeline,
    log_level_note,
    logs,
    msg_of,
    ns_to_s,
    parse_ts,
    run,
    short_ref,
    table,
)

# Golden actors live in a reserved atespace, because the suspend workflow
# relies on it to always take a full snapshot (template_reconciler.go).
GOLDEN_ATESPACE = "ate-golden"
HERE = os.path.dirname(os.path.abspath(__file__))
# goldenSnapshotWarmup in template_reconciler.go, which this cannot read. A
# change there needs mirroring here, or --warmup to override it.
GOLDEN_SNAPSHOT_WARMUP = 20.0


def warmup_for(tmpl, warmup):
    """The fixed delay between the golden actor's boot and its checkpoint.

    Mirrors goldenSnapshotWarmupFor in template_reconciler.go: zero when every
    container declares a readyz probe, the full delay otherwise. The SWE-bench
    templates declare none, so they pay all of it. Reported alongside the build
    wall clock.
    """
    containers = tmpl.get("containers", [])
    if not containers or any(not c.get("readyz") for c in containers):
        return warmup
    return 0.0


def build_segments(tmpl, api, name, t0, t1):
    """Split the build wall clock at instants the control plane recorded.

    The cuts are the template's createTime, the reconciler's "Added actor
    template to work queue" log, and the template's takeGoldenSnapshotAt:

      create -> golden snapshot                  whole build (t0 -> t1)
      harness prologue                           this script: crane digest, atespace
                                                 check, (possibly) deleting the old template
      template created -> reconciler noticed     wait for the periodic resync to pick
                                                 up the template (0-20 s, varies run to run)
      reconciler noticed -> snapshot deadline    golden actor placed, image fetched,
                                                 booted, then the fixed warmup
      template created -> snapshot deadline      the two rows above combined, used only
                                                 when the work-queue log is missing
      snapshot deadline -> golden tag            checkpoint and upload, plus up to one
                                                 5 s poll of swebench-template.sh
    """
    created = parse_ts(tmpl.get("metadata", {}).get("createTime"))
    deadline = parse_ts(tmpl.get("status", {})
                        .get("goldenSnapshotStatus", {}).get("takeGoldenSnapshotAt"))
    queued = None
    if created:
        seen = [r["_ts"] for r in api
                if msg_of(r) == "Added actor template to work queue"
                and name in r["_raw"] and r["_ts"] and r["_ts"] >= created]
        queued = min(seen) if seen else None
    return {
        "create -> golden snapshot": t1 - t0,
        "  harness prologue (crane, atespace)": (created - t0) if created else None,
        "  template created -> reconciler noticed": (queued - created) if queued else None,
        "  reconciler noticed -> snapshot deadline": (deadline - queued) if deadline and queued else None,
        "  template created -> snapshot deadline": (deadline - created) if deadline and created and not queued else None,
        "  snapshot deadline -> golden tag": (t1 - deadline) if deadline else None,
    }


def build_template(script, instance, atespace, env):
    """Run swebench-template.sh with RECREATE=1 and time it.

    RECREATE=1 deletes any existing template of the same name, and with it the
    old golden snapshot, so the golden actor boots from spec again. The script
    returns once the new golden snapshot is ready. Returns (t0, t1, stdout);
    t0..t1 covers the boot, the warmup and the checkpoint, and is the window
    the logs are filtered to.
    """
    cmd = f"RECREATE=1 ATESPACE={atespace} {env} {script} {instance}"
    t0 = time.time()
    p = run(cmd, check=False)
    t1 = time.time()
    if p.returncode != 0:
        raise SystemExit(f"template build failed:\n{p.stdout.strip()}\n{p.stderr.strip()}")
    return t0, t1, p.stdout.strip()


def template_json(kubectl_ate, atespace, name):
    """Fetch the template, whose UID is also the golden actor's name.

    Newer kubectl-ate wraps `get -o json` in {"actorTemplates": [...]} even for
    a single name; unwrap it so both shapes read the same.
    """
    p = run(f"{kubectl_ate} get actor-template {name} -a {atespace} -o json", check=False)
    if p.returncode != 0:
        raise SystemExit(f"could not read actor template {atespace}/{name}:\n{p.stderr.strip()}")
    data = json.loads(p.stdout)
    if "actorTemplates" in data:
        items = data["actorTemplates"] or []
        if not items:
            raise SystemExit(f"actor template {atespace}/{name} not found in kubectl-ate output")
        data = items[0]
    return data


def image_digests(tmpl):
    """Map each digest the template's containers pin to its short ref.

    atelet's image logs name the digest but not the actor, so the digest is how
    a pull or stream is tied to this template.
    """
    out = {}
    for c in tmpl.get("containers", []):
        m = re.search(r"@(sha256:[0-9a-f]+)", c.get("image", ""))
        if m:
            out[m.group(1)] = short_ref(c.get("image", ""))
    return out


def image_size(ref):
    """Compressed image size in bytes (sum of manifest layer sizes), or None.

    Used to report pull throughput in MB/s.
    """
    p = run(f"crane manifest {ref}", check=False)
    if p.returncode != 0:
        return None
    try:
        mf = json.loads(p.stdout)
    except json.JSONDecodeError:
        return None
    return sum(l.get("size", 0) for l in mf.get("layers", []))


def cold_pull(records, digests, sizes, streamed=()):
    """The golden actor's image cache outcome (HIT/MISS) and pull duration.

    Records are already limited to the build window, so the first outcome seen
    for a digest is the golden actor's. Adds MB/s when the size is known, and a
    note when the image was already cached. Digests in streamed were served by
    image streaming and never touch the layer cache, so they get no note.
    """
    out, notes = {}, []
    for r in records:
        m, digest = msg_of(r), r.get("digest", "")
        if digest not in digests:
            continue
        ref = digests[digest]
        if m in ("Image cache hit", "Image cache miss"):
            out.setdefault(ref, m.rsplit(" ", 1)[1].upper())
        elif m == "Image pulled into layer cache":
            took = ns_to_s(r.get("took"))
            out[f"pull {ref} ({r.get('layers')} layers)"] = took
            size = sizes.get(digest)
            if size and took:
                out[f"  {size / 1e6:.0f} MB at"] = f"{size / 1e6 / took:.0f} MB/s"
    if not any(v == "MISS" for v in out.values()) and not set(digests) <= set(streamed):
        notes.append(
            "image was already in this node's layer cache: this run measures a "
            "warm boot, not a cold pull. Recreating a template does not evict "
            "layers; run on fresh nodes or pick an instance this cluster has not pulled."
        )
    return out, notes


def boot_path(records):
    """Confirm from ateapi's logs that the golden actor booted from spec.

    Returns the worker pod it was placed on, plus notes: the boot-path message
    ("Booting from ActorTemplate spec", or "NOT a cold boot" if it restored)
    and "worker <pod> on <node>". The reconciler calls ResumeActor in process,
    so there is no "Handle RPC" total; the build wall clock stands in for it.
    """
    facts, notes = {}, []
    for r in records:
        m = msg_of(r)
        # Matched on the clause rather than an "Actor has " prefix, which the
        # unrelated "Actor has no assigned worker pod during delete" warning
        # from the golden actor's teardown also starts with.
        if "Booting from ActorTemplate spec" in m:
            notes.append(m)
        elif "Restoring from snapshot" in m:
            notes.append(f"NOT a cold boot: {m}")
        elif m == "Picked worker":
            w = str(r.get("worker", ""))
            pod = re.search(r'worker_pod:"([^"]+)"', w)
            node = re.search(r'node_name:"([^"]+)"', w)
            if pod:
                facts["_worker_pod"] = pod.group(1)
            notes.append(f"worker {pod.group(1) if pod else '?'} on {node.group(1) if node else '?'}")
    return facts, notes


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--instance", default="matplotlib__matplotlib-23476",
                   help="SWE-bench Verified instance; the fetch is cold only on nodes that have not pulled it")
    p.add_argument("--atespace", default="ate-profiling")
    p.add_argument("--worker-namespace", default="ate-profiling")
    p.add_argument("--worker-pool", default="profiling")
    p.add_argument("--worker-selector", default="")
    p.add_argument("--kubectl-ate", default="kubectl-ate")
    p.add_argument("--script", default=os.path.join(HERE, "swebench-template.sh"))
    p.add_argument("--env", default="", help="extra VAR=value settings for the template script")
    p.add_argument("--warmup", type=float, default=GOLDEN_SNAPSHOT_WARMUP,
                   help="seconds the reconciler waits before checkpointing the golden actor")
    p.add_argument("--json", default="")
    args = p.parse_args()

    selector = args.worker_selector or f"ate.dev/worker-pool={args.worker_pool}"
    name = "swebench-" + re.sub(r"-+", "-", args.instance.replace("_", "-"))

    # Read before build_template starts its clock so it stays out of t0..t1.
    flags = atelet_flags()
    level_note = log_level_note(flags)

    print(f"\nbuilding {args.atespace}/{name} from {args.instance} ...")
    t0, t1, out = build_template(args.script, args.instance, args.atespace, args.env)
    print(out)

    tmpl = template_json(args.kubectl_ate, args.atespace, name)
    uid = tmpl.get("metadata", {}).get("uid", "")
    digests = image_digests(tmpl)
    sizes = {d: image_size(c["image"]) for c in tmpl.get("containers", [])
             for d in digests if d in c.get("image", "")}

    # One scrape covering the build, then filtered to it. Overshooting --since
    # only costs log lines; undershooting loses the event outright.
    since = int(t1 - t0) + 60
    window = lambda rs: [r for r in rs if r["_ts"] is not None and t0 - 2 <= r["_ts"] <= t1 + 2]
    api = window(logs("app=ate-api-server", ATE_SYSTEM, since)) # ateapi logs
    let = window(logs("app=atelet", ATE_SYSTEM, since)) # atelet logs
    wrk = sorted(window(logs(selector, args.worker_namespace, since, prefix=True)), # worker logs
                 key=lambda r: r["_ts"])

    facts, notes = boot_path(api)
    streaming, stream_notes, streamed = atelet_streaming(let, digests)
    pulls, pull_notes = cold_pull(let, digests, sizes, streamed)
    pull_notes += stream_notes + ([level_note] if level_note else [])
    phases, meta = ateom_timeline(wrk, "boot", uid)
    ckpt, ckpt_meta = ateom_timeline(wrk, "suspend", uid)

    print("\n" + "=" * 68)
    print(f" GOLDEN ACTOR COLD BOOT   {args.atespace}/{name}")
    print("=" * 68)
    print(f"  golden actor {GOLDEN_ATESPACE}/{uid}")
    print(f"  atelet --image-streamer={flags['image_streamer']} --log-level={flags['log_level']}")
    for n in notes + pull_notes:
        print(f"  - {n}")
    if not notes:
        print("  - no ateapi workflow record in the window; boot path unconfirmed")

    # Each row is a measured span between recorded instants; the warmup is
    # reported, not subtracted.
    warmup = warmup_for(tmpl, args.warmup)
    segments = build_segments(tmpl, api, name, t0, t1)
    table("build wall clock", segments)
    if warmup:
        print()
        for line in (
            "the build wall clock is not a boot latency. Two waits dominate it:",
            f"  - a fixed {warmup:.0f}s golden-snapshot warmup inside 'reconciler noticed ->",
            "    snapshot deadline' (no container declares a readyz probe).",
            "  - a 0-20s wait for the reconciler's periodic resync to notice the template.",
            "  The first and last rows include this script's own overhead.",
            "Compare the image pull / streaming rows below: they are measured directly.",
        ):
            print(f"  {line}")
    table("atelet image cache (cold pull baseline)", pulls)
    table("atelet image streaming", streaming)
    table("ateom worker steps (boot from spec)", {**phases, "ateom_total": meta.get("ateom_total")})
    table("ateom worker steps (checkpoint to golden)",
          {**ckpt, "ateom_total": ckpt_meta.get("ateom_total")})

    if not phases:
        print("\n  no ateom boot window found for this actor. The worker logs name")
        print("  the actor, so check that the golden actor ran in --worker-namespace")
        print(f"  {args.worker_namespace} under selector {selector}.")

    if args.json:
        with open(args.json, "w") as f:
            json.dump({
                "instance": args.instance,
                "template": f"{args.atespace}/{name}",
                "golden_actor": f"{GOLDEN_ATESPACE}/{uid}",
                "digests": digests, "image_bytes": sizes,
                "build_wall": t1 - t0, "warmup": warmup,
                "build_segments": segments,
                "notes": notes + pull_notes,
                "ateapi": facts, "images": pulls,
                "atelet_flags": flags, "streaming": streaming,
                "ateom_boot": phases, "ateom_boot_meta": meta,
                "ateom_checkpoint": ckpt, "ateom_checkpoint_meta": ckpt_meta,
            }, f, indent=2)
        print(f"\nwrote {args.json}")


if __name__ == "__main__":
    sys.exit(main())
