#!/usr/bin/env python3
"""Profile the cold boot and cold registry pull hidden inside template creation.

profile_lifecycle.py cannot see either. Every actor it creates comes from a
template that already has a golden snapshot, so every activation is a restore,
and by then the image is already in the node's layer cache.

The cold boot is real, but it happens to a different actor. Creating a template
makes the reconciler boot a *golden actor* from the template spec. That actor
has no snapshot, so ateapi takes the boot-from-spec path, and if the image has
never been on the node it also pays the full registry pull. The golden actor is
checkpointed into the golden snapshot and deleted
(cmd/ateapi/internal/controlapi/template_reconciler.go).

So this script creates the template rather than reading one back afterwards.
Scraping after the fact does not work: kubelet rotates container logs by size,
and ate-api-server is chatty enough that its window is tens of minutes. Driving
the creation keeps the scrape seconds behind the event, and gives an exact
wall-clock window to filter on instead of a guess at --since.

    profiling/profile_coldboot.py --instance matplotlib__matplotlib-23476

The pull is the number worth keeping: it is the baseline any future image
streaming work has to beat, and on a cold node it dwarfs everything else
(measured: 14.66 s for a 1.03 GB image, ~97% of the activation).

A pull is only cold the first time a digest reaches a node. Re-running against
an instance already profiled recreates the template but not the cache, and
reports HIT. Use an instance this cluster has not seen.
"""

import argparse
import json
import os
import re
import sys
import time

from profile_lifecycle import (
    ATE_SYSTEM,
    atelet_restore,
    ateom_timeline,
    logs,
    msg_of,
    ns_to_s,
    parse_ts,
    run,
    short_ref,
    table,
)

# Golden actors live in a reserved atespace, because the suspend workflow
# relies on it to always take a full snapshot (template_reconciler.go:188-193).
GOLDEN_ATESPACE = "ate-golden"
HERE = os.path.dirname(os.path.abspath(__file__))
# goldenSnapshotWarmup in template_reconciler.go, which this cannot read. A
# change there needs mirroring here, or --warmup to override it.
GOLDEN_SNAPSHOT_WARMUP = 20.0


def warmup_for(tmpl, warmup):
    """The delay the reconciler inserts before checkpointing the golden actor.

    Mirrors goldenSnapshotWarmupFor: zero when every container declares a readyz
    probe, because ResumeActor already blocked until the workload reported 200,
    and the full delay otherwise. The SWE-bench templates declare none, so they
    pay it in full.

    Used to annotate the report, not to adjust it. The delay is deterministic
    and inherent to creating a template -- ResumeActor returning sets a deadline
    in the template status, and the reconciler requeues on the exact time
    remaining until it (template_reconciler.go:260-261) -- so it is a real part
    of the wall clock, not noise and not an artifact of profiling. Subtracting
    it would report a duration nothing ever took. Naming it lets the reader do
    the arithmetic knowing what they are removing.
    """
    containers = tmpl.get("containers", [])
    if not containers or any(not c.get("readyz") for c in containers):
        return warmup
    return 0.0


def build_segments(tmpl, api, name, t0, t1):
    """Break the build wall clock into segments that were each measured.

    Better than subtracting the warmup constant, which would report a duration
    nothing took. Three recorded instants cut the span where responsibility
    changes hands -- the template's own createTime and takeGoldenSnapshotAt,
    plus the reconciler's "Added actor template to work queue":

      t0 -> createTime          this script: crane digest, atespace check
      createTime -> queued      waiting to be noticed (see below)
      queued -> deadline        the control plane: pull, boot, and the warmup
      deadline -> t1            checkpoint and upload, plus this script's poll

    The second row is worth isolating because it is neither work nor a fixed
    cost. Creating a template does not enqueue it: the only queue.Add is in
    resync, so the periodic list is the event source
    (template_reconciler.go:96-105,123). Discovery therefore waits a uniform
    draw over the resync interval -- 0-20s by default, mean 10s -- and it is
    the main reason two builds of the same image differ.
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
    """Recreate the template and time the whole golden-snapshot cycle.

    swebench-template.sh already resolves the digest, writes the protojson
    manifest and polls for the golden tag, so this reuses it rather than
    keeping a second copy of the manifest in sync with it. RECREATE=1 drops any
    existing template first, which deletes its golden actor and snapshot and so
    forces the boot to happen again.

    Returns the wall-clock span to filter logs by. It brackets the cold boot,
    the warmup, and the checkpoint that follows it, not the boot alone.
    """
    cmd = f"RECREATE=1 ATESPACE={atespace} {env} {script} {instance}"
    t0 = time.time()
    p = run(cmd, check=False)
    t1 = time.time()
    if p.returncode != 0:
        raise SystemExit(f"template build failed:\n{p.stdout.strip()}\n{p.stderr.strip()}")
    return t0, t1, p.stdout.strip()


def template_json(kubectl_ate, atespace, name):
    """Fetch the template, whose UID is also the golden actor's name."""
    p = run(f"{kubectl_ate} get actor-template {name} -a {atespace} -o json", check=False)
    if p.returncode != 0:
        raise SystemExit(f"could not read actor template {atespace}/{name}:\n{p.stderr.strip()}")
    return json.loads(p.stdout)


def image_digests(tmpl):
    """The digests this template's containers pin, keyed digest -> short ref.

    The image-cache logs carry ref, digest, layers and took but no actor, so
    the digest is the only exact way to tie a pull to this template rather than
    to whatever else the node was doing in the same window.
    """
    out = {}
    for c in tmpl.get("containers", []):
        m = re.search(r"@(sha256:[0-9a-f]+)", c.get("image", ""))
        if m:
            out[m.group(1)] = short_ref(c.get("image", ""))
    return out


def image_size(ref):
    """Compressed transfer size from the registry manifest, or None.

    The pull duration alone is not comparable across instances; bytes per
    second is, and that is the figure image streaming has to move.
    """
    p = run(f"crane manifest {ref}", check=False)
    if p.returncode != 0:
        return None
    try:
        mf = json.loads(p.stdout)
    except json.JSONDecodeError:
        return None
    return sum(l.get("size", 0) for l in mf.get("layers", []))


def cold_pull(records, digests, sizes):
    """Cache outcome and pull duration for this template's images.

    Records are already bounded to the build window, so unlike the steady-state
    reading in profile_lifecycle.atelet_images the first outcome seen for a
    digest is the golden actor's own, and later hits cannot overwrite it.
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
    if not any(v == "MISS" for v in out.values()):
        notes.append(
            "image was already in this node's layer cache: this run measures a "
            "warm boot, not a cold pull. Recreating a template does not evict "
            "layers; pick an instance this cluster has not pulled before."
        )
    return out, notes


def boot_path(records):
    """ateapi's account of the golden actor's activation.

    The reconciler calls ResumeActor in process rather than over gRPC, so the
    interceptor that logs "Handle RPC" never fires and there is no server-side
    total to read: the build wall time stands in for it. What the workflow
    itself logs still distinguishes a boot from a restore, which is the claim
    this whole script rests on.
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
                   help="SWE-bench Verified instance; must be one this cluster has not pulled")
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
    pulls, pull_notes = cold_pull(let, digests, sizes)
    phases, meta = ateom_timeline(wrk, "boot", uid)
    ckpt, ckpt_meta = ateom_timeline(wrk, "suspend", uid)

    print("\n" + "=" * 68)
    print(f" GOLDEN ACTOR COLD BOOT   {args.atespace}/{name}")
    print("=" * 68)
    print(f"  golden actor {GOLDEN_ATESPACE}/{uid}")
    for n in notes + pull_notes:
        print(f"  - {n}")
    if not notes:
        print("  - no ateapi workflow record in the window; boot path unconfirmed")

    # Split on the timestamps the reconciler recorded rather than subtracting
    # the warmup, so every row is something that was measured.
    warmup = warmup_for(tmpl, args.warmup)
    segments = build_segments(tmpl, api, name, t0, t1)
    table("build wall clock", segments)
    if warmup:
        print()
        for line in (
            "the build wall clock is not a boot latency. Two waits dominate it:",
            f"  - a fixed {warmup:.0f}s golden-snapshot warmup, inside 'reconciler noticed ->",
            "    snapshot deadline', between the boot finishing and the checkpoint starting.",
            "    Deterministic and inherent to creating any template: not jitter, and not an",
            "    artifact of profiling. It applies because no container here declares a",
            "    readyz probe; one that did would drop it to zero.",
            "  - a variable 0-20s wait to be noticed. Creating a template does not enqueue",
            "    it; the reconciler's periodic resync is the only event source. This is the",
            "    main reason two builds of the same image differ, and it is also real.",
            "  The first and last rows are this script's own cost: crane and kubectl round",
            "  trips before the create, and up to a 5s poll interval after the tag.",
            "The pull below is the figure to compare: it is measured directly.",
        ):
            print(f"  {line}")
    table("atelet image cache (cold pull baseline)", pulls)
    table("atelet phases", atelet_restore(let))
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
                "ateom_boot": phases, "ateom_boot_meta": meta,
                "ateom_checkpoint": ckpt, "ateom_checkpoint_meta": ckpt_meta,
            }, f, indent=2)
        print(f"\nwrote {args.json}")


if __name__ == "__main__":
    sys.exit(main())
