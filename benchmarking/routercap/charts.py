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

"""Renders the capacity run's charts from a run directory.

    python3 charts.py benchmarking/routercap/runs/<timestamp>

Reads only ``arm-*/samples.jsonl``, ``arm-*/worker-cpu.jsonl`` and ``arm-*/run.json``
— never a log — so charts regenerate from any past run, including one whose
cluster is long gone.

Standard library only, and the SVG is emitted by hand, so the harness runs in
automation without a plotting stack.

Outputs, all in the run directory:

    timeseries-<arm>c.svg   five stacked panels sharing one x-axis
    report.html             self-contained: headline cards, the charts, the table
    summary.json            the same numbers, machine-readable
"""

import argparse
import datetime
import glob
import html
import json
import math
import os
import re
import sys

# The repository root, for spelling user-facing paths the same way no matter
# what directory a command ran from. charts.py lives in benchmarking/routercap.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Fixed per role, so the same container is the same colour in every chart and
# across every arm.
COLORS = {
    "offered": "#0b0b0b",
    "achieved": "#2a78d6",
    "success": "#1baf7a",
    "in_flight": "#eb6834",
    "connections": "#4a3aa7",
    "p50": "#1baf7a",
    "p95": "#eda100",
    "envoy": "#2a78d6",
    "atenet-router": "#eb6834",
    "loadgen": "#898781",
}

# The hops of one request, bottom of the stack first: (legend label, field in
# Sample.spans, colour). Colours match the CPU panels — blue is envoy, orange
# the sidecar, grey the generator — and labels stay under about fifteen
# characters, which is what the right gutter holds at this font size.
SPAN_LAYERS = [
    ("before Envoy", "before_envoy_ms", "#898781"),
    ("Envoy itself", "envoy_internal_ms", "#2a78d6"),
    ("sidecar", "sidecar_ms", "#eb6834"),
    ("worker leg", "worker_ms", "#1baf7a"),
]
ARM_COLORS = ["#2a78d6", "#1baf7a", "#eda100", "#dc2626", "#4a3aa7", "#0891b2"]

RUNG_FILL = "#f3f4f6"
THRESHOLD_PORTS = "#d03b3b"

# Tooltip text is monospace so the background rectangle can be sized from the
# longest line without measuring glyphs. 6.32px is the advance width of
# ui-monospace at 10.5px, rounded up.
TIP_FONT_PX = 10.5
TIP_CHAR_PX = 6.32
TIP_LINE_PX = 13.5


# --------------------------------------------------------------------------
# loading


def parse_time(s):
    """Parses Go's RFC3339 output, whose sub-second field can be nanoseconds."""
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    if "." in s:
        head, rest = s.split(".", 1)
        frac, _, tz = rest.partition("+")
        s = "%s.%s+%s" % (head, frac[:6], tz) if tz else "%s.%s" % (head, frac[:6])
    try:
        return datetime.datetime.fromisoformat(s)
    except ValueError:
        return None


def read_jsonl(path):
    out = []
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                # A run killed mid-write leaves a torn final line; everything
                # before it is still good.
                continue
    return out


class Arm:
    """One arm's directory, loaded."""

    def __init__(self, path):
        self.path = path
        self.name = os.path.basename(path)
        self.header = {}
        hp = os.path.join(path, "run.json")
        if os.path.exists(hp):
            with open(hp, encoding="utf-8") as fh:
                self.header = json.load(fh)
        self.samples = read_jsonl(os.path.join(path, "samples.jsonl"))
        for s in self.samples:
            s["_t0"] = parse_time(s.get("t0"))
            s["_t1"] = parse_time(s.get("t1"))
            s["_t"] = parse_time(s.get("t"))
        self.samples = [s for s in self.samples if s["_t"]]
        # Kept on the instance, not left a local: the rung schedule is placed
        # against the same zero the samples are.
        self.origin = min((s["_t0"] for s in self.samples if s["_t0"]), default=None)
        for s in self.samples:
            o = self.origin
            s["_x"] = (s["_t"] - o).total_seconds() if o and s["_t"] else 0.0
            s["_x0"] = (s["_t0"] - o).total_seconds() if o and s["_t0"] else 0.0
            s["_x1"] = (s["_t1"] - o).total_seconds() if o and s["_t1"] else 0.0
        # Per-worker-thread CPU from the /proc sampler, when the run carried
        # one. Its timestamps are unix epochs stamped by busybox, not ISO
        # strings.
        self.worker_cpu = read_jsonl(os.path.join(path, "worker-cpu.jsonl"))
        if self.origin:
            oe = self.origin.timestamp()
            for w in self.worker_cpu:
                w["_x"] = (w.get("t0", 0) + w.get("t1", 0)) / 2.0 - oe
            self.worker_cpu = [w for w in self.worker_cpu if w["_x"] > 0]
        else:
            self.worker_cpu = []
        self.rungs = self._schedule()

    def _schedule(self):
        """The ladder as the generator planned it: exact rung boundaries.

        run.json records what the pacer actually ran; sample windows cannot
        substitute, being cut on cAdvisor's clock with no shared rung edge.
        """
        results = self.header.get("results") or []
        if not results:
            return []
        res = next((r for r in results if r.get("arm_cores") == self.cores), results[0])
        out = []
        for r in res.get("rungs") or []:
            start = parse_time(r.get("start_at"))
            if not (start and self.origin):
                continue
            x0 = (start - self.origin).total_seconds()
            out.append({
                "index": r.get("index", 0),
                "qps": r.get("rate_qps", 0),
                "x0": x0,
                # hold and warmup are Go durations, so nanoseconds.
                "x1": x0 + (r.get("hold") or 0) / 1e9,
                "warmup_s": (r.get("warmup") or 0) / 1e9,
            })
        return out

    @property
    def cores(self):
        if self.samples:
            return self.samples[0].get("arm_cores", 0)
        return (self.header.get("arm_cores") or [0])[0]

    @property
    def replicas(self):
        # More than one router pod means the arm's shape describes each pod,
        # not the tier: capacity-per-core and the label both have to say so.
        # The header's router_pods list is the record of it.
        return max(len(self.header.get("router_pods") or []), 1)

    @property
    def label(self):
        # The pass suffix survives only so that a run directory from before the
        # two-pass mode was removed still labels its two same-size arms apart.
        p = self.samples[0].get("pass", 1) if self.samples else 1
        out = "%dc" % self.cores if p <= 1 else "%dc p%d" % (self.cores, p)
        if self.replicas > 1:
            out = "%d×%s" % (self.replicas, out)
        # Everything after arm-<N>c in the directory name is tags: a <M>t tag
        # is the thread count and reads as 8c/2t. Any other tag is carried
        # verbatim, so a merged-in diagnostic arm stays distinguishable from
        # the real arm of the same size.
        tagged = False
        for tag in re.findall(r"-([a-z0-9]+)", self.name.replace("arm-", "", 1)):
            if re.fullmatch(r"\d+t", tag):
                out += "/" + tag
            elif re.fullmatch(r"x\d+", tag):
                pass  # replica count, already on the label from the header
            else:
                out += " " + tag
            tagged = True
        if tagged:
            return out
        # No tags: the samples decide. An arm whose measured --concurrency
        # differs from its core count (run.sh RC_CONCURRENCY) says so, or its
        # chart is indistinguishable from the real arm.
        threads = 0
        for s in self.samples:
            c = (s.get("envoy") or {}).get("concurrency") or 0
            if c:
                threads = int(c)
                break
        if threads and threads != self.cores:
            out += "/%dt" % threads
        return out

    def measured(self):
        """Non-warmup samples: the ones an analysis is entitled to summarize."""
        return [s for s in self.samples if not s.get("warmup")]


def load_run(run_dir):
    arms = []
    for path in sorted(glob.glob(os.path.join(run_dir, "arm-*"))):
        if not os.path.isdir(path):
            continue
        a = Arm(path)
        if a.samples:
            arms.append(a)
        else:
            print("[charts] %s has no samples; skipping" % a.name, file=sys.stderr)
    # Cores, then thread count, then name — numerically, so a thread ladder
    # reads 2t, 4t, 8t, 16t. An arm without a -Nt suffix sorts at its own
    # thread count, placing 8c between 8c-4t and 8c-16t.
    def _threads(a):
        m = re.search(r"-(\d+)t$", a.name)
        return int(m.group(1)) if m else a.cores
    arms.sort(key=lambda a: (a.cores, _threads(a), a.name))
    return arms


# --------------------------------------------------------------------------
# accessors — one place that knows the record shape


def load_of(s, *keys, default=0.0):
    v = s.get("load") or {}
    for k in keys:
        v = (v or {}).get(k)
        if v is None:
            return default
    return v


def client_of(s, field, default=0.0):
    v = (s.get("client") or {}).get(field)
    return default if v is None else v


def container_of(s, role, field, default=0.0):
    c = (s.get("containers") or {}).get(role) or {}
    v = c.get(field)
    return default if v is None else v


# Above this, Envoy's whole-millisecond rounding is large enough to flip which
# hop looks largest. The record carries the ratio; where to stop trusting it
# is a presentation decision and lives here.
COARSE_SHARE = 0.05


def merge_spans(ranges):
    """Overlapping or touching [x0, x1) intervals, merged and sorted."""
    out = []
    for x0, x1 in sorted(ranges):
        if out and x0 <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], x1))
        else:
            out.append((x0, x1))
    return out


def span_shares(s):
    """The four hops of this window as percentages, or None if unmeasured.

    Negative spans are floored at zero and the rest renormalised to 100 — the
    chart's doing, not the collector's. The raw milliseconds, negative or
    not, stay in the hover readout.
    """
    sp = s.get("spans") or {}
    if not sp.get("measured"):
        return None
    vals = [max(sp.get(k) or 0.0, 0.0) for _, k, _ in SPAN_LAYERS]
    total = sum(vals)
    if total <= 0:
        return None
    return [v / total * 100.0 for v in vals]


def has_container(s, role):
    """True when this window actually measured role.

    cAdvisor can close a window before a container's sample ticked, leaving
    container_of at its 0.0 default — a spike to the floor that reads as "the
    sidecar stopped working". Series filter on this so an unsampled window is
    a gap, not a fabricated zero.
    """
    return ((s.get("containers") or {}).get(role) or {}).get("cpu_cores") is not None


# --------------------------------------------------------------------------
# a very small SVG plotter


class Axis:
    """Maps data values onto pixels, linearly or on a log scale."""

    def __init__(self, lo, hi, px0, px1, log=False):
        self.log = log and lo > 0
        if self.log:
            lo, hi = math.log10(max(lo, 1e-9)), math.log10(max(hi, 1e-9))
        if hi <= lo:
            hi = lo + 1.0
        self.lo, self.hi, self.px0, self.px1 = lo, hi, px0, px1

    def __call__(self, v):
        if self.log:
            v = math.log10(max(v, 1e-9))
        f = (v - self.lo) / (self.hi - self.lo)
        return self.px0 + f * (self.px1 - self.px0)

    def ticks(self, n=5):
        if self.log:
            out = []
            for e in range(int(math.floor(self.lo)), int(math.ceil(self.hi)) + 1):
                out.append(10.0 ** e)
            return [t for t in out if math.log10(t) >= self.lo - 1e-9]
        step = nice_step((self.hi - self.lo) / max(n, 1))
        first = math.ceil(self.lo / step) * step
        out, v = [], first
        while v <= self.hi + 1e-9:
            out.append(v)
            v += step
        return out


def nice_step(raw):
    if raw <= 0:
        return 1.0
    mag = 10.0 ** math.floor(math.log10(raw))
    for m in (1, 2, 2.5, 5, 10):
        if raw <= m * mag:
            return m * mag
    return 10 * mag


def fmt(v):
    if v == 0:
        return "0"
    a = abs(v)
    if a >= 1e9:
        return "%.1fG" % (v / 1e9)
    if a >= 1e6:
        return "%.1fM" % (v / 1e6)
    if a >= 1e4:
        return "%.0fk" % (v / 1e3)
    if a >= 100:
        return "%.0f" % v
    if a >= 1:
        return "%.1f" % v
    return "%.3g" % v


# fmt() abbreviates above 10,000, which is right on an axis and wrong wherever
# the exact figure is the point: "11k qps sustainable" and "20k in flight" both
# hide the digits a reader would want to compare against another number.
def comma(v):
    return "{:,}".format(int(round(v)))


def esc(s):
    return html.escape(str(s), quote=True)


# SVG has no text wrapping, so a long <text> silently runs off the canvas
# instead of reflowing. Greedy packing against a character budget is enough
# here: the notes are one font, one size, and prose rather than data, so a
# character count is a close enough proxy for width and errs on the safe side
# for the mostly-lowercase text it is used on.
def wrap(text, budget):
    lines, cur = [], ""
    for word in text.split():
        if cur and len(cur) + 1 + len(word) > budget:
            lines.append(cur)
            cur = word
        else:
            cur = word if not cur else cur + " " + word
    if cur:
        lines.append(cur)
    return lines


class Panel:
    """One plot area inside a chart."""

    # note is a single line under the panel title defining that panel's
    # series. Words next to the lines they describe get read, and they travel
    # with the SVG when it is embedded without the surrounding page.
    def __init__(self, title, ylabel, log=False, note="", fixed=None):
        self.title, self.ylabel, self.log, self.note = title, ylabel, log, note
        # fixed pins the y range instead of deriving it from the data, for a
        # panel whose axis means something on its own — a percentage stack has
        # to run 0 to 100 or the bands stop being shares.
        self.fixed = fixed
        self.series = []      # (label, color, [(x, y, [tooltip lines])])
        self.bands = []       # (x0, x1, fill)
        self.hlines = []      # (y, color, label)
        self.rung_labels = [] # (x_center, text)
        # Hatched spans drawn over everything else, for a stretch of x where the
        # data is there but should not be read. Over, not under: the point is to
        # obscure slightly.
        self.hatch = []       # (x0, x1)
        # A stacked area, drawn as filled polygons rather than lines.
        # stack_layers is bottom-first; stack_segs is contiguous runs of
        # (x, [value per layer]), split so a band is never drawn across
        # windows that were never measured.
        self.stack_layers = []  # (label, color)
        self.stack_segs = []    # [[(x, [v...]), ...], ...]
        # hovers replaces per-point tooltips with one full-height column per
        # x: the tooltip is written once instead of once per series, and the
        # hover target is the whole column instead of a 2.6px dot.
        self.hovers = []      # (x, [tooltip lines])

    def add(self, label, color, points):
        if points:
            self.series.append((label, color, points))

    def stack(self, layers, segs):
        self.stack_layers = layers
        self.stack_segs = [s for s in segs if s]

    def ymax(self):
        vs = [p[1] for _, _, pts in self.series for p in pts]
        vs += [y for y, _, _ in self.hlines]
        return max(vs) if vs else 1.0

    def ymin(self):
        vs = [p[1] for _, _, pts in self.series for p in pts if p[1] > 0]
        return min(vs) if vs else 0.1


class Chart:
    """A column of panels over one shared x-axis."""

    # PAD_T clears the chart title and subtitle above the first panel, and
    # PANEL_GAP clears each panel's own title plus up to NOTE_LINES wrapped
    # lines of note. Both are sized from those constants rather than guessed.
    PAD_L, PAD_R, PAD_B = 76, 150, 58
    PANEL_H, WIDTH = 200, 1180
    NOTE_LINES, NOTE_LEAD = 2, 15
    PANEL_GAP = 28 + NOTE_LINES * NOTE_LEAD
    # 77 puts the first panel's title 18px below the chart subtitle's baseline
    # at y=50.
    PAD_T = 77 + NOTE_LINES * NOTE_LEAD

    # Tooltips are revealed by CSS rather than drawn by JavaScript: report.html
    # and a bare SVG must both work from a file:// URL, and `display` toggling
    # on an SVG group satisfies both.
    CSS = (
        ".hg .tip{display:none;pointer-events:none}"
        ".hg:hover .tip{display:block}"
        ".hit{fill:#1d4ed8;fill-opacity:0}"
        ".hg:hover .hit{fill-opacity:0.07}"
        ".tipbg{fill:#0b0b0b;fill-opacity:0.5;stroke:#374151}"
        ".tiptx{fill:#f9fafb;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;"
        "font-size:%.1fpx;white-space:pre}"
    ) % TIP_FONT_PX

    def __init__(self, title, subtitle, xlabel):
        self.title, self.subtitle, self.xlabel = title, subtitle, xlabel
        self.panels = []

    def panel(self, *a, **kw):
        p = Panel(*a, **kw)
        self.panels.append(p)
        return p

    def render(self):
        n = len(self.panels)
        height = self.PAD_T + n * self.PANEL_H + (n - 1) * self.PANEL_GAP + self.PAD_B
        xs = [p[0] for pan in self.panels for _, _, pts in pan.series for p in pts]
        xs += [b[0] for pan in self.panels for b in pan.bands]
        xs += [b[1] for pan in self.panels for b in pan.bands]
        xs += [x for pan in self.panels for seg in pan.stack_segs for x, _ in seg]
        xlo, xhi = (min(xs), max(xs)) if xs else (0.0, 1.0)
        ax = Axis(xlo, xhi, self.PAD_L, self.WIDTH - self.PAD_R)

        o = [
            '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" '
            'viewBox="0 0 %d %d" font-family="ui-sans-serif,system-ui,sans-serif">'
            % (self.WIDTH, height, self.WIDTH, height),
            '<rect width="100%" height="100%" fill="#ffffff"/>',
            "<style>%s</style>" % self.CSS,
            '<defs><pattern id="hatch" width="7" height="7" patternTransform="rotate(45)" '
            'patternUnits="userSpaceOnUse">'
            '<line x1="0" y1="0" x2="0" y2="7" stroke="#ffffff" stroke-width="3.4"/>'
            "</pattern></defs>",
            '<text x="%d" y="30" font-size="17" font-weight="600" fill="#0b0b0b">%s</text>'
            % (self.PAD_L, esc(self.title)),
            '<text x="%d" y="50" font-size="12" fill="#898781">%s</text>'
            % (self.PAD_L, esc(self.subtitle)),
        ]

        body, tips = [], []
        for i, pan in enumerate(self.panels):
            top = self.PAD_T + i * (self.PANEL_H + self.PANEL_GAP)
            b, t = self._panel(pan, ax, top, top + self.PANEL_H, i == n - 1, height)
            body += b
            tips += t
        # Tooltips last: SVG has no z-index, so painter's order is the only
        # stacking control there is.
        o += body + tips
        o.append("</svg>")
        return "\n".join(o)

    def _tooltip(self, lines, anchor_x, panel_top, height, klass="tip"):
        """A hidden group that a :hover on the enclosing .hg reveals."""
        w = max(len(l) for l in lines) * TIP_CHAR_PX + 18
        h = len(lines) * TIP_LINE_PX + 13
        # Right of the cursor by default, flipped left when that would run off
        # the edge, then clamped so a tooltip near a corner stays whole.
        tx = anchor_x + 15
        if tx + w > self.WIDTH - 6:
            tx = anchor_x - 15 - w
        tx = max(6.0, min(tx, self.WIDTH - w - 6))
        ty = max(6.0, min(panel_top + 8.0, height - h - 6))
        spans = "".join(
            '<tspan x="%.1f" dy="%.1f">%s</tspan>' % (tx + 9, 0 if i == 0 else TIP_LINE_PX, esc(l))
            for i, l in enumerate(lines))
        # display="none" duplicates the stylesheet on purpose: GitHub sanitises
        # <style> out of inline SVGs, and without it every tooltip would paint
        # at once. The presentation attribute survives sanitising, and any CSS
        # rule outranks it.
        return ('<g class="%s" display="none"><rect class="tipbg" x="%.1f" y="%.1f" '
                'width="%.1f" height="%.1f" rx="5"/>'
                '<text class="tiptx" y="%.1f">%s</text></g>'
                % (klass, tx, ty, w, h, ty + 18, spans))

    def _panel(self, pan, ax, top, bot, last, height):
        o, tips = [], []
        if pan.fixed:
            lo, hi = pan.fixed
        else:
            lo = pan.ymin() * 0.8 if pan.log else 0.0
            # Extra headroom on the panel carrying the rung labels, so a
            # threshold label near the top of the range does not print through
            # the label row.
            hi = pan.ymax() * (1.24 if pan.rung_labels else 1.12) or 1.0
        ay = Axis(lo, hi, bot, top, log=pan.log)

        o.append('<rect x="%d" y="%d" width="%d" height="%d" fill="#ffffff" stroke="#e5e7eb"/>'
                 % (ax.px0, top, ax.px1 - ax.px0, bot - top))
        # The budget is the plot's full width — the note runs under the whole
        # panel including the right gutter, where nothing else is drawn.
        nl = wrap(pan.note, int((self.WIDTH - ax.px0 - 8) / 5.1)) if pan.note else []
        nl = nl[:self.NOTE_LINES]
        o.append('<text x="%d" y="%d" font-size="12.5" font-weight="600" fill="#374151">%s</text>'
                 % (ax.px0, top - 9 - len(nl) * self.NOTE_LEAD, esc(pan.title)))
        for i, line in enumerate(nl):
            o.append('<text x="%d" y="%d" font-size="10.5" fill="#898781">%s</text>'
                     % (ax.px0, top - 9 - (len(nl) - 1 - i) * self.NOTE_LEAD, esc(line)))

        for x0, x1, fill in pan.bands:
            w = max(ax(x1) - ax(x0), 0.6)
            o.append('<rect x="%.1f" y="%d" width="%.1f" height="%d" fill="%s"/>'
                     % (ax(x0), top + 1, w, bot - top - 2, fill))
        for xc, text in pan.rung_labels:
            o.append('<text x="%.1f" y="%d" font-size="9" fill="#898781" text-anchor="middle">%s</text>'
                     % (ax(xc), top + 13, esc(text)))

        for t in ay.ticks():
            if t < lo or t > hi:
                continue
            y = ay(t)
            o.append('<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#eef2f7"/>'
                     % (ax.px0, y, ax.px1, y))
            o.append('<text x="%d" y="%.1f" font-size="10.5" fill="#898781" text-anchor="end">%s</text>'
                     % (ax.px0 - 8, y + 3.5, fmt(t)))
        o.append('<text x="14" y="%.1f" font-size="11" fill="#898781" transform="rotate(-90 14 %.1f)" '
                 'text-anchor="middle">%s</text>' % ((top + bot) / 2, (top + bot) / 2, esc(pan.ylabel)))

        for t in ax.ticks(8):
            x = ax(t)
            o.append('<line x1="%.1f" y1="%d" x2="%.1f" y2="%d" stroke="#eef2f7"/>' % (x, top, x, bot))
            if last:
                o.append('<text x="%.1f" y="%d" font-size="10.5" fill="#898781" text-anchor="middle">%s</text>'
                         % (x, bot + 18, fmt(t)))
        if last:
            o.append('<text x="%.1f" y="%d" font-size="11" fill="#898781" text-anchor="middle">%s</text>'
                     % ((ax.px0 + ax.px1) / 2, bot + 40, esc(self.xlabel)))

        # Stacked areas go under everything except the grid: the threshold lines
        # and any series on the same panel have to stay visible over them.
        for seg in pan.stack_segs:
            # A one-window segment has no width to make a polygon out of, so
            # it is drawn as a narrow column instead of being dropped.
            pxs = [ax(x) for x, _ in seg]
            if len(pxs) == 1:
                pxs = [pxs[0] - 1.5, pxs[0] + 1.5]
                seg = [seg[0], seg[0]]
            base = [ay(lo)] * len(seg)
            for i, (label, color) in enumerate(pan.stack_layers):
                cum = [sum(v[:i + 1]) for _, v in seg]
                topy = [ay(min(c, hi)) for c in cum]
                pts = ["%.1f,%.1f" % (x, y) for x, y in zip(pxs, topy)]
                pts += ["%.1f,%.1f" % (x, y) for x, y in zip(reversed(pxs), reversed(base))]
                o.append('<polygon points="%s" fill="%s" fill-opacity="0.82"/>'
                         % (" ".join(pts), color))
                base = topy

        for x0, x1 in pan.hatch:
            o.append('<rect x="%.1f" y="%d" width="%.1f" height="%d" fill="url(#hatch)" '
                     'opacity="0.75"/>'
                     % (ax(x0), top + 1, max(ax(x1) - ax(x0), 1.0), bot - top - 2))

        # Threshold labels sit *inside* the plot area, hard against the right
        # edge: nothing else draws there, so they cannot collide with the
        # series legend in the right gutter.
        for yv, color, label in pan.hlines:
            if yv < lo or yv > hi:
                continue
            y = ay(yv)
            o.append('<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="%s" stroke-width="1.1" '
                     'stroke-dasharray="6 4" opacity="0.8"/>' % (ax.px0, y, ax.px1, y, color))
            tw = len(label) * 5.5 + 8
            o.append('<rect x="%.1f" y="%.1f" width="%.1f" height="13" fill="#ffffff" opacity="0.88"/>'
                     % (ax.px1 - 6 - tw, y - 15, tw))
            o.append('<text x="%d" y="%.1f" font-size="10" fill="%s" text-anchor="end">%s</text>'
                     % (ax.px1 - 10, y - 5, color, esc(label)))

        ly = top + 12
        for label, color, pts in pan.series:
            d = " ".join("%s%.1f,%.1f" % ("M" if j == 0 else "L", ax(x), ay(y))
                         for j, (x, y, _) in enumerate(pts))
            o.append('<path d="%s" fill="none" stroke="%s" stroke-width="1.7" '
                     'stroke-linejoin="round"/>' % (d, color))
            for x, y, tip in pts:
                o.append('<circle cx="%.1f" cy="%.1f" r="2.6" fill="%s" opacity="0.85"/>'
                         % (ax(x), ay(y), color))
                # Per-point hover only where there is no column layer. On the
                # timeseries charts every panel shares one sample grid, so a
                # column describes the whole record once.
                if not pan.hovers and tip:
                    tips.append('<g class="hg"><circle class="hit" cx="%.1f" cy="%.1f" r="7" '
                                'pointer-events="all"/>%s</g>'
                                % (ax(x), ay(y), self._tooltip(tip, ax(x), top, height)))
            o.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" stroke-width="2.4"/>'
                     % (ax.px1 + 12, ly - 4, ax.px1 + 30, ly - 4, color))
            o.append('<text x="%d" y="%d" font-size="11" fill="#374151">%s</text>'
                     % (ax.px1 + 36, ly, esc(label)))
            ly += 17

        # Stack legend reads top-down in the order the bands appear on the
        # chart, which is the reverse of the order they are drawn in.
        for label, color in reversed(pan.stack_layers):
            o.append('<rect x="%d" y="%d" width="18" height="9" fill="%s" fill-opacity="0.82"/>'
                     % (ax.px1 + 12, ly - 9, color))
            o.append('<text x="%d" y="%d" font-size="11" fill="#374151">%s</text>'
                     % (ax.px1 + 36, ly, esc(label)))
            ly += 17
        if pan.hatch:
            o.append('<rect x="%d" y="%d" width="18" height="9" fill="#898781" fill-opacity="0.5"/>'
                     '<rect x="%d" y="%d" width="18" height="9" fill="url(#hatch)" opacity="0.75"/>'
                     % (ax.px1 + 12, ly - 9, ax.px1 + 12, ly - 9))
            o.append('<text x="%d" y="%d" font-size="11" fill="#374151">below the floor</text>'
                     % (ax.px1 + 36, ly))
            ly += 17

        if pan.hovers:
            # Columns tile the plot exactly, each running to the midpoint
            # between its sample and the next: windows vary from 10s to 20s,
            # and a fixed width would leave dead space where no tooltip shows.
            hv = sorted(pan.hovers)
            px = [ax(x) for x, _ in hv]
            edges = [ax.px0] + [(a + b) / 2 for a, b in zip(px, px[1:])] + [ax.px1]
            for i, (_, tip) in enumerate(hv):
                x0, x1 = edges[i], edges[i + 1]
                tips.append('<g class="hg"><rect class="hit" x="%.1f" y="%d" width="%.1f" height="%d" '
                            'pointer-events="all"/>%s</g>'
                            % (x0, top + 1, max(x1 - x0, 1.0), bot - top - 2,
                               self._tooltip(tip, px[i], top, height)))
        return o, tips


# --------------------------------------------------------------------------
# charts


def rung_bands(panel, arm, label=False):
    """Stripes alternate rungs, drawn from the schedule rather than the samples.

    Sample windows are cut on cAdvisor's clock and land up to a whole window
    off the real boundary; the schedule in run.json is what the pacer ran.
    The warmup head is not shaded, but warmup samples are still dropped from
    every summary.
    """
    for r in arm.rungs:
        # Every rung gets an entry, including the ones striped white: the
        # x-axis range is computed from the bands, and skipping white would
        # clip an odd last rung.
        panel.bands.append((r["x0"], r["x1"], RUNG_FILL if r["index"] % 2 == 0 else "#ffffff"))
        if label:
            panel.rung_labels.append(((r["x0"] + r["x1"]) / 2, "R%d" % r["index"]))


def sample_tip(s):
    """One window as aligned monospace lines, for the hover readout.

    Only what a reader needs to judge the window: the setpoint, whether the
    router kept up, what it cost, and where the mean request went. Everything
    else stays in samples.jsonl.
    """
    ach = load_of(s, "achieved_qps")
    suc = load_of(s, "success_qps")
    rate = (suc / ach * 100.0) if ach > 0 else 0.0
    out = [
        "rung %-3d %8s qps setpoint%s" % (s.get("rung", 0), fmt(s.get("rung_qps", 0)),
                                          "  [warmup]" if s.get("warmup") else ""),
        "-" * 30,
        "offered  %8s /s" % fmt(load_of(s, "offered_qps")),
        "achieved %8s /s (%.1f%% ok)" % (fmt(ach), rate),
        "p50 %8.1f ms  p95 %8.1f ms" % (load_of(s, "latency", "p50_ms"),
                                        load_of(s, "latency", "p95_ms")),
        "in-flight %6s  pool %7s" % (fmt(load_of(s, "in_flight_max")),
                                     fmt(client_of(s, "connections_in_use"))),
        "-" * 30,
        "envoy %6.2fc   sidecar %6.2fc" % (container_of(s, "envoy", "cpu_cores"),
                                           container_of(s, "atenet-router", "cpu_cores")),
    ]
    if s.get("_wrk_max") is not None:
        out.append("wrk hottest %.2fc  mean %.2fc" % (s["_wrk_max"], s.get("_wrk_mean") or 0.0))
    # The per-hop block prints the record's own milliseconds, including any
    # negative residual the chart had to floor at zero.
    sp = s.get("spans") or {}
    if sp.get("measured"):
        out += [
            "-" * 30,
            "mean request %8.2f ms" % (sp.get("total_client_ms") or 0.0),
            " before %7.2f  envoy %7.2f" % (sp.get("before_envoy_ms") or 0.0,
                                            sp.get("envoy_internal_ms") or 0.0),
            " sidecar%7.2f  worker%7.2f" % (sp.get("sidecar_ms") or 0.0,
                                            sp.get("worker_ms") or 0.0),
            " resume %7.2f (control plane)" % (sp.get("resume_ms") or 0.0),
        ]
    return out


def timeseries_chart(arm):
    hdr = arm.header
    # Fold the 5s worker samples into each window's tip: the per-thread max
    # and mean say whether a window's CPU was one drowning thread or an even
    # load.
    for s in arm.samples:
        inside = [w for w in arm.worker_cpu if s.get("_x0", 0) <= w.get("_x", -1) <= s.get("_x1", 0)]
        if inside:
            s["_wrk_max"] = max(w.get("max_worker") or 0.0 for w in inside)
            s["_wrk_mean"] = sum(w.get("mean_worker") or 0.0 for w in inside) / len(inside)
    hovers = [(s["_x"], sample_tip(s)) for s in arm.samples]
    c = Chart(
        "atenet-router capacity — %s" % arm.label,
        "%s · %s · window boundaries are cAdvisor's, so one vertical line is one interval in every panel"
        % (hdr.get("cluster", "?"), hdr.get("machine_type", "?")),
        "seconds since the arm started",
    )

    # Series definitions live in the report's legend section, not in a note
    # under the title: five panels of two-line captions was more ink than the
    # charts themselves.
    p = c.panel("Offered vs achieved throughput, and concurrency", "requests / s")
    rung_bands(p, arm, label=True)
    for key, label, color in (("offered_qps", "offered", "offered"),
                              ("achieved_qps", "achieved", "achieved"),
                              ("success_qps", "success", "success"),
                              ("in_flight_max", "in-flight (max)", "in_flight")):
        p.add(label, COLORS[color], [(s["_x"], load_of(s, key), sample_tip(s)) for s in arm.samples])
    # The generator's pool is not a knob: one stalled second makes blocked
    # requests dial thousands of connections that persist for IdleConnTimeout,
    # moving latency for a minute afterwards at unchanged offered load.
    # Plotting it beside in-flight and the port ceiling makes those minutes
    # readable as a pool transient rather than as router capacity.
    p.add("client pool", COLORS["connections"],
          [(s["_x"], client_of(s, "connections_in_use"), sample_tip(s)) for s in arm.samples])
    # Threshold labels say what happens at the line, not what the line is
    # called.
    breaker = hdr.get("circuit_breaker_limit") or 0
    if breaker:
        p.hlines.append((breaker, COLORS["in_flight"], "envoy sheds above %s in flight" % comma(breaker)))
    # The router pod's range caps Envoy's upstream connections — the in-flight
    # series. The generator's own (wider) budget is enforced by the
    # client_ports guard and recorded in the run header rather than drawn.
    avail = (hdr.get("port_range") or {}).get("high", 0) - (hdr.get("port_range") or {}).get("low", 0) + 1
    if avail > 1:
        p.hlines.append((avail, THRESHOLD_PORTS,
                         "router kernel out of upstream source ports at %s (%s)"
                         % (comma(avail), (hdr.get("port_range") or {}).get("source", "?"))))


    p = c.panel("Client latency from scheduled send time (log scale)", "milliseconds", log=True)
    rung_bands(p, arm)
    for key, label in (("p50_ms", "p50"), ("p95_ms", "p95")):
        p.add(label, COLORS[label], [(s["_x"], load_of(s, "latency", key), sample_tip(s))
                                     for s in arm.samples if load_of(s, "latency", key) > 0])

    # Share rather than milliseconds: the mean request runs from ~3 ms to
    # ~1.7 s across a sweep, so a linear axis smears everything before the
    # collapse and a log axis cannot be stacked. Skipped entirely for a run
    # recorded before the spans existed — an empty axis would read as "every
    # hop took zero".
    segs, cur, coarse = [], [], []
    for s in arm.samples:
        sh = span_shares(s)
        if sh is None:
            if cur:
                segs.append(cur)
                cur = []
            continue
        cur.append((s["_x"], sh))
        if ((s.get("spans") or {}).get("resolution_ms_share") or 0.0) > COARSE_SHARE:
            coarse.append((s["_x0"], s["_x1"]))
    if cur:
        segs.append(cur)
    if segs:
        p = c.panel("Where the mean request spent its time", "% of mean request", fixed=(0.0, 100.0))
        rung_bands(p, arm)
        p.stack([(label, color) for label, _, color in SPAN_LAYERS], segs)
        p.hatch = merge_spans(coarse)

    p = c.panel("Router CPU, per container", "cores")
    rung_bands(p, arm)
    for role in ("envoy", "atenet-router"):
        p.add(role, COLORS[role], [(s["_x"], container_of(s, role, "cpu_cores"), sample_tip(s))
                                   for s in arm.samples if has_container(s, role)])
    if arm.cores:
        p.hlines.append((arm.cores, COLORS["envoy"],
                         "envoy CPU limit, %d cores — the variable under test" % arm.cores))

    # The threads get their own panel: per-thread values live between 0 and 1
    # core, and on a container-scaled axis the max-vs-mean gap is two pixels
    # tall. The fixed 1.0 ceiling is absolute — a thread there is saturated
    # no matter what the container average says.
    if arm.worker_cpu:
        p = c.panel("Per-thread CPU: hottest worker vs the mean", "cores of one thread",
                    fixed=(0.0, 1.15))
        rung_bands(p, arm)
        worker_tip = lambda w: (["worker threads", "",
                                 "max  %6.2f cores" % (w.get("max_worker") or 0.0),
                                 "mean %6.2f cores" % (w.get("mean_worker") or 0.0)] +
                                ["  wrk %-3s %5.2f" % (k, v)
                                 for k, v in sorted((w.get("workers") or {}).items(),
                                                    key=lambda kv: int(kv[0]))])
        p.add("hottest worker", "#4a3aa7",
              [(w["_x"], w.get("max_worker"), worker_tip(w)) for w in arm.worker_cpu])
        p.add("mean worker", "#9085e9",
              [(w["_x"], w.get("mean_worker"), worker_tip(w)) for w in arm.worker_cpu])
        p.hlines.append((1.0, "#4a3aa7", "one core — a single thread's ceiling"))

    p = c.panel("Router memory working set, per container", "bytes")
    rung_bands(p, arm)
    for role in ("envoy", "atenet-router"):
        p.add(role, COLORS[role], [(s["_x"], container_of(s, role, "memory_working_set_bytes"), sample_tip(s))
                                   for s in arm.samples if has_container(s, role)])

    # Every panel shares the sample grid, so the whole record is described once
    # per column instead of once per line per column.
    for p in c.panels:
        p.hovers = hovers
    return c.render()


def sustainable_qps(arm, ratio=0.99, p50_ceiling_ms=100.0):
    """Highest rung the router served: (nominal offered QPS, rung index).

    A rung is served when all three hold over the rung's measured windows,
    summed: achieved >= ratio x offered, success >= ratio x offered, and no
    single window's p50 above p50_ceiling_ms. Every rung is tested and the
    highest one that passes wins; a rung that fails does not stop the scan,
    so a transient below the ceiling cannot truncate the answer.

    Aggregated per rung, not per window, because windows close on cAdvisor's
    clock and clip in-flight requests either way. The scale is the rung's
    nominal setpoint, not its measured mean, because windows straddle rung
    boundaries. See RESULTS.md for the failure each term was added to fix.

    The rung index comes back so the report can say whether a guard trip
    landed above the sustainable rung (a footnote; the headline stands) or on
    or below it (the headline is measuring the rig). The definition travels
    with the value into summary.json.
    """
    by_rung = {}
    for s in arm.measured():
        by_rung.setdefault(s.get("rung", 0), []).append(s)
    best, best_rung = 0.0, -1
    for rung in sorted(by_rung):
        ss = by_rung[rung]
        offered = sum(load_of(s, "offered_qps") for s in ss)
        if offered <= 0:
            continue
        if sum(load_of(s, "achieved_qps") for s in ss) < ratio * offered:
            continue
        if sum(load_of(s, "success_qps") for s in ss) < ratio * offered:
            continue
        if max(load_of(s, "latency", "p50_ms") for s in ss) > p50_ceiling_ms:
            continue
        # Fall back to the measured mean when a record predates rung_qps.
        nominal = max((s.get("rung_qps") or 0) for s in ss)
        nominal = nominal or (offered / len(ss))
        if nominal >= best:
            best, best_rung = nominal, rung
    return best, best_rung


# --------------------------------------------------------------------------
# summary and report


def summarize(arms):
    out = {"arms": [], "definition": {
        "sustainable_qps": "nominal QPS of the highest rung whose measured windows, summed, kept both "
                           "achieved and success >= 0.99 x offered with no window's p50 above 100 ms. "
                           "Aggregated per rung, not per window, because windows close on cAdvisor's "
                           "clock and clip in-flight requests either way. Every rung is tested: a rung "
                           "that fails does not stop the scan, so a transient below the ceiling cannot "
                           "truncate the answer",
        "span_*_ms": "the mean request of this rung split across the hops it passed through: before_envoy "
                     "(the generator's own queueing and dial), envoy (Envoy's own work plus the loopback "
                     "wire to the sidecar), sidecar (the ext_proc handler), worker (Envoy's upstream_rq_time "
                     "on the actor cluster). The four sum to span_total_ms. span_resume_ms is the "
                     "control-plane round trip and is not one of the four: it happens inside the sidecar's "
                     "handler per request, but the two means are over different populations and it can read "
                     "larger than span_sidecar_ms once the parking lot starts shedding. Means throughout, because percentiles "
                     "do not decompose. Request-weighted across the rung's windows. span_count_spread is "
                     "how far apart the four instruments' request counts were in the worst window, as a "
                     "fraction: above ~0.1 the four spans describe different populations and the split "
                     "should not be read closely. span_resolution_share is Envoy's whole-millisecond "
                     "rounding as a fraction of span_total_ms, worst window of the rung: it bounds how far "
                     "the envoy and before_envoy spans are overstated, and above ~0.05 the split is mostly "
                     "an artefact of the instrument",
        "worker_*": "the per-worker-thread view, all worst-cases within the rung because skew is the point. "
                    "worker_cores_max is the busiest single Envoy thread (from the /proc sampler, 5s "
                    "intervals; 0 when the run carried no sampler) — one thread at ~1.0 is saturated no "
                    "matter what the container total reads. worker_loop_us_worst is the worst worker's mean "
                    "event-loop iteration in the worst window, microseconds; healthy is under ~500, and "
                    "10000+ means requests wait that long just for the loop to come around. "
                    "worker_accept_spread is max minus min connections accepted per worker in a window — "
                    "connections pin to their worker for life, so persistent spread becomes load skew. "
                    "watchdog_misses sums event-loop stalls past 200ms/1s across workers and windows",
        "contention_*": "Envoy mutex contention over the rung (requires --enable-mutex-tracing): "
                        "acquisitions that blocked, and cycles spent blocked, summed across windows. Cycles, "
                        "not seconds — read against its own baseline",
        "ate_h2_*": "the ext_proc leg's HTTP/2 connections at the closing scrape, worst window: streams "
                    "open (one per in-flight request through the router) and bytes buffered unsent. "
                    "Nonzero pending bytes means requests are queued on the wire to the sidecar itself",
    }}
    for arm in arms:
        ms = arm.measured()
        rungs = {}
        for s in ms:
            r = rungs.setdefault(s.get("rung", 0), {
                "rung": s.get("rung", 0), "rung_qps": s.get("rung_qps", 0.0), "windows": 0,
                "offered_qps": 0.0, "achieved_qps": 0.0, "success_qps": 0.0,
                "p50_ms": 0.0, "p95_ms": 0.0,
                "dispatch_lag_p95_ms": 0.0, "in_flight_max": 0.0,
                "envoy_cores": 0.0, "sidecar_cores": 0.0,
                # Counted separately from "windows": a window can carry load
                # data yet no cAdvisor sample for a container, and dividing a
                # partial sum by the full count would bias cores low — the
                # denominator of QPS-per-core, the headline number.
                "envoy_cpu_windows": 0, "sidecar_cpu_windows": 0,
                # The peak window as well as the mean: sizing is done against
                # the peak (the utilisation RESULTS.md quotes), the mean
                # belongs in table columns. Both are recorded and labelled.
                "envoy_cores_max": 0.0,
                "envoy_memory_bytes": 0.0, "sidecar_memory_bytes": 0.0,
                "envoy_throttled_seconds": 0.0,
                # The per-hop breakdown, request-weighted (see below).
                # span_requests is the divisor and doubles as the flag: zero
                # means no window in this rung produced a breakdown.
                "span_before_ms": 0.0, "span_envoy_ms": 0.0, "span_sidecar_ms": 0.0,
                "span_worker_ms": 0.0, "span_resume_ms": 0.0,
                "span_total_ms": 0.0, "span_requests": 0.0, "span_count_spread": 0.0,
                "span_resolution_share": 0.0,
                # The per-worker-thread view, all worst-cases: skew is the
                # point, and averaging skew away is how it stayed invisible.
                "worker_loop_us_worst": 0.0, "worker_accept_spread": 0.0,
                "watchdog_misses": 0.0,
                "contention_count": 0.0, "contention_wait_cycles": 0.0,
                "ate_h2_streams_max": 0.0, "ate_h2_pending_bytes_max": 0.0,
                "worker_cores_max": 0.0,
            })
            r["windows"] += 1
            r["offered_qps"] += load_of(s, "offered_qps")
            r["achieved_qps"] += load_of(s, "achieved_qps")
            r["success_qps"] += load_of(s, "success_qps")
            if has_container(s, "envoy"):
                r["envoy_cores"] += container_of(s, "envoy", "cpu_cores")
                r["envoy_cores_max"] = max(r["envoy_cores_max"], container_of(s, "envoy", "cpu_cores"))
                r["envoy_cpu_windows"] += 1
            if has_container(s, "atenet-router"):
                r["sidecar_cores"] += container_of(s, "atenet-router", "cpu_cores")
                r["sidecar_cpu_windows"] += 1
            r["envoy_memory_bytes"] = max(r["envoy_memory_bytes"], container_of(s, "envoy", "memory_working_set_bytes"))
            r["sidecar_memory_bytes"] = max(r["sidecar_memory_bytes"],
                                            container_of(s, "atenet-router", "memory_working_set_bytes"))
            r["envoy_throttled_seconds"] += container_of(s, "envoy", "throttled_seconds")
            # Maxima, not means: a tail is not something to average away.
            for k in ("p50_ms", "p95_ms"):
                r[k] = max(r[k], load_of(s, "latency", k))
            r["dispatch_lag_p95_ms"] = max(r["dispatch_lag_p95_ms"], load_of(s, "dispatch_lag", "p95_ms"))
            r["in_flight_max"] = max(r["in_flight_max"], load_of(s, "in_flight_max"))
            # Weighted by the requests each window's mean was taken over, so a
            # rung's number is the mean of its requests rather than of its
            # windows. count_spread is carried as the worst window's, because
            # one window of instrument disagreement makes the rung's average
            # suspect.
            sp = s.get("spans") or {}
            if sp.get("measured"):
                w = sp.get("client_requests") or 0.0
                if w > 0:
                    r["span_requests"] += w
                    for key, field in (("span_before_ms", "before_envoy_ms"),
                                       ("span_envoy_ms", "envoy_internal_ms"),
                                       ("span_sidecar_ms", "sidecar_ms"),
                                       ("span_worker_ms", "worker_ms"),
                                       ("span_resume_ms", "resume_ms"),
                                       ("span_total_ms", "total_client_ms")):
                        r[key] += (sp.get(field) or 0.0) * w
                    r["span_count_spread"] = max(r["span_count_spread"], sp.get("count_spread") or 0.0)
                    r["span_resolution_share"] = max(r["span_resolution_share"],
                                                     sp.get("resolution_ms_share") or 0.0)
            e = s.get("envoy") or {}
            workers = e.get("workers") or []
            if workers:
                r["worker_loop_us_worst"] = max([r["worker_loop_us_worst"]] +
                                                [w.get("mean_loop_us") or 0.0 for w in workers])
                r["watchdog_misses"] += sum((w.get("watchdog_miss") or 0.0) +
                                            (w.get("watchdog_mega_miss") or 0.0) for w in workers)
                accepts = [w.get("accepted_cx") or 0.0 for w in workers]
                if sum(accepts) > 0:
                    r["worker_accept_spread"] = max(r["worker_accept_spread"],
                                                    max(accepts) - min(accepts))
            cont = e.get("contention") or {}
            r["contention_count"] += cont.get("num_contentions") or 0.0
            r["contention_wait_cycles"] += cont.get("wait_cycles") or 0.0
            ate = (e.get("clusters") or {}).get("ate-cluster") or {}
            r["ate_h2_streams_max"] = max(r["ate_h2_streams_max"], ate.get("http2_streams_active") or 0.0)
            r["ate_h2_pending_bytes_max"] = max(r["ate_h2_pending_bytes_max"],
                                                ate.get("http2_pending_send_bytes") or 0.0)
            # The /proc sampler's intervals inside this window, worst worker.
            for w in arm.worker_cpu:
                if s["_x0"] <= w["_x"] < s["_x1"]:
                    r["worker_cores_max"] = max(r["worker_cores_max"], w.get("max_worker") or 0.0)
        for r in rungs.values():
            n = max(r["windows"], 1)
            for k in ("offered_qps", "achieved_qps", "success_qps"):
                r[k] /= n
            # Each container's mean is over the windows that actually sampled
            # it, not over every window in the rung.
            r["envoy_cores"] /= max(r["envoy_cpu_windows"], 1)
            r["sidecar_cores"] /= max(r["sidecar_cpu_windows"], 1)
            if r["span_requests"] > 0:
                for k in ("span_before_ms", "span_envoy_ms", "span_sidecar_ms",
                          "span_worker_ms", "span_resume_ms", "span_total_ms"):
                    r[k] /= r["span_requests"]
        trips = {}
        for s in arm.samples:
            for g in s.get("guards") or []:
                # first_detail is kept alongside detail: detail belongs to the
                # largest |value| seen (for a minimum-threshold guard that is
                # the healthiest window), first_detail to the window that
                # actually stopped the ladder.
                t = trips.setdefault(g.get("guard", "?"), {
                    "guard": g.get("guard"), "count": 0, "fatal": False,
                    "worst": 0.0, "detail": "",
                    "first_rung": s.get("rung", -1), "first_detail": g.get("detail", ""),
                })
                t["count"] += 1
                t["fatal"] = t["fatal"] or bool(g.get("fatal"))
                if abs(g.get("value", 0.0)) >= abs(t["worst"]):
                    t["worst"], t["detail"] = g.get("value", 0.0), g.get("detail", "")
        q, q_rung = sustainable_qps(arm)
        # An arm that sustains its ladder's top rung never met a wall — its
        # number is a floor, not a capacity. Without the flag a short
        # diagnostic ladder's card reads as a cap, the one conclusion the run
        # cannot support.
        top = max((r["index"] for r in arm.rungs), default=-1)
        out["arms"].append({
            "name": arm.name,
            "label": arm.label,
            "cores": arm.cores,
            "windows": len(arm.samples),
            "measured_windows": len(ms),
            "sustainable_qps": q,
            "sustainable_rung": q_rung,
            "ladder_topped_out": q_rung >= 0 and q_rung == top,
            "ladder_top_rung": top,
            # Per core of the whole tier: a 2-replica arm's denominator is
            # both pods' cores, or splitting a pod would look like doubling
            # its efficiency.
            "qps_per_core": q / (arm.cores * arm.replicas) if arm.cores else 0.0,
            "replicas": arm.replicas,
            "peak_in_flight": max((load_of(s, "in_flight_max") for s in ms), default=0),
            "max_alignment_spread_ms": max((s.get("alignment_spread_ms", 0.0) for s in arm.samples), default=0.0),
            "guard_trips": sorted(trips.values(), key=lambda t: -t["count"]),
            "missing_containers": sorted({m for s in arm.samples for m in (s.get("missing") or [])}),
            "rungs": [rungs[k] for k in sorted(rungs)],
            "header": arm.header,
        })
    return out


# Each arm's headline number and a one-line status. The card answers exactly
# two questions — what did this arm sustain, and can the number be trusted —
# and everything explanatory lives in summary.json or the note above the
# cards.
def arm_card(a):
    sust, sr = a["sustainable_qps"], a.get("sustainable_rung", -1)

    # Guard trips appear here only when one lands AT or BELOW the sustainable
    # rung — that changes what the number *is* (a floor, not a measurement),
    # so it stays, in red. Trips above it are footnotes for RESULTS.md.
    note = ""
    fatal = [t for t in a["guard_trips"] if t["fatal"]]
    if fatal:
        first = min(fatal, key=lambda t: t["first_rung"])
        if not first["first_rung"] > sr >= 0:
            note = ("<div class=cardn><span class=bad>rig-limited at rung %d (%s) — a floor, "
                    "not a measurement</span></div>" % (first["first_rung"], esc(first["guard"])))

    # A "≥" and a different unit line: an arm that held its ladder's top rung
    # was never pushed to a wall, and its card must not print the same claim
    # as an arm that was.
    big, unit = comma(sust), "qps sustainable"
    if a.get("ladder_topped_out"):
        big, unit = "≥ " + comma(sust), "qps — top rung held, wall not reached"

    top = a.get("ladder_top_rung", -1)
    held = ("held through rung %d of %d" % (sr, top) if sr >= 0 and top >= 0
            else "held through rung %d" % sr if sr >= 0 else "no rung held")
    return ("<div class=card><div class=cardh>%s</div>"
            "<div class=big>%s <span class=unit>%s</span></div>"
            "<div class=cardl>%s · %s qps per core</div>%s</div>"
            % (esc(a["label"]), big, unit, held, comma(a["qps_per_core"]), note))


def legend_html():
    """The report's one legend, grouped by panel.

    Only the series a reader cannot decode from its name are listed. Swatches
    use the exact chart colours, so the legend doubles as the colour key —
    blue is always envoy, orange always the sidecar.
    """
    def sw(color, hatch=False):
        extra = ("background-image:repeating-linear-gradient(45deg,#fff 0 3px,transparent 3px 6px);"
                 if hatch else "")
        return '<span class=sw style="background:%s;%s"></span>' % (color, extra)

    groups = [
        ("Throughput panel", [
            (sw(COLORS["offered"]), "offered", "requests the pacer scheduled — the independent variable"),
            (sw(COLORS["achieved"]), "achieved", "requests that came back at all"),
            (sw(COLORS["success"]), "success", "the achieved subset that returned HTTP 200"),
            (sw(COLORS["in_flight"]), "in-flight", "sent and not yet answered — a count, not a rate"),
            (sw(COLORS["connections"]), "client pool", "TCP connections the generator holds open; steps up "
             "when a stall makes blocked requests dial, and holds for the 120 s idle timeout"),
        ]),
        ("Time-share panel (stacked bands, bottom up)", [
            (sw(c), lbl, {"before Envoy": "queued in the generator, plus the dial",
                          "Envoy itself": "Envoy's own work plus the loopback to the sidecar",
                          "sidecar": "the ext_proc handler, actor resume included",
                          "worker leg": "request on the wire to the worker and back"}[lbl])
            for lbl, _, c in SPAN_LAYERS
        ] + [
            (sw("#898781", hatch=True), "hatched", "Envoy rounds request times to whole milliseconds; where "
             "hatched, that rounding exceeds %d%% of the mean request and the split is mostly instrument"
             % int(COARSE_SHARE * 100)),
        ]),
        ("CPU and memory panels", [
            (sw(COLORS["envoy"]), "envoy", "the proxy — its CPU limit is the variable under test"),
            (sw(COLORS["atenet-router"]), "atenet-router", "the Go ext_proc sidecar that picks the worker "
             "and resumes the actor; same limit in every arm"),
        ]),
        ("Per-thread CPU panel", [
            (sw("#4a3aa7"), "hottest worker", "the busiest single Envoy thread (from /proc): a connection "
             "stays on its thread for life, so one thread can saturate while the container average reads idle"),
            (sw("#9085e9"), "mean worker", "the same threads' average — the lines hugging is balanced "
             "load, a gap opening is skew: one thread drowning; at 1.0 the hottest thread is saturated "
             "outright"),
        ]),
        ("Chart furniture", [
            ('<span class=sw style="background:#f3f4f6"></span>', "grey stripes", "alternate rungs, "
             "labelled R0, R1, … along the top"),
            ('<span class=sw style="height:0;border-radius:0;margin-bottom:3px;'
             'border-top:2px dashed #898781;opacity:0.8"></span>', "dashed lines", "ceilings, each labelled at the line itself. \"envoy sheds above "
             "20,000 in flight\" is the actor cluster's circuit breaker: past it Envoy answers 503 itself "
             "and counts the rejection, set below the port budget on purpose so overload shows up as an "
             "Envoy counter instead of the kernel's opaque connect failure"),
            ('<span class=sw style="background:#fff;border:1px solid #d1d5db"></span>', "gaps",
             "a hole in a CPU or memory line is a window where cAdvisor returned no fresh sample — "
             "nobody looked is not the same as it went idle"),
        ]),
    ]
    out = []
    for title, entries in groups:
        items = "".join("<li>%s<b>%s</b> — %s</li>" % (swatch, esc(label), esc(text))
                        for swatch, label, text in entries)
        out.append("<div class=lgroup><div class=lhead>%s</div><ul class=legend>%s</ul></div>" % (title, items))
    return "".join(out)


def report_html(run_dir, arms, summary, charts):
    rows = []
    for a in summary["arms"]:
        for r in a["rungs"]:
            ach, suc = r["achieved_qps"], r["success_qps"]
            # The sustainable rung is the row the whole table exists to
            # locate, so it carries the highlight.
            cls = ' class=sust' if r["rung"] == a.get("sustainable_rung", -1) else ""
            rows.append(
                "<tr%s><td>%s</td><td>%d</td><td>%s</td><td>%s</td><td>%s</td><td>%.2f%%</td>"
                "<td>%.1f</td><td>%.1f</td><td>%s</td><td>%.2f</td><td>%.2f</td><td>%s</td></tr>"
                % (cls, esc(a["label"]), r["rung"], fmt(r["offered_qps"]), fmt(ach), fmt(suc),
                   (suc / ach * 100.0) if ach > 0 else 0.0,
                   r["p50_ms"], r["p95_ms"], fmt(r["in_flight_max"]),
                   r["envoy_cores"], r["sidecar_cores"], fmt(r["envoy_memory_bytes"])))

    hdr = arms[0].header if arms else {}
    armcards = "".join(arm_card(a) for a in summary["arms"])
    legend = legend_html()

    return """<!doctype html>
<!--
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
<meta charset="utf-8">
<title>atenet-router capacity</title>
<style>
 :root{--ink:#0b0b0b;--dim:#898781;--line:#e5e7eb}
 body{font:14px/1.6 ui-sans-serif,system-ui,sans-serif;margin:0;color:var(--ink);background:#fff}
 main{max-width:1240px;margin:0 auto;padding:40px 24px 80px}
 header{border-bottom:1px solid var(--line);padding-bottom:20px;margin-bottom:32px}
 h1{font-size:24px;font-weight:600;margin:0 0 6px;letter-spacing:-0.01em}
 h2{font-size:15px;font-weight:600;margin:44px 0 6px;text-transform:uppercase;
    letter-spacing:0.06em;color:var(--dim)}
 h2+.note{margin:0 0 16px}
 .sub,.note{color:var(--dim);font-size:12.5px}
 .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(232px,1fr));gap:14px;margin-top:8px}
 .card{border:1px solid var(--line);border-radius:10px;padding:14px 16px}
 .cardh{font-size:12px;text-transform:uppercase;letter-spacing:0.06em;color:var(--dim)}
 .big{font-size:26px;font-weight:600;line-height:1.25;margin:2px 0 6px;letter-spacing:-0.02em}
 .unit{font-size:12px;font-weight:400;color:var(--dim);letter-spacing:0}
 .cardl{font-size:12px;color:#4b5563}
 .cardn{font-size:12px;line-height:1.45;margin-top:9px;padding-top:9px;
        border-top:1px solid var(--line)}
 .ok{color:#0ca30c} .bad{color:#b91c1c} .dim{color:var(--dim)}
 .cardn b{font-weight:600}
 table{border-collapse:collapse;font-size:12px;width:100%%;
       font-variant-numeric:tabular-nums}
 th,td{border-bottom:1px solid var(--line);padding:5px 9px;text-align:right}
 th{background:#f9fafb;position:sticky;top:0;font-weight:600;color:#374151;
    border-bottom:1px solid #d1d5db}
 td:first-child,th:first-child{text-align:left}
 tbody tr:hover{background:#f9fafb}
 tr.sust td{background:#eff6ff;font-weight:600}
 tr.sust:hover td{background:#e0edfd}
 svg{max-width:100%%;height:auto}
 .chart{border:1px solid var(--line);border-radius:10px;margin-bottom:20px;overflow:hidden}
 ul{color:#4b5563;font-size:12.5px;padding-left:20px}
 code{background:#f3f4f6;padding:1px 5px;border-radius:3px;font-size:0.92em}
 .legends{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:4px 28px;margin-top:8px}
 .lhead{font-size:12px;font-weight:600;color:#374151;margin-bottom:2px}
 ul.legend{list-style:none;padding:0;margin:0 0 10px;font-size:12px;color:#4b5563}
 ul.legend li{margin:3px 0;line-height:1.45}
 ul.legend b{font-weight:600;color:var(--ink)}
 .sw{display:inline-block;width:14px;height:9px;border-radius:2px;margin-right:7px;vertical-align:baseline}
 footer{margin-top:48px;padding-top:16px;border-top:1px solid var(--line);
        color:var(--dim);font-size:12px}
</style>
<main>
<header>
 <h1>atenet-router capacity</h1>
 <div class=sub>Generated by running <code>python3 %(cmd)s</code></div>
</header>

<h2>Headline</h2>
<div class=note>Sustainable is the highest rung whose measured windows, summed, kept both completed and
successful requests within 1%% of what was offered, with no window's median above 100 ms.</div>
<div class=cards>%(cards)s</div>

<h2>Legend</h2>
<div class=legends>%(legend)s</div>

<h2>Per arm, over time</h2>
<div class=note>One chart per arm. The panels share one x-axis and one set of window boundaries, so a
vertical line through them is a single interval. Hover any column for that window's full numbers.</div>
%(timeseries)s

<h2>Every measured rung</h2>
<div class=note>Warmup windows excluded. Latency, in-flight and memory are the worst window in the rung;
throughput and CPU are the mean across its windows. The highlighted row is each arm's last sustained rung.</div>
<table><thead><tr><th>arm</th><th>rung</th><th>offered</th><th>achieved</th><th>success</th><th>success %%</th>
<th>p50 ms</th><th>p95 ms</th><th>in-flight</th>
<th>envoy c mean</th><th>sidecar c mean</th><th>envoy mem</th></tr></thead><tbody>%(rows)s</tbody></table>

</main>
""" % {
        "started": esc(str(hdr.get("started_at") or "")[:10] or "undated"),
        "cluster": esc(hdr.get("cluster", "?")),
        "machine": esc(hdr.get("machine_type", "?")),
        # The regeneration command, spelled from the repo root regardless of
        # where this invocation ran: both paths are recomputed relative to the
        # root, which sits two levels above this file.
        "cmd": esc("%s %s" % (
            os.path.relpath(os.path.abspath(__file__), REPO_ROOT),
            os.path.relpath(os.path.abspath(run_dir), REPO_ROOT))),
        "cards": armcards,
        "legend": legend,
        "timeseries": "".join('<div class=chart>%s</div>' % s for s in charts["timeseries"]),
        "rows": "".join(rows),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("run_dir", help="A run directory containing arm-*/ subdirectories.")
    args = ap.parse_args()

    arms = load_run(args.run_dir)
    if not arms:
        print("[charts] no arm directories with samples under %s" % args.run_dir, file=sys.stderr)
        return 1

    charts = {"timeseries": []}
    for arm in arms:
        svg = timeseries_chart(arm)
        path = os.path.join(args.run_dir, "timeseries-%s.svg" % arm.name.replace("arm-", ""))
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(svg)
        charts["timeseries"].append(svg)

    summary = summarize(arms)
    with open(os.path.join(args.run_dir, "summary.json"), "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
        fh.write("\n")
    with open(os.path.join(args.run_dir, "report.html"), "w", encoding="utf-8") as fh:
        fh.write(report_html(args.run_dir, arms, summary, charts))

    for a in summary["arms"]:
        flag = " GUARDS: %s" % ", ".join(t["guard"] for t in a["guard_trips"]) if a["guard_trips"] else ""
        # "≥" marks an arm that held its ladder's top rung: a floor, not a wall.
        qual = "≥" if a.get("ladder_topped_out") else " "
        print("[charts] %-14s sustainable %s%8s qps  %7.0f qps/core  peak in-flight %s%s"
              % (a["name"], qual, fmt(a["sustainable_qps"]), a["qps_per_core"], fmt(a["peak_in_flight"]), flag),
              file=sys.stderr)
    print("[charts] wrote report.html, summary.json and %d SVGs to %s"
          % (len(arms), args.run_dir), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
