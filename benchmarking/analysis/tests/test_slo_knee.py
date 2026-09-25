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

"""Unit tests for slo_knee -- pure stdlib, runnable offline with `pytest` or
directly via `python3 test_slo_knee.py` (a tiny built-in runner is provided so
the suite does not depend on pytest being installed in the analysis venv)."""

from __future__ import annotations

import io
import json
import os
import sys

# Make the analysis package importable whether run from the repo root, the
# analysis/ dir, or the tests/ dir.
_HERE = os.path.dirname(os.path.abspath(__file__))
_ANALYSIS = os.path.dirname(_HERE)
_BENCH = os.path.dirname(_ANALYSIS)
for _p in (_BENCH, _ANALYSIS):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import slo_knee  # noqa: E402

FIXTURES = os.path.join(_HERE, "fixtures")


def _fx(name: str) -> str:
    return os.path.join(FIXTURES, name)


# ---------------------------------------------------------------------------
# normalize_percentile_key
# ---------------------------------------------------------------------------


def test_normalize_percentile_variants():
    assert slo_knee.normalize_percentile_key("p95") == "p95"
    assert slo_knee.normalize_percentile_key("95") == "p95"
    assert slo_knee.normalize_percentile_key("95%") == "p95"
    assert slo_knee.normalize_percentile_key("P95") == "p95"
    assert slo_knee.normalize_percentile_key("p99.9") == "p99_9"
    assert slo_knee.normalize_percentile_key("99.9") == "p99_9"
    assert slo_knee.normalize_percentile_key("99.99%") == "p99_99"
    assert slo_knee.normalize_percentile_key(" p50 ") == "p50"


# ---------------------------------------------------------------------------
# _to_float / failure_ratio
# ---------------------------------------------------------------------------


def test_to_float_placeholders():
    assert slo_knee._to_float("12.5") == 12.5
    assert slo_knee._to_float("") is None
    assert slo_knee._to_float("N/A") is None
    assert slo_knee._to_float(None) is None
    assert slo_knee._to_float("garbage") is None


def test_failure_ratio_guards_zero_and_missing():
    p = slo_knee.Point(
        tag="t", test_name="n", metric="m", rps=1.0,
        request_count=0.0, failure_count=0.0, latencies_ms={},
    )
    assert p.failure_ratio is None  # request_count == 0 -> undefined, not div0
    p2 = slo_knee.Point(
        tag="t", test_name="n", metric="m", rps=1.0,
        request_count=100.0, failure_count=3.0, latencies_ms={},
    )
    assert abs(p2.failure_ratio - 0.03) < 1e-9


# ---------------------------------------------------------------------------
# record_to_point: only p* keys become latencies
# ---------------------------------------------------------------------------


def test_record_to_point_extracts_latencies_and_rps():
    rec = {
        "tag": "abc",
        "test_name": "load_x",
        "metric": "grpc_ResumeActor",
        "measurements": {
            "request_count": "1000",
            "failure_count": "2",
            "requests_per_s": "42.5",
            "p50": "100",
            "p95": "900",
            "p99_9": "1500",
            "median_response_time": "100",  # NOT a p* key -> excluded
            "average_response_time": "150",
        },
    }
    pt = slo_knee.record_to_point(rec, users_map={"load_x": 5})
    assert pt.rps == 42.5
    assert pt.users == 5
    assert pt.latency("p50") == 100
    assert pt.latency("p95") == 900
    assert pt.latency("p99_9") == 1500
    # non-percentile measurements must not leak into latencies_ms
    assert "median_response_time" not in pt.latencies_ms
    assert "average_response_time" not in pt.latencies_ms
    assert abs(pt.failure_ratio - 0.002) < 1e-9


def test_record_to_point_na_latency_dropped():
    rec = {
        "tag": "t", "test_name": "n", "metric": "m",
        "measurements": {"p95": "N/A", "requests_per_s": "5", "request_count": "10", "failure_count": "0"},
    }
    pt = slo_knee.record_to_point(rec)
    assert pt.latency("p95") is None


# ---------------------------------------------------------------------------
# compute_knee: clean sweep, ceiling crossing mid-ladder
# ---------------------------------------------------------------------------


def _load(name: str, users_map=None) -> list[slo_knee.Point]:
    recs = list(slo_knee.iter_jsonl_records([_fx(name)]))
    return [slo_knee.record_to_point(r, users_map) for r in recs]


def test_knee_clean_sweep_p95_1s_and_5s():
    users = {
        "glutton_baseline_1_user": 1,
        "glutton_baseline_5_users": 5,
        "glutton_baseline_10_users": 10,
        "glutton_oversubscribe_15_users": 15,
    }
    pts = _load("sweep_clean.jsonl", users)

    # ceiling 1000ms @ p95: only 1_user(300) & 5_users(800) qualify;
    # knee = higher-rps = 5_users @ 48 rps.
    r1 = slo_knee.compute_knee(pts, "p95", 1000.0, slo_knee.DEFAULT_MAX_FAILURE_RATIO)
    assert r1.knee_rps == 48.0
    assert r1.knee_users == 5
    assert r1.knee_test_name == "glutton_baseline_5_users"
    assert r1.knee_latency_ms == 800
    assert r1.slo_ever_met is True
    assert r1.n_under_ceiling == 2
    assert r1.n_valid == 2
    assert r1.n_points == 4

    # ceiling 5000ms @ p95: 1,5,10_users qualify (2200<=5000); 15_users(10500) no.
    # knee = 10_users @ 92 rps.
    r5 = slo_knee.compute_knee(pts, "p95", 5000.0, slo_knee.DEFAULT_MAX_FAILURE_RATIO)
    assert r5.knee_rps == 92.0
    assert r5.knee_users == 10
    assert r5.n_under_ceiling == 3


def test_knee_clean_sweep_p50():
    users = {
        "glutton_baseline_1_user": 1,
        "glutton_baseline_5_users": 5,
        "glutton_baseline_10_users": 10,
        "glutton_oversubscribe_15_users": 15,
    }
    pts = _load("sweep_clean.jsonl", users)
    # p50 @ 1000ms: 120,300,700 under; 3200 over. knee = 10_users @ 92 rps.
    r = slo_knee.compute_knee(pts, "p50", 1000.0, slo_knee.DEFAULT_MAX_FAILURE_RATIO)
    assert r.knee_rps == 92.0
    assert r.knee_users == 10


# ---------------------------------------------------------------------------
# compute_knee: failure-ratio guard excludes a fast-failing high-RPS point
# ---------------------------------------------------------------------------


def test_knee_failure_guard_excludes_fast_failing_point():
    pts = _load("sweep_failing.jsonl")
    # All three p95 (120,450,300) are <= 1000ms, but load_high_failing has
    # failure_ratio 0.30 >> 0.01, so despite its 200 rps it must NOT be the knee.
    r = slo_knee.compute_knee(pts, "p95", 1000.0, slo_knee.DEFAULT_MAX_FAILURE_RATIO)
    assert r.n_under_ceiling == 3
    assert r.n_valid == 2  # load_high_failing filtered out
    assert r.knee_rps == 60.0  # load_mid, not the 200-rps fast-failer
    assert r.knee_test_name == "load_mid"


def test_knee_failure_guard_relaxed_admits_failing_point():
    pts = _load("sweep_failing.jsonl")
    # With an absurdly loose tolerance the 200-rps point becomes eligible again.
    r = slo_knee.compute_knee(pts, "p95", 1000.0, 0.99)
    assert r.n_valid == 3
    assert r.knee_rps == 200.0
    assert r.knee_test_name == "load_high_failing"


# ---------------------------------------------------------------------------
# compute_knee: SLO never met
# ---------------------------------------------------------------------------


def test_knee_slo_never_met():
    pts = _load("sweep_never_met.jsonl")
    r = slo_knee.compute_knee(pts, "p95", 5000.0, slo_knee.DEFAULT_MAX_FAILURE_RATIO)
    assert r.knee_rps is None
    assert r.slo_ever_met is False
    assert r.n_under_ceiling == 0
    assert r.n_valid == 0
    # table rendering should say so explicitly
    txt = slo_knee.format_table([r])
    assert "SLO NEVER MET" in txt


def test_knee_no_valid_but_slo_met_message():
    # A point under the ceiling but over the failure tolerance -> "no valid knee"
    # (distinct from SLO-never-met).
    pts = [
        slo_knee.Point(
            tag="t", test_name="n", metric="m", rps=10.0,
            request_count=100.0, failure_count=50.0, latencies_ms={"p95": 200.0},
        )
    ]
    r = slo_knee.compute_knee(pts, "p95", 1000.0, slo_knee.DEFAULT_MAX_FAILURE_RATIO)
    assert r.knee_rps is None
    assert r.slo_ever_met is True  # it WAS under the ceiling
    assert r.n_under_ceiling == 1
    assert r.n_valid == 0
    txt = slo_knee.format_table([r])
    assert "no valid knee" in txt


# ---------------------------------------------------------------------------
# analyze: multi-metric grouping
# ---------------------------------------------------------------------------


def test_analyze_groups_by_tag_and_metric():
    records = [
        {"tag": "T", "test_name": "n1", "metric": "grpc_GetActor",
         "measurements": {"request_count": "100", "failure_count": "0", "requests_per_s": "10", "p95": "200"}},
        {"tag": "T", "test_name": "n2", "metric": "grpc_GetActor",
         "measurements": {"request_count": "200", "failure_count": "0", "requests_per_s": "20", "p95": "400"}},
        {"tag": "T", "test_name": "n1", "metric": "grpc_ResumeActor",
         "measurements": {"request_count": "100", "failure_count": "0", "requests_per_s": "5", "p95": "900"}},
        {"tag": "T", "test_name": "n2", "metric": "grpc_ResumeActor",
         "measurements": {"request_count": "200", "failure_count": "0", "requests_per_s": "8", "p95": "3000"}},
    ]
    results = slo_knee.analyze(records, percentiles=["p95"], ceilings_ms=[1000.0])
    # one KneeResult per (tag, metric) group at this single percentile/ceiling
    by_metric = {r.metric: r for r in results}
    assert set(by_metric) == {"grpc_GetActor", "grpc_ResumeActor"}
    # GetActor: both under 1000 -> knee = higher rps (n2 @ 20)
    assert by_metric["grpc_GetActor"].knee_rps == 20.0
    # ResumeActor: only n1 (900) under 1000; n2 (3000) over -> knee = n1 @ 5
    assert by_metric["grpc_ResumeActor"].knee_rps == 5.0


def test_analyze_metric_filter():
    records = list(slo_knee.iter_jsonl_records([_fx("sweep_clean.jsonl")]))
    results = slo_knee.analyze(
        records, percentiles=["p95"], ceilings_ms=[1000.0],
        metric_filter="ResumeActor",
    )
    assert results and all("ResumeActor" in r.metric for r in results)
    results_none = slo_knee.analyze(
        records, percentiles=["p95"], ceilings_ms=[1000.0],
        metric_filter="NoSuchOp",
    )
    assert results_none == []


# ---------------------------------------------------------------------------
# expand_inputs / iter_jsonl_records
# ---------------------------------------------------------------------------


def test_expand_inputs_dir_walk_and_dedup():
    # directory walk finds the fixtures; passing the dir AND a file de-dups.
    one = _fx("sweep_clean.jsonl")
    got = slo_knee.expand_inputs([FIXTURES, one])
    assert one in got
    assert len(got) == len(set(got))  # no dupes


def test_iter_jsonl_skips_blank_lines(tmp_path=None):
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        f.write('{"tag":"t","test_name":"n","metric":"m","measurements":{}}\n')
        f.write("\n")
        f.write("   \n")
        f.write('{"tag":"t","test_name":"n2","metric":"m","measurements":{}}\n')
        path = f.name
    try:
        recs = list(slo_knee.iter_jsonl_records([path]))
        assert len(recs) == 2
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# parse_users_by_name
# ---------------------------------------------------------------------------


def test_parse_users_by_name():
    assert slo_knee.parse_users_by_name("a=1,b=5,c=10") == {"a": 1, "b": 5, "c": 10}
    assert slo_knee.parse_users_by_name("") == {}
    assert slo_knee.parse_users_by_name(None) == {}
    try:
        slo_knee.parse_users_by_name("bad_entry_without_equals")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError on entry without '='")


# ---------------------------------------------------------------------------
# main() CLI end-to-end (JSON output)
# ---------------------------------------------------------------------------


def test_main_json_output_end_to_end(capsys=None):
    argv = [
        _fx("sweep_clean.jsonl"),
        "-p", "p95",
        "--ceiling-ms", "1000",
        "--ceiling-ms", "5000",
        "--users-by-name",
        "glutton_baseline_1_user=1,glutton_baseline_5_users=5,"
        "glutton_baseline_10_users=10,glutton_oversubscribe_15_users=15",
        "--json",
    ]
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        rc = slo_knee.main(argv)
    finally:
        sys.stdout = old
    assert rc == 0
    out = json.loads(buf.getvalue())
    # two ceilings -> two results for the single (tag, metric) group
    assert len(out) == 2
    by_ceiling = {r["ceiling_ms"]: r for r in out}
    assert by_ceiling[1000.0]["knee_rps"] == 48.0
    assert by_ceiling[1000.0]["knee_users"] == 5
    assert by_ceiling[5000.0]["knee_rps"] == 92.0


def test_main_no_inputs_resolved_returns_2():
    buf = io.StringIO()
    old = sys.stderr
    sys.stderr = buf
    try:
        rc = slo_knee.main([_fx("does_not_exist_*.jsonl")])
    finally:
        sys.stderr = old
    assert rc == 2


# ---------------------------------------------------------------------------
# Minimal stdlib runner (so the suite works without pytest installed).
# ---------------------------------------------------------------------------


def _run_all() -> int:
    fns = [
        (name, obj)
        for name, obj in sorted(globals().items())
        if name.startswith("test_") and callable(obj)
    ]
    failed = 0
    for name, fn in fns:
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {name}: {type(e).__name__}: {e}")
        else:
            print(f"ok   {name}")
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(_run_all())
