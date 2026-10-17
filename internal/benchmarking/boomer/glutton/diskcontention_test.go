// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package glutton

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.ate.dev/substrate/internal/benchmarking/boomer/dynconfig"
	"go.ate.dev/substrate/internal/benchmarking/boomer/userclass"
	"go.ate.dev/substrate/internal/benchmarking/glutton/fake"
	gluttonpb "go.ate.dev/substrate/internal/proto/glutton"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newDCHolder is a DiskContentionUser holder with payload in force.
func newDCHolder(t *testing.T, payload string) *dynconfig.Holder {
	t.Helper()
	h := dynconfig.NewHolder(dcCodec{})
	if _, _, err := h.Apply([]byte(payload)); err != nil {
		t.Fatalf("Apply(%s): %v", payload, err)
	}
	return h
}

// newTestDCRuntime is a dcRuntime over cfg, whose holder carries dcConfig,
// with the probe view initDiskContention would build.
func newTestDCRuntime(cfg *userclass.Config) *dcRuntime {
	probeCfg := *cfg
	probeCfg.Dyn = dynconfig.NewHolder(gluttonCodec)
	rt := &dcRuntime{cfg: cfg, views: []*dynconfig.Holder{probeCfg.Dyn}}
	rt.probe = &taskRuntime{cfg: &probeCfg, metricPrefix: dcProbePrefix, waitFn: rt.probeWait}
	return rt
}

func TestDCCodec(t *testing.T) {
	t.Run("decodes dc knobs and keeps the payload", func(t *testing.T) {
		payload := `{"dc_probe_users": 3, "dc_noise_mode": "disk", "dc_noise_template": "glutton-durdir-data",
			"dc_noise_write_size_bytes": 67108864, "dc_noise_files": 4, "dc_noise_rate_mbps": 36.25,
			"dc_probe_min_wait_time": 20.0, "dc_probe_max_wait_time": 30.0, "dc_noise_no_fsync": 1,
			"min_wait_time": 0.5, "max_wait_time": 1.5, "mem_target": "512Mi"}`
		got := dynconfig.Get[dcConfig](newDCHolder(t, payload))
		want := dcKnobs{
			ProbeUsers:     3,
			ProbeMinWait:   dynconfig.Seconds(20 * time.Second),
			ProbeMaxWait:   dynconfig.Seconds(30 * time.Second),
			NoiseMode:      noiseModeDisk,
			NoiseTemplate:  "glutton-durdir-data",
			NoiseWriteSize: 64 << 20,
			NoiseFiles:     4,
			NoiseRateMBps:  36.25,
			NoiseNoFsync:   1,
		}
		if got.dcKnobs != want {
			t.Errorf("dcKnobs = %+v, want %+v", got.dcKnobs, want)
		}
		if string(got.payload) != payload {
			t.Errorf("payload = %s, want the payload applied", got.payload)
		}
	})

	t.Run("null keys keep the defaults", func(t *testing.T) {
		got := dynconfig.Get[dcConfig](newDCHolder(t, `{"dc_probe_users": null, "dc_noise_mode": null}`))
		if got.dcKnobs != (dcKnobs{}) {
			t.Errorf("dcKnobs = %+v, want the zero defaults", got.dcKnobs)
		}
	})

	t.Run("probe wait may lie outside the GluttonUser window", func(t *testing.T) {
		got := dynconfig.Get[dcConfig](newDCHolder(t, `{"max_wait_time": 30, "dc_probe_min_wait_time": 40, "dc_probe_max_wait_time": 50}`))
		if got.ProbeMinWait != dynconfig.Seconds(40*time.Second) || got.ProbeMaxWait != dynconfig.Seconds(50*time.Second) {
			t.Errorf("probe wait = [%v, %v], want [40s, 50s]", got.ProbeMinWait, got.ProbeMaxWait)
		}
	})

	for _, tc := range []struct {
		name, payload, wantErr string
	}{
		{"negative probe users", `{"dc_probe_users": -1}`, "dc_probe_users"},
		{"unknown noise mode", `{"dc_noise_mode": "bogus"}`, "dc_noise_mode"},
		{"negative write size", `{"dc_noise_write_size_bytes": -1}`, "dc_noise_write_size_bytes"},
		{"write size over 2 GiB", `{"dc_noise_write_size_bytes": 2147483648}`, "dc_noise_write_size_bytes"},
		{"negative files", `{"dc_noise_files": -1}`, "dc_noise_files"},
		{"negative probe wait", `{"dc_probe_min_wait_time": -1}`, "dc_probe_{min,max}_wait_time"},
		{"inverted probe wait", `{"dc_probe_min_wait_time": 3, "dc_probe_max_wait_time": 2}`, "probe wait window is inverted"},
		// An unset probe bound falls back to its GluttonUser counterpart, so
		// the window those two make must not be inverted either.
		{"probe min above max_wait_time", `{"max_wait_time": 30, "dc_probe_min_wait_time": 40}`, "probe wait window is inverted"},
		{"probe max below min_wait_time", `{"min_wait_time": 10, "max_wait_time": 20, "dc_probe_max_wait_time": 5}`, "probe wait window is inverted"},
		{"negative noise rate", `{"dc_noise_rate_mbps": -1}`, "dc_noise_rate_mbps"},
		// The probes run GluttonUser, so a payload it refuses is refused too.
		{"payload the probes refuse", `{"min_wait_time": 2, "max_wait_time": 1}`, "max_wait_time"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := dynconfig.NewHolder(dcCodec{})
			_, _, err := h.Apply([]byte(tc.payload))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Apply(%s) = %v, want an error naming %s", tc.payload, err, tc.wantErr)
			}
		})
	}
}

// fakeSweperfKnobs stands in for SweperfUser's knobs: this test binary does
// not link the sweperf class, so dcCodec would never consult its codec.
type fakeSweperfKnobs struct {
	Refuse bool `json:"fake_sweperf_refuse"`
}

var registerFakeSweperf = sync.OnceFunc(func() {
	userclass.Add(userclass.Entry{
		Name:      "sweperf",
		UserClass: "SweperfUser",
		Config: dynconfig.Typed[fakeSweperfKnobs]{Validate: func(k fakeSweperfKnobs) error {
			if k.Refuse {
				return errors.New("fake_sweperf_refuse is set")
			}
			return nil
		}},
	})
})

// The sweperf noise VUs run SweperfUser, so a payload it refuses is refused.
func TestDCCodecRefusesWhatSweperfRefuses(t *testing.T) {
	registerFakeSweperf()
	h := dynconfig.NewHolder(dcCodec{})
	if _, _, err := h.Apply([]byte(`{"dc_noise_mode": "sweperf", "fake_sweperf_refuse": true}`)); err == nil || !strings.Contains(err.Error(), "fake_sweperf_refuse") {
		t.Errorf("Apply = %v, want the sweperf class's refusal", err)
	}
	if _, _, err := h.Apply([]byte(`{"dc_noise_mode": "sweperf", "fake_sweperf_refuse": false}`)); err != nil {
		t.Errorf("Apply = %v, want it accepted", err)
	}
}

// The probe and sweperf VUs read their knobs from views, which syncViews
// keeps on the payload in force, including a view added after the payload.
func TestSyncViewsAppliesThePayload(t *testing.T) {
	rt := newTestDCRuntime(&userclass.Config{Dyn: newDCHolder(t, `{"min_wait_time": 2, "max_wait_time": 2}`)})
	rt.syncViews()
	if got := dynconfig.Get[gluttonKnobs](rt.probe.cfg.Dyn).MinWait; got != dynconfig.Seconds(2*time.Second) {
		t.Errorf("probe view min_wait_time = %v, want 2s", got)
	}

	late := dynconfig.NewHolder(gluttonCodec)
	rt.addView(late)
	if got := dynconfig.Get[gluttonKnobs](late).MinWait; got != dynconfig.Seconds(2*time.Second) {
		t.Errorf("late view min_wait_time = %v, want 2s", got)
	}

	if _, _, err := rt.cfg.Dyn.Apply([]byte(`{"min_wait_time": 3, "max_wait_time": 3}`)); err != nil {
		t.Fatal(err)
	}
	rt.syncViews()
	for name, h := range map[string]*dynconfig.Holder{"probe": rt.probe.cfg.Dyn, "late": late} {
		if got := dynconfig.Get[gluttonKnobs](h).MinWait; got != dynconfig.Seconds(3*time.Second) {
			t.Errorf("%s view min_wait_time = %v after the new payload, want 3s", name, got)
		}
	}
}

func TestPickRole(t *testing.T) {
	tests := []struct {
		name string
		slot int64
		dyn  dcKnobs
		want dcRole
	}{
		{"first slot is a probe", 0, dcKnobs{ProbeUsers: 2, NoiseMode: noiseModeDisk}, roleProbe},
		{"last probe slot", 1, dcKnobs{ProbeUsers: 2, NoiseMode: noiseModeDisk}, roleProbe},
		{"disk noise after probes", 2, dcKnobs{ProbeUsers: 2, NoiseMode: noiseModeDisk}, roleNoiseDisk},
		{"sweperf noise after probes", 5, dcKnobs{ProbeUsers: 2, NoiseMode: noiseModeSweperf}, roleNoiseSweperf},
		{"no noise mode idles extra VUs", 3, dcKnobs{ProbeUsers: 2}, roleIdle},
		{"explicit none idles extra VUs", 3, dcKnobs{ProbeUsers: 2, NoiseMode: noiseModeNone}, roleIdle},
		{"zero probes makes every VU noise", 0, dcKnobs{NoiseMode: noiseModeDisk}, roleNoiseDisk},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := pickRole(tc.slot, tc.dyn); got != tc.want {
				t.Errorf("pickRole(%d) = %v, want %v", tc.slot, got, tc.want)
			}
		})
	}
}

func TestRoleForIsStablePerGoroutine(t *testing.T) {
	rt := &dcRuntime{}
	dyn := dcKnobs{ProbeUsers: 1, NoiseMode: noiseModeDisk}
	if got := rt.roleFor(100, dyn); got != roleProbe {
		t.Fatalf("first VU: got %v, want probe", got)
	}
	if got := rt.roleFor(200, dyn); got != roleNoiseDisk {
		t.Fatalf("second VU: got %v, want noise-disk", got)
	}
	// A VU keeps its role on every later iteration.
	if got := rt.roleFor(100, dyn); got != roleProbe {
		t.Errorf("first VU again: got %v, want probe", got)
	}
}

// After a boomer stop, the VUs of the next spawn get slots from 0 again, so
// a restarted run gets its probes back.
func TestRolesStartOverAfterAStop(t *testing.T) {
	rt := &dcRuntime{}
	dyn := dcKnobs{ProbeUsers: 1, NoiseMode: noiseModeDisk}
	rt.onSpawn(1, 1)
	if got := rt.roleFor(100, dyn); got != roleProbe {
		t.Fatalf("first VU: got %v, want probe", got)
	}
	// Locust sends a spawn message for every ramp step; those keep the
	// roles handed out so far.
	rt.onSpawn(2, 1)
	if got := rt.roleFor(200, dyn); got != roleNoiseDisk {
		t.Fatalf("second VU: got %v, want noise-disk", got)
	}

	rt.onStop()
	// boomer cancels the old VUs after the stop event, so one of them can
	// start one more iteration. It keeps its role and takes no slot.
	if got := rt.roleFor(200, dyn); got != roleNoiseDisk {
		t.Fatalf("old VU after the stop: got %v, want noise-disk", got)
	}
	rt.onSpawn(2, 1)
	if got := rt.roleFor(300, dyn); got != roleProbe {
		t.Errorf("first VU after the restart: got %v, want probe", got)
	}
	if got := rt.roleFor(400, dyn); got != roleNoiseDisk {
		t.Errorf("second VU after the restart: got %v, want noise-disk", got)
	}
}

func TestNoisePause(t *testing.T) {
	tests := []struct {
		name    string
		size    int64
		rate    float64
		elapsed time.Duration
		want    time.Duration
	}{
		{"uncapped", 64 << 20, 0, time.Millisecond, 0},
		{"10MB at 10MB/s took 200ms", 10_000_000, 10, 200 * time.Millisecond, 800 * time.Millisecond},
		{"slower than the cap", 10_000_000, 10, 2 * time.Second, -time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := noisePause(tc.size, tc.rate, tc.elapsed); got != tc.want {
				t.Errorf("noisePause = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDiskNoiseResumesOnceThenWrites(t *testing.T) {
	const size = 4096
	srv := &fake.Server{Data: make([]byte, size)}
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench",
		Dyn:      newDCHolder(t, `{"dc_noise_mode": "disk", "dc_noise_write_size_bytes": 4096}`),
	})
	rt := newTestDCRuntime(cfg)

	for range 2 {
		rt.iterateDiskNoise(1, dynconfig.Get[dcConfig](cfg.Dyn).dcKnobs)
	}
	if _, _, err := cfg.Dyn.Apply([]byte(`{"dc_noise_mode": "disk", "dc_noise_write_size_bytes": 4096, "dc_noise_no_fsync": 1}`)); err != nil {
		t.Fatal(err)
	}
	rt.iterateDiskNoise(1, dynconfig.Get[dcConfig](cfg.Dyn).dcKnobs)

	wantGRPC := []string{"CreateAtespace", "CreateActor", "ResumeActor"}
	if got := fakeCtrl.recordedCalls(); !reflect.DeepEqual(got, wantGRPC) {
		t.Errorf("gRPC calls: got %v, want %v", got, wantGRPC)
	}
	wantHTTP := []string{fake.WriteDiskRoute, fake.WriteDiskRoute, fake.WriteDiskRoute}
	if got := srv.RecordedPaths(); !reflect.DeepEqual(got, wantHTTP) {
		t.Errorf("HTTP calls: got %v, want %v", got, wantHTTP)
	}
	for i, s := range srv.RecordedWriteSizes() {
		if s != size {
			t.Errorf("write %d: size %d, want %d", i, s, size)
		}
	}
	wantModes := []gluttonpb.WriteMode{
		gluttonpb.WriteMode_WRITE_MODE_OVERWRITE,
		gluttonpb.WriteMode_WRITE_MODE_OVERWRITE,
		gluttonpb.WriteMode_WRITE_MODE_OVERWRITE,
	}
	if got := srv.RecordedWriteModes(); !reflect.DeepEqual(got, wantModes) {
		t.Errorf("write modes: got %v, want %v", got, wantModes)
	}
	wantFsyncs := []bool{true, true, false}
	if got := srv.RecordedWriteFsyncs(); !reflect.DeepEqual(got, wantFsyncs) {
		t.Errorf("write fsyncs: got %v, want %v", got, wantFsyncs)
	}

	// Shutdown deletes the noise actor in any state, without a suspend.
	rt.shutdown(context.Background())
	dels := fakeCtrl.recordedDeleteRequests()
	if len(dels) != 1 || !dels[0].GetAnyState() {
		t.Fatalf("delete requests: got %v, want one AnyState delete", dels)
	}
	for _, c := range fakeCtrl.recordedCalls() {
		if c == "SuspendActor" || c == "PauseActor" {
			t.Errorf("noise actor was hibernated (%s); want delete only", c)
		}
	}
}

// Shutdown deletes the noise actors before it hibernates and deletes the
// probes, so noise I/O does not slow the probes' teardown.
func TestShutdownDeletesNoiseBeforeProbes(t *testing.T) {
	srv := &fake.Server{Data: make([]byte, 4096)}
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench",
		Dyn: newDCHolder(t, `{"dc_probe_users": 1, "dc_noise_mode": "disk", "dc_noise_write_size_bytes": 4096,
			"min_wait_time": 0, "max_wait_time": 0}`),
	})
	rt := newTestDCRuntime(cfg)
	rt.syncViews()
	rt.probe.iterate()
	rt.iterateDiskNoise(1, dynconfig.Get[dcConfig](cfg.Dyn).dcKnobs)

	rt.shutdown(context.Background())
	var deleted []string
	for _, d := range fakeCtrl.recordedDeleteRequests() {
		deleted = append(deleted, d.GetActor().GetName())
	}
	if len(deleted) != 2 || !strings.HasPrefix(deleted[0], "sb-noise-") || strings.HasPrefix(deleted[1], "sb-noise-") {
		t.Errorf("deleted %v, want the noise actor, then the probe", deleted)
	}
}

// A noise actor whose resume fails is handled by the probes' rule: replaced
// at once when it is gone, kept through cluster-wide errors, and replaced
// when any other error persists.
func TestDiskNoiseStartFailures(t *testing.T) {
	for _, tc := range []struct {
		name        string
		resumeErrs  []error
		wantCreates int
		wantDeletes int
	}{
		{"gone", []error{status.Error(codes.NotFound, "gone")}, 2, 1},
		{"cluster-wide", slices.Repeat([]error{status.Error(codes.Unavailable, "busy")}, maxConsecutiveFailures), 1, 0},
		{"persistent", slices.Repeat([]error{status.Error(codes.Internal, "boom")}, maxConsecutiveFailures), 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := &fake.Server{Data: make([]byte, 4096)}
			fakeCtrl := &fakeControlClient{resumeErrs: tc.resumeErrs}
			cfg := newTestConfig(t, srv, &userclass.Config{
				APIStub:  fakeCtrl,
				Atespace: "bench",
				Dyn:      newDCHolder(t, `{"dc_noise_mode": "disk", "dc_noise_write_size_bytes": 4096}`),
			})
			rt := newTestDCRuntime(cfg)
			var slept []time.Duration
			rt.sleepFn = func(d time.Duration) { slept = append(slept, d) }

			// One iteration per error, then one that resumes and writes.
			for range len(tc.resumeErrs) + 1 {
				rt.iterateDiskNoise(1, dynconfig.Get[dcConfig](cfg.Dyn).dcKnobs)
			}

			calls := fakeCtrl.recordedCalls()
			if got := countCalls(calls, "CreateActor"); got != tc.wantCreates {
				t.Errorf("CreateActor count = %d, want %d; calls = %v", got, tc.wantCreates, calls)
			}
			if got := countCalls(calls, "DeleteActor"); got != tc.wantDeletes {
				t.Errorf("DeleteActor count = %d, want %d; calls = %v", got, tc.wantDeletes, calls)
			}
			if got := srv.RecordedPaths(); !reflect.DeepEqual(got, []string{fake.WriteDiskRoute}) {
				t.Errorf("HTTP calls: got %v, want one WriteDisk after the last resume", got)
			}
			if want := slices.Repeat([]time.Duration{noiseErrorBackoff}, len(tc.resumeErrs)); !reflect.DeepEqual(slept, want) {
				t.Errorf("pauses = %v, want %v (one backoff per failure)", slept, want)
			}
		})
	}
}

// WRITE_MODE_OVERWRITE reports the size of the whole file, which is larger
// than the write once the write size is lowered mid-run.
func TestNoiseWriteRefusesOnlyShortWrites(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fileSize int
		wantErr  bool
	}{
		{"file as large as the write", 4096, false},
		{"file larger than the write", 8192, false},
		{"short write", 1024, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := &fake.Server{Data: make([]byte, tc.fileSize)}
			n := newNoiseActor(newTestConfig(t, srv, &userclass.Config{Atespace: "bench"}))
			got, err := n.write(context.Background(), dcKnobs{NoiseWriteSize: 4096})
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "short write") {
					t.Errorf("write = %d, %v; want a short write error", got, err)
				}
				return
			}
			if err != nil || got != 4096 {
				t.Errorf("write = %d, %v; want 4096, nil", got, err)
			}
		})
	}
}

// requestCount reads locust_requests_total for one row and outcome
// ("success" or "failure") from the default Prometheus registry, where the
// boomer metrics package registers it.
func requestCount(t *testing.T, method, name, outcome string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var total float64
	for _, mf := range mfs {
		if mf.GetName() != "locust_requests_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["method"] == method && labels["name"] == name && labels["status"] == outcome {
				total += m.GetCounter().GetValue()
			}
		}
	}
	return total
}

func TestProbeRowsArePrefixed(t *testing.T) {
	srv := &fake.Server{}
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench",
	})
	beforeResume := requestCount(t, "grpc", "ProbeResumeActor", "success")
	beforeSuspend := requestCount(t, "grpc", "ProbeSuspendActor", "success")
	beforePlain := requestCount(t, "grpc", "ResumeActor", "success")

	a := &gluttonActor{cfg: cfg, actorName: "sb-x", metricPrefix: dcProbePrefix}
	if err := a.resume(context.Background()); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if err := a.suspend(context.Background()); err != nil {
		t.Fatalf("suspend: %v", err)
	}

	if got := requestCount(t, "grpc", "ProbeResumeActor", "success") - beforeResume; got != 1 {
		t.Errorf("ProbeResumeActor rows: got %v, want 1", got)
	}
	if got := requestCount(t, "grpc", "ProbeSuspendActor", "success") - beforeSuspend; got != 1 {
		t.Errorf("ProbeSuspendActor rows: got %v, want 1", got)
	}
	if got := requestCount(t, "grpc", "ResumeActor", "success") - beforePlain; got != 0 {
		t.Errorf("unprefixed ResumeActor rows: got %v, want 0", got)
	}
}

func TestProbeWaitUsesProbeBounds(t *testing.T) {
	rt := newTestDCRuntime(&userclass.Config{Dyn: newDCHolder(t, `{"min_wait_time": 0, "max_wait_time": 0,
		"dc_probe_min_wait_time": 2, "dc_probe_max_wait_time": 2}`)})
	rt.syncViews()
	if got := rt.probeWait(); got != 2*time.Second {
		t.Errorf("probeWait = %v, want 2s (probe bounds)", got)
	}
	if _, _, err := rt.cfg.Dyn.Apply([]byte(`{"min_wait_time": 3, "max_wait_time": 3}`)); err != nil {
		t.Fatal(err)
	}
	rt.syncViews()
	if got := rt.probeWait(); got != 3*time.Second {
		t.Errorf("probeWait = %v, want 3s (GluttonUser bounds)", got)
	}
}

// Each probe bound falls back to its GluttonUser counterpart on its own.
func TestProbeWaitBounds(t *testing.T) {
	w := dynconfig.WaitTime{MinWait: dynconfig.Seconds(time.Second), MaxWait: dynconfig.Seconds(5 * time.Second)}
	for _, tc := range []struct {
		name   string
		k      dcKnobs
		lo, hi time.Duration
	}{
		{"neither set", dcKnobs{}, time.Second, 5 * time.Second},
		{"both set", dcKnobs{ProbeMinWait: dynconfig.Seconds(2 * time.Second), ProbeMaxWait: dynconfig.Seconds(3 * time.Second)}, 2 * time.Second, 3 * time.Second},
		{"only min set", dcKnobs{ProbeMinWait: dynconfig.Seconds(2 * time.Second)}, 2 * time.Second, 5 * time.Second},
		{"only max set", dcKnobs{ProbeMaxWait: dynconfig.Seconds(3 * time.Second)}, time.Second, 3 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if lo, hi := probeWaitBounds(tc.k, w); lo != tc.lo || hi != tc.hi {
				t.Errorf("probeWaitBounds = [%v, %v], want [%v, %v]", lo, hi, tc.lo, tc.hi)
			}
		})
	}
}
