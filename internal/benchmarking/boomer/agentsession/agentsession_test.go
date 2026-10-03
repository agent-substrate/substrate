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

package agentsession

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton/fake"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestEmbeddedScriptsAreValid keeps every built-in variant loadable: a file
// under scripts/ that fails Validate cannot merge.
func TestEmbeddedScriptsAreValid(t *testing.T) {
	names := Names()
	if !slices.Contains(names, DefaultScript) {
		t.Fatalf("Names() = %v, missing DefaultScript %q", names, DefaultScript)
	}
	for _, n := range names {
		if _, err := Load(n); err != nil {
			t.Errorf("Load(%q): %v", n, err)
		}
	}
	if _, err := Load("no-such-script"); err == nil {
		t.Error("Load of an unknown name must fail")
	}
}

// TestDefaultScriptShape pins what the README promises about the default
// variant: 20 steps, a 1Gi floor, and declared bytes that stay well under
// it. The declared budget is far below the floor on purpose: the guest's
// real peak (kernel, kata-agent, allocator transients) sits on top of it.
func TestDefaultScriptShape(t *testing.T) {
	s, err := Load(DefaultScript)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Steps) != 20 {
		t.Errorf("default script has %d steps, want 20", len(s.Steps))
	}
	if s.MinActorMemory != 1<<30 {
		t.Errorf("min_actor_memory = %d, want 1Gi", s.MinActorMemory)
	}
	b := Budgets(s.Steps)
	if b.RAM > 128<<20 || b.Disk > 256<<20 {
		t.Errorf("declared RAM %d / disk %d outgrew the 128Mi / 256Mi budgets; revisit the memory guidance", b.RAM, b.Disk)
	}
}

// TestEncodeDecodeRoundTrip proves Encode writes exactly what Decode reads,
// so a script dumped from Go and one authored by hand are the same thing.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	s, err := Load(DefaultScript)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Encode(s)
	if err != nil {
		t.Fatal(err)
	}
	back, err := Decode(out)
	if err != nil {
		t.Fatalf("Decode(Encode(s)): %v\n%s", err, out)
	}
	if !reflect.DeepEqual(s, back) {
		t.Errorf("round trip changed the script:\n%s", out)
	}
}

const validScript = `
name: tiny
min_actor_memory: 64Mi
steps:
- name: 01_fill
  agent: fills
  think: 1s
  ops:
  - fill_ram: {key: ctx, size: 4Mi}
  - ingest: {key: repo, size: 1Mi}
- name: 02_use
  agent: uses
  think: 500ms
  ops:
  - walk_ram: {key: ctx}
  - read_disk_data: {key: repo}
  - burn_cpu: {millis: 10}
  - dwell: {millis: 5}
  - ping: {}
`

// TestDecodeAcceptsValidScript covers the argument defaults: parallel is
// optional and reads as 1.
func TestDecodeAcceptsValidScript(t *testing.T) {
	s, err := Decode([]byte(validScript))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := s.Steps[1].Ops[2], (op{kind: opBurnCPU, millis: 10, parallel: 1}); got != want {
		t.Errorf("burn_cpu decoded as %+v, want %+v", got, want)
	}
	if got, want := s.Steps[0].Ops[0], (op{kind: opFillRAM, key: "ctx", bytes: 4 << 20}); got != want {
		t.Errorf("fill_ram decoded as %+v, want %+v", got, want)
	}
	if got, want := s.Steps[1].Ops[3], (op{kind: opDwell, millis: 5}); got != want {
		t.Errorf("dwell decoded as %+v, want %+v", got, want)
	}
}

// TestDecodeRejects lists the mistakes a hand-written script can make; each
// must fail at load with a message that names the problem.
func TestDecodeRejects(t *testing.T) {
	for _, tc := range []struct {
		name, edit, want string
	}{
		{"unknown op kind", "- ping: {}", "unknown op kind"},
		{"unknown field", "think: 1s", "field typo not found"},
		{"two-key op", "- ping: {}", "single-key map"},
		{"ping with key", "- ping: {}", "takes no key"},
		{"missing size", "- ingest: {key: repo, size: 1Mi}", "size is required"},
		{"bad key", "- ingest: {key: repo, size: 1Mi}", "must match"},
		{"zero millis", "- burn_cpu: {millis: 10}", "millis must be positive"},
		{"dwell with parallel", "- dwell: {millis: 5}", "takes no parallel"},
		{"ping with millis", "- ping: {}", "takes no millis"},
		{"walk before fill", "- fill_ram: {key: ctx, size: 4Mi}", "before any fill_ram"},
		{"read before write", "- ingest: {key: repo, size: 1Mi}", "before any ingest"},
		{"duplicate step", "name: 02_use", "duplicate step name"},
		{"zero think", "think: 500ms", "think time must be positive"},
		{"no min memory", "min_actor_memory: 64Mi", "min_actor_memory is required"},
		{"min below declared", "min_actor_memory: 64Mi", "below the declared"},
		{"bad script name", "name: tiny", "script name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := validScript
			switch tc.name {
			case "unknown op kind":
				doc = strings.Replace(doc, tc.edit, "- sleep: {}", 1)
			case "unknown field":
				doc = strings.Replace(doc, tc.edit, "think: 1s\n  typo: x", 1)
			case "two-key op":
				doc = strings.Replace(doc, tc.edit, "- {ping: {}, walk_ram: {key: ctx}}", 1)
			case "ping with key":
				doc = strings.Replace(doc, tc.edit, "- ping: {key: ctx}", 1)
			case "missing size":
				doc = strings.Replace(doc, tc.edit, "- ingest: {key: repo}", 1)
			case "bad key":
				doc = strings.Replace(doc, tc.edit, "- ingest: {key: ../repo, size: 1Mi}", 1)
			case "zero millis":
				doc = strings.Replace(doc, tc.edit, "- burn_cpu: {millis: 0}", 1)
			case "dwell with parallel":
				doc = strings.Replace(doc, tc.edit, "- dwell: {millis: 5, parallel: 2}", 1)
			case "ping with millis":
				doc = strings.Replace(doc, tc.edit, "- ping: {millis: 5}", 1)
			case "walk before fill":
				doc = strings.Replace(doc, tc.edit, "- ping: {}", 1)
			case "read before write":
				doc = strings.Replace(doc, tc.edit, "- ping: {}", 1)
			case "duplicate step":
				doc = strings.Replace(doc, tc.edit, "name: 01_fill", 1)
			case "zero think":
				doc = strings.Replace(doc, tc.edit, "think: 0s", 1)
			case "no min memory":
				doc = strings.Replace(doc, tc.edit, "", 1)
			case "min below declared":
				doc = strings.Replace(doc, tc.edit, "min_actor_memory: 4Mi", 1)
			case "bad script name":
				doc = strings.Replace(doc, tc.edit, "name: Tiny Script", 1)
			}
			if doc == validScript {
				t.Fatal("test edit did not change the script")
			}
			_, err := Decode([]byte(doc))
			if err == nil {
				t.Fatalf("Decode accepted the script:\n%s", doc)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

// TestExecOpAgainstFake replays every op of the default script against the
// fake glutton server, proving each op marshals a request the actor-side
// routes accept.
func TestExecOpAgainstFake(t *testing.T) {
	fakeSrv := &fake.Server{Data: []byte("filecontents")}
	ts := fakeSrv.Start(t)

	u := &sessionUser{
		cfg: &userclass.Config{
			HTTPClient: http.DefaultClient,
			RouterURL:  ts.URL,
			Atespace:   "benchmark",
			Dyn:        dynconfig.Static(knobs{}),
		},
		actorName: "agent-test",
	}
	script, err := Load(DefaultScript)
	if err != nil {
		t.Fatal(err)
	}

	var opCount int
	for _, s := range script.Steps {
		for i, o := range s.Ops {
			// Cap ingest payloads in the unit test: transport shape is what
			// matters here, not moving tens of MiB through httptest.
			if o.kind == opIngest && o.bytes > 1<<10 {
				o.bytes = 1 << 10
			}
			if o.kind == opBurnCPU || o.kind == opDwell {
				o.millis = 1
			}
			if err := u.execOp(context.Background(), o); err != nil {
				t.Fatalf("step %q op %d: %v", s.Name, i, err)
			}
			// A dwell is the one op that sends nothing.
			if o.kind != opDwell {
				opCount++
			}
		}
	}
	if got := len(fakeSrv.RecordedPaths()); got != opCount {
		t.Errorf("fake served %d requests, want %d", got, opCount)
	}
	for _, n := range fakeSrv.RecordedIngestSizes() {
		if n > 1<<10 {
			t.Errorf("ingest payload of %d bytes reached the server, cap is %d", n, 1<<10)
		}
	}
	for _, ms := range fakeSrv.RecordedBurnMillis() {
		if ms != 1 {
			t.Errorf("burn of %dms reached the server, override is 1ms", ms)
		}
	}
}

// TestLoadScriptFollowsTheKnob: the script is resolved from dynconfig when
// a session starts (an empty knob means the default), an unknown name is an
// error that leaves the previous script in place, and a changed knob loads
// the new script for sessions that start afterwards.
func TestLoadScriptFollowsTheKnob(t *testing.T) {
	rt := &runtime{cfg: &userclass.Config{Dyn: dynconfig.Static(knobs{Script: "no-such-script"})}}
	if _, err := rt.loadScript(); err == nil {
		t.Fatal("loadScript accepted an unknown script name")
	}
	if rt.loaded != nil {
		t.Fatal("a failed load must not cache a script")
	}

	rt.cfg.Dyn = dynconfig.Static(knobs{})
	s, err := rt.loadScript()
	if err != nil {
		t.Fatal(err)
	}
	if s.Name != DefaultScript {
		t.Errorf("empty knob loaded %q, want %q", s.Name, DefaultScript)
	}
	if len(s.ingestBuf) == 0 {
		t.Error("ingestBuf not sized after load")
	}
	if again, err := rt.loadScript(); err != nil || again != s {
		t.Errorf("unchanged knob: loadScript = (%v, %v), want the cached script", again, err)
	}

	// A knob that names something that does not load is an error, and the
	// previous script stays available to sessions that already run on it.
	rt.cfg.Dyn = dynconfig.Static(knobs{Script: "no-such-script"})
	if _, err := rt.loadScript(); err == nil {
		t.Error("loadScript accepted an unknown name after a successful load")
	}
	if rt.loaded != s {
		t.Error("a failed reload must not replace the loaded script")
	}

	// A knob that names a different, valid script is picked up.
	path := filepath.Join(t.TempDir(), "tiny.yaml")
	if err := os.WriteFile(path, []byte(validScript), 0o600); err != nil {
		t.Fatal(err)
	}
	rt.cfg.Dyn = dynconfig.Static(knobs{ScriptFile: path})
	next, err := rt.loadScript()
	if err != nil {
		t.Fatal(err)
	}
	if next.Name != "tiny" {
		t.Errorf("changed knob loaded %q, want tiny", next.Name)
	}
	if int64(len(next.ingestBuf)) != 1<<20 {
		t.Errorf("ingestBuf = %d bytes, want sized to tiny's largest ingest (1Mi)", len(next.ingestBuf))
	}

	// The same path with new content is a new script: a redeployed
	// ConfigMap arrives as a rewritten file, not a new knob value.
	if err := os.WriteFile(path, []byte(strings.Replace(validScript, "name: tiny", "name: tiny-v2", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	rewritten, err := rt.loadScript()
	if err != nil {
		t.Fatal(err)
	}
	if rewritten.Name != "tiny-v2" {
		t.Errorf("rewritten file loaded %q, want tiny-v2", rewritten.Name)
	}
	if again, err := rt.loadScript(); err != nil || again != rewritten {
		t.Errorf("unchanged file: loadScript = (%v, %v), want the cached script", again, err)
	}
}

// TestShutdownFansOut: at thousands of sessions a serial suspend+delete
// sweep leaks most actors before boomer's shutdown budget runs out, so the
// hook must run sessions concurrently and still clean up every one.
func TestShutdownFansOut(t *testing.T) {
	const sessions = 40
	ctl := &fakeControlClient{deleteDelay: 20 * time.Millisecond}
	u := newTestUser(t, &fake.Server{}, ctl, knobs{})
	rt := &runtime{cfg: u.cfg}
	for i := range sessions {
		rt.users.Store(int64(i), &sessionUser{cfg: u.cfg, actorName: "agent-" + strconv.Itoa(i)})
	}

	start := time.Now()
	rt.shutdown(context.Background())
	elapsed := time.Since(start)

	if got := countCalls(ctl.recordedCalls(), "DeleteActor"); got != sessions {
		t.Fatalf("DeleteActor calls = %d, want %d", got, sessions)
	}
	if ctl.maxInFlight.Load() < 2 {
		t.Errorf("max concurrent DeleteActor = %d, want > 1", ctl.maxInFlight.Load())
	}
	if serial := sessions * ctl.deleteDelay; elapsed >= serial {
		t.Errorf("shutdown took %v, no faster than a serial sweep (%v)", elapsed, serial)
	}
	rt.users.Range(func(_, val any) bool {
		if !val.(*sessionUser).cleanedUp {
			t.Errorf("session %s not cleaned up", val.(*sessionUser).actorName)
		}
		return true
	})
}

// TestLoadFile reads a script from disk and reports the path on errors.
func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "tiny.yaml")
	if err := os.WriteFile(good, []byte(validScript), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := LoadFile(good)
	if err != nil {
		t.Fatal(err)
	}
	if s.Name != "tiny" || len(s.Steps) != 2 {
		t.Errorf("LoadFile = %q with %d steps, want tiny with 2", s.Name, len(s.Steps))
	}

	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(bad, []byte(strings.Replace(validScript, "- ping: {}", "- nap: {}", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFile(bad); err == nil || !strings.Contains(err.Error(), bad) || !strings.Contains(err.Error(), "unknown op kind") {
		t.Errorf("LoadFile(bad) = %v, want an error naming the file and the op", err)
	}
	if _, err := LoadFile(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Error("LoadFile of a missing path must fail")
	}
}

// TestLoadScriptPrefersFile: a file path beats a built-in name.
func TestLoadScriptPrefersFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tiny.yaml")
	if err := os.WriteFile(path, []byte(validScript), 0o600); err != nil {
		t.Fatal(err)
	}
	rt := &runtime{cfg: &userclass.Config{Dyn: dynconfig.Static(knobs{
		Script:     DefaultScript,
		ScriptFile: path,
	})}}
	s, err := rt.loadScript()
	if err != nil {
		t.Fatal(err)
	}
	if s.Name != "tiny" {
		t.Errorf("loaded %q, want the file's script", s.Name)
	}
}

// TestStartUserChecksTemplateMemory: a template smaller than the script's
// floor, or with a limit that does not parse, is refused before any actor
// is created; a big enough one, or one with no limit, proceeds.
func TestStartUserChecksTemplateMemory(t *testing.T) {
	script, err := Load(DefaultScript)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, memory string
		wantStart    bool
		wantErr      string
	}{
		{"too small", "512Mi", false, "min_actor_memory"},
		{"unparseable", "1Gi-ish", false, "memory limit"},
		{"exact", "1Gi", true, ""},
		{"no limit", "", true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctl := &fakeControlClient{templateMemory: tc.memory}
			u := newTestUser(t, &fake.Server{}, ctl, knobs{})
			rt := &runtime{cfg: u.cfg}

			started, err := rt.startUser(context.Background(), &loadedScript{Script: script, ingestBuf: makeIngestBuf(script.Steps)})
			created := countCalls(ctl.recordedCalls(), "CreateActor") > 0
			if tc.wantStart {
				if err != nil || !created {
					t.Fatalf("startUser = %v, CreateActor called = %v; want a started session", err, created)
				}
				if len(started.steps) != len(script.Steps) || len(started.ingestBuf) != len(makeIngestBuf(script.Steps)) {
					t.Errorf("session captured %d steps and a %d-byte buffer, want the loaded script's own", len(started.steps), len(started.ingestBuf))
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("startUser = %v, want a refusal mentioning %q", err, tc.wantErr)
			}
			if created {
				t.Error("CreateActor was called despite the refusal")
			}
		})
	}
}

// TestTemplateMemoryRefusalExpires: a refusal is re-checked after
// templateRecheckInterval, so redeploying the workloads with a bigger limit
// is picked up without restarting the workers.
func TestTemplateMemoryRefusalExpires(t *testing.T) {
	script, err := Load(DefaultScript)
	if err != nil {
		t.Fatal(err)
	}
	ctl := &fakeControlClient{templateMemory: "512Mi"}
	u := newTestUser(t, &fake.Server{}, ctl, knobs{})
	rt := &runtime{cfg: u.cfg}

	if err := rt.checkTemplateMemory(context.Background(), script); err == nil {
		t.Fatal("512Mi template passed the check")
	}
	ctl.templateMemory = "1Gi" // the operator redeployed
	if err := rt.checkTemplateMemory(context.Background(), script); err == nil {
		t.Fatal("refusal must stand for templateRecheckInterval")
	}
	if got := countCalls(ctl.recordedCalls(), "GetActorTemplate"); got != 1 {
		t.Errorf("GetActorTemplate calls = %d, want 1 while the refusal is cached", got)
	}
	rt.templateErrAt = time.Now().Add(-2 * templateRecheckInterval)
	if err := rt.checkTemplateMemory(context.Background(), script); err != nil {
		t.Fatalf("check after the interval = %v, want the redeployed template to pass", err)
	}
	if err := rt.checkTemplateMemory(context.Background(), script); err != nil || countCalls(ctl.recordedCalls(), "GetActorTemplate") != 2 {
		t.Errorf("success must be cached: err=%v calls=%d", err, countCalls(ctl.recordedCalls(), "GetActorTemplate"))
	}
}

// TestRunningSessionKeepsItsScript: changing the knob affects sessions that
// start afterwards, not one already walking its steps.
func TestRunningSessionKeepsItsScript(t *testing.T) {
	ctl := &fakeControlClient{templateMemory: "1Gi"}
	u := newTestUser(t, &fake.Server{}, ctl, knobs{})
	rt := &runtime{cfg: u.cfg}
	first, err := rt.loadScript()
	if err != nil {
		t.Fatal(err)
	}
	session, err := rt.startUser(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "tiny.yaml")
	if err := os.WriteFile(path, []byte(validScript), 0o600); err != nil {
		t.Fatal(err)
	}
	rt.cfg.Dyn = dynconfig.Static(knobs{ScriptFile: path})
	second, err := rt.loadScript()
	if err != nil {
		t.Fatal(err)
	}
	if second.Name != "tiny" || len(session.steps) != len(first.Steps) || len(session.ingestBuf) != len(first.ingestBuf) {
		t.Errorf("after the knob change: new script %q, running session has %d steps and a %d-byte buffer (want %d / %d)", second.Name, len(session.steps), len(session.ingestBuf), len(first.Steps), len(first.ingestBuf))
	}
}

// TestThinkScaling checks the think-time multiplier and its jitter bounds.
func TestThinkScaling(t *testing.T) {
	r := &runtime{
		cfg: &userclass.Config{
			Dyn: dynconfig.Static(knobs{ThinkScale: 0.5}),
		},
	}
	s := Step{Think: 10 * time.Second}
	for i := 0; i < 100; i++ {
		got := r.think(s)
		if got < 4*time.Second || got > 6*time.Second {
			t.Fatalf("think = %v, want within [4s, 6s] (scale 0.5, jitter ±20%%)", got)
		}
	}

	// Zero scale reads as 1.0.
	r.cfg.Dyn = dynconfig.Static(knobs{})
	for i := 0; i < 100; i++ {
		got := r.think(s)
		if got < 8*time.Second || got > 12*time.Second {
			t.Fatalf("think = %v with unset scale, want within [8s, 12s]", got)
		}
	}
}

// fakeControlClient records control-plane calls and fails ResumeActor /
// SuspendActor with queued errors, in order, until each queue drains.
type fakeControlClient struct {
	ateapipb.ControlClient
	mu          sync.Mutex
	calls       []string
	resumeErrs  []error
	suspendErrs []error
	// sawDeadline is set when a call arrived with a context deadline.
	sawDeadline bool
	// templateMemory is the memory limit GetActorTemplate reports; "" means
	// the template sets none.
	templateMemory string
	// deleteDelay stalls each DeleteActor; inFlight and maxInFlight count
	// concurrent DeleteActor calls, to prove shutdown fans out.
	deleteDelay time.Duration
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func nextErr(errs *[]error) error {
	if len(*errs) == 0 {
		return nil
	}
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}

func (f *fakeControlClient) record(ctx context.Context, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
	if _, ok := ctx.Deadline(); ok {
		f.sawDeadline = true
	}
}

func (f *fakeControlClient) GetActorTemplate(ctx context.Context, in *ateapipb.GetActorTemplateRequest, opts ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	f.record(ctx, "GetActorTemplate")
	tmpl := &ateapipb.ActorTemplate{}
	if f.templateMemory != "" {
		tmpl.Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "memory", Quantity: f.templateMemory}}}
	}
	return tmpl, nil
}

func (f *fakeControlClient) CreateAtespace(ctx context.Context, in *ateapipb.CreateAtespaceRequest, opts ...grpc.CallOption) (*ateapipb.Atespace, error) {
	f.record(ctx, "CreateAtespace")
	return &ateapipb.Atespace{}, nil
}

func (f *fakeControlClient) CreateActor(ctx context.Context, in *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.record(ctx, "CreateActor")
	return &ateapipb.Actor{}, nil
}

func (f *fakeControlClient) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	f.record(ctx, "ResumeActor")
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := nextErr(&f.resumeErrs); err != nil {
		return nil, err
	}
	return &ateapipb.ResumeActorResponse{}, nil
}

func (f *fakeControlClient) SuspendActor(ctx context.Context, in *ateapipb.SuspendActorRequest, opts ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	f.record(ctx, "SuspendActor")
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := nextErr(&f.suspendErrs); err != nil {
		return nil, err
	}
	return &ateapipb.SuspendActorResponse{}, nil
}

func (f *fakeControlClient) PauseActor(ctx context.Context, in *ateapipb.PauseActorRequest, opts ...grpc.CallOption) (*ateapipb.PauseActorResponse, error) {
	f.record(ctx, "PauseActor")
	return &ateapipb.PauseActorResponse{}, nil
}

func (f *fakeControlClient) DeleteActor(ctx context.Context, in *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.record(ctx, "DeleteActor")
	n := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for {
		cur := f.maxInFlight.Load()
		if n <= cur || f.maxInFlight.CompareAndSwap(cur, n) {
			break
		}
	}
	if f.deleteDelay > 0 {
		time.Sleep(f.deleteDelay)
	}
	return &ateapipb.Actor{}, nil
}

func (f *fakeControlClient) recordedCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func countCalls(calls []string, name string) int {
	n := 0
	for _, c := range calls {
		if c == name {
			n++
		}
	}
	return n
}

func newTestUser(t *testing.T, srv *fake.Server, ctl *fakeControlClient, dyn knobs) *sessionUser {
	t.Helper()
	ts := srv.Start(t)
	return &sessionUser{
		cfg: &userclass.Config{
			APIStub:    ctl,
			HTTPClient: ts.Client(),
			RouterURL:  ts.URL,
			Atespace:   "benchmark",
			Dyn:        dynconfig.Static(dyn),
			Tracer:     otel.Tracer("test"),
		},
		actorName: "agent-test",
	}
}

var pingStep = Step{Name: "01_test", Agent: "testing", Think: time.Second, Ops: []op{ping()}}

// A retryable suspend failure strands the actor RUNNING. The next step must
// finish the suspend rather than wake an already-awake actor, which would
// book a few-ms WakeFirstTouch success.
func TestRunStep_RetriesStrandedHibernate(t *testing.T) {
	srv := &fake.Server{}
	ctl := &fakeControlClient{suspendErrs: []error{status.Error(codes.Unavailable, "ate-api-server restarting")}}
	u := newTestUser(t, srv, ctl, knobs{})

	if !u.runStep(context.Background(), pingStep) {
		t.Fatal("runStep = false; the step's ops succeeded and must count")
	}
	if !u.hibernatePending || u.broken {
		t.Fatalf("after failed suspend: hibernatePending=%v broken=%v, want true/false", u.hibernatePending, u.broken)
	}
	served := len(srv.RecordedPaths())

	if u.runStep(context.Background(), pingStep) {
		t.Error("runStep = true while re-driving the hibernate; the step must not advance")
	}
	calls := ctl.recordedCalls()
	if got := calls[len(calls)-1]; got != "SuspendActor" {
		t.Errorf("last call = %q, want SuspendActor; calls = %v", got, calls)
	}
	if got := len(srv.RecordedPaths()); got != served {
		t.Errorf("router requests = %d, want %d (no wake ping against a stranded actor)", got, served)
	}
	if u.hibernatePending || u.broken {
		t.Errorf("after re-driven suspend: hibernatePending=%v broken=%v, want false/false", u.hibernatePending, u.broken)
	}

	if !u.runStep(context.Background(), pingStep) {
		t.Error("runStep = false once the suspend cleared")
	}
}

// ateapi reports a CRASHED actor on ResumeActor; it never recovers, so the
// session must replace it on the first failure, not the third.
func TestRunStep_ReplacesCrashedActorImmediately(t *testing.T) {
	ctl := &fakeControlClient{resumeErrs: []error{status.Error(codes.Aborted, "actor benchmark/agent-test crashed")}}
	u := newTestUser(t, &fake.Server{}, ctl, knobs{Lifecycle: dynconfig.Lifecycle{ResumeMode: dynconfig.ResumeModeExplicit}})

	if u.runStep(context.Background(), pingStep) {
		t.Fatal("runStep = true with a crashed actor")
	}
	if !u.broken {
		t.Error("broken = false after a crashed verdict; want immediate replacement")
	}
}

// In implicit mode the wake is an HTTP request, so the gRPC verdict arrives
// from the hibernate that follows the failed step: FailedPrecondition means
// a state nothing the driver can call moves the actor out of.
func TestRunStep_ReplacesStuckActorAfterFailedWake(t *testing.T) {
	ctl := &fakeControlClient{suspendErrs: []error{status.Error(codes.FailedPrecondition, "MarkSuspending prerequisite not met (got: ACTOR_STATE_CRASHED)")}}
	u := newTestUser(t, &fake.Server{Status: 503}, ctl, knobs{})

	if u.runStep(context.Background(), pingStep) {
		t.Fatal("runStep = true with a failing router")
	}
	if !u.broken {
		t.Error("broken = false after FailedPrecondition on suspend; want immediate replacement")
	}
}

// A saturated pool reports ResourceExhausted to every VU at once; a
// replacement would need the same capacity, so the actor must be kept.
func TestRunStep_KeepsActorThroughCapacityShortage(t *testing.T) {
	const rounds = maxConsecutiveStepFailures + 2
	errs := make([]error, rounds)
	for i := range errs {
		errs[i] = status.Error(codes.ResourceExhausted, "no worker has room for the actor")
	}
	ctl := &fakeControlClient{resumeErrs: errs}
	u := newTestUser(t, &fake.Server{}, ctl, knobs{Lifecycle: dynconfig.Lifecycle{ResumeMode: dynconfig.ResumeModeExplicit}})

	for range rounds {
		if u.runStep(context.Background(), pingStep) {
			t.Fatal("runStep = true while resume is failing")
		}
	}
	if u.broken || u.consecutiveFailures != 0 {
		t.Errorf("broken=%v consecutiveFailures=%d after capacity errors, want false/0", u.broken, u.consecutiveFailures)
	}
}

// A router 503 is the fleet being full or the control plane being busy,
// the HTTP face of ResourceExhausted and Unavailable. Replacing the actor
// would ask for the room that is missing, so it must not count.
func TestRunStep_KeepsActorThroughRouterCapacityErrors(t *testing.T) {
	for _, status := range []int{503, 504, 429} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			ctl := &fakeControlClient{}
			u := newTestUser(t, &fake.Server{Status: status}, ctl, knobs{})
			for range maxConsecutiveStepFailures + 2 {
				if u.runStep(context.Background(), pingStep) {
					t.Fatal("runStep = true with a failing router")
				}
			}
			if u.broken || u.consecutiveFailures != 0 {
				t.Errorf("HTTP %d: broken=%v consecutiveFailures=%d, want false/0", status, u.broken, u.consecutiveFailures)
			}
		})
	}
}

// A router 404 means the actor record is gone; nothing the driver can call
// brings it back, so it is replaced on the first failure.
func TestRunStep_ReplacesActorOnRouterNotFound(t *testing.T) {
	ctl := &fakeControlClient{}
	u := newTestUser(t, &fake.Server{Status: 404}, ctl, knobs{})
	u.runStep(context.Background(), pingStep)
	if !u.broken {
		t.Error("broken = false after a 404 wake; want immediate replacement")
	}
}

// Other HTTP failures carry no verdict, so they count toward the threshold:
// an actor whose sandbox is dead but whose record looks healthy is replaced
// after maxConsecutiveStepFailures steps.
func TestRunStep_ReplacesActorAfterRepeatedStepFailures(t *testing.T) {
	ctl := &fakeControlClient{}
	u := newTestUser(t, &fake.Server{Status: 502}, ctl, knobs{})

	for i := 1; i <= maxConsecutiveStepFailures; i++ {
		u.runStep(context.Background(), pingStep)
		if want := i == maxConsecutiveStepFailures; u.broken != want {
			t.Fatalf("after %d failed steps: broken = %v, want %v", i, u.broken, want)
		}
	}
}

func TestControlRPCsCarryADeadline(t *testing.T) {
	ctl := &fakeControlClient{}
	u := newTestUser(t, &fake.Server{}, ctl, knobs{Lifecycle: dynconfig.Lifecycle{ResumeMode: dynconfig.ResumeModeExplicit}})

	u.runStep(context.Background(), pingStep)
	u.suspendAndDelete(context.Background())

	if !ctl.sawDeadline {
		t.Error("control-plane calls arrived without a context deadline")
	}
	for _, name := range []string{"ResumeActor", "SuspendActor", "DeleteActor"} {
		if got := countCalls(ctl.recordedCalls(), name); got == 0 {
			t.Errorf("%s was never called; calls = %v", name, ctl.recordedCalls())
		}
	}
}

func TestBurnRatePerGoroutine(t *testing.T) {
	rate, ok := burnRatePerGoroutine(op{kind: opBurnCPU, millis: 500, parallel: 2}, 10_000)
	if !ok || rate != 10_000 {
		t.Errorf("burnRatePerGoroutine(500ms x2, 10k) = %v, %v; want 10000, true", rate, ok)
	}
	if _, ok := burnRatePerGoroutine(op{kind: opBurnCPU, parallel: 1}, 5); ok {
		t.Error("a zero-duration burn must not report a rate")
	}
}

// TestDwell: a dwell idles for its duration without any request, and a
// canceled context ends it early with the context's error.
func TestDwell(t *testing.T) {
	fakeSrv := &fake.Server{}
	ts := fakeSrv.Start(t)
	u := &sessionUser{
		cfg: &userclass.Config{
			HTTPClient: http.DefaultClient,
			RouterURL:  ts.URL,
			Atespace:   "benchmark",
			Dyn:        dynconfig.Static(knobs{}),
		},
		actorName: "agent-test",
	}
	start := time.Now()
	if err := u.execOp(context.Background(), op{kind: opDwell, millis: 30}); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 30*time.Millisecond {
		t.Errorf("dwell returned after %v, want at least 30ms", took)
	}
	if n := len(fakeSrv.RecordedPaths()); n != 0 {
		t.Errorf("dwell sent %d requests, want none", n)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := u.execOp(ctx, op{kind: opDwell, millis: 10_000}); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled dwell returned %v, want context.Canceled", err)
	}
}
