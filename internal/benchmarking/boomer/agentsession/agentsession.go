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

// Package agentsession implements the AgentSessionUser locust test: each VU
// is one coding-agent session replaying a scripted step sequence (a YAML
// file under scripts/, selected by --agentsession-script) against a glutton
// actor. After every step the actor is suspended for the
// step's "LLM thinking" gap; by default the next step's first request wakes
// it through the atenet router (request parking), so the benchmark measures
// the user-visible wake latency Substrate's oversubscription story rests on.
//
// Per-step timings land in locust stats as Step_<name>. The first request
// after each suspend is additionally recorded as WakeFirstTouch — in
// implicit resume mode that row IS the parking wake latency distribution.
// TimeToWake is the whole wake, from the first ResumeActor attempt to that
// first request, including the time a session spent retrying while the
// fleet had no room for its actor (--agentsession-resume-retry-budget).
// --agentsession-phase-in staggers session starts so a fleet spawned in one
// ramp does not reach every later step in a single wave.
package agentsession

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mathrand "math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/benchmarking/boomer/boomerutil"
	"go.ate.dev/substrate/internal/benchmarking/boomer/dynconfig"
	bmetrics "go.ate.dev/substrate/internal/benchmarking/boomer/metrics"
	"go.ate.dev/substrate/internal/benchmarking/boomer/userclass"
	"go.ate.dev/substrate/internal/benchmarking/glutton"
	gluttonpb "go.ate.dev/substrate/internal/proto/glutton"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	agentSessionUserClass = "AgentSessionUser"
	// templateNS is the atespace holding the benchmark ActorTemplates.
	templateNS = "benchmark-workloads"
	// templateName is the ActorTemplate instantiated per session: the stock
	// glutton actor. The script needs it deployed with --actor-memory 1Gi
	// (see benchmarking/README.md).
	templateName = "glutton"

	// controlRPCTimeout bounds every control-plane RPC. The router HTTP
	// client already times out at 30s; without a deadline here a
	// server-side hang in SuspendActor or DeleteActor would park the VU
	// goroutine for the rest of the run.
	controlRPCTimeout = 60 * time.Second

	// maxConsecutiveStepFailures is how many ReplaceIfPersistent failures in
	// a row a session tolerates before its actor is deleted and recreated.
	maxConsecutiveStepFailures = 3

	// defaultResumeRetryBudget is how long a step keeps retrying a wake the
	// fleet had no room for when --agentsession-resume-retry-budget is unset;
	// a budget of 0 gives the wake one attempt.
	defaultResumeRetryBudget = 60 * time.Second
	// The backoff between wake retries, the same window as glutton's spawn
	// path: short enough that a slot freed by another session's suspend is
	// taken within a second. Each wait is lengthened by up to this fraction
	// of itself, so sessions that hit a full fleet in the same instant
	// drift apart over their attempts instead of retrying in step.
	wakeRetryInitialBackoff = 50 * time.Millisecond
	wakeRetryMaxBackoff     = time.Second
	wakeRetryJitterFraction = 0.1
)

// errStopping is startUser's answer once shutdown has begun: the sweep has
// passed, so no session may start.
var errStopping = errors.New("agentsession: shutting down, no session started")

// burnRate is the per-goroutine sha256 iteration rate BurnCPU reports.
// BurnCPU runs for a fixed wall-clock duration, so Step_* latency stays flat
// when the sandbox is CPU-starved; this rate is what drops.
var burnRate = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "agentsession_burn_cpu_iterations_per_second",
	Help:    "BurnCPU sha256 iterations per second per goroutine; drops under CPU contention while Step_* latency stays flat.",
	Buckets: prometheus.ExponentialBuckets(256, 2, 16),
})

func init() {
	prometheus.MustRegister(burnRate)
	userclass.Add(userclass.Entry{
		Name:       "agentsession",
		LocustFile: "agentsession.py",
		UserClass:  agentSessionUserClass,
		Config:     knobsCodec,
		Init:       initAgentSession,
	})
}

// knobs is the AgentSessionUser slice of the runtime config. The master
// populates the keys from the --agentsession-* locust flags
// (common/agentsession_config.py).
type knobs struct {
	dynconfig.Lifecycle
	// Script is the built-in script variant; "" falls back to the default.
	Script string `json:"agentsession_script"`
	// ScriptFile is a script YAML on the worker; wins over Script when set.
	ScriptFile string `json:"agentsession_script_file"`
	// ThinkScale multiplies the script's per-step think times; 0 reads as 1.
	ThinkScale float64 `json:"agentsession_think_scale"`
	// SnapshotSteps is a comma-separated list of 1-based step numbers
	// after which the actor is hibernated; "" hibernates after every step.
	SnapshotSteps string `json:"agentsession_snapshot_steps"`
	// PhaseIn spreads session starts: each VU waits a uniform random share
	// of it before creating its actor, so a fleet does not move through the
	// script in lockstep waves; 0 starts every session at once.
	PhaseIn dynconfig.Seconds `json:"agentsession_phase_in"`
	// ResumeRetryBudget bounds how long a step keeps retrying a wake the
	// fleet had no room for before it gives up on the step; 0 gives the
	// wake one attempt. Unset, it is defaultResumeRetryBudget.
	ResumeRetryBudget dynconfig.Seconds `json:"agentsession_resume_retry_budget"`
}

var knobsCodec = dynconfig.Typed[knobs]{
	Defaults: knobs{ResumeRetryBudget: dynconfig.Seconds(defaultResumeRetryBudget)},
	Validate: func(k knobs) error {
		if err := k.Lifecycle.Validate(); err != nil {
			return err
		}
		if k.ThinkScale < 0 {
			return fmt.Errorf("agentsession_think_scale cannot be negative: %f", k.ThinkScale)
		}
		if k.PhaseIn < 0 {
			return fmt.Errorf("agentsession_phase_in cannot be negative: %v", k.PhaseIn)
		}
		if k.ResumeRetryBudget < 0 {
			return fmt.Errorf("agentsession_resume_retry_budget cannot be negative: %v", k.ResumeRetryBudget)
		}
		if steps, err := parseSnapshotSteps(k.SnapshotSteps); err != nil {
			return err
		} else if len(steps) > 0 && k.LifecycleMode == dynconfig.LifecycleModeNone {
			return fmt.Errorf("agentsession_snapshot_steps cannot be used with lifecycle_mode %q", dynconfig.LifecycleModeNone)
		}
		return nil
	},
}

// parseSnapshotSteps parses a comma-separated list of 1-based step numbers
// (for example, "3,10,20,30,40,50,61") into a set. An empty string returns
// nil, nil (meaning hibernate after every step).
func parseSnapshotSteps(raw string) (map[int]bool, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	steps := make(map[int]bool)
	for _, entry := range strings.Split(raw, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(entry))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid agentsession_snapshot_steps entry %q: must be a positive 1-based step number", entry)
		}
		steps[n] = true
	}
	return steps, nil
}

func initAgentSession(cfg *userclass.Config) (taskFn func(), shutdown func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/agentsession")
	}
	rt := &runtime{cfg: cfg}
	return rt.iterate, rt.shutdown
}

// makeIngestBuf pre-generates one random payload sized to the script's
// largest ingest. Generating bytes per op would count client-side
// crypto/rand time inside the step timings and allocate tens of MiB per VU
// per op; glutton only writes the payload, so every session can safely
// slice the same buffer.
func makeIngestBuf(steps []Step) []byte {
	var maxBytes int64
	for _, s := range steps {
		for _, o := range s.Ops {
			if o.kind == opIngest && o.bytes > maxBytes {
				maxBytes = o.bytes
			}
		}
	}
	buf := make([]byte, maxBytes)
	if _, err := rand.Read(buf); err != nil {
		// Payload content is irrelevant to the benchmark; zeros still move
		// the same bytes over the wire.
		slog.Warn("failed to randomize ingest payload; using zeros", slog.String("err", err.Error()))
	}
	return buf
}

// runtime is the per-worker state shared by every boomer goroutine. Each
// goroutine keeps its own session in users, keyed by goroutine ID, because
// boomer offers no per-VU context.
type runtime struct {
	cfg   *userclass.Config
	users sync.Map // goroutineID -> *sessionUser
	// phasedIn holds the goroutines that have served their phase-in wait,
	// so a start that failed and is retried does not wait a second time.
	phasedIn sync.Map // goroutineID -> struct{}
	// stopping is set by shutdown. A VU that comes out of its phase-in
	// wait after that starts no session, and one whose start finishes
	// after that cleans up its own, since the sweep has passed.
	stopping atomic.Bool
	// starting counts the starts past their phase-in wait that iterate has
	// not yet stored or cleaned up, so shutdown can wait for them: main
	// exits when it returns. startMu orders setting stopping against
	// joining the count, so no start slips in between the two.
	startMu  sync.Mutex
	starting sync.WaitGroup

	// loaded is what the knobs currently select, resolved by loadScript when
	// a session starts and re-resolved when the knobs name something else or
	// the named file's content changes. A session captures the script it
	// started on, so a change applies to new sessions only.
	scriptMu sync.Mutex
	loaded   *loadedScript

	// The template memory check is tied to the loaded script. Success is
	// cached until the script changes; a refusal is cached for
	// templateRecheckInterval so a redeploy with a bigger limit is picked
	// up without restarting the workers.
	templateMu     sync.Mutex
	templateOKFor  *Script
	templateErr    error
	templateErrAt  time.Time
	templateErrFor *Script
}

// templateRecheckInterval is how long a template memory refusal stands
// before the next startUser asks ateapi again.
const templateRecheckInterval = 30 * time.Second

// loadedScript is a resolved script together with what was derived from
// it, so a session never pairs one script's steps with another's buffer.
type loadedScript struct {
	*Script
	// source is the file path or built-in name the knobs resolved to, and
	// sum the content it had when loaded; together they say whether the
	// knobs still select this script.
	source string
	sum    [sha256.Size]byte
	// ingestBuf is the shared read-only ingest payload, sized to the
	// script's largest ingest op.
	ingestBuf []byte
}

// scriptSource is what the knobs select: a file named by
// --agentsession-script-file wins, else the built-in variant named by
// --agentsession-script, else the default.
func scriptSource(dyn knobs) (source string, fromFile bool) {
	if dyn.ScriptFile != "" {
		return dyn.ScriptFile, true
	}
	if dyn.Script != "" {
		return dyn.Script, false
	}
	return DefaultScript, false
}

// loadScript resolves the script the knobs currently select, loading it the
// first time and again whenever the knobs change or, for a file, its content
// changes: a redeployed ConfigMap reaches a running pod as a rewritten file
// at the same path. It cannot happen at Init: the operator's knob values
// arrive with the first spawn message, after Init, so a load there would
// only ever see --config-json. A failed load is returned each time, so new
// sessions block loudly until the knob is corrected and a new swarm
// started; sessions already running keep the script they started on.
func (r *runtime) loadScript() (*loadedScript, error) {
	r.scriptMu.Lock()
	defer r.scriptMu.Unlock()
	source, fromFile := scriptSource(dynconfig.Get[knobs](r.cfg.Dyn))
	data, err := readScript(source, fromFile)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	if r.loaded != nil && source == r.loaded.source && sum == r.loaded.sum {
		return r.loaded, nil
	}
	s, err := decodeScript(data, source, fromFile)
	if err != nil {
		return nil, err
	}
	b := Budgets(s.Steps)
	slog.Info("agentsession: loaded script",
		slog.String("script", s.Name),
		slog.String("source", source),
		slog.Int("steps", len(s.Steps)),
		slog.String("declared_ram", FormatSize(b.RAM)),
		slog.String("declared_disk", FormatSize(b.Disk)),
		slog.String("min_actor_memory", FormatSize(s.MinActorMemory)))
	if r.loaded != nil {
		slog.Warn("agentsession: script changed; sessions already running keep the old one",
			slog.String("old", r.loaded.Name), slog.String("new", s.Name))
	}
	r.loaded = &loadedScript{Script: s, source: source, sum: sum, ingestBuf: makeIngestBuf(s.Steps)}
	return r.loaded, nil
}

// checkTemplateMemory refuses to start sessions against a template whose
// memory limit is below the script's min_actor_memory, or whose limit does
// not parse. A too-small actor OOMs partway through the script and shows up
// as flaky steps, which is far harder to read than a refusal at start. A
// template with no memory limit at all is allowed through with a warning.
func (r *runtime) checkTemplateMemory(ctx context.Context, script *Script) error {
	r.templateMu.Lock()
	defer r.templateMu.Unlock()
	if r.templateOKFor == script {
		return nil
	}
	if r.templateErr != nil && r.templateErrFor == script && time.Since(r.templateErrAt) < templateRecheckInterval {
		return r.templateErr
	}
	ref := &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName}
	tmpl, err := r.cfg.APIStub.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: ref})
	if err != nil {
		// Not a verdict: the next startUser asks again.
		return fmt.Errorf("GetActorTemplate %s/%s: %w", templateNS, templateName, err)
	}
	refuse := func(err error) error {
		r.templateErr, r.templateErrAt, r.templateErrFor = err, time.Now(), script
		return err
	}
	limit, found, err := memoryLimit(tmpl)
	switch {
	case err != nil:
		return refuse(fmt.Errorf("template %s/%s: %w", templateNS, templateName, err))
	case !found:
		slog.Warn("agentsession: template sets no memory limit; cannot check it against the script",
			slog.String("template", templateName),
			slog.String("min_actor_memory", FormatSize(script.MinActorMemory)))
	case limit < script.MinActorMemory:
		return refuse(fmt.Errorf("template %s/%s memory limit %s is below script %q min_actor_memory %s; redeploy the workloads with --actor-memory %s",
			templateNS, templateName, FormatSize(limit), script.Name, FormatSize(script.MinActorMemory), FormatSize(script.MinActorMemory)))
	}
	r.templateOKFor = script
	r.templateErr = nil
	return nil
}

// memoryLimit reads the template's memory limit in bytes. found is false
// when the template sets none.
func memoryLimit(tmpl *ateapipb.ActorTemplate) (limit int64, found bool, err error) {
	limit = tmpl.GetResources().GetLimits().GetMemoryBytes()
	return limit, limit != 0, nil
}

// think returns the suspended "LLM thinking" gap before step s, which is the
// script's think time scaled by --agentsession-think-scale (0 reads as 1.0),
// with ±20% jitter so a fleet of sessions doesn't move in lockstep.
func (r *runtime) think(s Step) time.Duration {
	scale := dynconfig.Get[knobs](r.cfg.Dyn).ThinkScale
	if scale <= 0 {
		scale = 1.0
	}
	jitter := 0.8 + 0.4*mathrand.Float64()
	return time.Duration(float64(s.Think) * scale * jitter)
}

// phaseIn makes a VU that has no session yet wait its share of
// --agentsession-phase-in before it starts one. Locust spawns the fleet in
// one ramp and every session begins at step 1, so without it the sessions
// reach each later step together, in a wave the spawn ramp alone spreads
// over seconds: with a step that dwells for longer than that, the wave
// asks every worker for more slots at once than it has. The wait is drawn
// once per VU; a start that fails and is retried does not wait again.
func (r *runtime) phaseIn(gid int64) {
	if _, done := r.phasedIn.LoadOrStore(gid, struct{}{}); done {
		return
	}
	if d := r.phaseInDelay(); d > 0 {
		time.Sleep(d)
	}
}

// phaseInDelay draws one VU's wait: uniform in [0, --agentsession-phase-in),
// or none when the window is unset.
func (r *runtime) phaseInDelay() time.Duration {
	window := dynconfig.Get[knobs](r.cfg.Dyn).PhaseIn.Duration()
	if window <= 0 {
		return 0
	}
	return time.Duration(mathrand.Int64N(int64(window)))
}

// iterate is the boomer task function: one scripted step per call. The VU's
// session is created lazily on first use and wraps back to step 1 after the
// script completes, so one actor keeps producing suspend/resume cycles for
// the whole run.
func (r *runtime) iterate() {
	gid := boomerutil.GoroutineID()
	val, loaded := r.users.Load(gid)
	if !loaded {
		loaded, err := r.loadScript()
		if err != nil {
			slog.Error("agentsession: cannot load script; goroutine will retry next iter",
				slog.String("err", err.Error()))
			time.Sleep(2 * time.Second)
			return
		}
		u, err := r.startUser(context.Background(), gid, loaded)
		if errors.Is(err, errStopping) {
			return
		}
		if err != nil {
			slog.Warn("agentsession start failed; goroutine will retry next iter",
				slog.String("err", err.Error()))
			time.Sleep(2 * time.Second)
			return
		}
		val, _ = r.users.LoadOrStore(gid, u)
		stopping := r.stopping.Load()
		if stopping {
			// shutdown ran while the session started, and its sweep may
			// or may not have seen the entry. Deleting the entry claims
			// the cleanup, on either side, so the actor is suspended and
			// deleted exactly once and does not outlive the run.
			if _, ok := r.users.LoadAndDelete(gid); ok {
				u.suspendAndDelete(context.Background())
			}
		}
		// The start is complete, cleanup included; shutdown may return.
		r.starting.Done()
		if stopping {
			return
		}
	}
	u := val.(*sessionUser)

	step := u.steps[u.stepIndex]

	// The LLM is "thinking": the actor stays suspended for the gap.
	time.Sleep(r.think(step))

	if u.runStep(context.Background(), step) {
		u.stepIndex = (u.stepIndex + 1) % len(u.steps)
		if u.stepIndex == 0 {
			slog.Info("agent session completed the full script; starting over",
				slog.String("actor", u.actorName))
		}
	}

	if u.broken {
		slog.Warn("agent session wedged; deleting its actor and starting a fresh session",
			slog.String("actor", u.actorName),
			slog.Int("consecutive_failures", u.consecutiveFailures))
		u.suspendAndDelete(context.Background())
		r.users.Delete(gid)
	}
}

// startUser creates the session's actor, waits for its sandbox to serve,
// then suspends it so the first scripted step begins — like every later
// step — with a wake from suspension. The VU's phase-in wait comes after
// the preflight checks, so a refusal that a redeploy fixes a minute into
// the run does not leave every VU whose wait elapsed meanwhile starting
// together once the check clears. It returns errStopping when shutdown
// began during the wait. A start that gets past the wait is counted in
// starting until iterate has stored or cleaned up the session; a start
// that fails leaves the count itself.
func (r *runtime) startUser(ctx context.Context, gid int64, loaded *loadedScript) (*sessionUser, error) {
	if err := r.checkTemplateMemory(ctx, loaded.Script); err != nil {
		return nil, err
	}
	dyn := dynconfig.Get[knobs](r.cfg.Dyn)
	if err := checkSnapshotSteps(dyn, loaded.Script); err != nil {
		return nil, err
	}
	r.phaseIn(gid)
	r.startMu.Lock()
	if r.stopping.Load() {
		r.startMu.Unlock()
		return nil, errStopping
	}
	r.starting.Add(1)
	r.startMu.Unlock()
	u, err := r.createSession(ctx, dyn, loaded)
	if err != nil {
		r.starting.Done()
		return nil, err
	}
	return u, nil
}

// createSession is the part of a start that touches the cluster: it creates
// the actor, waits for it to serve, and parks it unless the session stays
// resident.
func (r *runtime) createSession(ctx context.Context, dyn knobs, loaded *loadedScript) (*sessionUser, error) {
	u := &sessionUser{
		cfg:       r.cfg,
		actorName: "agent-" + uuid.NewString(),
		steps:     loaded.Steps,
		ingestBuf: loaded.ingestBuf,
	}
	slog.Info("Creating agent session", slog.String("actor", u.actorName))
	bmetrics.UpdateUsers(agentSessionUserClass, 1)

	if err := u.ensureAtespace(ctx); err != nil {
		bmetrics.UpdateUsers(agentSessionUserClass, -1)
		return nil, fmt.Errorf("ensureAtespace: %w", err)
	}
	if err := u.create(ctx); err != nil {
		bmetrics.UpdateUsers(agentSessionUserClass, -1)
		return nil, fmt.Errorf("createActor: %w", err)
	}
	if err := u.waitServing(ctx); err != nil {
		// suspendAndDelete decrements the user gauge; no extra decrement here.
		u.suspendAndDelete(ctx)
		return nil, fmt.Errorf("waitServing: %w", err)
	}
	u.awake = true
	// Park the fresh actor so step 01 wakes it like any other step, unless
	// the session stays resident (--lifecycle-mode none or
	// --agentsession-snapshot-steps). A failed park is re-driven at the top
	// of the first step.
	if u.shouldHibernateAtStart(dyn) {
		u.hibernate(ctx)
	}
	return u, nil
}

// checkSnapshotSteps refuses to start a session when --agentsession-snapshot-steps
// names a 1-based step number beyond the loaded script's length, so a typo
// (or 61 against a shorter script) does not silently run resident.
func checkSnapshotSteps(dyn knobs, script *Script) error {
	steps, err := parseSnapshotSteps(dyn.SnapshotSteps)
	if err != nil {
		return err
	}
	for step := range steps {
		if step > len(script.Steps) {
			return fmt.Errorf("agentsession_snapshot_steps entry %d exceeds script %q length (%d steps)", step, script.Name, len(script.Steps))
		}
	}
	return nil
}

// shutdownConcurrency bounds how many sessions suspendAndDelete at once.
// Boomer gives the hook about a minute; at thousands of sessions a serial
// sweep leaks most of the actors, while this many in flight clears them
// without flooding ateapi.
const shutdownConcurrency = 64

// shutdown suspends and deletes every session's actor, shutdownConcurrency
// at a time, until done or ctx expires. It first marks the runtime as
// stopping, so a VU that is still waiting out its phase-in starts no
// session once it wakes, and one whose start finishes during the sweep
// cleans up its own session (see iterate). After the sweep it waits for
// those in-flight starts, within the same ctx, since main exits once it
// returns.
func (r *runtime) shutdown(ctx context.Context) {
	r.startMu.Lock()
	r.stopping.Store(true)
	r.startMu.Unlock()
	var wg sync.WaitGroup
	sem := make(chan struct{}, shutdownConcurrency)
	r.users.Range(func(key, val any) bool {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return false
		}
		// Deleting the entry claims its cleanup; a VU that stored its
		// session during the sweep claims its own the same way.
		if _, ok := r.users.LoadAndDelete(key); !ok {
			<-sem
			return true
		}
		wg.Add(1)
		go func(u *sessionUser) {
			defer wg.Done()
			defer func() { <-sem }()
			u.suspendAndDelete(ctx)
		}(val.(*sessionUser))
		return true
	})
	wg.Wait()

	started := make(chan struct{})
	go func() {
		r.starting.Wait()
		close(started)
	}()
	select {
	case <-started:
	case <-ctx.Done():
	}
}

// sessionUser is one coding-agent session: a single actor plus its progress
// through the script. Owned by one boomer goroutine; no locking needed.
type sessionUser struct {
	cfg       *userclass.Config
	actorName string
	stepIndex int
	cleanedUp bool
	// steps is the script this session started on; a knob change mid-run
	// does not move a session to another script.
	steps []Step
	// ingestBuf is the runtime's shared read-only ingest payload.
	ingestBuf []byte
	// awake is true when the actor is resident (RUNNING) rather than
	// suspended. Zero value (false) means parked.
	awake bool
	// hibernatePending is set by a failed SuspendActor: the actor is
	// stranded RUNNING or SUSPENDING. Waking it from there would misreport
	// WakeFirstTouch, so runStep finishes the hibernate first.
	hibernatePending bool
	// consecutiveFailures counts ReplaceIfPersistent failures since the
	// last successful step; see noteFailure.
	consecutiveFailures int
	// broken is set once the actor should be replaced: a CRASHED or
	// otherwise stuck actor never recovers on its own, and without
	// replacement it would wedge its VU for the rest of the run.
	broken bool
}

// httpError is a router reply with a status of 400 or above. It keeps the
// status so a failure can be classified the way a gRPC code would be: the
// router maps ateapi's ResourceExhausted and Unavailable to 503 and its
// deadline to 504, and in implicit resume mode the wake ping is the only
// place those verdicts reach the driver.
type httpError struct {
	route  string
	status int
	body   string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("%s: HTTP %d: %s", e.route, e.status, e.body)
}

// classifyHTTP maps a router status onto a failure action. 503, 504, and
// 429 are the fleet or the control plane being busy, which a replacement
// would only add to; 404 means the actor record is gone. Anything else is
// counted toward replacement, as a non-status error would be.
func classifyHTTP(status int) boomerutil.FailureAction {
	switch status {
	case http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusTooManyRequests:
		return boomerutil.RetryLater
	case http.StatusNotFound:
		return boomerutil.ReplaceNow
	}
	return boomerutil.ReplaceIfPersistent
}

// noteFailure classifies a failed step or lifecycle call and marks the actor
// broken when a replacement is warranted. Cluster-wide errors (no capacity,
// ate-api-server restarting) are not the actor's fault and do not count,
// whether they arrive as a gRPC status or as the router's HTTP mapping of
// one. Replacing an actor on a capacity error would be self-defeating: the
// replacement's first activation needs the very room the error reported
// missing, so a fleet near its slots would churn actors instead of waiting.
func (u *sessionUser) noteFailure(err error) {
	action := boomerutil.ClassifyLifecycleFailure(err)
	var he *httpError
	if errors.As(err, &he) {
		action = classifyHTTP(he.status)
	}
	switch action {
	case boomerutil.ReplaceNow:
		u.broken = true
	case boomerutil.RetryLater:
	default:
		u.consecutiveFailures++
		if u.consecutiveFailures >= maxConsecutiveStepFailures {
			u.broken = true
		}
	}
}

// noteSuccess clears the failure count: the actor just completed a whole
// step, so the earlier failures were transient after all.
func (u *sessionUser) noteSuccess() {
	u.consecutiveFailures = 0
}

func (u *sessionUser) ref() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: u.cfg.Atespace, Name: u.actorName}
}

// shouldHibernateAtStart reports whether a freshly started session should be
// parked before step 1. In resident mode (LifecycleModeNone) or when
// snapshot steps are configured, the actor stays awake after waitServing.
func (u *sessionUser) shouldHibernateAtStart(dyn knobs) bool {
	if dyn.LifecycleMode == dynconfig.LifecycleModeNone {
		return false
	}
	steps, err := parseSnapshotSteps(dyn.SnapshotSteps)
	return err != nil || len(steps) == 0
}

// shouldHibernateAfterStep reports whether the actor should be parked after
// completing the current step (1-based index u.stepIndex+1).
func (u *sessionUser) shouldHibernateAfterStep(dyn knobs) bool {
	if dyn.LifecycleMode == dynconfig.LifecycleModeNone {
		return false
	}
	steps, err := parseSnapshotSteps(dyn.SnapshotSteps)
	if err != nil || len(steps) == 0 {
		return true
	}
	return steps[u.stepIndex+1]
}

// runStep wakes the actor, executes the step's ops in order, and suspends
// it again, reporting whether everything succeeded. The wake is measured by
// a dedicated ping sent ahead of the step's ops and recorded as
// WakeFirstTouch; in implicit resume mode that ping is also what triggers
// the parked wake, so the row measures the wake alone rather than the wake
// plus whatever heavy op happens to lead the step.
func (u *sessionUser) runStep(ctx context.Context, step Step) bool {
	// A failed hibernate left the actor awake. In implicit mode the wake
	// ping would return in a few ms and be booked as a WakeFirstTouch
	// success; in explicit mode ResumeActor would fail FailedPrecondition.
	// SuspendActor is re-entrant in either mode, so finish the hibernate
	// and pick the step up next iteration.
	if u.hibernatePending {
		u.hibernate(ctx)
		return false
	}

	dyn := dynconfig.Get[knobs](u.cfg.Dyn)
	wasParked := !u.awake && dyn.LifecycleMode != dynconfig.LifecycleModeNone
	// A parked actor's wake, from the first attempt to the ping that finds it
	// serving, is one TimeToWake sample. A fleet with no room for the actor
	// is retried within the budget, so the wait for a slot lands in that row
	// as latency instead of vanishing into failed rows and a skipped step.
	wakeStart := time.Now()
	deadline := wakeStart.Add(dyn.ResumeRetryBudget.Duration())
	if wasParked && dyn.ResumeMode == dynconfig.ResumeModeExplicit {
		if err := retryWhileFull(ctx, deadline, func() error { return u.resume(ctx) }); err != nil {
			u.noteFailure(err)
			u.recordTimeToWake(wakeStart, err)
			if isCapacityError(err) {
				slog.Warn("agent session found no room within the retry budget",
					slog.String("actor", u.actorName),
					slog.String("step", step.Name),
					slog.Duration("waited", time.Since(wakeStart)),
					slog.String("err", err.Error()))
			}
			return false
		}
	}

	var err error
	if wasParked {
		err = retryWhileFull(ctx, deadline, func() error { return u.wakePing(ctx) })
	} else {
		err = u.execOp(ctx, ping())
	}
	if err != nil {
		u.noteFailure(err)
		if wasParked {
			u.recordTimeToWake(wakeStart, err)
		}
		slog.Warn("agent session wake failed",
			slog.String("actor", u.actorName),
			slog.String("step", step.Name),
			slog.Duration("waited", time.Since(wakeStart)),
			slog.String("err", err.Error()))
		if wasParked || u.shouldHibernateAfterStep(dyn) {
			u.hibernate(ctx)
		}
		return false
	}
	u.awake = true
	if wasParked {
		u.recordTimeToWake(wakeStart, nil)
	}

	metricName := "Step_" + step.Name
	stepStart := time.Now()
	for i, o := range step.Ops {
		if err := u.execOp(ctx, o); err != nil {
			u.noteFailure(err)
			bmetrics.RecordFailure("http", metricName, agentSessionUserClass, time.Since(stepStart), err.Error())
			slog.Warn("agent session step failed",
				slog.String("actor", u.actorName),
				slog.String("step", step.Name),
				slog.Int("op", i),
				slog.String("err", err.Error()))
			if wasParked || u.shouldHibernateAfterStep(dyn) {
				u.hibernate(ctx)
			}
			return false
		}
	}
	u.noteSuccess()
	bmetrics.RecordSuccess("http", metricName, agentSessionUserClass, time.Since(stepStart), 0)

	if u.shouldHibernateAfterStep(dyn) {
		u.hibernate(ctx)
	}
	return true
}

// wakePing sends the ping that measures a parked actor's wake, recorded as
// a WakeFirstTouch sample of its own: in explicit resume mode the round trip
// after ResumeActor returned, in implicit mode the parked request itself.
func (u *sessionUser) wakePing(ctx context.Context) error {
	start := time.Now()
	if err := u.execOp(ctx, ping()); err != nil {
		bmetrics.RecordFailure("http", "WakeFirstTouch", agentSessionUserClass, time.Since(start), err.Error())
		return err
	}
	bmetrics.RecordSuccess("http", "WakeFirstTouch", agentSessionUserClass, time.Since(start), 0)
	return nil
}

// recordTimeToWake books one step's wake as a TimeToWake sample: the time
// from the first ResumeActor attempt (in implicit mode, the first wake ping)
// to the ping that found the actor serving, retries included.
func (u *sessionUser) recordTimeToWake(start time.Time, err error) {
	if err != nil {
		bmetrics.RecordFailure("summary", "TimeToWake", agentSessionUserClass, time.Since(start), err.Error())
		return
	}
	bmetrics.RecordSuccess("summary", "TimeToWake", agentSessionUserClass, time.Since(start), 0)
}

// retryWhileFull runs call and, while it fails with a capacity error and
// the next attempt would still land before deadline, waits with exponential
// backoff and runs it again. Each attempt records its own sample inside
// call, as glutton's spawn path does. Any other error returns at once.
func retryWhileFull(ctx context.Context, deadline time.Time, call func() error) error {
	delay := wakeRetryInitialBackoff
	for {
		err := call()
		if err == nil || !isCapacityError(err) {
			return err
		}
		wait := delay + time.Duration(mathrand.Float64()*wakeRetryJitterFraction*float64(delay))
		if time.Now().Add(wait).After(deadline) {
			return err
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return err
		}
		delay = min(delay*2, wakeRetryMaxBackoff)
	}
}

// isCapacityError reports whether err says the fleet or the control plane
// had no room for the actor just now: ateapi's ResourceExhausted ("no
// worker has room for the actor") or Unavailable, or the router's 503, 504
// and 429 faces of them. The actor itself is fine, and the next try may find
// a slot another session's suspend has freed.
func isCapacityError(err error) bool {
	var he *httpError
	if errors.As(err, &he) {
		return classifyHTTP(he.status) == boomerutil.RetryLater
	}
	switch status.Code(err) {
	case codes.ResourceExhausted, codes.Unavailable:
		return true
	}
	return false
}

// execOp performs one scripted op as a glutton RPC through the router.
func (u *sessionUser) execOp(ctx context.Context, o op) error {
	switch o.kind {
	case opIngest:
		payload := u.ingestBuf
		if int64(len(payload)) < o.bytes {
			// Fallback for callers (tests) that built the user by hand.
			payload = make([]byte, o.bytes)
		}
		return u.postProto(ctx, glutton.IngestRoute,
			&gluttonpb.IngestRequest{Key: o.key, Payload: payload[:o.bytes]},
			&gluttonpb.IngestResponse{})
	case opBurnCPU:
		resp := &gluttonpb.BurnCPUResponse{}
		if err := u.postProto(ctx, glutton.BurnCPURoute,
			&gluttonpb.BurnCPURequest{DurationMs: o.millis, Parallelism: o.parallel}, resp); err != nil {
			return err
		}
		observeBurnRate(o, resp.GetIterations())
		return nil
	case opWriteDisk:
		return u.postProto(ctx, glutton.WriteDiskRoute,
			&gluttonpb.WriteDiskRequest{Key: o.key, Size: int32(o.bytes), WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE},
			&gluttonpb.WriteDiskResponse{})
	case opReadDiskDigest:
		return u.postProto(ctx, glutton.ReadDiskRoute,
			&gluttonpb.ReadDiskRequest{Key: o.key, ReadMode: gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY},
			&gluttonpb.ReadDiskResponse{})
	case opReadDiskData:
		return u.postProto(ctx, glutton.ReadDiskRoute,
			&gluttonpb.ReadDiskRequest{Key: o.key, ReadMode: gluttonpb.ReadMode_READ_MODE_DATA},
			&gluttonpb.ReadDiskResponse{})
	case opFillRAM:
		// OVERWRITE grows the array on first touch and re-randomizes it in
		// place on later laps; TRUNCATE would reallocate and transiently
		// double the guest heap, which OOMs a tightly-sized sandbox.
		return u.postProto(ctx, glutton.WriteRAMRoute,
			&gluttonpb.WriteRAMRequest{Key: o.key, Size: fmt.Sprintf("%d", o.bytes), WriteMode: gluttonpb.WriteMode_WRITE_MODE_OVERWRITE},
			&gluttonpb.WriteRAMResponse{})
	case opChurnRAM:
		return u.postProto(ctx, glutton.WriteRAMRoute,
			&gluttonpb.WriteRAMRequest{Key: o.key, Size: fmt.Sprintf("%d", o.bytes), WriteMode: gluttonpb.WriteMode_WRITE_MODE_OVERWRITE_ROTATE},
			&gluttonpb.WriteRAMResponse{})
	case opWalkRAM:
		return u.postProto(ctx, glutton.ReadRAMRoute,
			&gluttonpb.ReadRAMRequest{Key: o.key},
			&gluttonpb.ReadRAMResponse{})
	case opPing:
		return u.postProto(ctx, glutton.PingRoute,
			&gluttonpb.PingRequest{Message: "think"},
			&gluttonpb.PingResponse{})
	case opDwell:
		// No request: the actor stays resident while the driver waits, the
		// way a gateway sits in an LLM round trip or a typing gap.
		return dwell(ctx, time.Duration(o.millis)*time.Millisecond)
	default:
		return fmt.Errorf("unknown op kind %d", o.kind)
	}
}

// dwell idles for d or until ctx ends, whichever comes first.
func dwell(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// observeBurnRate records a burn's iterations per goroutine-second.
func observeBurnRate(o op, iterations int64) {
	if rate, ok := burnRatePerGoroutine(o, iterations); ok {
		burnRate.Observe(rate)
	}
}

// burnRatePerGoroutine normalizes a BurnCPU result by the burn's goroutine
// count and wall-clock seconds. A zero-duration burn has no rate.
func burnRatePerGoroutine(o op, iterations int64) (float64, bool) {
	if o.millis <= 0 {
		return 0, false
	}
	goroutines := max(int64(o.parallel), 1)
	return float64(iterations) / float64(goroutines) / (float64(o.millis) / 1000), true
}

// postProto POSTs req to the actor's route through the router and
// unmarshals the reply into resp. HTTP status >= 400 is an error carrying
// the response body.
func (u *sessionUser) postProto(ctx context.Context, route string, req, resp proto.Message) error {
	body, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.cfg.RouterURL+route, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set(atenet.TargetActorHeader, u.cfg.Atespace+"/"+u.actorName)
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(httpReq.Header))

	httpResp, err := u.cfg.HTTPClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return err
	}
	if httpResp.StatusCode >= 400 {
		return &httpError{route: route, status: httpResp.StatusCode, body: strings.TrimSpace(string(respBody))}
	}
	return proto.Unmarshal(respBody, resp)
}

// ensureAtespace creates the configured atespace, treating AlreadyExists as
// success so concurrent sessions racing the first creation all proceed.
func (u *sessionUser) ensureAtespace(ctx context.Context) error {
	return u.tracedCall(ctx, "CreateAtespace", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.CreateAtespace(callCtx, &ateapipb.CreateAtespaceRequest{
			Atespace: &ateapipb.Atespace{
				Metadata: &ateapipb.ResourceMetadata{Name: u.cfg.Atespace},
			},
		}, grpc.Trailer(tr))
		if s, ok := status.FromError(err); ok && s.Code() == codes.AlreadyExists {
			return nil
		}
		return err
	})
}

func (u *sessionUser) create(ctx context.Context) error {
	return u.tracedCall(ctx, "CreateActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.CreateActor(callCtx, &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: u.cfg.Atespace, Name: u.actorName},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
			},
		}, grpc.Trailer(tr))
		return err
	})
}

// waitServing pings the actor through the router until glutton answers. The
// first request is also what wakes a newly created actor, so this doubles
// as the initial activation.
func (u *sessionUser) waitServing(ctx context.Context) error {
	const maxRetries = 30
	const retryInterval = 2 * time.Second
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		lastErr = u.postProto(ctx, glutton.PingRoute,
			&gluttonpb.PingRequest{Message: "hello"}, &gluttonpb.PingResponse{})
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryInterval):
		}
	}
	return fmt.Errorf("actor %s never served: %w", u.actorName, lastErr)
}

// resume issues ResumeActor, retrying concurrent-update conflicts inside
// the traced call so the reported latency spans every attempt.
func (u *sessionUser) resume(ctx context.Context) error {
	err := u.tracedCall(ctx, "ResumeActor", func(callCtx context.Context, tr *metadata.MD) error {
		return boomerutil.RetryOnConflict(callCtx, func() error {
			_, err := u.cfg.APIStub.ResumeActor(callCtx, &ateapipb.ResumeActorRequest{Actor: u.ref()}, grpc.Trailer(tr))
			return err
		})
	})
	if err != nil {
		if boomerutil.IsCrashed(err) {
			bmetrics.RecordFailure("actor", "CrashCount", agentSessionUserClass, 0, "actor entered ACTOR_STATE_CRASHED")
		}
		// A full fleet is retried by the caller and reported once when the
		// budget runs out; a line per attempt would flood the log.
		if !isCapacityError(err) {
			slog.Error("ResumeActor failed", slog.String("actor", u.actorName), slog.String("err", err.Error()))
		}
	}
	return err
}

// hibernate suspends the actor at the end of a step, asynchronously by default
// (async LifecycleMode) and synchronously in sync LifecycleMode. In
// LifecycleModeNone the actor stays resident and hibernate is a no-op. A
// failure leaves hibernatePending set so the next step re-drives it instead of
// waking a stranded actor.
func (u *sessionUser) hibernate(ctx context.Context) {
	mode := dynconfig.Get[knobs](u.cfg.Dyn).LifecycleMode
	if mode == dynconfig.LifecycleModeNone {
		u.hibernatePending = false
		return
	}
	var err error
	if mode == dynconfig.LifecycleModeSync {
		err = u.tracedCall(ctx, "SuspendActor", func(callCtx context.Context, tr *metadata.MD) error {
			_, err := u.cfg.APIStub.SuspendActor(callCtx, &ateapipb.SuspendActorRequest{Actor: u.ref(), Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS}}, grpc.Trailer(tr))
			return err
		})
	} else {
		err = u.tracedCall(ctx, "SuspendActorAsync", func(callCtx context.Context, tr *metadata.MD) error {
			_, err := u.cfg.APIStub.SuspendActor(callCtx, &ateapipb.SuspendActorRequest{Actor: u.ref(), Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS}}, grpc.Trailer(tr))
			return err
		})
	}
	u.hibernatePending = err != nil
	if err != nil {
		u.noteFailure(err)
		return
	}
	u.awake = false
}

// suspendAndDelete releases the actor and its worker. Suspend first: a
// worker only frees an actor it can account for, and an actor still awake
// at delete risks being left CRASHED. Safe to call more than once.
func (u *sessionUser) suspendAndDelete(ctx context.Context) {
	if u.cleanedUp {
		return
	}
	suspendCtx, cancel := context.WithTimeout(ctx, controlRPCTimeout)
	_, _ = u.cfg.APIStub.SuspendActor(suspendCtx, &ateapipb.SuspendActorRequest{Actor: u.ref()})
	cancel()
	_ = u.tracedCall(ctx, "DeleteActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.DeleteActor(callCtx, &ateapipb.DeleteActorRequest{Actor: u.ref()}, grpc.Trailer(tr))
		return err
	})
	bmetrics.UpdateUsers(agentSessionUserClass, -1)
	u.cleanedUp = true
}

// tracedCall runs one control-plane RPC under a span and a deadline, and
// records it as a locust stats row of the same name, preferring the
// server-measured elapsed time from the response trailer.
func (u *sessionUser) tracedCall(ctx context.Context, name string, do func(context.Context, *metadata.MD) error) error {
	ctx, cancel := context.WithTimeout(ctx, controlRPCTimeout)
	defer cancel()
	ctx, span := u.cfg.Tracer.Start(ctx, name)
	defer span.End()

	start := time.Now()
	var tr metadata.MD
	err := do(ctx, &tr)
	latency := time.Since(start)

	serverLatency, source := boomerutil.ElapsedFromMD(tr, ateinterceptors.ServerElapsedTrailer, latency)
	boomerutil.LogSampledTrace(span, name, serverLatency, source, err)
	if err != nil {
		bmetrics.RecordFailure("grpc", name, agentSessionUserClass, latency, err.Error())
		return err
	}
	bmetrics.RecordSuccess("grpc", name, agentSessionUserClass, latency, 0)
	return nil
}
