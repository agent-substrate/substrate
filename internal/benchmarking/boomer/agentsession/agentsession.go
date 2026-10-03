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
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/boomerutil"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	bmetrics "github.com/agent-substrate/substrate/internal/benchmarking/boomer/metrics"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton"
	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/resource"
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
)

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
}

var knobsCodec = dynconfig.Typed[knobs]{
	Validate: func(k knobs) error {
		if err := k.Lifecycle.Validate(); err != nil {
			return err
		}
		if k.ThinkScale < 0 {
			return fmt.Errorf("agentsession_think_scale cannot be negative: %f", k.ThinkScale)
		}
		return nil
	},
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
// when the template sets none; a limit that does not parse is an error.
func memoryLimit(tmpl *ateapipb.ActorTemplate) (limit int64, found bool, err error) {
	for _, l := range tmpl.GetResources().GetLimits() {
		if l.GetName() != "memory" {
			continue
		}
		q, err := resource.ParseQuantity(l.GetQuantity())
		if err != nil {
			return 0, true, fmt.Errorf("memory limit %q: %w", l.GetQuantity(), err)
		}
		n, ok := q.AsInt64()
		if !ok {
			return 0, true, fmt.Errorf("memory limit %q is not a whole byte count", l.GetQuantity())
		}
		return n, true, nil
	}
	return 0, false, nil
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
		u, err := r.startUser(context.Background(), loaded)
		if err != nil {
			slog.Warn("agentsession start failed; goroutine will retry next iter",
				slog.String("err", err.Error()))
			time.Sleep(2 * time.Second)
			return
		}
		val, _ = r.users.LoadOrStore(gid, u)
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
// step — with a wake from suspension.
func (r *runtime) startUser(ctx context.Context, loaded *loadedScript) (*sessionUser, error) {
	if err := r.checkTemplateMemory(ctx, loaded.Script); err != nil {
		return nil, err
	}
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
	// Park the fresh actor; step 01 wakes it like any other step. A failed
	// park is re-driven at the top of the first step.
	u.hibernate(ctx)
	return u, nil
}

// shutdownConcurrency bounds how many sessions suspendAndDelete at once.
// Boomer gives the hook about a minute; at thousands of sessions a serial
// sweep leaks most of the actors, while this many in flight clears them
// without flooding ateapi.
const shutdownConcurrency = 64

// shutdown suspends and deletes every session's actor, shutdownConcurrency
// at a time, until done or ctx expires.
func (r *runtime) shutdown(ctx context.Context) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, shutdownConcurrency)
	r.users.Range(func(_, val any) bool {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return false
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
	// hibernatePending is set by a failed Pause/Suspend: the actor is
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
	// SuspendActor and PauseActor are re-entrant, so finish the hibernate
	// and pick the step up next iteration.
	if u.hibernatePending {
		u.hibernate(ctx)
		return false
	}

	if dynconfig.Get[knobs](u.cfg.Dyn).ResumeMode == dynconfig.ResumeModeExplicit {
		if err := u.resume(ctx); err != nil {
			u.noteFailure(err)
			return false
		}
	}

	wakeStart := time.Now()
	if err := u.execOp(ctx, ping()); err != nil {
		u.noteFailure(err)
		bmetrics.RecordFailure("http", "WakeFirstTouch", agentSessionUserClass, time.Since(wakeStart), err.Error())
		slog.Warn("agent session wake failed",
			slog.String("actor", u.actorName),
			slog.String("step", step.Name),
			slog.String("err", err.Error()))
		u.hibernate(ctx)
		return false
	}
	bmetrics.RecordSuccess("http", "WakeFirstTouch", agentSessionUserClass, time.Since(wakeStart), 0)

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
			u.hibernate(ctx)
			return false
		}
	}
	u.noteSuccess()
	bmetrics.RecordSuccess("http", metricName, agentSessionUserClass, time.Since(stepStart), 0)

	u.hibernate(ctx)
	return true
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
	default:
		return fmt.Errorf("unknown op kind %d", o.kind)
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
		slog.Error("ResumeActor failed", slog.String("actor", u.actorName), slog.String("err", err.Error()))
	}
	return err
}

// hibernate suspends (or pauses, per LifecycleMode) the actor at the end of
// a step. A failure leaves hibernatePending set so the next step re-drives
// it instead of waking a stranded actor.
func (u *sessionUser) hibernate(ctx context.Context) {
	var err error
	if dynconfig.Get[knobs](u.cfg.Dyn).LifecycleMode == dynconfig.LifecycleModePause {
		err = u.tracedCall(ctx, "PauseActor", func(callCtx context.Context, tr *metadata.MD) error {
			_, err := u.cfg.APIStub.PauseActor(callCtx, &ateapipb.PauseActorRequest{Actor: u.ref()}, grpc.Trailer(tr))
			return err
		})
	} else {
		err = u.tracedCall(ctx, "SuspendActor", func(callCtx context.Context, tr *metadata.MD) error {
			_, err := u.cfg.APIStub.SuspendActor(callCtx, &ateapipb.SuspendActorRequest{Actor: u.ref()}, grpc.Trailer(tr))
			return err
		})
	}
	u.hibernatePending = err != nil
	if err != nil {
		u.noteFailure(err)
	}
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
		_, err := u.cfg.APIStub.DeleteActor(callCtx, &ateapipb.DeleteActorRequest{Actor: u.ref(), AnyState: true}, grpc.Trailer(tr))
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
