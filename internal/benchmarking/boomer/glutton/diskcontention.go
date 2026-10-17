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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/myzhan/boomer"
	"go.ate.dev/substrate/internal/benchmarking/boomer/boomerutil"
	"go.ate.dev/substrate/internal/benchmarking/boomer/dynconfig"
	bmetrics "go.ate.dev/substrate/internal/benchmarking/boomer/metrics"
	"go.ate.dev/substrate/internal/benchmarking/boomer/userclass"
	gluttonpb "go.ate.dev/substrate/internal/proto/glutton"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// The disk-contention benchmark measures how much the disk I/O of
// co-located actors slows the suspend/resume path. It runs two kinds of VU
// in one locust run:
//
//   - Probe VUs (the first --dc-probe-users VUs) run the GluttonUser cycle
//     unchanged, resume → ping → suspend, with every lifecycle row prefixed
//     "Probe" (ProbeResumeActor, ProbeSuspendActor). Their snapshot size is
//     fixed by --mem-target, so their latency is the dependent variable.
//   - Noise VUs (every later VU) generate the neighbor load. "disk" noise
//     resumes one glutton actor whose data dir is on the host disk and then
//     writes to it at --dc-noise-rate-mbps until the run ends, fsyncing each
//     write unless --dc-noise-no-fsync is set; it never suspends, so it adds
//     no rows to the probe's lifecycle metrics. "sweperf" noise runs the
//     SweperfUser replay, a real SWE-bench agent.
//
// Keeping both kinds in one run (instead of two runs side by side) puts
// their stats in one stats.jsonl and makes the noise level part of the test
// definition. Roles are handed out again after a boomer stop, so a stopped
// and restarted run gets its probes back; noise actors from the stopped run
// stay up until shutdown deletes them.
const (
	dcUserClass   = "DiskContentionUser"
	dcProbePrefix = "Probe"

	defaultNoiseTemplate        = "glutton-durdir-data"
	defaultNoiseWriteSize int64 = 64 << 20
	defaultNoiseFiles           = 4

	// Pause after a failed noise call, so a broken actor does not spin.
	noiseErrorBackoff = time.Second

	// Bound on creating a noise actor, which runs outside the run context so
	// shutdown cannot strand an actor the server created but nothing tracks.
	noiseCreateTimeout = 30 * time.Second
)

// Noise modes: what the noise VUs run.
const (
	noiseModeNone    = "none"
	noiseModeDisk    = "disk"
	noiseModeSweperf = "sweperf"
)

// dcKnobs is the DiskContentionUser slice of the payload.
type dcKnobs struct {
	// ProbeUsers is how many VUs, the first spawned, run probe actors; the
	// rest are noise.
	ProbeUsers int `json:"dc_probe_users"`
	// ProbeMinWait and ProbeMaxWait are the probes' own think time, so
	// sweperf noise keeps min_wait_time and max_wait_time. Each falls back to
	// its min_wait_time or max_wait_time counterpart when unset or zero.
	ProbeMinWait dynconfig.Seconds `json:"dc_probe_min_wait_time"`
	ProbeMaxWait dynconfig.Seconds `json:"dc_probe_max_wait_time"`
	// NoiseMode is noiseModeNone, noiseModeDisk, or noiseModeSweperf; empty
	// reads as none.
	NoiseMode string `json:"dc_noise_mode"`
	// NoiseTemplate is the ActorTemplate of disk noise actors; empty falls
	// back to defaultNoiseTemplate.
	NoiseTemplate string `json:"dc_noise_template"`
	// NoiseWriteSize is the bytes per noise WriteDisk call; 0 falls back to
	// defaultNoiseWriteSize.
	NoiseWriteSize int64 `json:"dc_noise_write_size_bytes"`
	// NoiseFiles is how many files each noise actor rotates through; 0
	// falls back to defaultNoiseFiles.
	NoiseFiles int `json:"dc_noise_files"`
	// NoiseRateMBps caps each noise actor's write rate in MB/s; 0 writes as
	// fast as the disk allows.
	NoiseRateMBps float64 `json:"dc_noise_rate_mbps"`
	// NoiseNoFsync, when nonzero, skips the fsync after each noise write.
	NoiseNoFsync int `json:"dc_noise_no_fsync"`
}

var dcKnobsCodec = dynconfig.Typed[dcKnobs]{
	Validate: func(k dcKnobs) error {
		if k.ProbeUsers < 0 {
			return fmt.Errorf("dc_probe_users cannot be negative: %d", k.ProbeUsers)
		}
		switch k.NoiseMode {
		case "", noiseModeNone, noiseModeDisk, noiseModeSweperf:
		default:
			return fmt.Errorf("invalid dc_noise_mode %q: must be %q, %q or %q", k.NoiseMode, noiseModeNone, noiseModeDisk, noiseModeSweperf)
		}
		if k.NoiseWriteSize < 0 || k.NoiseWriteSize > math.MaxInt32 {
			return fmt.Errorf("dc_noise_write_size_bytes must be in [0, %d], got: %d", math.MaxInt32, k.NoiseWriteSize)
		}
		if k.NoiseFiles < 0 {
			return fmt.Errorf("dc_noise_files cannot be negative: %d", k.NoiseFiles)
		}
		if k.ProbeMinWait < 0 || k.ProbeMaxWait < 0 {
			return fmt.Errorf("dc_probe_{min,max}_wait_time cannot be negative: %v, %v", k.ProbeMinWait, k.ProbeMaxWait)
		}
		if k.NoiseRateMBps < 0 {
			return fmt.Errorf("dc_noise_rate_mbps cannot be negative: %f", k.NoiseRateMBps)
		}
		return nil
	},
}

// dcConfig is what a DiskContentionUser holder carries: the class's own
// knobs, and the payload they came from. Probe and sweperf VUs run other
// classes, which read their knobs from holders of their own types, so the
// runtime re-applies the payload to those holders (see syncViews).
type dcConfig struct {
	dcKnobs
	payload []byte
}

// String keeps the payload bytes out of the "config now in force" log.
func (c dcConfig) String() string { return fmt.Sprintf("%+v", c.dcKnobs) }

// dcCodec decodes a payload for DiskContentionUser. It refuses one that the
// probes' GluttonUser knobs or the sweperf noise knobs refuse, so a bad
// payload is refused at fetch time rather than half applied.
type dcCodec struct{}

func (dcCodec) Initial() any { return dcConfig{dcKnobs: dcKnobsCodec.Defaults} }

func (dcCodec) Decode(payload []byte) (any, error) {
	decoded, err := dcKnobsCodec.Decode(payload)
	if err != nil {
		return nil, err
	}
	k := decoded.(dcKnobs)
	glutton, err := gluttonCodec.Decode(payload)
	if err != nil {
		return nil, err
	}
	if lo, hi := probeWaitBounds(k, glutton.(gluttonKnobs).WaitTime); hi < lo {
		return nil, fmt.Errorf("probe wait window is inverted: max %v < min %v (dc_probe_min_wait_time and dc_probe_max_wait_time each fall back to min_wait_time and max_wait_time)", hi, lo)
	}
	if entry, ok := userclass.Lookup("sweperf"); ok && entry.Config != nil {
		if _, err := entry.Config.Decode(payload); err != nil {
			return nil, err
		}
	}
	return dcConfig{dcKnobs: k, payload: bytes.Clone(payload)}, nil
}

// probeWaitBounds is the probes' think time window: each dc_probe_* bound
// when set, else the matching GluttonUser bound.
func probeWaitBounds(k dcKnobs, w dynconfig.WaitTime) (lo, hi time.Duration) {
	lo, hi = w.MinWait.Duration(), w.MaxWait.Duration()
	if k.ProbeMinWait > 0 {
		lo = k.ProbeMinWait.Duration()
	}
	if k.ProbeMaxWait > 0 {
		hi = k.ProbeMaxWait.Duration()
	}
	return lo, hi
}

func init() {
	userclass.Add(userclass.Entry{
		Name:       "diskcontention",
		LocustFile: "diskcontention.py",
		UserClass:  dcUserClass,
		Config:     dcCodec{},
		Init:       initDiskContention,
	})
}

func initDiskContention(cfg *userclass.Config) (taskFn func(), shutdown func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/diskcontention")
	}
	runCtx, cancel := context.WithCancel(context.Background())
	rt := &dcRuntime{
		cfg:    cfg,
		runCtx: runCtx,
		cancel: cancel,
	}
	// Probes run the GluttonUser cycle, which reads gluttonKnobs, so they
	// get a config whose holder carries those.
	probeCfg := *cfg
	probeCfg.Dyn = dynconfig.NewHolder(gluttonCodec)
	rt.views = []*dynconfig.Holder{probeCfg.Dyn}
	rt.probe = &taskRuntime{
		cfg:           &probeCfg,
		metricPrefix:  dcProbePrefix,
		userClassName: dcUserClass,
		waitFn:        rt.probeWait,
		sleepFn:       rt.sleep,
	}
	if err := boomer.Events.Subscribe(boomer.EVENT_STOP, rt.onStop); err != nil {
		slog.Warn("diskcontention: roles will not reset after a boomer stop", slog.String("err", err.Error()))
	}
	if err := boomer.Events.Subscribe(boomer.EVENT_SPAWN, rt.onSpawn); err != nil {
		slog.Warn("diskcontention: roles will not reset after a boomer stop", slog.String("err", err.Error()))
	}
	return rt.iterate, rt.shutdown
}

type dcRole int

const (
	roleProbe dcRole = iota
	roleNoiseDisk
	roleNoiseSweperf
	roleIdle
)

func (r dcRole) String() string {
	switch r {
	case roleProbe:
		return "probe"
	case roleNoiseDisk:
		return "noise-disk"
	case roleNoiseSweperf:
		return "noise-sweperf"
	default:
		return "idle"
	}
}

type dcRuntime struct {
	cfg   *userclass.Config // its holder carries dcConfig
	probe *taskRuntime

	// views are the holders of the classes the VUs run, kept on the payload
	// in force by syncViews; synced is the payload they last got.
	syncMu sync.Mutex
	views  []*dynconfig.Holder
	synced []byte

	runCtx        context.Context
	cancel        context.CancelFunc
	mu            sync.Mutex
	stopping      bool
	noiseInflight sync.WaitGroup
	probeInflight sync.WaitGroup

	roles    sync.Map // goroutineID → dcRole
	nextSlot atomic.Int64
	// stopped is set by a boomer stop and cleared by the spawn after it,
	// which hands the roles out again (see onSpawn).
	stopped atomic.Bool

	noise sync.Map // goroutineID → *noiseActor

	// sleepFn, when set, replaces the sleep between noise calls; tests use
	// it to record the pauses instead of waiting them out.
	sleepFn func(time.Duration)

	sweperfOnce     sync.Once
	sweperfTask     func()
	sweperfShutdown func(context.Context)
	sweperfErr      error
}

// syncViews re-applies the payload in force to the views, once per payload.
func (r *dcRuntime) syncViews() {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	payload := dynconfig.Get[dcConfig](r.cfg.Dyn).payload
	if bytes.Equal(payload, r.synced) {
		return
	}
	for _, h := range r.views {
		if _, _, err := h.Apply(payload); err != nil {
			// dcCodec.Decode accepted this payload for every view's codec.
			slog.Warn("diskcontention: a sub-class refused the config", slog.String("err", err.Error()))
		}
	}
	r.synced = payload
}

// addView starts h on the payload in force and keeps it there from now on.
func (r *dcRuntime) addView(h *dynconfig.Holder) {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	if r.synced != nil {
		if _, _, err := h.Apply(r.synced); err != nil {
			slog.Warn("diskcontention: a sub-class refused the config", slog.String("err", err.Error()))
		}
	}
	r.views = append(r.views, h)
}

// onStop and onSpawn hand the roles out again after a boomer stop. The
// spawn after a stop starts a fresh set of VUs, which get slots from 0
// again, so the first ProbeUsers of them are probes. The reset waits for
// that spawn because boomer publishes the stop before it cancels the old
// VUs: an old VU starting one last iteration in between keeps its role
// instead of taking a fresh slot from the new run.
func (r *dcRuntime) onStop() { r.stopped.Store(true) }

// onSpawn receives boomer's spawn arguments, the user count and the spawn
// rate, and needs neither.
func (r *dcRuntime) onSpawn(int, float64) {
	if r.stopped.CompareAndSwap(true, false) {
		r.roles.Clear()
		r.nextSlot.Store(0)
	}
}

// roleFor gives each VU goroutine a role the first time it runs. Slots are
// handed out in spawn order, so the first ProbeUsers VUs are probes.
func (r *dcRuntime) roleFor(gid int64, dyn dcKnobs) dcRole {
	if v, ok := r.roles.Load(gid); ok {
		return v.(dcRole)
	}
	slot := r.nextSlot.Add(1) - 1
	role := pickRole(slot, dyn)
	actual, _ := r.roles.LoadOrStore(gid, role)
	slog.Info("diskcontention VU assigned",
		slog.Int64("slot", slot),
		slog.String("role", actual.(dcRole).String()))
	return actual.(dcRole)
}

// pickRole maps a VU's spawn slot to its role.
func pickRole(slot int64, dyn dcKnobs) dcRole {
	if slot < int64(dyn.ProbeUsers) {
		return roleProbe
	}
	switch dyn.NoiseMode {
	case noiseModeDisk:
		return roleNoiseDisk
	case noiseModeSweperf:
		return roleNoiseSweperf
	default:
		return roleIdle
	}
}

func (r *dcRuntime) enter(wg *sync.WaitGroup) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return false
	}
	wg.Add(1)
	return true
}

func (r *dcRuntime) isStopping() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopping
}

func waitGroupWithContext(ctx context.Context, wg *sync.WaitGroup) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (r *dcRuntime) iterate() {
	if r.isStopping() {
		return
	}

	gid := boomerutil.GoroutineID()
	r.syncViews()
	dyn := dynconfig.Get[dcConfig](r.cfg.Dyn).dcKnobs
	switch r.roleFor(gid, dyn) {
	case roleProbe:
		if !r.enter(&r.probeInflight) {
			return
		}
		defer r.probeInflight.Done()
		r.probe.iterate()
	case roleNoiseDisk:
		if !r.enter(&r.noiseInflight) {
			return
		}
		defer r.noiseInflight.Done()
		r.iterateDiskNoise(gid, dyn)
	case roleNoiseSweperf:
		r.iterateSweperfNoise()
	default:
		r.sleep(time.Second)
	}
}

func (r *dcRuntime) runContext() context.Context {
	if r.runCtx != nil {
		return r.runCtx
	}
	return context.Background()
}

func (r *dcRuntime) sleep(d time.Duration) {
	if d <= 0 {
		return
	}
	if r.sleepFn != nil {
		r.sleepFn(d)
		return
	}
	if r.runCtx == nil {
		time.Sleep(d)
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-r.runCtx.Done():
	}
}

func (r *dcRuntime) iterateSweperfNoise() {
	r.sweperfOnce.Do(func() {
		entry, ok := userclass.Lookup("sweperf")
		if !ok {
			r.sweperfErr = fmt.Errorf("sweperf user class is not registered in this binary")
			return
		}
		// SweperfUser reads its own knobs type, from a holder of its own.
		sweperfCfg := *r.cfg
		sweperfCfg.Dyn = dynconfig.NewHolder(entry.Config)
		r.addView(sweperfCfg.Dyn)
		r.sweperfTask, r.sweperfShutdown = entry.Init(&sweperfCfg)
	})
	if r.sweperfErr != nil {
		bmetrics.RecordFailure("actor", "NoiseSweperfInit", dcUserClass, 0, r.sweperfErr.Error())
		r.sleep(noiseErrorBackoff)
		return
	}
	r.sweperfTask()
}

func (r *dcRuntime) iterateDiskNoise(gid int64, dyn dcKnobs) {
	// Noise actors reuse gluttonActor, so they take the probes' config,
	// whose holder carries gluttonKnobs.
	v, ok := r.noise.Load(gid)
	if !ok {
		v, _ = r.noise.LoadOrStore(gid, newNoiseActor(r.probe.cfg))
	}
	n := v.(*noiseActor)
	ctx := r.runContext()

	if !n.running {
		if err := n.start(ctx, dyn); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("diskcontention noise actor start failed; retrying",
				slog.String("actor", n.ga.actorName), slog.String("err", err.Error()))
			// The probes' rule: replace an actor that is gone at once, keep
			// one through cluster-wide errors, and replace one that keeps
			// failing otherwise.
			if n.ga.noteFailure(err) {
				n.reset(ctx)
			}
			r.sleep(noiseErrorBackoff)
			return
		}
		n.ga.noteSuccess()
	}

	start := time.Now()
	size, err := n.write(ctx, dyn)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		// A WriteDisk error is not a gRPC status, so every one counts
		// toward replacing the actor.
		if n.ga.noteFailure(err) {
			n.reset(ctx)
		}
		r.sleep(noiseErrorBackoff)
		return
	}
	n.ga.noteSuccess()
	if pause := noisePause(size, dyn.NoiseRateMBps, time.Since(start)); pause > 0 {
		r.sleep(pause)
	}
}

// noisePause is how long to wait after writing size bytes in elapsed so the
// actor averages rateMBps. Zero rate means no cap.
func noisePause(size int64, rateMBps float64, elapsed time.Duration) time.Duration {
	if rateMBps <= 0 || size <= 0 {
		return 0
	}
	target := time.Duration(float64(size) / (rateMBps * 1e6) * float64(time.Second))
	return target - elapsed
}

func (r *dcRuntime) shutdown(ctx context.Context) {
	r.mu.Lock()
	r.stopping = true
	r.mu.Unlock()
	if r.cancel != nil {
		r.cancel()
	}

	// Stop and delete noise actors first so they do not contend with probe
	// actors suspending during teardown.
	waitGroupWithContext(ctx, &r.noiseInflight)
	r.noise.Range(func(_, v any) bool {
		v.(*noiseActor).reset(ctx)
		return true
	})
	r.sweperfOnce.Do(func() {})
	if r.sweperfShutdown != nil {
		r.sweperfShutdown(ctx)
	}
	waitGroupWithContext(ctx, &r.probeInflight)
	r.probe.shutdown(ctx)
}

// noiseActor is one disk-noise actor. It reuses gluttonActor for the RPC
// plumbing (tracedCall, postProto, delete) and its failure count, but has
// its own template and never suspends.
type noiseActor struct {
	ga      *gluttonActor
	running bool
	created bool
	writes  int
}

func newNoiseActor(cfg *userclass.Config) *noiseActor {
	return &noiseActor{
		ga: &gluttonActor{
			cfg:           cfg,
			actorName:     "sb-noise-" + uuid.NewString(),
			userClassName: dcUserClass,
		},
	}
}

func (n *noiseActor) start(ctx context.Context, dyn dcKnobs) error {
	tmpl := dyn.NoiseTemplate
	if tmpl == "" {
		tmpl = defaultNoiseTemplate
	}
	if !n.created {
		createCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), noiseCreateTimeout)
		defer cancel()
		if err := n.ga.ensureAtespace(createCtx); err != nil {
			return err
		}
		err := n.ga.tracedCall(createCtx, "NoiseCreateActor", func(callCtx context.Context, tr *metadata.MD) error {
			_, err := n.ga.cfg.APIStub.CreateActor(callCtx, &ateapipb.CreateActorRequest{
				Actor: &ateapipb.Actor{
					Metadata:      &ateapipb.ResourceMetadata{Atespace: n.ga.cfg.Atespace, Name: n.ga.actorName},
					ActorTemplate: &ateapipb.ObjectRef{Atespace: templateAtespace, Name: tmpl},
				},
			}, grpc.Trailer(tr))
			return err
		})
		if err != nil {
			return err
		}
		n.created = true
		bmetrics.UpdateUsers(dcUserClass, 1)
	}
	err := n.ga.tracedCall(ctx, "NoiseResumeActor", func(callCtx context.Context, tr *metadata.MD) error {
		return boomerutil.RetryOnConflict(callCtx, func() error {
			_, err := n.ga.cfg.APIStub.ResumeActor(callCtx, &ateapipb.ResumeActorRequest{Actor: n.ga.ref()}, grpc.Trailer(tr))
			return err
		})
	})
	if err != nil {
		return err
	}
	n.running = true
	return nil
}

// write issues one WriteDisk to the actor and records it as a
// NoiseWriteDisk row whose response bytes are the bytes written, so the
// stats give the noise throughput the actors achieved.
//
// WRITE_MODE_OVERWRITE is used instead of WRITE_MODE_TRUNCATE so that in
// no-fsync attribution runs (--dc-noise-no-fsync) reopening a rotating file
// does not call O_TRUNC (truncate_inode_pages), which would discard dirty
// page-cache pages in RAM before kernel writeback flushes them to disk.
func (n *noiseActor) write(ctx context.Context, dyn dcKnobs) (int64, error) {
	size := dyn.NoiseWriteSize
	if size <= 0 {
		size = defaultNoiseWriteSize
	}
	files := dyn.NoiseFiles
	if files <= 0 {
		files = defaultNoiseFiles
	}
	req := &gluttonpb.WriteDiskRequest{
		Key:       fmt.Sprintf("noise-%d", n.writes%files),
		Size:      int32(size),
		WriteMode: gluttonpb.WriteMode_WRITE_MODE_OVERWRITE,
		Fsync:     dyn.NoiseNoFsync == 0,
	}
	n.writes++
	ctx, span := n.ga.cfg.Tracer.Start(ctx, "NoiseWriteDisk")
	defer span.End()
	start := time.Now()
	resp := &gluttonpb.WriteDiskResponse{}
	err := n.ga.postProto(ctx, writeDiskRoute, req, resp)
	latency := time.Since(start)
	// OVERWRITE reports the size of the whole file, which can exceed this
	// write after the write size is lowered mid-run.
	if err == nil && resp.GetSize() < size {
		err = fmt.Errorf("WriteDisk short write: got %d, want at least %d", resp.GetSize(), size)
	}
	if err != nil && errors.Is(ctx.Err(), context.Canceled) {
		return 0, err
	}
	boomerutil.LogSampledTrace(span, "NoiseWriteDisk", latency, boomerutil.SourceClient, err)
	if err != nil {
		bmetrics.RecordFailure("http", "NoiseWriteDisk", dcUserClass, latency, err.Error())
		return 0, err
	}
	bmetrics.RecordSuccess("http", "NoiseWriteDisk", dcUserClass, latency, size)
	return size, nil
}

// reset deletes the actor in any state (a noise actor's data is throwaway,
// so there is no reason to pay for a suspend) and starts the slot over with
// a fresh name.
func (n *noiseActor) reset(ctx context.Context) {
	if n.created {
		n.ga.delete(ctx)
		bmetrics.UpdateUsers(dcUserClass, -1)
	}
	cfg := n.ga.cfg
	*n = *newNoiseActor(cfg)
}

// probeWait is the probes' think time, drawn from probeWaitBounds.
func (r *dcRuntime) probeWait() time.Duration {
	lo, hi := probeWaitBounds(dynconfig.Get[dcConfig](r.cfg.Dyn).dcKnobs, dynconfig.Get[gluttonKnobs](r.probe.cfg.Dyn).WaitTime)
	return dynconfig.Uniform(lo, hi)
}
