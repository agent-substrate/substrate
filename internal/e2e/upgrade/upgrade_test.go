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

// Package upgrade is the rolling upgrade suite. It takes a cluster that runs
// the previous release with actors on it and upgrades it to this checkout in
// the order of docs/upgrade.md: podcertificate-controller, atelet, the
// WorkerPool's workerImage, the API server, ate-controller, atenet, the
// SandboxConfig, then the rest of the system bundle. Every step except the
// workerImage edit is one ate-setup command. The suite stands in for the
// actors' harness and suspends an actor as soon as its worker pod starts
// terminating. It passes when no actor is CRASHED at any step and every actor
// resumes on a new worker with its memory.
//
// The suite upgrades the cluster it runs on, so it lives outside
// internal/e2e/suites and the default e2e run leaves it out. The actors run on
// the counter demo the previous release installed (go run ./cmd/ate-setup
// --kind deploy demo counter, from that checkout), or on counter-microvm with
// E2E_SANDBOX_CLASS=microvm. From this checkout:
//
//	hack/run-e2e-kind.sh ./internal/e2e/upgrade -timeout 60m
//
// ate-setup runs with --config E2E_ATE_SETUP_CONFIG, the previous release's
// install record, as the runbook does. Without it, ate-setup runs with --kind,
// or with E2E_ATE_SETUP_FLAGS for another cluster, and the test's kube context.
// A record of pre-built images has to name the new tag, as the runbook's
// new.yaml does, and the pool rolls to the worker image at that tag.
// .github/workflows/upgrade-e2e.yaml runs the suite for every push to a
// release branch, against the newest release on that branch.
package upgrade

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/google"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"

	"go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/portforward"
	"go.ate.dev/substrate/internal/resources"
)

const (
	// sigtermSleepSeconds is how long the counter keeps running after
	// SIGTERM: a day, so a snapshot taken after SIGTERM restores with most
	// of a day left before the exit fires. The eviction contract asks the
	// same of an actor: stay up until suspended.
	sigtermSleepSeconds = 86400

	pollInterval = time.Second
	stateTimeout = 2 * time.Minute
	rollTimeout  = 15 * time.Minute
)

// The upgrade steps, in order. The pool roll sits between atelet and the API
// server; the others are ate-setup subcommands.
const (
	stepPodcert       = "deploy podcertificate-controller"
	stepAtelet        = "deploy atelet --rollout-timeout=1h"
	stepPoolRoll      = "edit workerImage"
	stepAPIServer     = "deploy apiserver"
	stepController    = "deploy ate-controller"
	stepAtenet        = "deploy atenet"
	stepSandboxConfig = "deploy sandboxconfig"
	stepSystem        = "deploy ate-system --setup-csi=none"
)

// podcertNamespace is where podcertificate-controller runs.
const podcertNamespace = "podcertificate-controller-system"

// counterResponse matches the counter demo's reply. Every call bumps both
// counters, and the template's snapshots carry both over across every resume.
var counterResponse = regexp.MustCompile(`preserved memory count: (\d+) \| preserved file counter: (\d+)`)

// startState is how seedActor leaves an actor ahead of the upgrade. A paused
// actor is suspended asynchronously: SUSPENDED, with its snapshot on the node
// until atelet uploads it in the background. A suspended one waits for the
// upload.
type startState string

const (
	startRunning   startState = "running"
	startPaused    startState = "paused"
	startSuspended startState = "suspended"
)

// actor is one of the suite's counter actors and what it expects of it.
type actor struct {
	atespace, name string
	// before is how the actor is left ahead of the upgrade.
	before startState
	// calls is the counter value the actor last reported.
	calls int
}

func (a *actor) ref() *ateapi.ObjectRef {
	return &ateapi.ObjectRef{Atespace: a.atespace, Name: a.name}
}

func (a *actor) routerRef() resources.ActorRef {
	return resources.ActorRef{Atespace: a.atespace, Name: a.name}
}

func TestRollingUpgrade(t *testing.T) {
	ctx := context.Background()
	suffix := strconv.FormatUint(uint64(rand.Uint32()), 36)
	api := &api{c: e2e.GetClients()}
	t.Cleanup(api.close)
	k8s := e2e.GetClients().K8s
	fixture := e2e.SubstrateCounterFixture()
	pool := types.NamespacedName{Namespace: fixture.PoolNamespace, Name: fixture.PoolName}

	installed := describeInstall(ctx, t, api.get(), fixture)
	t.Logf("installed: atelet %s, %d %s workers on %s", installed.atelet, installed.workers, installed.sandboxClass, installed.workerImage)
	if installed.workers < 2 {
		t.Fatalf("WorkerPool %s has %d workers, want at least 2", pool, installed.workers)
	}

	newActor := func(name string, before startState) *actor {
		return &actor{name: name + "-" + suffix, atespace: fixture.Atespace, before: before}
	}
	// Every worker but one runs an actor, so the suspended actor can resume
	// onto an old worker after the atelet step.
	var running []*actor
	for i := range installed.workers - 1 {
		running = append(running, newActor(fmt.Sprintf("running-%d", i), startRunning))
	}
	suspended := newActor("suspended", startSuspended)
	actors := append(slices.Clone(running), newActor("paused", startPaused), suspended)
	router := newRouter(ctx, t)
	for _, a := range actors {
		createActor(ctx, t, api, fixture, a)
		seedActor(ctx, t, api, router, a)
	}

	harness := startHarness(t, api, k8s, actors)
	defer harness.stop()

	runStep(ctx, t, api, harness, actors, pool, stepPodcert)
	runStep(ctx, t, api, harness, actors, pool, stepAtelet)

	// The new atelet serves the old workers: the actors that kept running
	// answer, and a suspended one resumes onto the spare old worker.
	router = newRouter(ctx, t)
	for _, a := range running {
		if got := state(ctx, t, api, a); got != ateapi.ActorState_ACTOR_STATE_RUNNING {
			t.Fatalf("actor %q is %v after %s, want it still running", a.name, got, stepAtelet)
		}
		callCounter(ctx, t, router, a, "after "+stepAtelet)
	}
	resumeActor(ctx, t, api, router, suspended)
	callCounter(ctx, t, router, suspended, "after "+stepAtelet)

	// These run on old workers, so the pool roll has to evict them.
	onOldWorkers := append(slices.Clone(running), suspended)
	var newImage string
	ok := t.Run(stepPoolRoll, func(t *testing.T) {
		harness.setStep(stepPoolRoll)
		newImage = newWorkerImage(ctx, t, installed.sandboxClass)
		if newImage == installed.workerImage {
			t.Fatalf("the new worker image %s is the installed one; nothing to upgrade", newImage)
		}
		rollPool(ctx, t, api.get(), fixture.PoolNamespace, fixture.PoolName, newImage)
		// Every old worker is gone, so the harness has suspended these.
		for _, a := range onOldWorkers {
			waitForState(ctx, t, api, a, ateapi.ActorState_ACTOR_STATE_SUSPENDED, stateTimeout)
		}
		assertNoneCrashed(ctx, t, api, actors, stepPoolRoll)
	})
	if !ok {
		t.FailNow()
	}

	// One actor runs through the control plane steps: the old API server
	// places it on a new worker and the new one has to suspend it later.
	router = newRouter(ctx, t)
	resumeActor(ctx, t, api, router, running[0])

	runStep(ctx, t, api, harness, actors, pool, stepAPIServer)
	templates := workerTemplates(ctx, t, k8s, pool)
	runStep(ctx, t, api, harness, actors, pool, stepController)
	// The worker pod template stays the same within a major, so the new
	// ate-controller must not roll the pool. It would start only once the
	// controller reconciles, so give it a minute.
	waitForPoolRoll(ctx, t, api.get(), fixture.PoolNamespace, fixture.PoolName, newImage, time.Minute)
	if got := workerTemplates(ctx, t, k8s, pool); got != templates {
		t.Fatalf("%s rolled the pool: pod templates %s, then %s", stepController, templates, got)
	}
	runStep(ctx, t, api, harness, actors, pool, stepAtenet)
	runStep(ctx, t, api, harness, actors, pool, stepSandboxConfig)
	runStep(ctx, t, api, harness, actors, pool, stepSystem)
	harness.stop()

	for _, a := range actors {
		evictions := harness.evictions(a)
		t.Logf("actor %q: before %v, evicted during %q", a.name, a.before, evictions)
		var want []string
		for _, old := range onOldWorkers {
			if old == a {
				want = []string{stepPoolRoll}
			}
		}
		// Only the pool roll may evict an actor.
		if strings.Join(evictions, ",") != strings.Join(want, ",") {
			t.Errorf("actor %q was evicted during %q, want %q", a.name, evictions, want)
		}
	}

	// Every actor resumes on a new worker with its memory, and so does one
	// created after the upgrade from the old golden snapshot.
	fresh := newActor("fresh", startSuspended)
	createActor(ctx, t, api, fixture, fresh)
	router = newRouter(ctx, t)
	for _, a := range append(actors, fresh) {
		verifyActor(ctx, t, api, router, k8s, a, newImage)
	}
	// Once more, so a snapshot the new build wrote restores on the new build.
	verifyActor(ctx, t, api, router, k8s, running[0], newImage)
}

// runStep runs one ate-setup step and checks the actors afterwards. No step may
// roll the workers. ate-controller would roll them only once it reconciles, so
// its caller checks that step.
func runStep(ctx context.Context, t *testing.T, api *api, h *harness, actors []*actor, pool types.NamespacedName, step string) {
	t.Helper()
	ok := t.Run(step, func(t *testing.T) {
		h.setStep(step)
		deployments := stepDeployments(step)
		before := make([]string, len(deployments))
		for i, d := range deployments {
			before[i] = deploymentImages(ctx, t, h.k8s, d)
		}
		var atelet string
		if step == stepAtelet {
			atelet = daemonSetImage(ctx, t, h.k8s, "atelet")
		}
		templates := workerTemplates(ctx, t, h.k8s, pool)
		ateSetup(t, strings.Fields(step)...)
		// deploy ate-system reapplies ate-api-server too.
		if step == stepAPIServer || step == stepSystem {
			api.reconnect(ctx, t)
		}
		for i, d := range deployments {
			if got := deploymentImages(ctx, t, h.k8s, d); got == before[i] {
				t.Errorf("Deployment %s still runs %s after %s", d, got, step)
			}
		}
		if step == stepAtelet {
			if got := daemonSetImage(ctx, t, h.k8s, "atelet"); got == atelet {
				t.Errorf("atelet still runs %s after %s", got, step)
			}
		}
		if step != stepController {
			if got := workerTemplates(ctx, t, h.k8s, pool); got != templates {
				t.Errorf("%s rolled the workers: pod templates %s, then %s", step, templates, got)
			}
		}
		assertNoneCrashed(ctx, t, api, actors, step)
	})
	if !ok {
		t.FailNow()
	}
}

// createActor creates a from the fixture's template, SUSPENDED, and deletes
// it when the test ends.
func createActor(ctx context.Context, t *testing.T, api *api, fixture e2e.SubstrateFixture, a *actor) {
	t.Helper()
	t.Logf("Creating Actor %q...", a.name)
	if _, err := api.get().SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: a.atespace, Name: a.name},
		ActorTemplate: &ateapi.ObjectRef{Atespace: fixture.Atespace, Name: fixture.Name},
	}}); err != nil {
		t.Fatalf("failed to create Actor %q: %v", a.name, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, _ = api.get().SubstrateAPI.DeleteActor(cleanupCtx, &ateapi.DeleteActorRequest{Actor: a.ref()})
	})
	waitForState(ctx, t, api, a, ateapi.ActorState_ACTOR_STATE_SUSPENDED, stateTimeout)
}

// seedActor runs a once and leaves it in its before state.
func seedActor(ctx context.Context, t *testing.T, api *api, router *e2e.RouterClient, a *actor) {
	t.Helper()
	resumeActor(ctx, t, api, router, a)
	callCounter(ctx, t, router, a, "before upgrade")
	switch a.before {
	case startRunning:
	case startPaused:
		suspendActor(ctx, t, api, a, ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS)
	case startSuspended:
		suspendActor(ctx, t, api, a, ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS)
	default:
		t.Fatalf("actor %q: unsupported starting state %v", a.name, a.before)
	}
}

// verifyActor has a running on a worker with image, checks the counter
// carried over, and suspends it again to free the worker for the next one.
func verifyActor(ctx context.Context, t *testing.T, api *api, router *e2e.RouterClient, k8s kubernetes.Interface, a *actor, image string) {
	t.Helper()
	running := resumeActor(ctx, t, api, router, a)
	wa := running.GetStatus().GetWorkerAssignment()
	pod, err := k8s.CoreV1().Pods(wa.GetWorkerNamespace()).Get(ctx, wa.GetWorkerPod(), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("actor %q: failed to get its worker pod %s/%s: %v", a.name, wa.GetWorkerNamespace(), wa.GetWorkerPod(), err)
	}
	if !podUsesImage(pod, image) {
		t.Errorf("actor %q resumed on worker pod %s, which does not run %s", a.name, pod.Name, image)
	}
	callCounter(ctx, t, router, a, "after upgrade")
	suspendActor(ctx, t, api, a, ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS)
}

// resumeActor brings a to RUNNING, or returns it as is when it already is.
func resumeActor(ctx context.Context, t *testing.T, api *api, router *e2e.RouterClient, a *actor) *ateapi.Actor {
	t.Helper()
	if act := getActor(ctx, t, api, a); act.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_RUNNING {
		return act
	}
	t.Logf("Resuming Actor %q...", a.name)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, api.get(), &ateapi.ResumeActorRequest{Actor: a.ref()}); err != nil {
		t.Fatalf("failed to resume Actor %q: %v", a.name, err)
	}
	act := waitForState(ctx, t, api, a, ateapi.ActorState_ACTOR_STATE_RUNNING, stateTimeout)
	// Re-sent after every resume; a restore that fell back to a cold boot
	// would otherwise start at the 15 s default.
	body := routerCallRetry(ctx, t, router, a, http.MethodGet, fmt.Sprintf("/set-sigterm-sleep?duration=%d", sigtermSleepSeconds))
	t.Logf("actor %q: %s", a.name, strings.TrimSpace(body))
	return act
}

func suspendActor(ctx context.Context, t *testing.T, api *api, a *actor, mode ateapi.DurabilityMode) {
	t.Helper()
	t.Logf("Suspending Actor %q (%v)...", a.name, mode)
	req := &ateapi.SuspendActorRequest{Actor: a.ref(), Options: &ateapi.SuspendOptions{Mode: mode}}
	if _, err := api.get().SubstrateAPI.SuspendActor(ctx, req); err != nil {
		t.Fatalf("failed to suspend Actor %q: %v", a.name, err)
	}
	waitForState(ctx, t, api, a, ateapi.ActorState_ACTOR_STATE_SUSPENDED, stateTimeout)
}

func getActor(ctx context.Context, t *testing.T, api *api, a *actor) *ateapi.Actor {
	t.Helper()
	act, err := api.get().SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{Actor: a.ref()})
	if err != nil {
		t.Fatalf("failed to get Actor %q: %v", a.name, err)
	}
	return act
}

func state(ctx context.Context, t *testing.T, api *api, a *actor) ateapi.ActorState {
	t.Helper()
	return getActor(ctx, t, api, a).GetStatus().GetState()
}

// waitForState polls until a reaches want. A CRASHED actor fails the test at
// once: there is no way back from it.
func waitForState(ctx context.Context, t *testing.T, api *api, a *actor, want ateapi.ActorState, timeout time.Duration) *ateapi.Actor {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last ateapi.ActorState
	for time.Now().Before(deadline) {
		act, err := api.get().SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{Actor: a.ref()})
		if err == nil {
			last = act.GetStatus().GetState()
			if last == want {
				return act
			}
			if last == ateapi.ActorState_ACTOR_STATE_CRASHED {
				t.Fatalf("actor %q is CRASHED (wanted %v)", a.name, want)
			}
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out after %v waiting for actor %q to be %v (last %v)", timeout, a.name, want, last)
	return nil
}

func assertNoneCrashed(ctx context.Context, t *testing.T, api *api, actors []*actor, step string) {
	t.Helper()
	for _, a := range actors {
		got := state(ctx, t, api, a)
		if got == ateapi.ActorState_ACTOR_STATE_CRASHED {
			t.Fatalf("%s: actor %q is CRASHED", step, a.name)
		}
		t.Logf("%s: actor %q is %v", step, a.name, got)
	}
}

// callCounter bumps a's counter through the router and checks both counters
// continued from the last call. A retried request may already have counted
// before its response was lost, so the value may run ahead by the retries.
func callCounter(ctx context.Context, t *testing.T, router *e2e.RouterClient, a *actor, stage string) {
	t.Helper()
	body, attempts := routerCallAttempts(ctx, t, router, a, http.MethodPost, "/")
	m := counterResponse.FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("[%s] actor %q: unexpected response: %s", stage, a.name, body)
	}
	memory, _ := strconv.Atoi(m[1])
	file, _ := strconv.Atoi(m[2])
	if memory != file || memory <= a.calls || memory > a.calls+attempts {
		t.Errorf("[%s] actor %q: counters %d/%d after %d calls and %d attempts: %s", stage, a.name, memory, file, a.calls, attempts, strings.TrimSpace(body))
	}
	a.calls = memory
}

// routerCallRetry calls a through the router, retrying for a minute: the
// router can answer 503 for a while after a resume.
func routerCallRetry(ctx context.Context, t *testing.T, router *e2e.RouterClient, a *actor, method, path string) string {
	t.Helper()
	body, _ := routerCallAttempts(ctx, t, router, a, method, path)
	return body
}

func routerCallAttempts(ctx context.Context, t *testing.T, router *e2e.RouterClient, a *actor, method, path string) (body string, attempts int) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	var lastErr error
	for attempts = 1; time.Now().Before(deadline); attempts++ {
		body, lastErr = routerCall(ctx, router, a, method, path)
		if lastErr == nil {
			return body, attempts
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("actor %q: %s %s did not answer: %v", a.name, method, path, lastErr)
	return "", attempts
}

func routerCall(ctx context.Context, router *e2e.RouterClient, a *actor, method, path string) (string, error) {
	var resp *http.Response
	var err error
	if method == http.MethodPost {
		resp, err = router.PostJSON(ctx, a.routerRef(), path, nil)
	} else {
		resp, err = router.Get(ctx, a.routerRef(), path)
	}
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	return string(body), nil
}

func newRouter(ctx context.Context, t *testing.T) *e2e.RouterClient {
	t.Helper()
	router, err := e2e.NewRouterClient(t, ctx)
	if err != nil {
		t.Fatalf("failed to connect to atenet-router: %v", err)
	}
	t.Cleanup(router.Close)
	return router
}

// api holds the ate API client and rebuilds it after the API server rolls,
// which takes the port-forward the client rides on with it.
type api struct {
	mu    sync.Mutex
	c     *e2e.Clients
	owned bool
}

func (a *api) get() *e2e.Clients {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.c
}

func (a *api) reconnect(ctx context.Context, t *testing.T) {
	t.Helper()
	fresh, err := e2e.NewClients(ctx)
	if err != nil {
		t.Fatalf("failed to reconnect to the API server: %v", err)
	}
	a.mu.Lock()
	old, owned := a.c, a.owned
	a.c, a.owned = fresh, true
	a.mu.Unlock()
	if owned {
		old.Close()
	}
}

// close releases the client reconnect made; the shared one belongs to
// RunTestMain.
func (a *api) close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.owned {
		a.c.Close()
		a.owned = false
	}
}

// harness stands in for the actors' operator during the upgrade. It watches
// the RUNNING actors' worker pods and suspends an actor as soon as its pod
// starts terminating, which is what the eviction path asks of every actor,
// and records which upgrade step evicted whom.
type harness struct {
	t      *testing.T
	api    *api
	k8s    kubernetes.Interface
	actors []*actor

	mu      sync.Mutex
	step    string
	evicted map[*actor][]string
	handled map[string]bool

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func startHarness(t *testing.T, api *api, k8s kubernetes.Interface, actors []*actor) *harness {
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{
		t:       t,
		api:     api,
		k8s:     k8s,
		actors:  actors,
		evicted: make(map[*actor][]string),
		handled: make(map[string]bool),
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	go h.run(ctx)
	return h
}

func (h *harness) setStep(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.step = name
}

// evictions lists the steps during which a's worker pod was terminated.
func (h *harness) evictions(a *actor) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.evicted[a]
}

func (h *harness) stop() {
	h.once.Do(func() {
		h.cancel()
		<-h.done
	})
}

func (h *harness) run(ctx context.Context) {
	defer close(h.done)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, a := range h.actors {
			h.check(ctx, a)
		}
	}
}

// check suspends a if its worker pod is terminating. Errors are left for the
// next tick: the API server is rolling for part of the upgrade, and the actor
// keeps running for a day after SIGTERM.
func (h *harness) check(ctx context.Context, a *actor) {
	clients := h.api.get()
	act, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{Actor: a.ref()})
	if err != nil || act.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING {
		return
	}
	wa := act.GetStatus().GetWorkerAssignment()
	if wa.GetWorkerPod() == "" {
		return
	}
	pod, err := h.k8s.CoreV1().Pods(wa.GetWorkerNamespace()).Get(ctx, wa.GetWorkerPod(), metav1.GetOptions{})
	if err != nil || pod.DeletionTimestamp == nil {
		return
	}
	h.mu.Lock()
	key := a.name + "/" + pod.Name
	step, handled := h.step, h.handled[key]
	h.mu.Unlock()
	if handled {
		return
	}
	h.t.Logf("harness: worker pod %s of actor %q is terminating during %q; suspending", pod.Name, a.name, step)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: a.ref()}); err != nil {
		h.t.Logf("harness: failed to suspend actor %q: %v", a.name, err)
		return
	}
	h.mu.Lock()
	h.handled[key] = true
	h.evicted[a] = append(h.evicted[a], step)
	h.mu.Unlock()
}

// install is what the cluster ran before the upgrade.
type install struct {
	atelet       string
	workerImage  string
	sandboxClass atev1.SandboxClass
	workers      int
}

func describeInstall(ctx context.Context, t *testing.T, clients *e2e.Clients, fixture e2e.SubstrateFixture) install {
	t.Helper()
	dss, err := clients.K8s.AppsV1().DaemonSets(e2e.SystemNamespace()).List(ctx, metav1.ListOptions{LabelSelector: "app=atelet"})
	if err != nil || len(dss.Items) != 1 {
		t.Fatalf("expected one atelet DaemonSet, found %d (err %v)", len(dss.Items), err)
	}
	pool, err := clients.SubstrateK8s.AteV1().WorkerPools(fixture.PoolNamespace).Get(ctx, fixture.PoolName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get WorkerPool %s/%s (deploy with: %s): %v", fixture.PoolNamespace, fixture.PoolName, fixture.DeployWith, err)
	}
	return install{
		atelet:       dss.Items[0].Spec.Template.Spec.Containers[0].Image,
		workerImage:  pool.Spec.WorkerImage,
		sandboxClass: pool.Spec.DefaultSandboxClass(),
		workers:      int(pool.Spec.Replicas),
	}
}

func daemonSetImage(ctx context.Context, t *testing.T, k8s kubernetes.Interface, app string) string {
	t.Helper()
	dss, err := k8s.AppsV1().DaemonSets(e2e.SystemNamespace()).List(ctx, metav1.ListOptions{LabelSelector: "app=" + app})
	if err != nil || len(dss.Items) != 1 {
		t.Fatalf("expected one %s DaemonSet, found %d (err %v)", app, len(dss.Items), err)
	}
	return dss.Items[0].Spec.Template.Spec.Containers[0].Image
}

// stepDeployments are the Deployments a step has to move to this checkout's
// images. atelet is a DaemonSet, which runStep checks on its own.
func stepDeployments(step string) []types.NamespacedName {
	system := e2e.SystemNamespace()
	switch step {
	case stepPodcert:
		return []types.NamespacedName{{Namespace: podcertNamespace, Name: "podcertificate-controller"}}
	case stepAPIServer:
		return []types.NamespacedName{{Namespace: system, Name: e2e.ResourceName("ate-api-server")}}
	case stepController:
		return []types.NamespacedName{{Namespace: system, Name: e2e.ResourceName("ate-controller")}}
	case stepAtenet:
		return []types.NamespacedName{
			{Namespace: system, Name: e2e.ResourceName("atenet-router")},
			{Namespace: system, Name: e2e.ResourceName("atenet-egress")},
		}
	}
	return nil
}

// deploymentImages lists the images in a Deployment's pod template.
func deploymentImages(ctx context.Context, t *testing.T, k8s kubernetes.Interface, d types.NamespacedName) string {
	t.Helper()
	dep, err := k8s.AppsV1().Deployments(d.Namespace).Get(ctx, d.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get Deployment %s: %v", d, err)
	}
	var images []string
	for _, c := range dep.Spec.Template.Spec.Containers {
		images = append(images, c.Image)
	}
	return strings.Join(images, ",")
}

func podUsesImage(pod *corev1.Pod, image string) bool {
	for _, c := range pod.Spec.Containers {
		if c.Image == image {
			return true
		}
	}
	return false
}

// ateSetup runs this checkout's ate-setup.
func ateSetup(t *testing.T, args ...string) {
	t.Helper()
	name, cmd := ateSetupCommand(t, args...)
	e2e.RunCmd(t, name, cmd...)
}

func ateSetupCommand(t *testing.T, args ...string) (name string, cmd []string) {
	t.Helper()
	root, err := e2e.FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}
	cmd = []string{"run", filepath.Join(root, "cmd", "ate-setup")}
	switch {
	case os.Getenv("E2E_ATE_SETUP_CONFIG") != "":
		cmd = append(cmd, "--config="+os.Getenv("E2E_ATE_SETUP_CONFIG"))
	case os.Getenv("E2E_ATE_SETUP_FLAGS") != "":
		cmd = append(cmd, strings.Fields(os.Getenv("E2E_ATE_SETUP_FLAGS"))...)
	default:
		cmd = append(cmd, "--kind")
	}
	if e2e.KubeContext != "" {
		cmd = append(cmd, "--context="+e2e.KubeContext)
	}
	// The 60 s default leaves little room on a loaded CI runner. The atelet
	// step passes the runbook's timeout, which comes later and wins.
	cmd = append(cmd, "--rollout-timeout=5m")
	cmd = append(cmd, args...)
	return "go", cmd
}

// newWorkerImage returns the worker image for class to roll the pool to. With
// pre-built images it is the one at the record's tag, pinned by digest as the
// runbook does. From source, this checkout builds and pushes it.
func newWorkerImage(ctx context.Context, t *testing.T, class atev1.SandboxClass) string {
	t.Helper()
	repo, tag := prebuiltImages(t)
	if repo == "" {
		return publishWorkerImage(t, class)
	}
	ref := repo + "/ateworker-" + string(class) + ":" + tag
	parsed, err := name.ParseReference(ref)
	if err != nil {
		t.Fatalf("%s is not a valid image reference: %v", ref, err)
	}
	desc, err := remote.Head(parsed, remote.WithContext(ctx),
		remote.WithAuthFromKeychain(authn.NewMultiKeychain(authn.DefaultKeychain, google.Keychain)))
	if err != nil {
		t.Fatalf("failed to resolve %s to a digest: %v", ref, err)
	}
	return ref + "@" + desc.Digest.String()
}

// prebuiltImages returns the image repository and tag of E2E_ATE_SETUP_CONFIG,
// or empty strings when the install builds from source.
func prebuiltImages(t *testing.T) (repo, tag string) {
	t.Helper()
	path := os.Getenv("E2E_ATE_SETUP_CONFIG")
	if path == "" {
		return "", ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read the install record: %v", err)
	}
	var record struct {
		Images struct {
			Repo string `json:"repo"`
			Tag  string `json:"tag"`
		} `json:"images"`
	}
	if err := yaml.Unmarshal(data, &record); err != nil {
		t.Fatalf("failed to parse the install record %s: %v", path, err)
	}
	return strings.TrimSuffix(record.Images.Repo, "/"), record.Images.Tag
}

// publishWorkerImage builds and pushes this checkout's worker image for class
// and returns its ref.
func publishWorkerImage(t *testing.T, class atev1.SandboxClass) string {
	t.Helper()
	image := "ateworker-" + string(class)
	name, cmd := ateSetupCommand(t, "publish", "worker-images")
	out := e2e.RunCmdOutput(t, nil, name, cmd...)
	for line := range strings.Lines(string(out)) {
		if ref, ok := strings.CutPrefix(strings.TrimSpace(line), image+": "); ok {
			return ref
		}
	}
	t.Fatalf("ate-setup publish worker-images printed no %s ref:\n%s", image, out)
	return ""
}

// rollPool points the pool at image and waits until every worker pod runs it.
func rollPool(ctx context.Context, t *testing.T, clients *e2e.Clients, namespace, name, image string) {
	t.Helper()
	pools := clients.SubstrateK8s.AteV1().WorkerPools(namespace)
	pool, err := pools.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get WorkerPool %s/%s: %v", namespace, name, err)
	}
	t.Logf("WorkerPool %s/%s: workerImage %s -> %s", namespace, name, pool.Spec.WorkerImage, image)
	patch, err := json.Marshal(map[string]any{"spec": map[string]any{"workerImage": image}})
	if err != nil {
		t.Fatalf("failed to build the WorkerPool patch: %v", err)
	}
	if _, err := pools.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		t.Fatalf("failed to patch WorkerPool %s/%s: %v", namespace, name, err)
	}
	waitForPoolRoll(ctx, t, clients, namespace, name, image, 0)
}

// waitForPoolRoll waits until every worker pod of the pool runs image from one
// pod template, is ready, and none is terminating, and has stayed that way for
// stable.
func waitForPoolRoll(ctx context.Context, t *testing.T, clients *e2e.Clients, namespace, name, image string, stable time.Duration) {
	t.Helper()
	pool, err := clients.SubstrateK8s.AteV1().WorkerPools(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get WorkerPool %s/%s: %v", namespace, name, err)
	}
	deadline := time.Now().Add(rollTimeout)
	var last string
	var rolledSince time.Time
	for time.Now().Before(deadline) {
		pods, err := clients.K8s.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: "ate.dev/worker-pool=" + name})
		if err == nil {
			rolled := len(pods.Items) == int(pool.Spec.Replicas)
			templates := map[string]bool{}
			var states []string
			for i := range pods.Items {
				pod := &pods.Items[i]
				onImage, ready, terminating := podUsesImage(pod, image), portforward.IsPodReady(pod), pod.DeletionTimestamp != nil
				if !onImage || !ready || terminating {
					rolled = false
				}
				templates[pod.Labels["pod-template-hash"]] = true
				states = append(states, fmt.Sprintf("%s new=%t ready=%t terminating=%t %s", pod.Name, onImage, ready, terminating, e2e.DescribePodState(pod)))
			}
			if len(templates) > 1 {
				rolled = false
			}
			last = strings.Join(states, "; ")
			switch {
			case !rolled:
				rolledSince = time.Time{}
			case rolledSince.IsZero():
				rolledSince = time.Now()
			}
			if rolled && time.Since(rolledSince) >= stable {
				t.Logf("WorkerPool %s/%s rolled: %s", namespace, name, last)
				return
			}
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out after %v waiting for WorkerPool %s/%s to roll; pods: %s", rollTimeout, namespace, name, last)
}

// workerTemplates lists, by pod-template-hash, the pod templates of the pool's
// worker pods that are not terminating.
func workerTemplates(ctx context.Context, t *testing.T, k8s kubernetes.Interface, pool types.NamespacedName) string {
	t.Helper()
	pods, err := k8s.CoreV1().Pods(pool.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "ate.dev/worker-pool=" + pool.Name})
	if err != nil {
		t.Fatalf("failed to list the worker pods of WorkerPool %s: %v", pool, err)
	}
	var hashes []string
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp == nil {
			hashes = append(hashes, pod.Labels["pod-template-hash"])
		}
	}
	slices.Sort(hashes)
	return strings.Join(slices.Compact(hashes), ",")
}
