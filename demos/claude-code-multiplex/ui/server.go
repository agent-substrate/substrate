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

// Demo UI server — substrate multiplex visualization.
//
// Tiny stdlib HTTP server. Reads worker + actor state from the substrate
// ateapi gRPC service, reads pod logs from the k8s API via client-go,
// exposes JSON endpoints, and serves index.html that polls them.
//
// No kubectl shellouts — all data flows go through:
//  1. ateapi gRPC (workers / actors / actor names) — mirrors the pattern
//     in demos/sandbox/client/main.go.
//  2. client-go corev1 typed client (pod logs) — uses the operator's
//     kubeconfig when running outside the cluster; in-cluster service
//     account credentials otherwise.
//
// Connecting to ateapi: by default the server auto-port-forwards to
// svc/api in ate-system, mints an ate-client ServiceAccount token, and
// verifies ateapi's serving cert against the live ClusterTrustBundle —
// the same authenticated path kubectl-ate uses. It needs a kube context
// with access to ate-system (a laptop/dev kubeconfig, or an in-cluster
// ServiceAccount with the ate-client RBAC):
//
//	PORT=8090 DEMO_NAMESPACE=claude-multiplex-demo go run ./server.go
//
// To target an already-forwarded endpoint instead of auto-forwarding,
// set ATEAPI_ADDR (still authenticated — token + CTB verification):
//
//	kubectl port-forward svc/api 8443:443 -n ate-system &
//	PORT=8090 ATEAPI_ADDR=localhost:8443 DEMO_NAMESPACE=claude-multiplex-demo go run ./server.go
//
// (Pick a UI PORT that doesn't collide with any manual port-forward.)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/ateclient"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	defaultPort      = "8080"
	defaultNamespace = "claude-multiplex-demo"
	maxAssignments   = 50
	rpcTimeout       = 10 * time.Second
	actorOpTimeout   = 30 * time.Second
	logTailLines     = int64(25)
	rawLogTailLines  = int64(500)

	// Assignment lifecycle states the UI badge logic reads:
	// queued (waiting for a worker pod or resuming), running (actor is
	// ACTOR_STATE_RUNNING on a worker pod), and completed (actor suspended).
	stateQueued    = "queued"
	stateRunning   = "running"
	stateCompleted = "completed"
)

var predefinedTasks = []string{
	"Review server.js and suggest two reliability improvements",
	"Explain how a Kubernetes ReplicaSet differs from a Deployment",
	"Write a JavaScript function to detect duplicate items in an array",
	"Summarize the difference between a Pod and a Job in Kubernetes",
	"List three best practices for writing testable Node.js services",
	"Draft a one-paragraph summary of V8 garbage collection",
	"Suggest two ways to make a Kubernetes Service more resilient",
	"Write a unit test for a function that returns the max of two integers",
	"Explain the role of an admission controller in Kubernetes",
	"Outline a backoff strategy for a flaky HTTP client",
}

type assignment struct {
	ID          string   `json:"id"`
	Agent       string   `json:"agent"`
	Task        string   `json:"task"`
	State       string   `json:"state"`
	CreatedAt   float64  `json:"created_at"`
	StartedAt   *float64 `json:"started_at"`
	CompletedAt *float64 `json:"completed_at"`
	dispatched  bool
}

// podSummary is the per-worker shape the UI's index.html renders. Field
// names match the original kubectl-shellout JSON contract so the page
// doesn't need to change. Some k8s-specific fields (Node, StartedAt,
// Image) aren't surfaced by ateapi today; we backfill with substrate-
// native semantics: Node ← worker_pool, StartedAt ← "" (omit), and the
// Image field is dropped since the UI doesn't read it.
type podSummary struct {
	Name      string `json:"name"`
	Node      string `json:"node"`
	Phase     string `json:"phase"`
	Ready     bool   `json:"ready"`
	StartedAt string `json:"started_at"`
}

// actorSummary mirrors the original kubectl-plugin JSON contract. Kind
// is always "Actor" now (ActorTemplates / WorkerPools are not surfaced
// via ateapi today); Phase is derived from the proto Status enum.
type actorSummary struct {
	Kind    string `json:"kind"`
	Name    string `json:"name"`
	Phase   string `json:"phase"`
	Message string `json:"message"`
}

var (
	namespace       = envOr("DEMO_NAMESPACE", defaultNamespace)
	ateapiAddr      = os.Getenv("ATEAPI_ADDR") // empty → auto port-forward to svc/api in ate-system
	rootDir         = mustRootDir()
	mu              sync.Mutex
	lifecycleMu     sync.Mutex
	assignments     = make([]*assignment, 0, maxAssignments) // newest first
	runningAgents   = make(map[string]bool)
	taskRunDuration = 10 * time.Second

	ateClient  ateapi.ControlClient
	ateAPI     *ateclient.Client
	kubeClient *kubernetes.Clientset
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// mustRootDir returns the directory holding this server's index.html.
// Use os.Executable when available (covers `go build` + run); fall
// back to the current working directory (covers `go run` where the
// executable is in /tmp).
func mustRootDir() string {
	if exe, err := os.Executable(); err == nil {
		if d := filepath.Dir(exe); fileExists(filepath.Join(d, "index.html")) {
			return d
		}
	}
	if wd, err := os.Getwd(); err == nil {
		return wd
	}
	return "."
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// newKubeClient returns a typed kubernetes client. Tries in-cluster
// config first (works when running as a pod); falls back to the
// operator's kubeconfig (works when running on a dev VM after
// `gcloud container clusters get-credentials` / `kind export
// kubeconfig`). Returns nil + nil error when neither is available
// — log endpoints will then 503 gracefully without crashing the
// server (handy when iterating on the demo locally with no
// cluster context).
func newKubeClient() (*kubernetes.Clientset, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return kubernetes.NewForConfig(cfg)
	}

	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loader, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		// No usable kube config — return nil clientset, no error;
		// handleLogs will degrade to a clear 503.
		log.Printf("[ui] no kube context available (logs disabled): %v", err)
		return nil, nil
	}
	return kubernetes.NewForConfig(cfg)
}

// actorStateString maps the proto State enum to the human-readable
// phase string the UI's badge logic understands (running / suspended
// / etc).
func actorStateString(s ateapi.ActorState) string {
	switch s {
	case ateapi.ActorState_ACTOR_STATE_RESUMING:
		return "Resuming"
	case ateapi.ActorState_ACTOR_STATE_RUNNING:
		return "Running"
	case ateapi.ActorState_ACTOR_STATE_SUSPENDING:
		return "Suspending"
	case ateapi.ActorState_ACTOR_STATE_SUSPENDED:
		return "Suspended"
	case ateapi.ActorState_ACTOR_STATE_DELETING:
		return "Deleting"
	default:
		return "?"
	}
}

// workerPhase derives a pod-like phase string from a substrate Worker.
// A worker hosting an actor is "Running"; an idle worker is "Idle".
// The UI's badgeFor() treats "running" as green; "idle" falls through
// to the neutral badge, which is the right visual treatment.
func workerPhase(w *ateapi.Worker) string {
	if w.GetStatus().GetAllocated().GetActors() > 0 {
		return "Running"
	}
	return "Idle"
}

// listActorNames returns current actor names in the namespace via
// ateapi. Replaces the prior kubectl-shellout fallback chain.
func listActorNames(ctx context.Context) []string {
	if ateClient == nil {
		return nil
	}
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := ateClient.ListActors(rctx, &ateapi.ListActorsRequest{Atespace: namespace})
	if err != nil {
		log.Printf("[ui] ListActors error: %v", err)
		return nil
	}
	names := make([]string, 0, len(resp.GetActors()))
	for _, a := range resp.GetActors() {
		if namespace != "" && a.GetMetadata().GetAtespace() != "" && a.GetMetadata().GetAtespace() != namespace {
			continue
		}
		if id := a.GetMetadata().GetName(); id != "" && !isUUID(id) {
			names = append(names, id)
		}
	}
	sort.Strings(names)
	return names
}

func countPoolWorkers(ctx context.Context) int {
	if ateClient == nil {
		return 2
	}
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := ateClient.ListWorkers(rctx, &ateapi.ListWorkersRequest{})
	if err != nil {
		return 2
	}
	count := 0
	for _, w := range resp.GetWorkers() {
		if namespace == "" || w.GetWorkerNamespace() == "" || w.GetWorkerNamespace() == namespace {
			count++
		}
	}
	if count <= 0 {
		return 2
	}
	return count
}

// dispatchQueuedAssignments starts background execution for queued assignments
// in FIFO order up to the worker pool capacity (one active task per agent).
func dispatchQueuedAssignments() {
	if ateClient == nil {
		return
	}
	poolSize := countPoolWorkers(context.Background())

	var toStart []*assignment
	mu.Lock()
	for i := len(assignments) - 1; i >= 0 && len(runningAgents) < poolSize; i-- {
		asg := assignments[i]
		if asg.State == stateQueued && !asg.dispatched && !runningAgents[asg.Agent] {
			asg.dispatched = true
			runningAgents[asg.Agent] = true
			toStart = append(toStart, asg)
		}
	}
	mu.Unlock()

	for _, asg := range toStart {
		go runAssignment(asg)
	}
}

func runAssignment(asg *assignment) {
	defer func() {
		mu.Lock()
		delete(runningAgents, asg.Agent)
		mu.Unlock()
		dispatchQueuedAssignments()
	}()

	ctx := context.Background()
	ensureActorResumed(ctx, asg.Agent)

	mu.Lock()
	started := nowSec()
	asg.State = stateRunning
	asg.StartedAt = &started
	mu.Unlock()

	time.Sleep(taskRunDuration)

	lifecycleMu.Lock()
	suspendActorByName(ctx, asg.Agent)
	lifecycleMu.Unlock()

	mu.Lock()
	completed := nowSec()
	asg.State = stateCompleted
	asg.CompletedAt = &completed
	mu.Unlock()
}

func ensureActorResumed(ctx context.Context, target string) {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()

	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	actorsResp, err := ateClient.ListActors(rctx, &ateapi.ListActorsRequest{Atespace: namespace})
	cancel()
	if err == nil {
		var running []string
		for _, a := range actorsResp.GetActors() {
			if namespace != "" && a.GetMetadata().GetAtespace() != "" && a.GetMetadata().GetAtespace() != namespace {
				continue
			}
			if a.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_RUNNING {
				if a.GetMetadata().GetName() == target {
					return
				}
				running = append(running, a.GetMetadata().GetName())
			}
		}
		for poolSize := countPoolWorkers(ctx); len(running) >= poolSize && len(running) > 0; running = running[1:] {
			suspendActorByName(ctx, running[0])
		}
	}

	resumeCtx, resumeCancel := context.WithTimeout(ctx, actorOpTimeout)
	defer resumeCancel()
	if _, err := ateClient.ResumeActor(resumeCtx, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: namespace, Name: target},
	}); err != nil {
		log.Printf("[ui] ResumeActor(%s/%s) error: %v", namespace, target, err)
	}
}

func suspendActorByName(ctx context.Context, name string) {
	sctx, cancel := context.WithTimeout(ctx, actorOpTimeout)
	defer cancel()
	if _, err := ateClient.SuspendActor(sctx, &ateapi.SuspendActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: namespace, Name: name},
	}); err != nil {
		log.Printf("[ui] SuspendActor(%s/%s) error: %v", namespace, name, err)
	}
}

func nowSec() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

// giveTask picks a random task + idle agent and dispatches the assignment.
// Returns nil-string error if no agents.
func giveTask(ctx context.Context) (*assignment, string) {
	agents := listActorNames(ctx)
	if len(agents) == 0 {
		return nil, "no agents available in namespace"
	}
	now := nowSec()

	mu.Lock()
	busy := make(map[string]bool, len(agents))
	for _, a := range assignments {
		if a.State == stateQueued || a.State == stateRunning {
			busy[a.Agent] = true
		}
	}
	candidates := make([]string, 0, len(agents))
	for _, name := range agents {
		if !busy[name] {
			candidates = append(candidates, name)
		}
	}
	if len(candidates) == 0 {
		candidates = agents
	}
	asg := &assignment{
		ID:        fmt.Sprintf("asg-%d", time.Now().UnixNano()),
		Agent:     candidates[rand.Intn(len(candidates))],
		Task:      predefinedTasks[rand.Intn(len(predefinedTasks))],
		State:     stateQueued,
		CreatedAt: now,
	}
	assignments = append([]*assignment{asg}, assignments...)
	if len(assignments) > maxAssignments {
		assignments = assignments[:maxAssignments]
	}
	mu.Unlock()

	dispatchQueuedAssignments()
	return asg, ""
}

// writeJSON serializes body as JSON with no-store cache headers.
func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	data, err := json.Marshal(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		http.NotFound(w, r)
		return
	}
	f, err := os.Open(filepath.Join(rootDir, "index.html"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.Copy(w, f)
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok":          true,
		"namespace":   namespace,
		"ateapi_addr": ateapiAddr,
		"logs":        kubeClient != nil,
	})
}

// handlePods returns worker-shaped JSON sourced from ateapi.ListWorkers.
// The JSON shape mirrors the original kubectl-shellout contract so
// index.html doesn't need to change.
func handlePods(w http.ResponseWriter, r *http.Request) {
	if ateClient == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "ateapi client not initialized"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), rpcTimeout)
	defer cancel()
	resp, err := ateClient.ListWorkers(ctx, &ateapi.ListWorkersRequest{})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "ListWorkers: " + err.Error()})
		return
	}
	pods := make([]podSummary, 0, len(resp.GetWorkers()))
	for _, wk := range resp.GetWorkers() {
		if namespace != "" && wk.GetWorkerNamespace() != "" && wk.GetWorkerNamespace() != namespace {
			continue
		}
		ready := wk.GetStatus().GetAllocated().GetActors() > 0
		pods = append(pods, podSummary{
			Name:      wk.GetWorkerPod(),
			Node:      wk.GetWorkerPool(), // closest semantic analog
			Phase:     workerPhase(wk),
			Ready:     ready,
			StartedAt: "", // not exposed by ateapi today
		})
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	writeJSON(w, http.StatusOK, map[string][]podSummary{"pods": pods})
}

// handleActors returns actor-shaped JSON sourced from ateapi.ListActors.
// ActorTemplate / WorkerPool resources are no longer surfaced —
// substrate-native Actors are the canonical demo entity.
func handleActors(w http.ResponseWriter, r *http.Request) {
	if ateClient == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "ateapi client not initialized"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), rpcTimeout)
	defer cancel()
	resp, err := ateClient.ListActors(ctx, &ateapi.ListActorsRequest{Atespace: namespace})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "ListActors: " + err.Error()})
		return
	}
	actors := make([]actorSummary, 0, len(resp.GetActors()))
	for _, a := range resp.GetActors() {
		// The demo's templates live in the atespace named after the demo
		// namespace, so the template ref's atespace filters foreign actors.
		if namespace != "" && a.GetActorTemplate().GetAtespace() != "" && a.GetActorTemplate().GetAtespace() != namespace {
			continue
		}
		if isUUID(a.GetMetadata().GetName()) {
			continue
		}
		// Carry the template name as the meta message so the UI's
		// secondary line shows useful provenance (the original
		// kubectl path put k8s `status.message` here — for
		// substrate Actors there's no equivalent, so the template
		// name is the closest semantic match).
		msg := ""
		if t := a.GetActorTemplate().GetName(); t != "" {
			msg = "template: " + t
		}
		actors = append(actors, actorSummary{
			Kind:    "Actor",
			Name:    a.GetMetadata().GetName(),
			Phase:   actorStateString(a.GetStatus().GetState()),
			Message: msg,
		})
	}
	sort.Slice(actors, func(i, j int) bool { return actors[i].Name < actors[j].Name })
	writeJSON(w, http.StatusOK, map[string][]actorSummary{"actors": actors})
}

func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func isRunscLogLine(msg string) bool {
	t := strings.TrimSpace(msg)
	if len(t) >= 6 && strings.ContainsRune("IWEF", rune(t[0])) &&
		t[1] >= '0' && t[1] <= '9' && t[2] >= '0' && t[2] <= '9' &&
		t[3] >= '0' && t[3] <= '9' && t[4] >= '0' && t[4] <= '9' && t[5] == ' ' {
		return true
	}
	return strings.Contains(t, "**************** gVisor ****************")
}

func formatPodLogs(raw, atespace string, maxLines int) string {
	var out []string
	for _, line := range strings.Split(raw, "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &m); err != nil {
			continue
		}
		var actorName, actorAtespace, containerName string
		for _, k := range []string{"logging.googleapis.com/labels", "labels"} {
			if l, ok := m[k].(map[string]any); ok {
				actorName, _ = l["ate.actor.name"].(string)
				actorAtespace, _ = l["ate.atespace"].(string)
				containerName, _ = l["ate.actor.container.name"].(string)
				if actorName != "" {
					break
				}
			}
		}
		if actorName == "" || isUUID(actorName) || containerName == "_sandbox" || (atespace != "" && actorAtespace != "" && actorAtespace != atespace) {
			continue
		}
		msg, _ := m["message"].(string)
		if msg == "" {
			msg, _ = m["msg"].(string)
		}
		if msg == "" || isRunscLogLine(msg) {
			continue
		}
		if containerName == "" {
			out = append(out, fmt.Sprintf("[substrate:%s] %s", actorName, msg))
		} else {
			out = append(out, msg)
		}
	}
	if maxLines > 0 && len(out) > maxLines {
		out = out[len(out)-maxLines:]
	}
	return strings.Join(out, "\n")
}

// handleLogs streams the last N lines of a pod's logs via the typed
// k8s client. Replaces the prior `kubectl logs --tail=25` shellout.
func handleLogs(w http.ResponseWriter, r *http.Request) {
	pod := strings.TrimPrefix(r.URL.Path, "/api/logs/")
	pod = strings.Trim(pod, "/")
	if pod == "" || strings.Contains(pod, "/") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad pod ref"})
		return
	}
	if kubeClient == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "k8s client not initialized"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), rpcTimeout)
	defer cancel()
	tail := rawLogTailLines
	opts := &corev1.PodLogOptions{TailLines: &tail}
	req := kubeClient.CoreV1().Pods(namespace).GetLogs(pod, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer stream.Close()
	data, err := io.ReadAll(stream)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error(), "logs": string(data)})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"logs": formatPodLogs(string(data), namespace, int(logTailLines)), "stderr": ""})
}

func handleTaskStatus(w http.ResponseWriter, _ *http.Request) {
	dispatchQueuedAssignments()
	mu.Lock()
	snapshot := make([]*assignment, len(assignments))
	for i, a := range assignments {
		cp := *a
		snapshot[i] = &cp
	}
	mu.Unlock()
	writeJSON(w, http.StatusOK, map[string][]*assignment{"assignments": snapshot})
}

func handleGiveTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	// Drain request body if any (we don't actually need it; click semantics only).
	_, _ = io.Copy(io.Discard, r.Body)
	_ = r.Body.Close()

	asg, errMsg := giveTask(r.Context())
	if errMsg != "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": errMsg})
		return
	}
	writeJSON(w, http.StatusOK, asg)
}

func main() {
	port := envOr("PORT", defaultPort)

	// Open the ateapi connection up front so the UI surfaces a clear
	// startup error at boot rather than per-request failures with
	// cryptic gRPC messages. NewClient mints an ate-client token and
	// verifies ateapi's serving cert against the live ClusterTrustBundle;
	// an empty endpoint auto-port-forwards to svc/api in ate-system.
	log.Printf("[ui] connecting to ateapi (endpoint=%q; empty = auto port-forward to svc/api)", ateapiAddr)
	cli, err := ateclient.NewClient(context.Background(), "", "", ateapiAddr, "", false)
	if err != nil {
		log.Fatalf("connect ateapi: %v", err)
	}
	ateAPI = cli
	ateClient = cli
	defer ateAPI.Close()

	// Best-effort k8s client for logs; nil is OK (handleLogs degrades
	// to a 503 with a clear message). This lets the demo start even
	// when no cluster context is configured — useful for quick UI
	// shape iteration.
	kc, kerr := newKubeClient()
	if kerr != nil {
		log.Printf("[ui] kube client init error (logs disabled): %v", kerr)
	}
	kubeClient = kc

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/api/pods", handlePods)
	mux.HandleFunc("/api/actors", handleActors)
	mux.HandleFunc("/api/logs/", handleLogs)
	mux.HandleFunc("/api/task-status", handleTaskStatus)
	mux.HandleFunc("/api/give-task", handleGiveTask)

	addr := "0.0.0.0:" + port
	log.Printf("[ui] serving %s (namespace=%s ateapi=%s logs=%t)", addr, namespace, ateapiAddr, kubeClient != nil)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}
