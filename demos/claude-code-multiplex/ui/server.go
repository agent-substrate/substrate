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
// Prereq when running outside the cluster:
//
//	kubectl port-forward svc/ateapi 8080:8080 -n ate-system &
//	PORT=8090 ATEAPI_ADDR=localhost:8080 DEMO_NAMESPACE=claude-multiplex-demo go run ./server.go
//
// (Pick a UI PORT that doesn't collide with the port-forward.)
package main

import (
	"context"
	"crypto/tls"
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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	defaultPort       = "8080"
	defaultNamespace  = "claude-multiplex-demo"
	defaultAteapiAddr = "localhost:8080"
	maxAssignments    = 50
	rpcTimeout        = 10 * time.Second
	logTailLines      = int64(25)

	// Assignment lifecycle states the UI badge logic reads.
	// queued → running → completed; computeState drives the
	// transitions from elapsed-since-creation.
	stateQueued    = "queued"
	stateRunning   = "running"
	stateCompleted = "completed"
)

var predefinedTasks = []string{
	"Review main.py and suggest two improvements",
	"Explain how a Kubernetes ReplicaSet differs from a Deployment",
	"Write a Python function to detect duplicate items in a list",
	"Summarize the difference between a Pod and a Job in Kubernetes",
	"List three best practices for writing testable Python code",
	"Draft a one-paragraph summary of Python garbage collection",
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
	QueueFor    float64  `json:"queue_for"`
	RunFor      float64  `json:"run_for"`
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
	namespace   = envOr("DEMO_NAMESPACE", defaultNamespace)
	ateapiAddr  = envOr("ATEAPI_ADDR", defaultAteapiAddr)
	rootDir     = mustRootDir()
	mu          sync.Mutex
	assignments = make([]*assignment, 0, maxAssignments) // newest first

	ateClient  ateapipb.ControlClient
	ateConn    *grpc.ClientConn
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

// dialAteAPI opens a gRPC client to the substrate ateapi service.
// Mirrors demos/sandbox/client/main.go: TLS with InsecureSkipVerify
// (ateapi serves a self-signed cert; the demo trusts whichever
// instance the port-forward / in-cluster DNS resolves to).
func dialAteAPI(endpoint string) (ateapipb.ControlClient, *grpc.ClientConn, error) {
	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, err
	}
	return ateapipb.NewControlClient(conn), conn, nil
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

// actorStatusString maps the proto Status enum to the human-readable
// phase string the UI's badge logic understands (running / suspended
// / etc).
func actorStatusString(s ateapipb.Actor_Status) string {
	switch s {
	case ateapipb.Actor_STATUS_RESUMING:
		return "Resuming"
	case ateapipb.Actor_STATUS_RUNNING:
		return "Running"
	case ateapipb.Actor_STATUS_SUSPENDING:
		return "Suspending"
	case ateapipb.Actor_STATUS_SUSPENDED:
		return "Suspended"
	default:
		return "?"
	}
}

// workerPhase derives a pod-like phase string from a substrate Worker.
// A worker hosting an actor is "Running"; an idle worker is "Idle".
// The UI's badgeFor() treats "running" as green; "idle" falls through
// to the neutral badge, which is the right visual treatment.
func workerPhase(w *ateapipb.Worker) string {
	if w.GetActorId() != "" {
		return "Running"
	}
	return "Idle"
}

// listActorNames returns current actor IDs in the namespace via
// ateapi. Replaces the prior kubectl-shellout fallback chain.
func listActorNames(ctx context.Context) []string {
	if ateClient == nil {
		return nil
	}
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := ateClient.ListActors(rctx, &ateapipb.ListActorsRequest{})
	if err != nil {
		log.Printf("[ui] ListActors error: %v", err)
		return nil
	}
	names := make([]string, 0, len(resp.GetActors()))
	for _, a := range resp.GetActors() {
		if id := a.GetActorId(); id != "" {
			names = append(names, id)
		}
	}
	sort.Strings(names)
	return names
}

// computeState returns the timer-driven UI state for an assignment.
// queued → running → completed (purely client-time-driven; the
// substrate side has no concept of these per-task states).
func computeState(asg *assignment) string {
	elapsed := nowSec() - asg.CreatedAt
	if elapsed < asg.QueueFor {
		return stateQueued
	}
	if elapsed < asg.QueueFor+asg.RunFor {
		return stateRunning
	}
	return stateCompleted
}

// applyComputedStates walks current assignments and stamps started_at /
// completed_at as states advance. Caller must NOT hold mu.
func applyComputedStates() {
	mu.Lock()
	defer mu.Unlock()
	for _, asg := range assignments {
		newState := computeState(asg)
		if newState == asg.State {
			continue
		}
		asg.State = newState
		if newState == stateRunning && asg.StartedAt == nil {
			v := asg.CreatedAt + asg.QueueFor
			asg.StartedAt = &v
		} else if newState == stateCompleted && asg.CompletedAt == nil {
			v := asg.CreatedAt + asg.QueueFor + asg.RunFor
			asg.CompletedAt = &v
		}
	}
}

func nowSec() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

// giveTask picks a random task + random agent and records the assignment.
// Returns nil-string error if no agents.
func giveTask(ctx context.Context) (*assignment, string) {
	agents := listActorNames(ctx)
	if len(agents) == 0 {
		return nil, "no agents available in namespace"
	}
	now := nowSec()
	asg := &assignment{
		ID:        fmt.Sprintf("asg-%d", time.Now().UnixMilli()),
		Agent:     agents[rand.Intn(len(agents))],
		Task:      predefinedTasks[rand.Intn(len(predefinedTasks))],
		State:     stateQueued,
		CreatedAt: now,
		QueueFor:  roundOne(2.0 + rand.Float64()*3.0), // 2.0–5.0
		RunFor:    roundOne(9.0 + rand.Float64()*7.0), // 9.0–16.0
	}
	mu.Lock()
	defer mu.Unlock()
	assignments = append([]*assignment{asg}, assignments...)
	if len(assignments) > maxAssignments {
		assignments = assignments[:maxAssignments]
	}
	return asg, ""
}

func roundOne(v float64) float64 {
	return float64(int(v*10+0.5)) / 10
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
	resp, err := ateClient.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "ListWorkers: " + err.Error()})
		return
	}
	pods := make([]podSummary, 0, len(resp.GetWorkers()))
	for _, wk := range resp.GetWorkers() {
		// Filter to the demo namespace when set — workers may live
		// in their own pool namespace (worker_namespace) so we
		// compare against actor_namespace too.
		if namespace != "" && wk.GetActorNamespace() != "" && wk.GetActorNamespace() != namespace {
			continue
		}
		pods = append(pods, podSummary{
			Name:      wk.GetWorkerPod(),
			Node:      wk.GetWorkerPool(), // closest semantic analog
			Phase:     workerPhase(wk),
			Ready:     wk.GetActorId() != "",
			StartedAt: "", // not exposed by ateapi today
		})
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	writeJSON(w, http.StatusOK, map[string][]podSummary{"pods": pods})
}

// handleActors returns actor-shaped JSON sourced from ateapi.ListActors.
// ActorTemplates / WorkerPools (k8s CRDs) are no longer surfaced —
// substrate-native Actors are the canonical demo entity.
func handleActors(w http.ResponseWriter, r *http.Request) {
	if ateClient == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "ateapi client not initialized"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), rpcTimeout)
	defer cancel()
	resp, err := ateClient.ListActors(ctx, &ateapipb.ListActorsRequest{})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "ListActors: " + err.Error()})
		return
	}
	actors := make([]actorSummary, 0, len(resp.GetActors()))
	for _, a := range resp.GetActors() {
		if namespace != "" && a.GetActorTemplateNamespace() != "" && a.GetActorTemplateNamespace() != namespace {
			continue
		}
		// Carry the template name as the meta message so the UI's
		// secondary line shows useful provenance (the original
		// kubectl path put k8s `status.message` here — for
		// substrate Actors there's no equivalent, so the template
		// name is the closest semantic match).
		msg := ""
		if t := a.GetActorTemplateName(); t != "" {
			msg = "template: " + t
		}
		actors = append(actors, actorSummary{
			Kind:    "Actor",
			Name:    a.GetActorId(),
			Phase:   actorStatusString(a.GetStatus()),
			Message: msg,
		})
	}
	sort.Slice(actors, func(i, j int) bool { return actors[i].Name < actors[j].Name })
	writeJSON(w, http.StatusOK, map[string][]actorSummary{"actors": actors})
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
	tail := logTailLines
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
	writeJSON(w, http.StatusOK, map[string]string{"logs": string(data), "stderr": ""})
}

func handleTaskStatus(w http.ResponseWriter, _ *http.Request) {
	applyComputedStates()
	mu.Lock()
	snapshot := make([]*assignment, len(assignments))
	copy(snapshot, assignments)
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
	// startup error if the operator forgot the port-forward, rather
	// than per-request failures with cryptic gRPC messages.
	log.Printf("[ui] dialing ateapi at %s", ateapiAddr)
	cli, conn, err := dialAteAPI(ateapiAddr)
	if err != nil {
		log.Fatalf("dial ateapi: %v", err)
	}
	ateClient = cli
	ateConn = conn
	defer ateConn.Close()

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
