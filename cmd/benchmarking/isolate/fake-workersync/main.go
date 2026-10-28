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

// Command fake-workersync stands in for atecontroller's WorkerPool
// controller and worker syncer in control-plane benchmarks. It honors every
// WorkerPool's spec.replicas with fake Workers that no pod backs, placed
// round robin on the nodes that run a fake-atelet pod, and has each Worker's
// capacity reported through the fake-atelet on its node, as worker reports
// through atelet. It writes each pool's status, so the scale subresource and
// `kubectl get workerpool` work. It leaves its Workers registered when it
// shuts down and adopts them when it starts, so a restart keeps every Actor
// placed; a deleted pool's Workers are retired while it runs.
//
// atecontroller must not run alongside it: its WorkerPool controller would
// create real worker pods for the same pools, and its worker syncer deletes
// every Worker with no live pod.
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os/signal"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/benchmarking/isolate/internal/fakeworker"
	"go.ate.dev/substrate/internal/benchmarking/boomer/boomerutil"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/serverboot"
	"go.ate.dev/substrate/internal/version"
	"go.ate.dev/substrate/pkg/client/clientset/versioned"
	ateinformers "go.ate.dev/substrate/pkg/client/informers/externalversions"
	atelisters "go.ate.dev/substrate/pkg/client/listers/ate/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

var (
	apiEndpoint  = pflag.String("api-endpoint", "k8s:///api.ate-system.svc.cluster.local:443", "ate-api-server gRPC dial target.")
	run          = pflag.String("run", "bench", "Run prefix of the fake Worker pod names.")
	ateletNS     = pflag.String("atelet-namespace", "ate-system", "Namespace of the fake-atelet pods.")
	ateletLabel  = pflag.String("fake-atelet-label", "ate.dev/fake-atelet=true", "Label selector for the fake-atelet pods.")
	relayPort    = pflag.Int("relay-port", fakeworker.DefaultRelayPort, "Port fake-atelet serves capacity reports on.")
	ateletID     = pflag.String("atelet-spiffe-id", installdefaults.SPIFFEID("ate-system", "atelet"), "SPIFFE ID the fake-atelet relay must present.")
	clientBundle = pflag.String("client-cred-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "Credential bundle presented to the relay.")
	trustBundle  = pflag.String("trust-bundle", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "Pod-identity trust bundle the relay's certificate must chain to.")
	resync       = pflag.Duration("resync", 10*time.Second, "Reconcile at least this often, besides on every WorkerPool change.")
	concurrency  = pflag.Int("concurrency", 32, "Worker calls in flight.")
	cleanup      = pflag.Bool("cleanup", false, "Delete this run's registered fake Workers, then exit. Stop the serving fake-workersync first, as by scaling its Deployment to zero: it would go on counting the deleted Workers until it restarts.")
	nodeGrace    = pflag.Duration("node-grace", 5*time.Minute, "How long a node keeps its fake Workers with no fake-atelet pod, as while its fake-atelet is replaced.")
	healthAddr   = pflag.String("health-listen-addr", ":9091", "Address to serve /healthz and /readyz on.")
	drainGrace   = pflag.Duration("drain-grace", time.Minute, "How long a draining fake Worker waits for its Actors to leave before it is deleted, releasing them. Stands in for the time worker takes to stop a terminating pod's Actors.")

	showVersion  = pflag.Bool("version", false, "Print version and exit.")
	logLevelFlag = pflag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")
	previewFlags = pflag.StringSlice("preview", nil, "Preview gates to enable.")
)

func main() {
	pflag.Parse()
	if *showVersion {
		fmt.Println(version.String())
		return
	}
	serverboot.InitLogger()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := serverboot.SetLogLevel(*logLevelFlag); err != nil {
		serverboot.Fatal(ctx, "Invalid --log-level", err)
	}
	if err := preview.Init(*previewFlags...); err != nil {
		serverboot.Fatal(ctx, "Invalid --preview", err)
	}
	if err := fakeworker.ValidateRun(*run); err != nil {
		serverboot.Fatal(ctx, "Invalid --run", err)
	}
	if *concurrency < 1 {
		serverboot.Fatal(ctx, "Invalid --concurrency", fmt.Errorf("must be at least 1, got %d", *concurrency))
	}
	if *nodeGrace < 0 {
		serverboot.Fatal(ctx, "Invalid --node-grace", fmt.Errorf("must not be negative, got %s", *nodeGrace))
	}

	conn, control, err := boomerutil.DialControl(*apiEndpoint)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to dial ate-api-server", err)
	}
	defer conn.Close()

	cfg, err := rest.InClusterConfig()
	if err != nil {
		serverboot.Fatal(ctx, "Failed to load in-cluster config", err)
	}
	kc, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create Kubernetes client", err)
	}
	ac, err := versioned.NewForConfig(cfg)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create ate client", err)
	}
	relay, err := newGRPCRelay(*clientBundle, *trustBundle, *ateletID)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to build the relay client", err)
	}
	defer relay.Close()

	factory := ateinformers.NewSharedInformerFactory(ac, 0)
	poolInformer := factory.Ate().V1().WorkerPools()
	changed := make(chan struct{}, 1)
	notify := func(any) {
		select {
		case changed <- struct{}{}:
		default:
		}
	}
	if _, err := poolInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    notify,
		UpdateFunc: func(_, obj any) { notify(obj) },
		DeleteFunc: notify,
	}); err != nil {
		serverboot.Fatal(ctx, "Failed to watch WorkerPools", err)
	}
	cl := &kubeCluster{
		kc:          kc,
		ac:          ac,
		pools:       poolInformer.Lister(),
		ateletNS:    *ateletNS,
		ateletLabel: *ateletLabel,
		relayPort:   *relayPort,
		nodeGrace:   *nodeGrace,
		now:         time.Now,
	}
	c := newController(control, relay, cl, *run, *concurrency, *drainGrace)

	// --cleanup is a one-off run that nothing probes, so it serves no health
	// endpoints.
	var ready atomic.Bool
	if !*cleanup {
		health := &http.Server{Addr: *healthAddr, Handler: healthHandler(&ready), ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := health.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				serverboot.Fatal(ctx, "Failed to serve health endpoints", err)
			}
		}()
		defer func() { _ = health.Shutdown(context.Background()) }()
	}

	if err := c.adoptUntilServed(ctx, time.Second, 30*time.Second); err != nil {
		if ctx.Err() != nil {
			return
		}
		serverboot.Fatal(ctx, "Failed to load registered fake Workers", err)
	}
	ready.Store(true)
	// A node whose fake-atelet is being replaced has no pod right now; its
	// adopted Workers keep it in the set.
	cl.remember(c.nodes())
	if *cleanup {
		if err := c.deleteAll(ctx); err != nil {
			serverboot.Fatal(ctx, "Failed to delete fake Workers", err)
		}
		return
	}

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
	ticker := time.NewTicker(*resync)
	defer ticker.Stop()
	for {
		if err := c.reconcile(ctx); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "Reconcile incomplete; retrying", slog.Any("err", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-changed:
		case <-ticker.C:
		}
	}
}

// healthHandler serves /healthz, 200 while the process runs, and /readyz,
// 200 once ready is set: after the registered fake Workers are adopted, so a
// rollout waits until ate-api-server has answered.
func healthHandler(ready *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// kubeCluster is the controller's view of Kubernetes.
type kubeCluster struct {
	kc          kubernetes.Interface
	ac          versioned.Interface
	pools       atelisters.WorkerPoolLister
	ateletNS    string
	ateletLabel string
	relayPort   int

	// nodeGrace is how long a node stays in the set with no fake-atelet pod.
	nodeGrace time.Duration
	// now is the clock nodeGrace is measured on.
	now func() time.Time

	mu sync.Mutex
	// seen holds when each node last had a fake-atelet pod scheduled to it.
	seen map[string]time.Time
}

func (k *kubeCluster) Pools() ([]*atev1.WorkerPool, error) {
	return k.pools.List(labels.Everything())
}

// remember adds nodes to the set Nodes draws on, as if a fake-atelet pod had
// just been scheduled to each.
func (k *kubeCluster) remember(nodes []string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.seen == nil {
		k.seen = map[string]time.Time{}
	}
	now := k.now()
	for _, n := range nodes {
		k.seen[n] = now
	}
}

// Nodes returns the nodes that still exist and have had a fake-atelet pod
// scheduled to them, ready or not, within nodeGrace. A node stays in the set
// while its fake-atelet is replaced, as a DaemonSet rollout does, so its
// Workers wait for the relay rather than moving; one that stops running
// fake-atelet for good leaves it, and its Workers move.
func (k *kubeCluster) Nodes(ctx context.Context) ([]node, error) {
	pods, err := k.kc.CoreV1().Pods(k.ateletNS).List(ctx, metav1.ListOptions{LabelSelector: k.ateletLabel})
	if err != nil {
		return nil, err
	}
	list, err := k.kc.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.seen == nil {
		k.seen = map[string]time.Time{}
	}
	now := k.now()
	for _, p := range pods.Items {
		if p.Spec.NodeName != "" {
			k.seen[p.Spec.NodeName] = now
		}
	}
	exists := map[string]bool{}
	for _, n := range list.Items {
		exists[n.Name] = true
	}
	maps.DeleteFunc(k.seen, func(name string, at time.Time) bool {
		return !exists[name] || now.Sub(at) > k.nodeGrace
	})
	nodes := make([]node, 0, len(k.seen))
	for _, n := range list.Items {
		if _, ok := k.seen[n.Name]; ok {
			nodes = append(nodes, node{name: n.Name, allocatable: n.Status.Allocatable})
		}
	}
	slices.SortFunc(nodes, func(a, b node) int { return cmp.Compare(a.name, b.name) })
	return nodes, nil
}

func (k *kubeCluster) Relays(ctx context.Context) (map[string]string, error) {
	pods, err := k.kc.CoreV1().Pods(k.ateletNS).List(ctx, metav1.ListOptions{LabelSelector: k.ateletLabel})
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, p := range pods.Items {
		if p.Status.Phase != corev1.PodRunning || p.Status.PodIP == "" || p.DeletionTimestamp != nil {
			continue
		}
		out[p.Spec.NodeName] = net.JoinHostPort(p.Status.PodIP, strconv.Itoa(k.relayPort))
	}
	return out, nil
}

func (k *kubeCluster) UpdateStatus(ctx context.Context, wp *atev1.WorkerPool) error {
	_, err := k.ac.AteV1().WorkerPools(wp.Namespace).UpdateStatus(ctx, wp, metav1.UpdateOptions{})
	return err
}
