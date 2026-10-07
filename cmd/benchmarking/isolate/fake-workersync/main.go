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
// round robin on the nodes carrying --node-label, and has each Worker's
// capacity reported through the fake-atelet on its node, as ateom reports
// through atelet. It writes each pool's status, so the scale subresource and
// `kubectl get workerpool` work, and deletes its Workers when it shuts down.
//
// atecontroller must not run alongside it: its WorkerPool controller would
// create real worker pods for the same pools, and its worker syncer deletes
// every Worker with no live pod.
package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os/signal"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/cmd/benchmarking/isolate/internal/fakeworker"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/boomerutil"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	ateinformers "github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	atelisters "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

var (
	apiEndpoint   = pflag.String("api-endpoint", "k8s:///api.ate-system.svc.cluster.local:443", "ate-api-server gRPC dial target.")
	run           = pflag.String("run", "bench", "Run prefix of the fake Worker pod names.")
	nodeLabel     = pflag.String("node-label", "ate.dev/fake-data-plane=true", "Label selector for the benchmark nodes that run fake-atelet.")
	ateletNS      = pflag.String("atelet-namespace", "ate-system", "Namespace of the fake-atelet pods.")
	ateletLabel   = pflag.String("fake-atelet-label", "ate.dev/fake-atelet=true", "Label selector for the fake-atelet pods.")
	relayPort     = pflag.Int("relay-port", fakeworker.DefaultRelayPort, "Port fake-atelet serves capacity reports on.")
	ateletID      = pflag.String("atelet-spiffe-id", installdefaults.SPIFFEID("ate-system", "atelet"), "SPIFFE ID the fake-atelet relay must present.")
	clientBundle  = pflag.String("client-cred-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "Credential bundle presented to the relay.")
	trustBundle   = pflag.String("trust-bundle", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "Pod-identity trust bundle the relay's certificate must chain to.")
	resync        = pflag.Duration("resync", 10*time.Second, "Reconcile at least this often, besides on every WorkerPool change.")
	concurrency   = pflag.Int("concurrency", 32, "Worker calls in flight.")
	cleanup       = pflag.Bool("cleanup", false, "Delete this run's registered fake Workers, then exit.")
	drainGrace    = pflag.Duration("drain-grace", time.Minute, "How long a draining fake Worker waits for its Actors to leave before it is deleted, releasing them. Stands in for the time ateom takes to stop a terminating pod's Actors.")
	deleteTimeout = pflag.Duration("delete-timeout", 5*time.Minute, "How long shutdown spends deleting the fake Workers.")

	showVersion  = pflag.Bool("version", false, "Print version and exit.")
	logLevelFlag = pflag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")
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
	if err := fakeworker.ValidateRun(*run); err != nil {
		serverboot.Fatal(ctx, "Invalid --run", err)
	}
	if *concurrency < 1 {
		serverboot.Fatal(ctx, "Invalid --concurrency", fmt.Errorf("must be at least 1, got %d", *concurrency))
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
	poolInformer := factory.Api().V1alpha1().WorkerPools()
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
		nodeLabel:   *nodeLabel,
		ateletNS:    *ateletNS,
		ateletLabel: *ateletLabel,
		relayPort:   *relayPort,
	}
	c := newController(control, relay, cl, *run, *concurrency, *drainGrace)

	if err := c.adopt(ctx); err != nil {
		serverboot.Fatal(ctx, "Failed to load registered fake Workers", err)
	}
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
			// Shutdown runs after the benchmark has deleted its actors
			// (teardown order), so no Actor is left on a deleted Worker.
			delCtx, cancel := context.WithTimeout(context.Background(), *deleteTimeout)
			defer cancel()
			if err := c.deleteAll(delCtx); err != nil {
				slog.ErrorContext(delCtx, "Failed to delete every fake Worker; run with --cleanup, or let the next atecontroller's startup sweep remove the rest", slog.Any("err", err))
			}
			return
		case <-changed:
		case <-ticker.C:
		}
	}
}

// kubeCluster is the controller's view of Kubernetes.
type kubeCluster struct {
	kc          kubernetes.Interface
	ac          versioned.Interface
	pools       atelisters.WorkerPoolLister
	nodeLabel   string
	ateletNS    string
	ateletLabel string
	relayPort   int
}

func (k *kubeCluster) Pools() ([]*atev1alpha1.WorkerPool, error) {
	return k.pools.List(labels.Everything())
}

func (k *kubeCluster) Nodes(ctx context.Context) ([]node, error) {
	list, err := k.kc.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: k.nodeLabel})
	if err != nil {
		return nil, err
	}
	nodes := make([]node, 0, len(list.Items))
	for _, n := range list.Items {
		nodes = append(nodes, node{name: n.Name, allocatable: n.Status.Allocatable})
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

func (k *kubeCluster) UpdateStatus(ctx context.Context, wp *atev1alpha1.WorkerPool) error {
	_, err := k.ac.ApiV1alpha1().WorkerPools(wp.Namespace).UpdateStatus(ctx, wp, metav1.UpdateOptions{})
	return err
}
