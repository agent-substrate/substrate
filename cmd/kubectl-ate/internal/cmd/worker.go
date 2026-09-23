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

package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

var (
	getWorkerNamespaceFlag string
	getWorkerAtespaceFlag  string
	getWorkerSelectorFlag  string
	getWorkerClassFlag     string
	topWorkerNamespaceFlag string
	topWorkerAtespaceFlag  string
	topWorkerSelectorFlag  string
	topWorkerClassFlag     string
)

var getWorkersCmd = &cobra.Command{
	Use:     "workers <worker-name ...>",
	Aliases: []string{"worker"},
	Short:   "List all workers or get one or more workers",
	RunE:    runGetWorkers,
}

var topWorkersCmd = &cobra.Command{
	Use:     "workers",
	Aliases: []string{"worker"},
	Short:   "Display resource (CPU/Memory) usage of workers",
	Args:    cobra.NoArgs,
	RunE:    runTopWorkers,
}

// WorkerLister abstracts ListWorkers RPC calls.
type WorkerLister interface {
	ListWorkers(ctx context.Context, req *ateapipb.ListWorkersRequest, opts ...grpc.CallOption) (*ateapipb.ListWorkersResponse, error)
}

// ActorLister abstracts ListActors RPC calls, which is how the worker commands
// resolve an atespace filter.
type ActorLister interface {
	ListActors(ctx context.Context, req *ateapipb.ListActorsRequest, opts ...grpc.CallOption) (*ateapipb.ListActorsResponse, error)
}

// WorkerGetter abstracts GetWorker RPC calls.
type WorkerGetter interface {
	GetWorker(ctx context.Context, req *ateapipb.GetWorkerRequest, opts ...grpc.CallOption) (*ateapipb.Worker, error)
}

// workersHostingAtespace names the Workers hosting an Actor in atespace. Asked
// of the Actors, because a Worker listing reports how full each Worker is, not
// which Actors it holds.
func workersHostingAtespace(ctx context.Context, lister ActorLister, atespace string) (map[string]bool, error) {
	hosting := map[string]bool{}
	pageToken := ""
	for {
		resp, err := lister.ListActors(ctx, &ateapipb.ListActorsRequest{
			PageSize:  1000,
			PageToken: pageToken,
			Atespace:  atespace,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list actors in atespace %q: %w", atespace, err)
		}
		for _, actor := range resp.GetActors() {
			if name := actor.GetStatus().GetWorkerAssignment().GetWorker().GetName(); name != "" {
				hosting[name] = true
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return hosting, nil
		}
	}
}

// listAllWorkers pages through ListWorkers and returns all workers.
func listAllWorkers(ctx context.Context, lister WorkerLister) ([]*ateapipb.Worker, error) {
	var workers []*ateapipb.Worker
	pageToken := ""
	for {
		resp, err := lister.ListWorkers(ctx, &ateapipb.ListWorkersRequest{
			PageSize:  1000,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list workers: %w", err)
		}
		workers = append(workers, resp.GetWorkers()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return workers, nil
}

// filterWorkers filters workers by Kubernetes namespace, assigned-actor
// atespace, worker pool label selector, and sandbox class. Empty values match
// everything; an atespace filter only matches workers with an assigned actor.
func filterWorkers(ctx context.Context, actors ActorLister, workers []*ateapipb.Worker, namespace, atespace, selector, sandboxClass string) ([]*ateapipb.Worker, error) {
	var labelSel labels.Selector
	if selector != "" {
		var err error
		labelSel, err = labels.Parse(selector)
		if err != nil {
			return nil, fmt.Errorf("invalid label selector %q: %w", selector, err)
		}
	}
	// A worker matches an atespace filter if any actor it hosts is in that
	// atespace; an idle worker hosts none and so matches nothing.
	var hostingAtespace map[string]bool
	if atespace != "" {
		var err error
		if hostingAtespace, err = workersHostingAtespace(ctx, actors, atespace); err != nil {
			return nil, err
		}
	}

	var filtered []*ateapipb.Worker
	for _, w := range workers {
		if namespace != "" && w.GetWorkerNamespace() != namespace {
			continue
		}
		if hostingAtespace != nil && !hostingAtespace[w.GetMetadata().GetName()] {
			continue
		}
		if labelSel != nil && !labelSel.Matches(labels.Set(w.GetLabels())) {
			continue
		}
		if sandboxClass != "" && w.GetSandboxClass() != sandboxClass {
			continue
		}
		filtered = append(filtered, w)
	}
	return filtered, nil
}

// GetWorkersRunner executes the get workers command logic.
type GetWorkersRunner struct {
	workerLister WorkerLister
	workerGetter WorkerGetter
	actorLister  ActorLister
	names        []string
	namespace    string
	atespace     string
	selector     string
	sandboxClass string
	outputFmt    string
	out          io.Writer
}

func (r *GetWorkersRunner) Run(ctx context.Context) error {
	if len(r.names) > 0 {
		if r.namespace != "" || r.atespace != "" || r.selector != "" || r.sandboxClass != "" {
			return fmt.Errorf("filter flags (--namespace, --atespace, --selector, --sandbox-class) cannot be used when getting workers by name")
		}
		workers := make([]*ateapipb.Worker, 0, len(r.names))
		for _, name := range r.names {
			worker, err := r.workerGetter.GetWorker(ctx, &ateapipb.GetWorkerRequest{Worker: &ateapipb.ObjectRef{Name: name}})
			if err != nil {
				return fmt.Errorf("failed to get worker %q: %w", name, err)
			}
			workers = append(workers, worker)
		}
		if len(workers) == 1 {
			return printer.PrintWorkerTo(r.out, workers[0], r.outputFmt)
		}
		return printer.PrintWorkersTo(r.out, workers, r.outputFmt)
	}

	workers, err := listAllWorkers(ctx, r.workerLister)
	if err != nil {
		return err
	}
	filtered, err := filterWorkers(ctx, r.actorLister, workers, r.namespace, r.atespace, r.selector, r.sandboxClass)
	if err != nil {
		return err
	}
	return printer.PrintWorkersTo(r.out, filtered, r.outputFmt)
}

func runGetWorkers(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer apiClient.Close()

	runner := &GetWorkersRunner{
		workerLister: apiClient,
		workerGetter: apiClient,
		actorLister:  apiClient,
		names:        args,
		namespace:    getWorkerNamespaceFlag,
		atespace:     getWorkerAtespaceFlag,
		selector:     getWorkerSelectorFlag,
		sandboxClass: getWorkerClassFlag,
		outputFmt:    outputFmt,
		out:          cmd.OutOrStdout(),
	}
	return runner.Run(ctx)
}

// PodMetricsLister abstracts fetching Kubernetes pod metrics.
type PodMetricsLister interface {
	ListPodMetrics(ctx context.Context, namespace string, opts metav1.ListOptions) ([]metricsv1beta1.PodMetrics, error)
}

type k8sPodMetricsLister struct {
	metricsClient metricsclient.Interface
}

func (l *k8sPodMetricsLister) ListPodMetrics(ctx context.Context, namespace string, opts metav1.ListOptions) ([]metricsv1beta1.PodMetrics, error) {
	list, err := l.metricsClient.MetricsV1beta1().PodMetricses(namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// TopWorkersRunner executes the top workers resource utilization command logic.
type TopWorkersRunner struct {
	workerLister     WorkerLister
	actorLister      ActorLister
	podMetricsLister PodMetricsLister
	namespace        string
	atespace         string
	selector         string
	sandboxClass     string
	outputFmt        string
	out              io.Writer
}

func (r *TopWorkersRunner) Run(ctx context.Context) error {
	allWorkers, err := listAllWorkers(ctx, r.workerLister)
	if err != nil {
		return err
	}
	filtered, err := filterWorkers(ctx, r.actorLister, allWorkers, r.namespace, r.atespace, r.selector, r.sandboxClass)
	if err != nil {
		return err
	}

	metricsMap := make(map[string]metricsv1beta1.PodMetrics)
	metricsUnavailable := false
	if r.podMetricsLister != nil {
		podMetricsList, err := r.podMetricsLister.ListPodMetrics(ctx, r.namespace, metav1.ListOptions{})
		if err != nil {
			metricsUnavailable = true
		} else {
			for _, pm := range podMetricsList {
				key := pm.Namespace + "/" + pm.Name
				metricsMap[key] = pm
			}
		}
	} else {
		metricsUnavailable = true
	}

	var items []*printer.WorkerTopItem
	for _, w := range filtered {
		ns := w.GetWorkerNamespace()
		podName := w.GetWorkerPod()
		pool := w.GetWorkerPool()

		cpuStr := "metrics unavailable"
		memStr := "metrics unavailable"

		if !metricsUnavailable {
			key := ns + "/" + podName
			if pm, ok := metricsMap[key]; ok {
				cpuStr, memStr = extractContainerUsage(pm)
			}
		}

		items = append(items, &printer.WorkerTopItem{
			Pod:       podName,
			Pool:      pool,
			Class:     w.GetSandboxClass(),
			Status:    printer.WorkerOccupancy(w),
			CPU:       cpuStr,
			Memory:    memStr,
			Namespace: ns,
		})
	}

	return printer.PrintWorkerTopTo(r.out, items, r.outputFmt)
}

func extractContainerUsage(pm metricsv1beta1.PodMetrics) (string, string) {
	var cpuQuant, memQuant *resource.Quantity
	for _, c := range pm.Containers {
		if c.Name == "ateom" {
			cpu := c.Usage[corev1.ResourceCPU]
			mem := c.Usage[corev1.ResourceMemory]
			cpuQuant = &cpu
			memQuant = &mem
			break
		}
	}
	if cpuQuant == nil && len(pm.Containers) > 0 {
		var totalCPU, totalMem resource.Quantity
		for _, c := range pm.Containers {
			totalCPU.Add(c.Usage[corev1.ResourceCPU])
			totalMem.Add(c.Usage[corev1.ResourceMemory])
		}
		cpuQuant = &totalCPU
		memQuant = &totalMem
	}
	if cpuQuant == nil || memQuant == nil {
		return "metrics unavailable", "metrics unavailable"
	}

	cpuStr := fmt.Sprintf("%dm", cpuQuant.MilliValue())
	memBytes := memQuant.Value()
	memStr := fmt.Sprintf("%dMi", memBytes/(1024*1024))
	return cpuStr, memStr
}

func runTopWorkers(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer apiClient.Close()

	var metricsLister PodMetricsLister
	metricsClient, err := ateclient.NewMetricsClientset(kubeconfig, k8sContext)
	if err == nil {
		metricsLister = &k8sPodMetricsLister{metricsClient: metricsClient}
	}

	runner := &TopWorkersRunner{
		workerLister:     apiClient,
		actorLister:      apiClient,
		podMetricsLister: metricsLister,
		namespace:        topWorkerNamespaceFlag,
		atespace:         topWorkerAtespaceFlag,
		selector:         topWorkerSelectorFlag,
		sandboxClass:     topWorkerClassFlag,
		outputFmt:        outputFmt,
		out:              cmd.OutOrStdout(),
	}

	return runner.Run(ctx)
}

func init() {
	getWorkersCmd.Flags().StringVarP(&getWorkerNamespaceFlag, "namespace", "n", "", "Scope output to a specific Kubernetes namespace")
	getWorkersCmd.Flags().StringVarP(&getWorkerAtespaceFlag, "atespace", "a", "", "Filter worker pods hosting actors in a specific atespace")
	getWorkersCmd.Flags().StringVarP(&getWorkerSelectorFlag, "selector", "l", "", "Filter by worker pool labels")
	getWorkersCmd.Flags().StringVar(&getWorkerClassFlag, "sandbox-class", "", "Filter by sandbox class (e.g. gvisor, microvm)")
	getCmd.AddCommand(getWorkersCmd)

	topWorkersCmd.Flags().StringVarP(&topWorkerNamespaceFlag, "namespace", "n", "", "Scope output to a specific Kubernetes namespace")
	topWorkersCmd.Flags().StringVarP(&topWorkerAtespaceFlag, "atespace", "a", "", "Filter worker pods hosting actors in a specific atespace")
	topWorkersCmd.Flags().StringVarP(&topWorkerSelectorFlag, "selector", "l", "", "Filter by worker pool labels")
	topWorkersCmd.Flags().StringVar(&topWorkerClassFlag, "sandbox-class", "", "Filter by sandbox class (e.g. gvisor, microvm)")
	topCmd.AddCommand(topWorkersCmd)
}
