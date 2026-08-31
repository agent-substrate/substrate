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
	"os"

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

var (
	topWorkerNamespaceFlag string
	topWorkerAtespaceFlag  string
	topWorkerSelectorFlag  string
	topWorkerClassFlag     string
)

var topWorkersCmd = &cobra.Command{
	Use:     "workers",
	Aliases: []string{"worker"},
	Short:   "Display resource (CPU/Memory) usage of workers",
	Args:    cobra.NoArgs,
	RunE:    runTopWorkers,
}

func init() {
	topWorkersCmd.Flags().StringVarP(&topWorkerNamespaceFlag, "namespace", "n", "", "Scope output to a specific Kubernetes namespace")
	topWorkersCmd.Flags().StringVarP(&topWorkerAtespaceFlag, "atespace", "a", "", "Filter worker pods hosting actors in a specific atespace")
	topWorkersCmd.Flags().StringVarP(&topWorkerSelectorFlag, "selector", "l", "", "Filter by worker pool labels")
	topWorkersCmd.Flags().StringVar(&topWorkerClassFlag, "sandbox-class", "", "Filter by sandbox class (e.g. gvisor, microvm)")
	topCmd.AddCommand(topWorkersCmd)
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
	filtered, err := filterWorkers(allWorkers, r.namespace, r.atespace, r.selector, r.sandboxClass)
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

		status := "FREE"
		assignedActor := "<none>"
		if wass := w.GetStatus().GetAssignment(); wass != nil && wass.GetActor() != nil {
			status = "ASSIGNED"
			if ref := wass.GetActorTemplateRef(); ref != nil {
				assignedActor = fmt.Sprintf("%s/%s/%s/%s",
					ref.GetAtespace(),
					ref.GetName(),
					wass.GetActor().GetAtespace(),
					wass.GetActor().GetName(),
				)
			} else if tpl := wass.GetActorTemplate(); tpl != nil && tpl.GetNamespace() != "" {
				assignedActor = fmt.Sprintf("%s/%s/%s/%s",
					tpl.GetNamespace(),
					tpl.GetName(),
					wass.GetActor().GetAtespace(),
					wass.GetActor().GetName(),
				)
			} else {
				assignedActor = fmt.Sprintf("%s/%s",
					wass.GetActor().GetAtespace(),
					wass.GetActor().GetName(),
				)
			}
		}

		cpuStr := "metrics unavailable"
		memStr := "metrics unavailable"

		if !metricsUnavailable {
			key := ns + "/" + podName
			if pm, ok := metricsMap[key]; ok {
				cpuStr, memStr = extractContainerUsage(pm)
			}
		}

		items = append(items, &printer.WorkerTopItem{
			Pod:           podName,
			Pool:          pool,
			Class:         w.GetSandboxClass(),
			Status:        status,
			AssignedActor: assignedActor,
			CPU:           cpuStr,
			Memory:        memStr,
			Namespace:     ns,
		})
	}

	outWriter := r.out
	if outWriter == nil {
		outWriter = os.Stdout
	}

	return printer.PrintWorkerTopTo(outWriter, items, r.outputFmt)
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
		podMetricsLister: metricsLister,
		namespace:        topWorkerNamespaceFlag,
		atespace:         topWorkerAtespaceFlag,
		selector:         topWorkerSelectorFlag,
		sandboxClass:     topWorkerClassFlag,
		outputFmt:        outputFmt,
		out:              os.Stdout,
	}

	return runner.Run(ctx)
}
