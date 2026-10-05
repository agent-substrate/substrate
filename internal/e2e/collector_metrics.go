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

package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/portforward"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	collectorNamespace = "otel-system"
	collectorService   = "opentelemetry-collector"
	collectorPromPort  = 8889
	// AgentGateway exposes native Prometheus metrics; it does not export these
	// instruments through the OTLP collector.
	agentGatewayRouterStatsPort = 15020
	// The egress Envoy exposes its Prometheus stats on the admin listener.
	egressEnvoyAdminPort = 15000

	// EgressExtProcIdentityStatPrefix is the Envoy ext_proc filter stat_prefix on
	// the outer CONNECT leg in atenet-egress.
	EgressExtProcIdentityStatPrefix = "egress_identity"
	// EgressExtProcPolicyMITMStatPrefix is the Envoy ext_proc filter stat_prefix
	// on the inner MITM HTTP leg in atenet-egress.
	EgressExtProcPolicyMITMStatPrefix = "egress_policy_mitm"

	// EgressConnectCacheHitCounter is the Envoy dynamic-module counter name for
	// CONNECT policy cache hits in atenet-egress.
	EgressConnectCacheHitCounter = "ate_egress_connect_cache_hit"
	// EgressConnectCacheMissCounter is the Envoy dynamic-module counter name for
	// CONNECT policy cache misses in atenet-egress.
	EgressConnectCacheMissCounter = "ate_egress_connect_cache_miss"
)

// PlatformMetricPrefixes are the Prometheus metric-name prefixes (OTLP dots
// mapped to underscores) the substrate platform must emit. The Collector's
// prometheus exporter appends unit and type suffixes (e.g. _seconds_bucket,
// _bytes_count), so matching is by prefix. This slice grows as each metric
// slice lands and as more components are wired to push to the collector.
var PlatformMetricPrefixes = []string{
	"ate_workerpool_workers",
	"ate_workerpool_desired_workers",
	"ate_workerpool_ready_workers",
	"ate_actor_crashes",
	"ate_actor_lifecycle_operation_duration",
	"ate_scheduler_assignment_duration",
	"ate_actor_restore_duration",
	"ate_actor_checkpoint_duration",
	"atelet_snapshot_size",
	"atenet_router_route_duration",
}

// ScrapeAgentGatewayRouterMetrics reads the AgentGateway router's native
// Prometheus stats endpoint. AgentGateway instruments are not OTLP exports.
func ScrapeAgentGatewayRouterMetrics(ctx context.Context) (string, error) {
	config, err := ateclient.LoadKubeConfig(KubeConfig, KubeContext)
	if err != nil {
		return "", fmt.Errorf("loading kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", fmt.Errorf("creating k8s client: %w", err)
	}
	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, SystemNamespace(), ResourceName("atenet-router"), agentGatewayRouterStatsPort)
	if err != nil {
		return "", err
	}
	defer stop()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/metrics", localPort), nil)
	if err != nil {
		return "", err
	}
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return "", fmt.Errorf("scraping AgentGateway metrics: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading AgentGateway metrics: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("AgentGateway metrics returned %d: %s", resp.StatusCode, body)
	}
	return string(body), nil
}

// ScrapeCollectorMetrics port-forwards the kind stack's OTel Collector and reads
// its Prometheus exporter surface, returning the raw exposition text.
func ScrapeCollectorMetrics(ctx context.Context) (string, error) {
	config, err := ateclient.LoadKubeConfig(KubeConfig, KubeContext)
	if err != nil {
		return "", fmt.Errorf("loading kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", fmt.Errorf("creating k8s client: %w", err)
	}

	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, collectorNamespace, collectorService, collectorPromPort)
	if err != nil {
		return "", err
	}
	defer stop()

	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", localPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return "", fmt.Errorf("scraping collector metrics: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading collector metrics: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("collector metrics returned %d: %s", resp.StatusCode, body)
	}
	return string(body), nil
}

// ScrapeEgressEnvoyMetrics port-forwards each ready atenet-egress pod's Envoy
// admin port and reads its /stats/prometheus endpoint, returning the
// concatenated exposition text.
func ScrapeEgressEnvoyMetrics(ctx context.Context) (string, error) {
	config, err := ateclient.LoadKubeConfig(KubeConfig, KubeContext)
	if err != nil {
		return "", fmt.Errorf("loading kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", fmt.Errorf("creating k8s client: %w", err)
	}

	ns := SystemNamespace()
	pods, err := clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app=atenet-egress",
	})
	if err != nil {
		return "", fmt.Errorf("listing atenet-egress pods: %w", err)
	}

	var b strings.Builder
	ready := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !portforward.IsPodReady(pod) {
			continue
		}
		ready++
		body, err := scrapeEgressEnvoyPod(ctx, config, clientset, ns, pod.Name)
		if err != nil {
			return "", err
		}
		b.WriteString(body)
	}
	if ready == 0 {
		return "", fmt.Errorf("no ready atenet-egress pods in %s", ns)
	}
	return b.String(), nil
}

func scrapeEgressEnvoyPod(ctx context.Context, config *rest.Config, clientset kubernetes.Interface, namespace, podName string) (string, error) {
	localPort, stop, err := portforward.PodPortForward(ctx, config, clientset, namespace, podName, egressEnvoyAdminPort)
	if err != nil {
		return "", err
	}
	defer stop()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/stats/prometheus", localPort), nil)
	if err != nil {
		return "", err
	}
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return "", fmt.Errorf("scraping egress Envoy metrics on %s: %w", podName, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading egress Envoy metrics on %s: %w", podName, err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("egress Envoy metrics on %s returned %d: %s", podName, resp.StatusCode, body)
	}
	return string(body), nil
}

// EgressExtProcStreamCounts returns the envoy_http_ext_proc_<prefix>_streams_started
// count for each egress ext_proc stat_prefix in an Envoy /stats/prometheus
// scrape, summed across instances.
func EgressExtProcStreamCounts(scrape string) map[string]int {
	counts := map[string]int{}
	for _, line := range strings.Split(scrape, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name := metricNameFromLine(line)
		rest, ok := strings.CutPrefix(name, "envoy_http_ext_proc_")
		if !ok {
			continue
		}
		statPrefix, ok := strings.CutSuffix(rest, "_streams_started")
		if !ok {
			statPrefix, ok = strings.CutSuffix(rest, "_streams_started_total")
		}
		if !ok || statPrefix == "" {
			continue
		}
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		counts[statPrefix] += int(v)
	}
	return counts
}

// EgressPolicyCacheCounts returns the CONNECT policy cache hit and miss counts
// from an Envoy /stats/prometheus scrape, summed across instances.
func EgressPolicyCacheCounts(scrape string) (hits, misses int) {
	for _, line := range strings.Split(scrape, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name := strings.TrimSuffix(metricNameFromLine(line), "_total")
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		switch name {
		case "envoy_dynamicmodulescustom_" + EgressConnectCacheHitCounter,
			"envoy_" + EgressConnectCacheHitCounter:
			hits += int(v)
		case "envoy_dynamicmodulescustom_" + EgressConnectCacheMissCounter,
			"envoy_" + EgressConnectCacheMissCounter:
			misses += int(v)
		}
	}
	return hits, misses
}

// MissingPlatformMetrics returns the prefixes with no matching series in the
// Prometheus exposition text. A metric matches when its name equals a prefix or
// begins with prefix+"_"; the underscore boundary stops "ate_actor_restore" from
// matching an unrelated "ate_actor_restored".
func MissingPlatformMetrics(scrape string, prefixes []string) []string {
	present := make(map[string]bool, len(prefixes))
	for _, line := range strings.Split(scrape, "\n") {
		name := metricNameFromLine(line)
		if name == "" {
			continue
		}
		for _, p := range prefixes {
			if name == p || strings.HasPrefix(name, p+"_") {
				present[p] = true
			}
		}
	}
	var missing []string
	for _, p := range prefixes {
		if !present[p] {
			missing = append(missing, p)
		}
	}
	return missing
}

// LifecycleEventMetric is the count connector's view of the actor lifecycle
// events. Reading it rather than a log store keeps this check on the metrics
// harness: kind has no place to query log records.
const LifecycleEventMetric = "substrate_actor_state_changes"

// LifecycleEventCounts returns the count the collector holds for each
// ate.actor.state. The counter is cumulative and the collector outlives any one
// test, so compare two reads rather than asserting a state is merely present:
// a stale count from an earlier run would pass a presence check even with the
// exporter turned off.
//
// One state can appear on several lines, one per emitting ateapi instance, so
// the counts are summed.
func LifecycleEventCounts(scrape string) map[string]float64 {
	counts := map[string]float64{}
	for _, line := range strings.Split(scrape, "\n") {
		name := metricNameFromLine(line)
		if name != LifecycleEventMetric && !strings.HasPrefix(name, LifecycleEventMetric+"_") {
			continue
		}
		state := promLabelValue(line, "ate_actor_state")
		if state == "" {
			continue
		}
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		counts[state] += v
	}
	return counts
}

// StatesNotAdvanced returns the states whose count did not rise between the two
// reads. An empty result means every state was emitted during the window.
func StatesNotAdvanced(before, after map[string]float64, states []string) []string {
	var stale []string
	for _, s := range states {
		if after[s] <= before[s] {
			stale = append(stale, s)
		}
	}
	return stale
}

// CollectorHasService reports whether any named service has pushed telemetry to
// the collector. Its prometheus exporter maps each pushed resource's service.name
// onto the job label, so a service that has exported at least one data point
// shows up there. Note the exporter never emits a service_name label, and a
// resource whose instruments have recorded nothing yet produces no series at all.
func CollectorHasService(scrape string, services ...string) bool {
	for _, svc := range services {
		if strings.Contains(scrape, `job="`+svc+`"`) {
			return true
		}
	}
	return false
}

// TargetInfoLabel returns one resource attribute of a service, or "" when it is
// absent. The Prometheus exporter keeps resource attributes off the series and
// publishes them once per resource in target_info, keyed by job.
func TargetInfoLabel(scrape, service, label string) string {
	for _, line := range strings.Split(scrape, "\n") {
		if metricNameFromLine(line) != "target_info" || !strings.Contains(line, `job="`+service+`"`) {
			continue
		}
		if v := promLabelValue(line, label); v != "" {
			return v
		}
	}
	return ""
}

// promLabelValue reads one label off an exposition line.
func promLabelValue(line, label string) string {
	key := label + `="`
	i := strings.Index(line, key)
	if i < 0 {
		return ""
	}
	rest := line[i+len(key):]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// metricNameFromLine extracts the metric name from one exposition line, handling
// the "# HELP name ...", "# TYPE name type", and "name{labels} value" forms.
// It returns "" for blank lines and other comments.
func metricNameFromLine(line string) string {
	line = strings.TrimSpace(line)
	if line == "" {
		return ""
	}
	if strings.HasPrefix(line, "#") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && (fields[1] == "HELP" || fields[1] == "TYPE") {
			return fields[2]
		}
		return ""
	}
	if i := strings.IndexAny(line, "{ \t"); i >= 0 {
		return line[:i]
	}
	return line
}
