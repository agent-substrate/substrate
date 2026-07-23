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
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"k8s.io/client-go/kubernetes"
)

const (
	collectorNamespace = "otel-system"
	collectorService   = "opentelemetry-collector"
	collectorPromPort  = 8889
)

// PlatformMetricPrefixes are the Prometheus metric-name prefixes (OTLP dots
// mapped to underscores) the substrate platform must emit. The Collector's
// prometheus exporter appends unit and type suffixes (e.g. _seconds_bucket,
// _bytes_count), so matching is by prefix. This slice grows as each metric
// slice lands and as more components are wired to push to the collector; today
// it pins the worker-count instrument introduced alongside this harness.
var PlatformMetricPrefixes = []string{
	"ate_workerpool_workers",
}

// ScrapeCollectorMetrics port-forwards the kind stack's OTel Collector and reads
// its Prometheus exporter surface, returning the raw exposition text.
func ScrapeCollectorMetrics(ctx context.Context) (string, error) {
	config, err := ateclient.LoadConfig(KubeConfig, KubeContext)
	if err != nil {
		return "", fmt.Errorf("loading kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", fmt.Errorf("creating k8s client: %w", err)
	}

	pod, _, err := firstReadyPodForService(ctx, clientset, collectorNamespace, collectorService)
	if err != nil {
		return "", err
	}
	localPort, stop, err := podPortForward(ctx, config, clientset, collectorNamespace, pod.Name, collectorPromPort)
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
