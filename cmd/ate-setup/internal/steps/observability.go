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

package steps

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// The ate-otel-config ConfigMap, and the annotation that records which
// telemetry mode applied it. The installer reads the annotation back: a deploy
// of one component, with no --observability, keeps the mode that the cluster
// has, and does not put the default over a collector that works.
const (
	otelConfigMapName  = "ate-otel-config"
	otelModeAnnotation = "ate.dev/observability-mode"
)

// gkeOtelNamespace is the namespace that the GKE managed OTel addon makes. The
// endpoint in manifests/ate-install/otel/gke/ate-otel-config.yaml names it.
const gkeOtelNamespace = "gke-managed-otel"

// otelConsumerDeployments are the Deployments that read the ConfigMap with
// envFrom. The atelet DaemonSets read it too; they carry a version suffix in
// their names, thus restartOtelConsumers restarts them by label.
//
// Both egress manifests declare one Deployment of the name atenet-egress, thus
// one entry covers the shipped gateway and the sdsmint one.
// TestOtelConsumersMatchTheManifests holds this list to the manifests.
var otelConsumerDeployments = []string{"ate-api-server", "ate-controller", "atenet-router", "atenet-egress"}

// clusterServicePattern matches the host of an endpoint that names a Service of
// this cluster: service.namespace.svc, with or without cluster.local.
var clusterServicePattern = regexp.MustCompile(`^([a-z0-9-]+)\.([a-z0-9-]+)\.svc(\.cluster\.local)?\.?$`)

// otelConfig is the ate-otel-config ConfigMap that this install applies, with
// the mode that supplies it and the endpoint in it.
type otelConfig struct {
	mode     string
	endpoint string
	obj      *unstructured.Unstructured
	// source names what decided the mode, for the report of the preflight.
	source string
}

// observabilityState caches the resolution and what the cluster had before it.
type observabilityState struct {
	resolved *otelConfig
	// clusterMode and clusterEndpoint are the values of the ConfigMap in the
	// cluster. Both stay empty when the cluster has no such ConfigMap.
	clusterMode     string
	clusterEndpoint string
	clusterRead     bool
	preflightDone   bool
	// changed records that this install gives the cluster a different
	// collector than the one it had. restartOtelConsumers reads it.
	changed bool
}

// otelConfigPath returns the manifest that supplies the ConfigMap of a mode.
// Mode otlp holds its address nowhere, thus it takes the file of another mode
// and renderOtelConfig puts the given endpoint in it.
//
// On a kind install that other file is the kind one, and not the none one. The
// file of mode kind carries OTEL_METRIC_EXPORT_INTERVAL and
// OTEL_METRIC_EXPORT_TIMEOUT, which shorten the 60s export tick of the SDK to
// 10s: a component is invisible to the collector until its first tick, and the
// metrics e2e suite asserts against the scrape of the collector on a bounded
// deadline. Those two are a property of the cluster and not of the collector,
// thus a kind cluster that names its own collector keeps them.
func (e *Env) otelConfigPath(mode string) string {
	switch {
	case mode == config.ObservabilityKind:
		return e.Cfg.Manifest("otel", "kind", "ate-otel-config.yaml")
	case mode == config.ObservabilityGKE:
		return e.Cfg.Manifest("otel", "gke", "ate-otel-config.yaml")
	case mode == config.ObservabilityOTLP && e.Cfg.Kind:
		return e.Cfg.Manifest("otel", "kind", "ate-otel-config.yaml")
	default:
		return e.Cfg.Manifest("otel", "none", "ate-otel-config.yaml")
	}
}

// readClusterObservability reads the mode and the endpoint of the ConfigMap in
// the cluster, one time. Both stay empty when there is no such ConfigMap, and
// when there is no cluster to ask.
func (e *Env) readClusterObservability(ctx context.Context) {
	if e.observability.clusterRead {
		return
	}
	e.observability.clusterRead = true

	cm, err := e.Kube.Typed.CoreV1().ConfigMaps(NamespaceAteSystem).
		Get(ctx, otelConfigMapName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Warnf("could not read the %s ConfigMap: %v", otelConfigMapName, err)
		}
		return
	}
	e.observability.clusterEndpoint = cm.Data["OTEL_EXPORTER_OTLP_ENDPOINT"]
	e.observability.clusterMode = cm.Annotations[otelModeAnnotation]
	if e.observability.clusterMode == "" {
		e.observability.clusterMode = e.modeOfEndpoint(e.observability.clusterEndpoint)
		return
	}

	// The annotation and the endpoint can disagree, because the ConfigMap has
	// more than one writer: an operator can edit it, and hack/install-ate.sh
	// patches the collector of a measurement into it. The components read the
	// endpoint, thus the endpoint is the collector of the cluster and the
	// annotation is stale. Taking the annotation instead would put the collector
	// of the stale mode back over one that works.
	//
	// Mode otlp is out of this test: no manifest holds its address, thus the
	// ConfigMap is the only source of it and the two cannot disagree.
	if e.observability.clusterMode == config.ObservabilityOTLP {
		return
	}
	if want := endpointInFile(e.otelConfigPath(e.observability.clusterMode)); want != e.observability.clusterEndpoint {
		log.Warnf("the %s ConfigMap says mode %s, whose collector is %q, and names %q; taking the endpoint",
			otelConfigMapName, e.observability.clusterMode, want, e.observability.clusterEndpoint)
		e.observability.clusterMode = e.modeOfEndpoint(e.observability.clusterEndpoint)
	}
}

// gkeOtelAddonPresent reports whether the collector of the GKE managed OTel
// addon is in this cluster. A first install of a cluster that has the addon
// then sends its telemetry there, and one that has no collector stays quiet.
//
// This is a test of the cluster, and not an assumption about it. The endpoint
// that this mode names is the Service that the test finds, thus the mode cannot
// select a collector that is absent: that is the fault this whole change
// removes. An install that wants no telemetry on such a cluster gives
// --observability=none.
//
// Both errors are the same answer as a negative: with no permission to read a
// namespace, or with no cluster to ask, the installer cannot say that the addon
// is there, thus it must not select the mode of it.
func (e *Env) gkeOtelAddonPresent(ctx context.Context) bool {
	present, err := e.namespaceExists(ctx, gkeOtelNamespace)
	if err != nil || !present {
		return false
	}
	namespace, service := endpointService(endpointInFile(e.otelConfigPath(config.ObservabilityGKE)))
	if namespace == "" {
		return false
	}
	_, err = e.Kube.Typed.CoreV1().Services(namespace).Get(ctx, service, metav1.GetOptions{})
	return err == nil
}

// modeOfEndpoint returns the mode that an endpoint belongs to. It is for a
// ConfigMap that carries no mode annotation, which is the ConfigMap of an
// install that came before the modes: the endpoint is the only evidence there,
// and without this the next deploy of one component would put the default over
// a collector that works.
func (e *Env) modeOfEndpoint(endpoint string) string {
	switch endpoint {
	case "":
		return config.ObservabilityNone
	case endpointInFile(e.otelConfigPath(config.ObservabilityGKE)):
		return config.ObservabilityGKE
	case endpointInFile(e.otelConfigPath(config.ObservabilityKind)):
		return config.ObservabilityKind
	default:
		return config.ObservabilityOTLP
	}
}

// endpointInFile returns the endpoint that a ConfigMap manifest names, or an
// empty string when the file cannot be read: an unreadable file makes no mode
// match, which is the same result as one that names a different endpoint.
func endpointInFile(path string) string {
	objs, err := kube.LoadPath(path)
	if err != nil || len(objs) == 0 {
		return ""
	}
	endpoint, _, _ := unstructured.NestedString(objs[0].Object, "data", "OTEL_EXPORTER_OTLP_ENDPOINT")
	return endpoint
}

// ResolveObservability decides the telemetry mode and builds the ConfigMap of
// it. The order is:
//
//  1. --observability, or ATE_OBSERVABILITY.
//  2. --otlp-endpoint, which gives mode otlp.
//  3. The mode of the cluster, thus a deploy of one component does not change
//     the collector that the cluster has.
//  4. Mode kind on a kind install, and mode none on each other install.
//
// The result is cached: one command must apply one ConfigMap.
func (e *Env) ResolveObservability(ctx context.Context) (*otelConfig, error) {
	if e.observability.resolved != nil {
		return e.observability.resolved, nil
	}

	mode, endpoint, source := e.Cfg.Observability, e.Cfg.OtlpEndpoint, "the --observability flag"
	switch {
	case mode != "":
	case endpoint != "":
		mode, source = config.ObservabilityOTLP, "the --otlp-endpoint flag"
	default:
		e.readClusterObservability(ctx)
		switch {
		case e.observability.clusterMode != "":
			mode, source = e.observability.clusterMode, "the cluster"
			// Only mode otlp keeps its address in the ConfigMap. Each other
			// mode holds it in a manifest.
			if mode == config.ObservabilityOTLP {
				endpoint = e.observability.clusterEndpoint
			}
		case e.Cfg.Kind:
			mode, source = config.ObservabilityKind, "the kind install"
		case e.gkeOtelAddonPresent(ctx):
			mode, source = config.ObservabilityGKE, "the GKE managed OTel addon"
		default:
			mode, source = config.ObservabilityNone, "the default"
		}
	}

	switch mode {
	case config.ObservabilityNone, config.ObservabilityOTLP, config.ObservabilityGKE, config.ObservabilityKind:
	default:
		return nil, fmt.Errorf("the telemetry mode from %s must be %s, %s, %s, or %s, got %q; "+
			"the %s annotation on the %s ConfigMap in %s holds that value, and --observability replaces it",
			source, config.ObservabilityNone, config.ObservabilityOTLP, config.ObservabilityGKE,
			config.ObservabilityKind, mode, otelModeAnnotation, otelConfigMapName, NamespaceAteSystem)
	}
	if mode == config.ObservabilityOTLP && endpoint == "" {
		return nil, fmt.Errorf("mode %s from %s needs the address of a collector: give --otlp-endpoint", mode, source)
	}

	obj, err := e.renderOtelConfig(mode, endpoint)
	if err != nil {
		return nil, err
	}
	rendered, _, _ := unstructured.NestedString(obj.Object, "data", "OTEL_EXPORTER_OTLP_ENDPOINT")
	e.observability.resolved = &otelConfig{mode: mode, endpoint: rendered, obj: obj, source: source}
	return e.observability.resolved, nil
}

// renderOtelConfig builds the ConfigMap of a mode. The endpoint is in the
// object before the apply, thus the install needs no patch after it. Mode otlp
// takes the file of mode none, thus it replaces the endpoint, the two exporter
// switches, and the mode annotation in it.
func (e *Env) renderOtelConfig(mode, endpoint string) (*unstructured.Unstructured, error) {
	path := e.otelConfigPath(mode)
	objs, err := kube.LoadPath(path)
	if err != nil {
		return nil, fmt.Errorf("while reading %s: %w", path, err)
	}
	if len(objs) != 1 {
		return nil, fmt.Errorf("%s must hold one ConfigMap, got %d objects", path, len(objs))
	}
	obj := objs[0]
	if mode != config.ObservabilityOTLP {
		return obj, nil
	}

	for key, value := range map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": endpoint,
		"OTEL_TRACES_EXPORTER":        config.ObservabilityOTLP,
		"OTEL_METRICS_EXPORTER":       config.ObservabilityOTLP,
	} {
		if err := unstructured.SetNestedField(obj.Object, value, "data", key); err != nil {
			return nil, fmt.Errorf("while setting %s in %s: %w", key, path, err)
		}
	}
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[otelModeAnnotation] = config.ObservabilityOTLP
	obj.SetAnnotations(annotations)
	return obj, nil
}

// applyOtelConfig applies the ate-otel-config ConfigMap of the selected mode.
// Every control plane component reads it with envFrom, thus it goes in ahead of
// the workloads: a container whose envFrom names an absent ConfigMap does not
// start. No kustomize bundle carries a copy, thus this is the one writer and no
// apply after it replaces the selected mode.
func (e *Env) applyOtelConfig(ctx context.Context) error {
	otel, err := e.ResolveObservability(ctx)
	if err != nil {
		return err
	}
	if err := e.preflightObservability(ctx, otel); err != nil {
		return err
	}
	e.noteOtelConfigChange(ctx, otel)
	return e.Kube.ApplyOne(ctx, otel.obj)
}

// preflightObservability tests the selected mode, then reports it. It runs one
// time for each install: each deploy path applies the ConfigMap, and the
// operator needs the report one time only.
func (e *Env) preflightObservability(ctx context.Context, otel *otelConfig) error {
	if e.observability.preflightDone {
		return nil
	}
	e.observability.preflightDone = true

	switch otel.mode {
	case config.ObservabilityNone:
		log.Infof("Observability: mode none (from %s). The control plane exports no telemetry.", otel.source)
		log.Infof("  ateapi, atelet, and atenet-router still serve their own /metrics endpoints.")
		if present, err := e.namespaceExists(ctx, gkeOtelNamespace); err == nil && present {
			log.Infof("  The namespace %s is present. To use that collector, install again with --observability=gke.",
				gkeOtelNamespace)
		}
		return nil
	case config.ObservabilityKind:
		// No test of the Service: the collector is in the same bundle as the
		// components, thus it does not exist before this install applies it.
		log.Infof("Observability: mode kind (from %s), endpoint %s", otel.source, otel.endpoint)
		return nil
	case config.ObservabilityGKE:
		log.Infof("Observability: mode gke (from %s), endpoint %s", otel.source, otel.endpoint)
		return e.checkOtlpEndpointReachable(ctx, otel.endpoint,
			"enable the managed OTel addon on the cluster, or install with --observability=otlp "+
				"and the address of your own collector, or with --observability=none")
	default:
		log.Infof("Observability: mode otlp (from %s), endpoint %s", otel.source, otel.endpoint)
		return e.checkOtlpEndpointReachable(ctx, otel.endpoint,
			"install the collector first, correct --otlp-endpoint, or install with --observability=none")
	}
}

// checkOtlpEndpointReachable tests that the Service of an in-cluster endpoint
// exists. An absent Service is an error: the components would fail to find the
// collector one time each minute, and the telemetry would stop with no message,
// which reads as a fault of the network and not as an absent dependency.
//
// An address outside the cluster gets a note only, because the installer cannot
// test it.
func (e *Env) checkOtlpEndpointReachable(ctx context.Context, endpoint, remedy string) error {
	namespace, service := endpointService(endpoint)
	if namespace == "" {
		log.Infof("  The installer does not test %s, because the address is not one of this cluster.", endpoint)
		return nil
	}

	present, err := e.namespaceExists(ctx, namespace)
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("the collector at %s is absent: there is no namespace %s; %s", endpoint, namespace, remedy)
	}
	if _, err := e.Kube.Typed.CoreV1().Services(namespace).Get(ctx, service, metav1.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("the collector at %s is absent: there is no Service %s in %s; %s",
				endpoint, service, namespace, remedy)
		}
		return fmt.Errorf("while getting service %s/%s: %w", namespace, service, err)
	}
	return nil
}

// endpointService returns the namespace and the Service of an endpoint that
// names a Service of this cluster, and two empty strings for each other
// address.
func endpointService(endpoint string) (namespace, service string) {
	host := endpoint
	if _, after, found := strings.Cut(host, "://"); found {
		host = after
	}
	host, _, _ = strings.Cut(host, "/")
	host, _, _ = strings.Cut(host, ":")
	match := clusterServicePattern.FindStringSubmatch(host)
	if match == nil {
		return "", ""
	}
	return match[2], match[1]
}

func (e *Env) namespaceExists(ctx context.Context, name string) (bool, error) {
	if _, err := e.Kube.Typed.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("while getting namespace %s: %w", name, err)
	}
	return true, nil
}

// noteOtelConfigChange compares the ConfigMap of this install with the one in
// the cluster. Nothing to compare on a first install, thus no change: the
// workloads that come after it read the new ConfigMap when they start.
func (e *Env) noteOtelConfigChange(ctx context.Context, otel *otelConfig) {
	e.readClusterObservability(ctx)
	if e.observability.clusterMode != "" &&
		(e.observability.clusterMode != otel.mode || e.observability.clusterEndpoint != otel.endpoint) {
		e.observability.changed = true
	}
	// Take the new values as the values of the cluster, because the caller
	// applies them. Thus a second deploy path in the same command finds no
	// change again, and restarts the workloads one time only.
	e.observability.clusterMode = otel.mode
	e.observability.clusterEndpoint = otel.endpoint
}

// restartOtelConsumers restarts the workloads that read ate-otel-config, but
// only when the collector changed. They read it with envFrom, thus each one
// takes a new endpoint at the start of a pod, and a new ConfigMap on its own
// starts no pod: the pod template stays the same, thus the apply starts no
// rollout and each running pod keeps the collector of the install before it.
//
// Call this at the end of a deploy path, after the waits for the rollouts. A
// restart during the rollout of the bundle makes the two rollouts compete, and
// the wait for the rollout can then exceed its timeout. An absent workload is
// not an error, because a deploy of one component has only that component.
func (e *Env) restartOtelConsumers(ctx context.Context) error {
	if !e.observability.changed {
		return nil
	}
	e.observability.changed = false

	log.Infof("The collector changed. Restarting the workloads that read %s.", otelConfigMapName)
	log.Infof("  ate-controller rolls each WorkerPool when it restarts, thus the running workers, " +
		"and the actors on them, are replaced.")
	now := time.Now()
	for _, name := range otelConsumerDeployments {
		if err := e.Kube.RolloutRestartDeployment(ctx, NamespaceAteSystem, name, now); err != nil {
			return err
		}
	}
	// By label, and not by name: an atelet DaemonSet carries the version suffix
	// of the install that made it, and a cluster in a rolling upgrade holds one
	// of each version. Each of them reads the ConfigMap.
	return e.RestartAteletDaemonSets(ctx)
}
