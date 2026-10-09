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
	"bytes"
	"context"
	"fmt"
	"maps"
	"net"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kustomize"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

const (
	// installDir is the manifest root, relative to the repository root.
	installDir = "manifests/ate-install"
	// Envoy dataplane image name
	envoyDataplaneImage = "envoy-dataplane"
	// Envoy dataplane Dockerfile path
	envoyDataplaneDockefile = "cmd/dataplane/envoy"
)

// cordonControlPlaneComponent is the kustomize component that pins each
// control plane workload to its own node, layered over every control plane
// apply under --cordon-control-plane.
const cordonControlPlaneComponent = installDir + "/components/cordon-control-plane"

// SystemOverlay picks the kustomization for a full control plane install.
//
// The choice is a product of two switches: kind vs GKE, and the atenet router
// dataplane. The plain GKE envoy install renders the base kustomization rather
// than the raw manifests/ate-install directory: the directory would also
// re-apply pod-certificate-controller.yaml (reverting the size10 flags and the
// WORKERS_PER_SIGNER value set earlier in the install), both atenet-egress
// variants, and the sandboxconfig files, all of which have their own apply
// steps.
func SystemOverlay(cfg *config.Config) string {
	switch {
	case cfg.Router == config.RouterAgentgateway && cfg.Kind:
		return installDir + "/kind-agentgateway"
	case cfg.Router == config.RouterAgentgateway:
		return installDir + "/agentgateway"
	case cfg.Kind:
		return installDir + "/kind"
	default:
		return installDir + "/base"
	}
}

// render emits the manifests at path, an absolute manifest file or
// kustomization directory, before image resolution. Under
// --cordon-control-plane it composes path with the cordon-control-plane
// component, so the same node pinning reaches every control plane workload
// whichever apply path delivers it. The component's patch has a name-regex
// target and kustomize leaves a stream alone when nothing in it matches, so
// wrapping a manifest that carries none of those workloads is harmless.
func (e *Env) render(path string) ([]byte, error) {
	if e.Cfg.CordonControlPlane {
		return kustomize.Compose(path, e.Cfg.Path(cordonControlPlaneComponent))
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("while reading %s: %w", path, err)
	}
	if info.IsDir() {
		return kustomize.Build(path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("while reading %s: %w", path, err)
	}
	return data, nil
}

// renderBytes is render for a manifest already held in memory.
func (e *Env) renderBytes(manifest []byte) ([]byte, error) {
	if e.Cfg.CordonControlPlane {
		return kustomize.ComposeBytes(manifest, e.Cfg.Path(cordonControlPlaneComponent))
	}
	return manifest, nil
}

// renderResolve renders path and resolves its image references.
func (e *Env) renderResolve(ctx context.Context, path string) ([]byte, error) {
	manifest, err := e.render(path)
	if err != nil {
		return nil, err
	}
	return e.ResolveManifestBytes(ctx, manifest)
}

// renderResolveApply renders path, resolves its images, and applies the result.
func (e *Env) renderResolveApply(ctx context.Context, path string) error {
	manifest, err := e.renderResolve(ctx, path)
	if err != nil {
		return err
	}
	return e.Kube.ApplyBytes(ctx, manifest)
}

// renderSystemManifests produces the full control plane manifest with all
// image references resolved.
func (e *Env) renderSystemManifests(ctx context.Context) ([]byte, error) {
	return e.renderResolve(ctx, e.Cfg.Path(SystemOverlay(e.Cfg)))
}

// renderAtenetRouterManifest produces the atenet router manifest for the
// selected dataplane.
func (e *Env) renderAtenetRouterManifest(ctx context.Context) ([]byte, error) {
	if e.Cfg.Router == config.RouterAgentgateway {
		return e.renderResolve(ctx, e.Cfg.Path(installDir+"/agentgateway-router"))
	}
	return e.renderResolve(ctx, e.Cfg.Manifest("atenet-router.yaml"))
}

// atenetEgressManifestPath returns the envoy egress gateway manifest.
func (e *Env) atenetEgressManifestPath() string {
	return e.Cfg.Manifest("atenet-egress.yaml")
}

// renderAtenetEgressManifest produces the atenet egress manifest.
func (e *Env) renderAtenetEgressManifest(ctx context.Context, provider config.CredentialProvider) ([]byte, error) {
	general := e.Cfg.AdditionalEgressExtprocService != ""

	if e.Cfg.Router == config.RouterAgentgateway {
		// Rejected during configuration loading, which names the channel the
		// value came from. This guards the invariant for a caller that built
		// a Config directly; an operator never sees it.
		if general {
			return nil, fmt.Errorf("internal: additional ext_proc filter reached rendering with dataplane %s", config.RouterAgentgateway)
		}
		raw, err := e.render(e.Cfg.Path(installDir + "/agentgateway-egress"))
		if err != nil {
			return nil, err
		}
		raw, err = patchAgentgatewayEgressInject(raw, provider)
		if err != nil {
			return nil, err
		}
		return e.ResolveManifestBytes(ctx, raw)
	}

	imageReference, err := e.dockerfileImage(ctx, envoyDataplaneImage, envoyDataplaneDockefile)
	if err != nil {
		return nil, err
	}

	// The general additional-ext_proc filter and egress credential injection are
	// independent splices with their own markers, so compose them. The cordon
	// render comes last, over the spliced stream, so the node pinning reaches
	// every variant.
	var raw []byte
	if general {
		raw, err = e.patchAtenetEgressManifest()
	} else {
		raw, err = os.ReadFile(e.atenetEgressManifestPath())
	}
	if err != nil {
		return nil, err
	}
	raw, err = e.patchAtenetEgressInject(raw, provider)
	if err != nil {
		return nil, err
	}
	raw = e.patchEnvoyDataplaneImage(raw, imageReference)
	rendered, err := e.renderBytes(raw)
	if err != nil {
		return nil, err
	}
	return e.ResolveManifestBytes(ctx, rendered)
}

// patchEnvoyDataplaneImage replaces the ${ENVOY_DATAPLANE_IMAGE} placeholder in
// the manifest with imageRef.
func (e *Env) patchEnvoyDataplaneImage(raw []byte, imageRef string) []byte {
	return bytes.ReplaceAll(raw, []byte("${ENVOY_DATAPLANE_IMAGE}"), []byte(imageRef))
}

// patchAtenetEgressInject replaces the #ATE_EGRESS_INJECT_FLAGS marker in the
// egress sidecar's args with the credential-provider flags, or removes it when
// injection is off, which leaves the gateway with no provider. It takes the
// manifest bytes so it can run after the general patch.
func (e *Env) patchAtenetEgressInject(raw []byte, provider config.CredentialProvider) ([]byte, error) {
	var flagsBlock string
	if provider.Enabled() {
		flagsBlock = emitEgressInjectFlags(provider.Name, provider.Address, provider.ServerName())
	}
	return replaceManifestMarker(raw, "#ATE_EGRESS_INJECT_FLAGS", flagsBlock)
}

// The HTTP and HTTPS listeners share the egress policy and its providers.
func patchAgentgatewayEgressInject(raw []byte, provider config.CredentialProvider) ([]byte, error) {
	var block string
	if provider.Enabled() {
		block = fmt.Sprintf(`credentialProviders:
- uriAuthority: %q
  target:
    host: %q
    policies:
      backendTLS:
        hostname: %q
        cert: /run/podidentity.podcert.ate.dev/credential-bundle.pem
        key: /run/podidentity.podcert.ate.dev/credential-bundle.pem
        root: /run/servicedns-ca/trust-bundle.pem`, provider.Name, provider.Address, provider.ServerName())
	}
	return replaceManifestMarker(raw, "#ATE_AGENTGATEWAY_CREDENTIAL_PROVIDERS", block)
}

func replaceManifestMarker(raw []byte, marker, block string) ([]byte, error) {
	var out []string
	replaced := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), marker) {
			indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
			if block != "" {
				for _, l := range strings.Split(block, "\n") {
					out = append(out, indent+l)
				}
			}
			replaced++
			continue
		}
		out = append(out, line)
	}
	if replaced != 1 {
		return nil, fmt.Errorf("expected 1 %s marker, found %d", marker, replaced)
	}
	return []byte(strings.Join(out, "\n")), nil
}

func emitEgressInjectFlags(name, address, serverName string) string {
	return fmt.Sprintf(`- --credential-provider-name=%s
- --credential-provider-address=%s
- --credential-provider-ca-file=/run/servicedns.podcert.ate.dev/trust-bundle.pem
- --credential-provider-client-cert=/run/podidentity.podcert.ate.dev/credential-bundle.pem
- --credential-provider-server-name=%s`, name, address, serverName)
}

func (e *Env) patchAtenetEgressManifest() ([]byte, error) {
	raw, err := os.ReadFile(e.atenetEgressManifestPath())
	if err != nil {
		return nil, fmt.Errorf("reading egress manifest: %w", err)
	}

	spec := e.Cfg.AdditionalEgressExtprocService
	parts := strings.Split(spec, "/")
	namespace := parts[0]
	svcPort := strings.Split(parts[1], ":")
	service := svcPort[0]
	port := svcPort[1]

	address := fmt.Sprintf("%s.%s.svc.cluster.local", service, namespace)
	serverName := fmt.Sprintf("%s.%s.svc", service, namespace)

	filterBlock := emitAdditionalEgressExtprocFilter()
	clusterBlock := emitAdditionalEgressExtprocCluster(address, port, serverName)

	lines := strings.Split(string(raw), "\n")
	var out []string
	filtersReplaced := 0
	clustersReplaced := 0

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#ATE_MITM_EXTPROC_FILTER") {
			indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
			for _, fLine := range strings.Split(filterBlock, "\n") {
				if fLine == "" {
					out = append(out, "")
				} else {
					out = append(out, indent+fLine)
				}
			}
			filtersReplaced++
			continue
		}
		if strings.HasPrefix(trimmed, "#ATE_MITM_EXTPROC_CLUSTER") {
			indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
			for _, cLine := range strings.Split(clusterBlock, "\n") {
				if cLine == "" {
					out = append(out, "")
				} else {
					out = append(out, indent+cLine)
				}
			}
			clustersReplaced++
			continue
		}
		out = append(out, line)
	}

	if filtersReplaced != 2 || clustersReplaced != 1 {
		return nil, fmt.Errorf("expected 2 filter markers and 1 cluster marker in %s, found %d and %d",
			e.atenetEgressManifestPath(), filtersReplaced, clustersReplaced)
	}

	return []byte(strings.Join(out, "\n")), nil
}

const additionalEgressExtprocCluster = "additional_egress_ext_proc"

func emitAdditionalEgressExtprocFilter() string {
	return `- name: envoy.filters.http.ext_proc
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
    grpc_service:
      envoy_grpc:
        cluster_name: additional_egress_ext_proc
      timeout: 2s
    failure_mode_allow: false
    message_timeout: 2s
    request_attributes:
    - filter_state['dev.ate.actor.identity']
    processing_mode:
      request_header_mode: SEND
      response_header_mode: SKIP
      request_body_mode: NONE
      response_body_mode: NONE
      request_trailer_mode: SKIP
      response_trailer_mode: SKIP
    mutation_rules:
      disallow_system: true
      disallow_is_error: true`
}

func emitAdditionalEgressExtprocCluster(address, port, serverName string) string {
	return fmt.Sprintf(`- name: %s
  type: STRICT_DNS
  lb_policy: ROUND_ROBIN
  connect_timeout: 1s
  typed_extension_protocol_options:
    envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
      "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
      explicit_http_config:
        http2_protocol_options: {}
  transport_socket:
    name: envoy.transport_sockets.tls
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext
      sni: %s
      common_tls_context:
        tls_params:
          tls_minimum_protocol_version: TLSv1_3
          tls_maximum_protocol_version: TLSv1_3
        tls_certificate_sds_secret_configs:
        - name: podidentity_client_cert
          sds_config:
            resource_api_version: V3
            path_config_source:
              path: /etc/envoy/sds-podidentity-cert.yaml
        combined_validation_context:
          default_validation_context:
            match_typed_subject_alt_names:
            - san_type: DNS
              matcher:
                exact: %s
          validation_context_sds_secret_config:
            name: servicedns_validation_context
            sds_config:
              resource_api_version: V3
              path_config_source:
                path: /etc/envoy/sds-servicedns-validation.yaml
  load_assignment:
    cluster_name: %s
    endpoints:
    - lb_endpoints:
      - endpoint:
          address:
            socket_address:
              address: %s
              port_value: %s`, additionalEgressExtprocCluster, serverName, serverName, additionalEgressExtprocCluster, address, port)
}

func (e *Env) applyAtenetEgress(ctx context.Context, provider config.CredentialProvider) error {
	manifests, err := e.renderAtenetEgressManifest(ctx, provider)
	if err != nil {
		return err
	}

	running, err := e.Kube.DeploymentExists(ctx, e.Namespace(), "atenet-egress")
	if err != nil {
		return err
	}

	if err := e.Kube.ApplyBytes(ctx, manifests); err != nil {
		return err
	}

	if running && (e.Cfg.AdditionalEgressExtprocService != "" || provider.Enabled()) {
		if err := e.Kube.RolloutRestartDeployment(ctx, e.Namespace(), "atenet-egress", time.Now()); err != nil {
			return err
		}
	}
	return nil
}

// otelConfigPath returns the environment's ate-otel-config ConfigMap.
//
// Every control plane component pulls this ConfigMap in via envFrom. A full
// install gets it as part of the rendered bundle, but the single-component
// redeploys apply raw manifests with no kustomize, so they have to select the
// right copy themselves: applying the base file on a kind cluster would
// overwrite it with the GKE endpoint and break telemetry everywhere at once.
func (e *Env) otelConfigPath() string {
	if e.Cfg.Kind {
		return e.Cfg.Manifest("kind", "ate-otel-config.yaml")
	}
	return e.Cfg.Manifest("ate-otel-config.yaml")
}

// applyOtelConfig applies the environment's ate-otel-config ConfigMap.
func (e *Env) applyOtelConfig(ctx context.Context) error {
	return e.Kube.ApplyPath(ctx, e.otelConfigPath())
}

// otelConfigMap is the ConfigMap every control plane component reads its
// telemetry settings from through envFrom.
const otelConfigMap = "ate-otel-config"

// otelEndpointKey is the collector address inside it.
const otelEndpointKey = "OTEL_EXPORTER_OTLP_ENDPOINT"

// otelExporterKeys select the OTLP push for traces and metrics. Only
// --otlp-endpoint=none sets them; no manifest does.
var otelExporterKeys = []string{"OTEL_TRACES_EXPORTER", "OTEL_METRICS_EXPORTER"}

// otelLogsExporterKey is set by the kind ConfigMap to send the actor events
// over OTLP.
const otelLogsExporterKey = "OTEL_LOGS_EXPORTER"

// otelOverrideDeployments are the control plane Deployments that read
// ate-otel-config. ate-controller additionally copies the values onto the
// ateom worker pods it creates, so one patch reaches the whole system.
var otelOverrideDeployments = []string{"ate-api-server", "ate-controller", "atenet-router"}

// otelOverridePatch returns the ate-otel-config keys that the configured
// endpoint sets, and a nil value for each key it removes.
//
// With none, the endpoint goes: atenet-router then turns off Envoy tracing,
// atelet starts no relay, and ate-controller gives the ateoms no endpoint. The
// exporters go to none so that no component falls back to the SDK default,
// localhost:4317. OTEL_LOGS_EXPORTER goes too, so the actor events stay on
// stdout.
//
// Without none, the exporter keys go, so that an install after a none install
// pushes again. The merge patch owns them, so the bundle apply does not remove
// them.
func otelOverridePatch(endpoint string) map[string]*string {
	patch := map[string]*string{}
	if endpoint == config.OtlpEndpointNone {
		none := "none"
		patch[otelEndpointKey] = nil
		patch[otelLogsExporterKey] = nil
		for _, k := range otelExporterKeys {
			patch[k] = &none
		}
		return patch
	}
	if endpoint != "" {
		patch[otelEndpointKey] = &endpoint
	}
	for _, k := range otelExporterKeys {
		patch[k] = nil
	}
	return patch
}

// otelPatchChanges reports whether patch changes data.
func otelPatchChanges(data map[string]string, patch map[string]*string) bool {
	for k, want := range patch {
		got, ok := data[k]
		if want == nil && ok || want != nil && (!ok || got != *want) {
			return true
		}
	}
	return false
}

// describeOtelPatch lists the changes of patch in a stable order, for the log.
func describeOtelPatch(patch map[string]*string) string {
	keys := slices.Sorted(maps.Keys(patch))
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		if v := patch[k]; v != nil {
			parts = append(parts, k+"="+*v)
		} else {
			parts = append(parts, "-"+k)
		}
	}
	return strings.Join(parts, " ")
}

// applyOtelEndpointOverride points all control plane telemetry at a different
// collector, or with none turns the OTLP push off. See
// benchmarking/telemetry/README.md.
//
// Call this AFTER every apply: the ate-system bundle carries its own copy of
// ate-otel-config, so applying it replaces an earlier patch and the endpoint
// silently returns to the cluster default.
//
// A ConfigMap change starts no rollout, because the pod template stays the
// same, so the consumers have to be restarted. Only on an actual change: a
// restart during the bundle's rollout makes the two compete, and the rollout
// wait can then exceed its timeout. An absent workload is not an error,
// because a single-component deploy has only that component.
func (e *Env) applyOtelEndpointOverride(ctx context.Context) error {
	cm, err := e.Kube.GetConfigMap(ctx, e.Namespace(), otelConfigMap)
	if err != nil {
		return err
	}
	var data map[string]string
	if cm != nil {
		data = cm.Data
	}
	patch := otelOverridePatch(e.Cfg.OtlpEndpoint)
	if !otelPatchChanges(data, patch) {
		return nil
	}

	log.Infof("Setting %s to %s", otelConfigMap, describeOtelPatch(patch))
	if err := e.Kube.MergePatchConfigMap(ctx, e.Namespace(), otelConfigMap, patch); err != nil {
		return err
	}

	now := time.Now()
	for _, name := range otelOverrideDeployments {
		if err := e.Kube.RolloutRestartDeployment(ctx, e.Namespace(), name, now); err != nil {
			return err
		}
	}
	// atelet DaemonSet names carry a version suffix; restart whichever
	// versions are installed.
	daemonSets, err := e.Kube.DaemonSetNames(ctx, e.Namespace(), "app=atelet")
	if err != nil {
		return err
	}
	for _, name := range daemonSets {
		if err := e.Kube.RolloutRestart(ctx, e.Namespace(), name, now); err != nil {
			return err
		}
	}
	return nil
}

// CheckOtelCollector refuses an install whose OTLP endpoint names an
// in-cluster Service that does not exist. Without this check, every component
// fails to resolve the collector at each export, and the telemetry stops with
// no clear error.
//
// The check is for the endpoint that the install sets: the --otlp-endpoint
// value, or else the one in the ate-otel-config manifest. It does not apply on
// kind, which deploys its collector in the same install, or to an endpoint
// outside the cluster, which it cannot see.
func (e *Env) CheckOtelCollector(ctx context.Context) error {
	if e.Cfg.Kind || e.Cfg.OtlpEndpoint == config.OtlpEndpointNone {
		return nil
	}
	endpoint := e.Cfg.OtlpEndpoint
	if endpoint == "" {
		var err error
		if endpoint, err = manifestOtelEndpoint(e.otelConfigPath()); err != nil {
			return err
		}
	}
	namespace, service, ok := clusterService(endpoint)
	if !ok {
		return nil
	}
	exists, err := e.Kube.ServiceExists(ctx, namespace, service)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("the OTLP endpoint %s names the Service %s/%s, which does not exist. "+
			"Deploy the collector (on GKE, enable the managed OpenTelemetry addon), "+
			"give --otlp-endpoint the address of a collector, "+
			"or give --otlp-endpoint=%s to export no OTLP telemetry",
			endpoint, namespace, service, config.OtlpEndpointNone)
	}
	return nil
}

// manifestOtelEndpoint reads the collector address from an ate-otel-config
// manifest.
func manifestOtelEndpoint(path string) (string, error) {
	objs, err := kube.LoadPath(path)
	if err != nil {
		return "", err
	}
	for _, obj := range objs {
		if obj.GetKind() != "ConfigMap" || obj.GetName() != otelConfigMap {
			continue
		}
		endpoint, _, err := unstructured.NestedString(obj.Object, "data", otelEndpointKey)
		if err != nil {
			return "", fmt.Errorf("while reading %s from %s: %w", otelEndpointKey, path, err)
		}
		return endpoint, nil
	}
	return "", fmt.Errorf("%s has no ConfigMap %s", path, otelConfigMap)
}

// clusterService returns the Service that an OTLP endpoint names, when its
// host is the cluster DNS name of a Service: <service>.<namespace>.svc, with
// or without the cluster domain after it. The endpoint is a URL, or host:port.
func clusterService(endpoint string) (namespace, service string, ok bool) {
	host := endpoint
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
		host = u.Hostname()
	} else if h, _, err := net.SplitHostPort(endpoint); err == nil {
		host = h
	}
	labels := strings.Split(host, ".")
	if len(labels) < 3 || labels[2] != "svc" || labels[0] == "" || labels[1] == "" {
		return "", "", false
	}
	return labels[1], labels[0], true
}
